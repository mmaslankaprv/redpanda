package kgo

import (
	"bytes"
	"errors"
	"fmt"
	"hash/crc32"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kbin"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type sink struct {
	cl     *Client // our owning client, for cfg, metadata triggering, context, etc.
	nodeID int32   // the node ID of the broker this sink belongs to

	// inflightSem controls the number of concurrent produce requests.  We
	// start with a limit of 1, which covers Kafka v0.11.0.0. On the first
	// response, we check what version was set in the request. If it is at
	// least 4, which 1.0.0 introduced, we upgrade the sem size.
	inflightSem    atomic.Value
	produceVersion int32 // atomic, negative is unset, positive is version

	drainState workLoop

	// seqRespsMu, guarded by seqRespsMu, contains responses that must
	// be handled sequentially. These responses are handled asyncronously,
	// but sequentially.
	seqRespsMu sync.Mutex
	seqResps   []*seqResp

	backoffMu   sync.Mutex // guards the following
	needBackoff bool
	backoffSeq  uint32 // prevents pile on failures

	// To work around KAFKA-12671, before we issue EndTxn, we check to see
	// that all sinks had a final successful response. If not, then we risk
	// running into KAFKA-12671 (out of order processing leading to
	// orphaned begun "transaction" in ProducerStateManager), so rather
	// than issuing EndTxn immediately, we wait a little bit.
	lastRespSuccessful bool

	// consecutiveFailures is incremented every backoff and cleared every
	// successful response. For simplicity, if we have a good response
	// following an error response before the error response's backoff
	// occurs, the backoff is not cleared.
	consecutiveFailures uint32

	recBufsMu    sync.Mutex // guards the following
	recBufs      []*recBuf  // contains all partition records for batch building
	recBufsStart int        // incremented every req to avoid large batch starvation
}

type seqResp struct {
	resp    kmsg.Response
	err     error
	done    chan struct{}
	promise func(kmsg.Response, error)
}

func (cl *Client) newSink(nodeID int32) *sink {
	s := &sink{
		cl:                 cl,
		nodeID:             nodeID,
		produceVersion:     -1,
		lastRespSuccessful: true,
	}
	s.inflightSem.Store(make(chan struct{}, 1))
	return s
}

// createReq returns a produceRequest from currently buffered records
// and whether there are more records to create more requests immediately.
func (s *sink) createReq(producerID int64, producerEpoch int16) (*produceRequest, *kmsg.AddPartitionsToTxnRequest, bool) {
	req := &produceRequest{
		txnID:   s.cl.cfg.txnID,
		acks:    s.cl.cfg.acks.val,
		timeout: int32(s.cl.cfg.produceTimeout.Milliseconds()),
		batches: make(seqRecBatches, 5),

		producerID:    producerID,
		producerEpoch: producerEpoch,
		idempotent:    s.cl.idempotent(),

		compressor: s.cl.compressor,

		wireLength:      s.cl.baseProduceRequestLength(), // start length with no topics
		wireLengthLimit: s.cl.cfg.maxBrokerWriteBytes,
	}
	txnBuilder := txnReqBuilder{
		txnID:         req.txnID,
		producerID:    producerID,
		producerEpoch: producerEpoch,
	}

	var moreToDrain bool

	s.recBufsMu.Lock()
	defer s.recBufsMu.Unlock()

	recBufsIdx := s.recBufsStart
	for i := 0; i < len(s.recBufs); i++ {
		recBuf := s.recBufs[recBufsIdx]
		recBufsIdx = (recBufsIdx + 1) % len(s.recBufs)

		recBuf.mu.Lock()
		if recBuf.failing || len(recBuf.batches) == recBuf.batchDrainIdx {
			recBuf.mu.Unlock()
			continue
		}

		batch := recBuf.batches[recBuf.batchDrainIdx]
		if added := req.tryAddBatch(atomic.LoadInt32(&s.produceVersion), recBuf, batch); !added {
			recBuf.mu.Unlock()
			moreToDrain = true
			continue
		}

		recBuf.batchDrainIdx++
		recBuf.seq += int32(len(batch.records))
		moreToDrain = moreToDrain || recBuf.tryStopLingerForDraining()
		recBuf.mu.Unlock()

		txnBuilder.add(recBuf)
	}

	// We could have lost our only record buffer just before we grabbed the
	// lock above, so we have to check there are recBufs.
	if len(s.recBufs) > 0 {
		s.recBufsStart = (s.recBufsStart + 1) % len(s.recBufs)
	}
	return req, txnBuilder.req, moreToDrain
}

type txnReqBuilder struct {
	txnID         *string
	req           *kmsg.AddPartitionsToTxnRequest
	addedTopics   map[string]int // topic => index into req
	producerID    int64
	producerEpoch int16
}

func (t *txnReqBuilder) add(rb *recBuf) {
	if t.txnID == nil {
		return
	}
	if rb.addedToTxn {
		return
	}
	rb.addedToTxn = true
	if t.req == nil {
		t.req = &kmsg.AddPartitionsToTxnRequest{
			TransactionalID: *t.txnID,
			ProducerID:      t.producerID,
			ProducerEpoch:   t.producerEpoch,
		}
		t.addedTopics = make(map[string]int, 10)
	}
	idx, exists := t.addedTopics[rb.topic]
	if !exists {
		idx = len(t.req.Topics)
		t.addedTopics[rb.topic] = idx
		t.req.Topics = append(t.req.Topics, kmsg.AddPartitionsToTxnRequestTopic{
			Topic: rb.topic,
		})
	}
	t.req.Topics[idx].Partitions = append(t.req.Topics[idx].Partitions, rb.partition)
}

func (s *sink) maybeDrain() {
	if s.cl.cfg.manualFlushing && atomic.LoadInt32(&s.cl.producer.flushing) == 0 {
		return
	}
	if s.drainState.maybeBegin() {
		go s.drain()
	}
}

func (s *sink) maybeBackoff() {
	s.backoffMu.Lock()
	backoff := s.needBackoff
	s.backoffMu.Unlock()

	if !backoff {
		return
	}
	defer s.clearBackoff()

	s.cl.triggerUpdateMetadata(false) // as good a time as any

	tries := int(atomic.AddUint32(&s.consecutiveFailures, 1))
	after := time.NewTimer(s.cl.cfg.retryBackoff(tries))
	defer after.Stop()

	select {
	case <-after.C:
	case <-s.cl.ctx.Done():
	}
}

func (s *sink) maybeTriggerBackoff(seq uint32) {
	s.backoffMu.Lock()
	defer s.backoffMu.Unlock()
	if seq == s.backoffSeq {
		s.needBackoff = true
	}
}

func (s *sink) clearBackoff() {
	s.backoffMu.Lock()
	defer s.backoffMu.Unlock()
	s.backoffSeq++
	s.needBackoff = false
}

// drain drains buffered records and issues produce requests.
//
// This function is harmless if there are no records that need draining.
// We rely on that to not worry about accidental triggers of this function.
func (s *sink) drain() {
	// If not lingering, before we begin draining, sleep a tiny bit. This
	// helps when a high volume new sink began draining with no linger;
	// rather than immediately eating just one record, we allow it to
	// buffer a bit before we loop draining.
	if s.cl.cfg.linger == 0 && !s.cl.cfg.manualFlushing {
		time.Sleep(5 * time.Millisecond)
	}

	s.cl.producer.incWorkers()
	defer s.cl.producer.decWorkers()

	again := true
	for again {
		if s.cl.producer.isAborting() {
			s.drainState.hardFinish()
			return
		}

		s.maybeBackoff()

		sem := s.inflightSem.Load().(chan struct{})
		select {
		case sem <- struct{}{}:
		case <-s.cl.ctx.Done():
			s.drainState.hardFinish()
			return
		}

		again = s.drainState.maybeFinish(s.produce(sem))
	}
}

func (s *sink) produce(sem <-chan struct{}) bool {
	var produced bool
	defer func() {
		if !produced {
			<-sem
		}
	}()

	// producerID can fail from:
	// - retry failure
	// - auth failure
	// - transactional: a produce failure that failed the producer ID
	// - AddPartitionsToTxn failure (see just below)
	//
	// All but the first error is fatal. Recovery may be possible with
	// EndTransaction in specific cases, but regardless, all buffered
	// records must fail.
	//
	// We init the producer ID before creating a request to ensure we are
	// always using the latest id/epoch with the proper sequence numbers.
	id, epoch, err := s.cl.producerID()
	if err != nil {
		switch err {
		case errProducerIDLoadFail:
			s.cl.bumpRepeatedLoadErr(err)
			s.cl.cfg.logger.Log(LogLevelWarn, "unable to load producer ID, bumping client's buffered record load errors by 1 and retrying")
			return true // whatever caused our produce, we did nothing, so keep going
		default:
			s.cl.cfg.logger.Log(LogLevelError, "fatal InitProducerID error, failing all buffered records", "broker", s.nodeID, "err", err)
			fallthrough
		case errClientClosing:
			s.cl.failBufferedRecords(err)
		}
		return false
	}

	req, txnReq, moreToDrain := s.createReq(id, epoch)

	if len(req.batches) == 0 { // everything was failing or lingering
		return moreToDrain
	}

	if txnReq != nil {
		// txnReq can fail from:
		// - retry failure
		// - auth failure
		// - producer id mapping / epoch errors
		// The latter case can potentially recover with the kip logic
		// we have defined in EndTransaction. Regardless, on failure
		// here, all buffered records must fail.
		// We do not need to clear the addedToTxn flag for any recBuf
		// it was set on, since producer id recovery resets the flag.
		if err := s.doTxnReq(req, txnReq); err != nil {
			switch {
			case isRetriableBrokerErr(err):
				s.cl.bumpRepeatedLoadErr(err)
				s.cl.cfg.logger.Log(LogLevelWarn, "unable to AddPartitionsToTxn due to retriable broker err, bumping client's buffered record load errors by 1 and retrying", "err", err)
				return moreToDrain || len(req.batches) > 0
			default:
				s.cl.failProducerID(id, epoch, err)
				s.cl.cfg.logger.Log(LogLevelError, "fatal AddPartitionsToTxn error, failing all buffered records (it is possible the client can recover after EndTransaction)", "broker", s.nodeID, "err", err)
				s.cl.failBufferedRecords(err)
			}
			return false
		}
	}

	if len(req.batches) == 0 { // txn req could have removed some partitions to retry later (unknown topic, etc.)
		return moreToDrain
	}

	req.backoffSeq = s.backoffSeq // safe to read outside mu since we are in drain loop

	// Add that we are working and then check if we are aborting: this
	// order ensures that we will do not produce after aborting is set.
	p := &s.cl.producer
	p.incWorkers()
	if p.isAborting() {
		p.decWorkers()
		return false
	}

	produced = true
	s.doSequenced(req, func(resp kmsg.Response, err error) {
		s.lastRespSuccessful = err == nil
		s.handleReqResp(req, resp, err)
		p.decWorkers()
		<-sem
	})
	return moreToDrain
}

// With handleSeqResps below, this function ensures that all request responses
// are handled in order. We use this guarantee while in handleReqResp below.
func (s *sink) doSequenced(
	req kmsg.Request,
	promise func(kmsg.Response, error),
) {
	wait := &seqResp{
		done:    make(chan struct{}),
		promise: promise,
	}

	br, err := s.cl.brokerOrErr(s.cl.ctx, s.nodeID, errUnknownBroker)
	if err != nil {
		wait.err = err
		close(wait.done)
	} else {
		br.do(s.cl.ctx, req, func(resp kmsg.Response, err error) {
			wait.resp = resp
			wait.err = err
			close(wait.done)
		})
	}

	s.seqRespsMu.Lock()
	defer s.seqRespsMu.Unlock()

	s.seqResps = append(s.seqResps, wait)
	if len(s.seqResps) == 1 {
		go s.handleSeqResps(s.seqResps[0])
	}
}

// Ensures that all request responses are processed in order.
func (s *sink) handleSeqResps(wait *seqResp) {
more:
	<-wait.done
	wait.promise(wait.resp, wait.err)

	s.seqRespsMu.Lock()
	s.seqResps = s.seqResps[1:]
	if len(s.seqResps) > 0 {
		wait = s.seqResps[0]
		s.seqRespsMu.Unlock()
		goto more
	}
	s.seqRespsMu.Unlock()
}

// Issues an AddPartitionsToTxnRequest before a produce request for all
// partitions that need to be added to a transaction.
func (s *sink) doTxnReq(
	req *produceRequest,
	txnReq *kmsg.AddPartitionsToTxnRequest,
) error {
	return s.cl.doWithConcurrentTransactions("AddPartitionsToTxn", func() error {
		return s.issueTxnReq(req, txnReq)
	})
}

func (s *sink) issueTxnReq(
	req *produceRequest,
	txnReq *kmsg.AddPartitionsToTxnRequest,
) error {
	resp, err := txnReq.RequestWith(s.cl.ctx, s.cl)
	if err != nil {
		return err
	}

	for _, topic := range resp.Topics {
		topicBatches, ok := req.batches[topic.Topic]
		if !ok {
			s.cl.cfg.logger.Log(LogLevelError, "Kafka replied with topic in AddPartitionsToTxnResponse that was not in request", "broker", s.nodeID, "topic", topic.Topic)
			continue
		}
		for _, partition := range topic.Partitions {
			if err := kerr.ErrorForCode(partition.ErrorCode); err != nil {
				// OperationNotAttempted is set for all partitions that are authorized
				// if any partition is unauthorized _or_ does not exist. We simply remove
				// unattempted partitions and treat them as retriable.
				if !kerr.IsRetriable(err) && err != kerr.OperationNotAttempted {
					return err // auth err, etc.
				}

				batch, ok := topicBatches[partition.Partition]
				if !ok {
					s.cl.cfg.logger.Log(LogLevelError, "Kafka replied with partition in AddPartitionsToTxnResponse that was not in request", "broker", s.nodeID, "topic", topic.Topic, "partition", partition.Partition)
					continue
				}

				// If we did not add this partition to the txn, then
				// this must be the first batch in the recBuf, because
				// this is the first time seeing it, which is why we
				// are trying to add it to the txn.
				//
				// Thus, we simply set that this is **not** added, and
				// we reset the drain index to re-try.
				batch.owner.mu.Lock()
				batch.owner.addedToTxn = false
				batch.owner.resetBatchDrainIdx()
				batch.owner.mu.Unlock()

				delete(topicBatches, partition.Partition)
			}
			if len(topicBatches) == 0 {
				delete(req.batches, topic.Topic)
			}
		}
	}
	return nil
}

// Resets the drain indices for any first-batch.
func (s *sink) requeueUnattemptedReq(req *produceRequest, err error) {
	var maybeDrain bool
	req.batches.tryResetFailingBatchesWith(&s.cl.cfg, false, func(seqRecBatch) {
		maybeDrain = true
	})
	if maybeDrain {
		s.maybeTriggerBackoff(req.backoffSeq)
		s.maybeDrain()
	}
}

// firstRespCheck is effectively a sink.Once. On the first response, if the
// used request version is at least 4, we upgrade our inflight sem.
//
// Starting on version 4, Kafka allowed five inflight requests while
// maintaining idempotency. Before, only one was allowed.
//
// We go through an atomic because drain can be waiting on the sem (with
// capacity one). We store four here, meaning new drain loops will load the
// higher capacity sem without read/write pointer racing a current loop.
//
// This logic does mean that we will never use the full potential 5 in flight
// outside of a small window during the store, but some pages in the Kafka
// confluence basically show that more than two in flight has marginal benefit
// anyway (although that may be due to their Java API).
func (s *sink) firstRespCheck(version int16) {
	if s.produceVersion < 0 { // this is the only place this can be checked non-atomically
		atomic.StoreInt32(&s.produceVersion, int32(version))
		if version >= 4 {
			s.inflightSem.Store(make(chan struct{}, 4))
		}
	}
}

// handleReqClientErr is called when the client errors before receiving a
// produce response.
func (s *sink) handleReqClientErr(req *produceRequest, err error) {
	switch {
	case err == errChosenBrokerDead:
		// A dead broker means the broker may have migrated, so we
		// retry to force a metadata reload.
		s.handleRetryBatches(req.batches, req.backoffSeq, false, false)

	case err == errClientClosing:
		s.cl.failBufferedRecords(errClientClosing)

	default:
		s.cl.cfg.logger.Log(LogLevelWarn, "random error while producing, requeueing unattempted request", "broker", s.nodeID, "err", err)
		fallthrough

	case isRetriableBrokerErr(err):
		s.requeueUnattemptedReq(req, err)
	}
}

func (s *sink) handleReqResp(req *produceRequest, resp kmsg.Response, err error) {
	if err != nil {
		s.handleReqClientErr(req, err)
		return
	}
	s.firstRespCheck(req.version)
	atomic.StoreUint32(&s.consecutiveFailures, 0)

	var b *bytes.Buffer
	debug := s.cl.cfg.logger.Level() >= LogLevelDebug

	if debug {
		b = bytes.NewBuffer(make([]byte, 0, 128))
		defer func() {
			update := b.String()
			update = strings.TrimSuffix(update, ", ")
			s.cl.cfg.logger.Log(LogLevelDebug, "produced", "broker", s.nodeID, "to", update)
		}()
	}

	// If we have no acks, we will have no response. The following block is
	// basically an extremely condensed version of everything that follows.
	// We *do* retry on error even with no acks, because an error would
	// mean the write itself failed.
	if req.acks == 0 {
		if debug {
			fmt.Fprintf(b, "noack ")
		}
		for topic, partitions := range req.batches {
			if debug {
				fmt.Fprintf(b, "%s[", topic)
			}
			for partition, batch := range partitions {
				batch.owner.mu.Lock()
				if batch.isOwnersFirstBatch() {
					s.cl.finishBatch(batch.recBatch, req.producerID, req.producerEpoch, partition, 0, nil)
					fmt.Fprintf(b, "%d{%d=>%d}, ", partition, 0, len(batch.records))
				} else {
					fmt.Fprintf(b, "%d{skipped}, ", partition)
				}
				batch.owner.mu.Unlock()
			}
			if debug {
				if bytes.HasSuffix(b.Bytes(), []byte(", ")) {
					b.Truncate(b.Len() - 2)
				}
				b.WriteString("], ")
			}
		}
		return
	}

	var reqRetry seqRecBatches // handled at the end

	pr := resp.(*kmsg.ProduceResponse)
	for _, rTopic := range pr.Topics {
		topic := rTopic.Topic
		partitions, ok := req.batches[topic]
		if !ok {
			s.cl.cfg.logger.Log(LogLevelError, "Kafka erroneously replied with topic in produce request that we did not produce to", "broker", s.nodeID, "topic", topic)
			continue // should not hit this
		}

		if debug {
			fmt.Fprintf(b, "%s[", topic)
		}

		for _, rPartition := range rTopic.Partitions {
			partition := rPartition.Partition
			batch, ok := partitions[partition]
			if !ok {
				s.cl.cfg.logger.Log(LogLevelError, "Kafka erroneously replied with partition in produce request that we did not produce to", "broker", s.nodeID, "topic", rTopic.Topic, "partition", partition)
				continue // should not hit this
			}
			delete(partitions, partition)

			retry := s.handleReqRespBatch(
				b,
				topic,
				partition,
				batch,
				req.producerID,
				req.producerEpoch,
				rPartition.BaseOffset,
				rPartition.ErrorCode,
			)
			if retry {
				reqRetry.addSeqBatch(topic, partition, batch)
			}
		}

		if debug {
			if bytes.HasSuffix(b.Bytes(), []byte(", ")) {
				b.Truncate(b.Len() - 2)
			}
			b.WriteString("], ")
		}

		if len(partitions) == 0 {
			delete(req.batches, topic)
		}
	}

	if len(req.batches) > 0 {
		s.cl.cfg.logger.Log(LogLevelError, "Kafka did not reply to all topics / partitions in the produce request! reenqueuing missing partitions", "broker", s.nodeID)
		s.handleRetryBatches(req.batches, 0, true, false)
	}
	if len(reqRetry) > 0 {
		s.handleRetryBatches(reqRetry, 0, true, true)
	}
}

func (s *sink) handleReqRespBatch(
	b *bytes.Buffer,
	topic string,
	partition int32,
	batch seqRecBatch,
	producerID int64,
	producerEpoch int16,
	baseOffset int64,
	errorCode int16,
) (retry bool) {
	batch.owner.mu.Lock()
	defer batch.owner.mu.Unlock()

	nrec := len(batch.records)

	debug := b != nil
	if debug {
		fmt.Fprintf(b, "%d{", partition)
	}

	// We only ever operate on the first batch in a record buf. Batches
	// work sequentially; if this is not the first batch then an error
	// happened and this later batch is no longer a part of a seq chain.
	if !batch.isOwnersFirstBatch() {
		if debug {
			if err := kerr.ErrorForCode(errorCode); err == nil {
				if len(batch.records) > 0 {
					fmt.Fprintf(b, "skipped@%d=>%d}, ", baseOffset, baseOffset+int64(nrec))
				} else {
					fmt.Fprintf(b, "skipped@%d}, ", baseOffset)
				}
			} else {
				if len(batch.records) > 0 {
					fmt.Fprintf(b, "skipped@%d,%d(%s)}, ", baseOffset, nrec, err)
				} else {
					fmt.Fprintf(b, "skipped@%d(%s)}, ", baseOffset, err)
				}
			}
		}
		return false
	}

	// Since we have received a response and we are the first batch, we can
	// at this point re-enable failing from load errors.
	batch.canFailFromLoadErrs = true

	err := kerr.ErrorForCode(errorCode)
	switch {
	case kerr.IsRetriable(err) &&
		err != kerr.CorruptMessage &&
		batch.tries < s.cl.cfg.produceRetries:

		if debug {
			fmt.Fprintf(b, "retrying@%d,%d(%s)}, ", baseOffset, nrec, err)
		}
		return true

	case err == kerr.OutOfOrderSequenceNumber,
		err == kerr.UnknownProducerID,
		err == kerr.InvalidProducerIDMapping,
		err == kerr.InvalidProducerEpoch:

		// OOOSN always means data loss 1.0.0+ and is ambiguous prior.
		// We assume the worst and only continue if requested.
		//
		// UnknownProducerID was introduced to allow some form of safe
		// handling, but KIP-360 demonstrated that resetting sequence
		// numbers is fundamentally unsafe, so we treat it like OOOSN.
		//
		// InvalidMapping is similar to UnknownProducerID, but occurs
		// when the txnal coordinator timed out our transaction.
		//
		// 2.5.0
		// =====
		// 2.5.0 introduced some behavior to potentially safely reset
		// the sequence numbers by bumping an epoch (see KIP-360).
		//
		// For the idempotent producer, the solution is to fail all
		// buffered records and then let the client user reset things
		// with the understanding that they cannot guard against
		// potential dups / reordering at that point. Realistically,
		// that's no better than a config knob that allows the user
		// to continue (our stopOnDataLoss flag), so for the idempotent
		// producer, if stopOnDataLoss is false, we just continue.
		//
		// For the transactional producer, we always fail the producerID.
		// EndTransaction will trigger recovery if possible.
		//
		// 2.7.0
		// =====
		// InvalidProducerEpoch became retriable in 2.7.0. Prior, it
		// was ambiguous (timeout? fenced?). Now, InvalidProducerEpoch
		// is only returned on produce, and then we can recover on other
		// txn coordinator requests, which have PRODUCER_FENCED vs
		// TRANSACTION_TIMED_OUT.

		if s.cl.cfg.txnID != nil || s.cl.cfg.stopOnDataLoss {
			s.cl.cfg.logger.Log(LogLevelInfo, "batch errored, failing the producer ID",
				"broker", s.nodeID,
				"topic", topic,
				"partition", partition,
				"producer_id", producerID,
				"producer_epoch", producerEpoch,
				"err", err,
			)
			s.cl.failProducerID(producerID, producerEpoch, err)

			s.cl.finishBatch(batch.recBatch, producerID, producerEpoch, partition, baseOffset, err)
			if debug {
				fmt.Fprintf(b, "fatal@%d,%d(%s)}, ", baseOffset, len(batch.records), err)
			}
			return false
		}
		if s.cl.cfg.onDataLoss != nil {
			s.cl.cfg.onDataLoss(topic, partition)
		}

		// For OOOSN, and UnknownProducerID
		//
		// The only recovery is to fail the producer ID, which ensures
		// that all batches reset sequence numbers and use a new producer
		// ID on the next batch.
		//
		// For InvalidProducerIDMapping && InvalidProducerEpoch,
		//
		// We should not be here, since this error occurs in the
		// context of transactions, which are caught above.
		s.cl.cfg.logger.Log(LogLevelInfo, fmt.Sprintf("batch errored with %s, failing the producer ID and resetting all sequence numbers", err.(*kerr.Error).Message),
			"broker", s.nodeID,
			"topic", topic,
			"partition", partition,
			"producer_id", producerID,
			"producer_epoch", producerEpoch,
			"err", err,
		)

		// After we fail here, any new produce (even new ones
		// happening concurrent with this function) will load
		// a new epoch-bumped producer ID and all first-batches
		// will reset sequence numbers appropriately.
		s.cl.failProducerID(producerID, producerEpoch, errReloadProducerID)
		if debug {
			fmt.Fprintf(b, "resetting@%d,%d(%s)}, ", baseOffset, len(batch.records), err)
		}
		return true

	case err == kerr.DuplicateSequenceNumber: // ignorable, but we should not get
		s.cl.cfg.logger.Log(LogLevelInfo, "received unexpected duplicate sequence number, ignoring and treating batch as successful",
			"broker", s.nodeID,
			"topic", topic,
			"partition", partition,
		)
		err = nil
		fallthrough
	default:
		if err != nil {
			s.cl.cfg.logger.Log(LogLevelInfo, "batch in a produce request failed",
				"broker", s.nodeID,
				"topic", topic,
				"partition", partition,
				"err", err,
				"err_is_retriable", kerr.IsRetriable(err),
				"max_retries_reached", batch.tries >= s.cl.cfg.produceRetries,
			)
		} else {
		}
		s.cl.finishBatch(batch.recBatch, producerID, producerEpoch, partition, baseOffset, err)
		if debug {
			if err != nil {
				fmt.Fprintf(b, "err@%d,%d(%s)}, ", baseOffset, len(batch.records), err)
			} else {
				fmt.Fprintf(b, "%d=>%d}, ", baseOffset, baseOffset+int64(len(batch.records)))
			}
		}
	}
	return false // no retry
}

// finishBatch removes a batch from its owning record buffer and finishes all
// records in the batch.
//
// This is safe even if the owning recBuf migrated sinks, since we are
// finishing based off the status of an inflight req from the original sink.
func (cl *Client) finishBatch(batch *recBatch, producerID int64, producerEpoch int16, partition int32, baseOffset int64, err error) {
	recBuf := batch.owner

	if err != nil {
		// We know that Kafka replied this batch is a failure. We can
		// fail this batch and all batches in this partition.
		// This will keep sequence numbers correct.
		recBuf.failAllRecords(err)
		return
	}

	// We know the batch made it to Kafka successfully without error.
	// We remove this batch and finish all records appropriately.
	recBuf.batch0Seq += int32(len(recBuf.batches[0].records))
	recBuf.batches[0] = nil
	recBuf.batches = recBuf.batches[1:]
	recBuf.batchDrainIdx--

	batch.mu.Lock()
	records, attrs := batch.records, batch.attrs
	batch.records = nil
	batch.mu.Unlock()

	for i, pnr := range records {
		pnr.Offset = baseOffset + int64(i)
		pnr.Partition = partition
		pnr.ProducerID = producerID
		pnr.ProducerEpoch = producerEpoch

		// A recBuf.attrs is updated when appending to be written. For
		// v0 && v1 produce requests, we set bit 8 in the attrs
		// corresponding to our own RecordAttr's bit 8 being no
		// timestamp type. Thus, we can directly convert the batch
		// attrs to our own RecordAttrs.
		pnr.Attrs = RecordAttrs{uint8(attrs)}

		cl.finishRecordPromise(pnr.promisedRec, err)
	}
}

// handleRetryBatches sets any first-buf-batch to failing and triggers a
// metadata that will eventually clear the failing state and re-drain.
func (s *sink) handleRetryBatches(
	retry seqRecBatches,
	backoffSeq uint32,
	updateMeta bool, // if we should maybe update the metadata
	canFail bool, // if records can fail if they are at limits
) {
	var needsMetaUpdate bool
	retry.tryResetFailingBatchesWith(&s.cl.cfg, canFail, func(batch seqRecBatch) {
		if updateMeta {
			batch.owner.failing = true
			needsMetaUpdate = true
		}
	})

	// If we are retrying without a metadata update, then we definitely
	// want to backoff a little bit: our chosen broker died, let's not
	// spin-loop re-requesting.
	//
	// If we do want to metadata update, we only do so if any batch was the
	// first batch in its buf / not concurrently failed.
	if needsMetaUpdate {
		s.cl.triggerUpdateMetadata(true)
	} else if !updateMeta {
		s.maybeTriggerBackoff(backoffSeq)
		s.maybeDrain()
	}
}

// addRecBuf adds a new record buffer to be drained to a sink and clears the
// buffer's failing state.
func (s *sink) addRecBuf(add *recBuf) {
	s.recBufsMu.Lock()
	add.recBufsIdx = len(s.recBufs)
	s.recBufs = append(s.recBufs, add)
	s.recBufsMu.Unlock()

	add.clearFailing()
}

// removeRecBuf removes a record buffer from a sink.
func (s *sink) removeRecBuf(rm *recBuf) {
	s.recBufsMu.Lock()
	defer s.recBufsMu.Unlock()

	if rm.recBufsIdx != len(s.recBufs)-1 {
		s.recBufs[rm.recBufsIdx], s.recBufs[len(s.recBufs)-1] =
			s.recBufs[len(s.recBufs)-1], nil

		s.recBufs[rm.recBufsIdx].recBufsIdx = rm.recBufsIdx
	} else {
		s.recBufs[rm.recBufsIdx] = nil // do not let this removal hang around
	}

	s.recBufs = s.recBufs[:len(s.recBufs)-1]
	if s.recBufsStart == len(s.recBufs) {
		s.recBufsStart = 0
	}
}

// recBuf is a buffer of records being produced to a partition and being
// drained by a sink. This is only not drained if the partition has a load
// error and thus does not a have a sink to be drained into.
type recBuf struct {
	cl *Client // for cfg, record finishing

	topic     string
	partition int32

	// The number of bytes we can buffer in a batch for this particular
	// topic/partition. This may be less than the configured
	// maxRecordBatchBytes because of produce request overhead.
	maxRecordBatchBytes int32

	// addedToTxn, for transactions only, signifies whether this partition
	// has been added to the transaction yet or not.
	//
	// This does not need to be under the mu since it is updated either
	// serially in building a req (the first time) or after failing to add
	// the partition to a txn (again serially), or in EndTransaction after
	// all buffered records are flushed (if the API is used correctly).
	addedToTxn bool

	mu sync.Mutex // guards r/w access to all fields below

	// sink is who is currently draining us. This can be modified
	// concurrently during a metadata update.
	//
	// The first set to a non-nil sink is done without a mutex.
	//
	// Since only metadata updates can change the sink, metadata updates
	// also read this without a mutex.
	sink *sink
	// recBufsIdx is our index into our current sink's recBufs field.
	// This exists to aid in removing the buffer from the sink.
	recBufsIdx int

	topicPartitionData // updated in metadata migrateProductionTo (same spot sink is updated)

	// seq is used for the seq in each record batch. It is incremented when
	// produce requests are made and can be reset on errors to batch0Seq.
	//
	// If idempotency is disabled, we just use "0" for the first sequence
	// when encoding our payload.
	seq int32
	// batch0Seq is the seq of the batch at batchDrainIdx 0. If we reset
	// the drain index, we reset seq with this number. If we successfully
	// finish batch 0, we bump this.
	batch0Seq int32
	// If we need to reset sequence numbers, we set needSeqReset, and then
	// when we use the **first** batch, we reset sequences to 0.
	needSeqReset bool

	// batches is our list of buffered records. Batches are appended as the
	// final batch crosses size thresholds or as drain freezes batches from
	// further modification.
	//
	// Most functions in a sink only operate on a batch if the batch is the
	// first batch in a buffer. This is necessary to ensure that all
	// records are truly finished without error in order.
	batches []*recBatch
	// batchDrainIdx is where the next batch will drain from. We only
	// remove from the head of batches when a batch is finished.
	// This is read while buffering and modified in a few places.
	batchDrainIdx int

	// lingering is a timer that avoids starting maybeDrain until expiry,
	// allowing for more records to be buffered in a single batch.
	//
	// Note that if something else starts a drain, if the first batch of
	// this buffer fits into the request, it will be used.
	//
	// This is on recBuf rather than Sink to avoid some complicated
	// interactions of triggering the sink to loop or not. Ideally, with
	// the sticky partition hashers, we will only have a few partitions
	// lingering and that this is on a RecBuf should not matter.
	lingering *time.Timer

	// failing is set when we encounter a temporary partition error during
	// producing, such as UnknownTopicOrPartition (signifying the partition
	// moved to a different broker).
	//
	// It is always cleared on metadata update.
	failing bool
}

// bufferRecord usually buffers a record, but does not if abortOnNewBatch is
// true and if this function would create a new batch.
//
// This returns whether the promised record was processed or not (buffered or
// immediately errored).
func (recBuf *recBuf) bufferRecord(pr promisedRec, abortOnNewBatch bool) bool {
	recBuf.mu.Lock()
	defer recBuf.mu.Unlock()

	// Timestamp after locking to ensure sequential, and truncate to
	// milliseconds to avoid some accumulated rounding error problems
	// (see Shopify/sarama#1455)
	pr.Timestamp = time.Now().Truncate(time.Millisecond)

	var (
		newBatch       = true
		onDrainBatch   = recBuf.batchDrainIdx == len(recBuf.batches)
		produceVersion = atomic.LoadInt32(&recBuf.sink.produceVersion)
	)

	if !onDrainBatch {
		batch := recBuf.batches[len(recBuf.batches)-1]
		appended, _ := batch.tryBuffer(pr, produceVersion, recBuf.maxRecordBatchBytes, false)
		newBatch = !appended
	}

	if newBatch {
		newBatch := recBuf.newRecordBatch()
		appended, aborted := newBatch.tryBuffer(pr, produceVersion, recBuf.maxRecordBatchBytes, abortOnNewBatch)

		switch {
		case aborted: // not processed
			return false
		case appended: // we return true below
		default: // processed as failure
			recBuf.cl.finishRecordPromise(pr, kerr.MessageTooLarge)
			return true
		}

		recBuf.batches = append(recBuf.batches, newBatch)
	}

	if recBuf.cl.cfg.linger == 0 {
		if onDrainBatch {
			recBuf.sink.maybeDrain()
		}
	} else {
		// With linger, if this is a new batch but not the first, we
		// stop lingering and begin draining. The drain loop will
		// restart our linger once this buffer has one batch left.
		if newBatch && !onDrainBatch ||
			// If this is the first batch, try lingering; if
			// we cannot, we are being flushed and must drain.
			onDrainBatch && !recBuf.lockedMaybeStartLinger() {
			recBuf.lockedStopLinger()
			recBuf.sink.maybeDrain()
		}
	}

	return true
}

// Stops lingering, potentially restarting it, and returns whether there is
// more to drain.
//
// If lingering, if there are more than one batches ready, there is definitely
// more to drain and we should not linger. Otherwise, if we cannot restart
// lingering, then we are flushing and also indicate there is more to drain.
func (recBuf *recBuf) tryStopLingerForDraining() bool {
	recBuf.lockedStopLinger()
	canLinger := recBuf.cl.cfg.linger == 0
	moreToDrain := !canLinger && len(recBuf.batches) > recBuf.batchDrainIdx ||
		canLinger && (len(recBuf.batches) > recBuf.batchDrainIdx+1 ||
			len(recBuf.batches) == recBuf.batchDrainIdx+1 && !recBuf.lockedMaybeStartLinger())
	return moreToDrain
}

// Begins a linger timer unless the producer is being flushed.
func (recBuf *recBuf) lockedMaybeStartLinger() bool {
	if atomic.LoadInt32(&recBuf.cl.producer.flushing) == 1 {
		return false
	}
	recBuf.lingering = time.AfterFunc(recBuf.cl.cfg.linger, recBuf.sink.maybeDrain)
	return true
}

func (recBuf *recBuf) lockedStopLinger() {
	if recBuf.lingering != nil {
		recBuf.lingering.Stop()
		recBuf.lingering = nil
	}
}

func (recBuf *recBuf) unlingerAndManuallyDrain() {
	recBuf.mu.Lock()
	defer recBuf.mu.Unlock()
	recBuf.lockedStopLinger()
	recBuf.sink.maybeDrain()
}

// bumpRepeatedLoadErr is provided to bump a buffer's number of consecutive
// load errors during metadata updates.
//
// Partition load errors are generally temporary (leader/listener/replica not
// available), and this try bump is not expected to do much. If for some reason
// a partition errors for a long time and we are not idempotent, this function
// drops all buffered records.
func (recBuf *recBuf) bumpRepeatedLoadErr(err error) {
	recBuf.mu.Lock()
	defer recBuf.mu.Unlock()
	if len(recBuf.batches) == 0 {
		return
	}
	recBuf.cl.cfg.logger.Log(LogLevelWarn, "produce partition load error, unable to produce on this partition", "broker", recBuf.sink.nodeID, "topic", recBuf.topic, "partition", recBuf.partition, "err", err)
	batch0 := recBuf.batches[0]
	batch0.tries++
	if (!recBuf.cl.idempotent() || batch0.canFailFromLoadErrs) &&
		(batch0.isTimedOut(recBuf.cl.cfg.recordTimeout) || batch0.tries > recBuf.cl.cfg.produceRetries || !kerr.IsRetriable(err)) {
		recBuf.failAllRecords(err)
	}
}

// failAllRecords fails all buffered records in this recBuf.
// This is used anywhere where we have to fail and remove an entire batch,
// if we just removed the one batch, the seq num chain would be broken.
//
//   - from fatal InitProducerID or AddPartitionsToTxn
//   - from client closing
//   - if not idempotent && hit retry / timeout limit
//   - if batch fails fatally when producing
func (recBuf *recBuf) failAllRecords(err error) {
	recBuf.lockedStopLinger()
	for _, batch := range recBuf.batches {
		// We need to guard our clearing of records against a
		// concurrent produceRequest's write, which can have this batch
		// buffered wile we are failing.
		//
		// We do not need to worry about concurrent recBuf
		// modifications to this batch because the recBuf is already
		// locked.
		batch.mu.Lock()
		for _, pnr := range batch.records {
			recBuf.cl.finishRecordPromise(pnr.promisedRec, err)
		}
		batch.records = nil
		batch.mu.Unlock()
	}
	recBuf.resetBatchDrainIdx()
	recBuf.batches = nil
}

// clearFailing clears a buffer's failing state if it is failing.
//
// This is called when a buffer is added to a sink (to clear a failing state
// from migrating buffers between sinks) or when a metadata update sees the
// sink is still on the same source.
func (recBuf *recBuf) clearFailing() {
	recBuf.mu.Lock()
	defer recBuf.mu.Unlock()

	recBuf.failing = false
	if len(recBuf.batches) != recBuf.batchDrainIdx {
		recBuf.sink.maybeDrain()
	}
}

func (recBuf *recBuf) resetBatchDrainIdx() {
	recBuf.seq = recBuf.batch0Seq
	recBuf.batchDrainIdx = 0
}

// promisedRec ties a record with the callback that will be called once
// a batch is finally written and receives a response.
type promisedRec struct {
	promise func(*Record, error)
	*Record
}

// promisedNumberedRecord ties a promised record to its calculated numbers.
type promisedNumberedRecord struct {
	recordNumbers
	promisedRec
}

// recBatch is the type used for buffering records before they are written.
type recBatch struct {
	owner *recBuf // who owns us

	tries int64 // if this was sent before and is thus now immutable

	// We can only fail a batch if we have never issued it, or we have
	// issued it and have received a response. If we do not receive a
	// response, we cannot know whether we actually wrote bytes that Kafka
	// processed or not. So, we set this to false every time we issue a
	// request with this batch, and then reset it to true whenever we
	// process a response.
	canFailFromLoadErrs bool

	wireLength   int32 // tracks total size this batch would currently encode as, including length prefix
	v1wireLength int32 // same as wireLength, but for message set v1

	attrs          int16 // updated during apending; read and converted to RecordAttrs on success
	firstTimestamp int64 // since unix epoch, in millis

	mu      sync.Mutex // guards appendTo's reading of records against failAllRecords emptying it
	records []promisedNumberedRecord
}

func (b *recBatch) v0wireLength() int32 { return b.v1wireLength - 8 } // no timestamp
func (b *recBatch) batchLength() int32  { return b.wireLength - 4 }   // no length prefix
func (b *recBatch) flexibleWireLength() int32 { // uvarint length prefix
	batchLength := b.batchLength()
	return int32(kbin.UvarintLen(uvar32(batchLength))) + batchLength
}

// appendRecord saves a new record to a batch.
//
// This is called under the owning recBuf's mu, meaning records cannot be
// concurrently modified by failing.
func (b *recBatch) appendRecord(pr promisedRec, nums recordNumbers) {
	b.wireLength += nums.wireLength()
	b.v1wireLength += messageSet1Length(pr.Record)
	if len(b.records) == 0 {
		b.firstTimestamp = pr.Timestamp.UnixNano() / 1e6
	}
	b.records = append(b.records, promisedNumberedRecord{
		nums,
		pr,
	})
}

// newRecordBatch returns a new record batch for a topic and partition.
func (recBuf *recBuf) newRecordBatch() *recBatch {
	const recordBatchOverhead = 4 + // array len
		8 + // firstOffset
		4 + // batchLength
		4 + // partitionLeaderEpoch
		1 + // magic
		4 + // crc
		2 + // attributes
		4 + // lastOffsetDelta
		8 + // firstTimestamp
		8 + // maxTimestamp
		8 + // producerID
		2 + // producerEpoch
		4 + // seq
		4 // record array length
	return &recBatch{
		owner:      recBuf,
		records:    make([]promisedNumberedRecord, 0, 10),
		wireLength: recordBatchOverhead,

		canFailFromLoadErrs: true, // until we send this batch, we can fail it
	}
}

// isOwnersFirstBatch returns if the batch in a recBatch is the first batch in
// a records. We only ever want to update batch / buffer logic if the batch is
// the first in the buffer.
func (b *recBatch) isOwnersFirstBatch() bool {
	return len(b.owner.batches) > 0 && b.owner.batches[0] == b
}

// Returns whether the first record in a batch is past the limit.
func (b *recBatch) isTimedOut(limit time.Duration) bool {
	if limit == 0 {
		return false
	}
	return time.Since(b.records[0].Timestamp) > limit
}

////////////////////
// produceRequest //
////////////////////

// produceRequest is a kmsg.Request that is used when we want to
// flush our buffered records.
//
// It is the same as kmsg.ProduceRequest, but with a custom AppendTo.
type produceRequest struct {
	version int16

	backoffSeq uint32

	txnID   *string
	acks    int16
	timeout int32
	batches seqRecBatches

	producerID    int64
	producerEpoch int16
	idempotent    bool

	compressor *compressor

	// wireLength is initially the size of sending a produce request,
	// including the request header, with no topics. We start with the
	// non-flexible size because it is strictly larger than flexible, but
	// we use the proper flexible numbers when calculating.
	wireLength      int32
	wireLengthLimit int32
}

func (r *produceRequest) tryAddBatch(produceVersion int32, recBuf *recBuf, batch *recBatch) bool {
	batchWireLength, flexible := batch.wireLengthForProduceVersion(produceVersion)
	batchWireLength += 4 // int32 partition prefix

	if partitions, exists := r.batches[recBuf.topic]; !exists {
		lt := int32(len(recBuf.topic))
		if flexible {
			batchWireLength += uvarlen(len(recBuf.topic)) + lt + 1 // compact string len, topic, compact array len for 1 item
		} else {
			batchWireLength += 2 + lt + 4 // string len, topic, partition array len
		}
	} else if flexible {
		// If the topic exists and we are flexible, adding this
		// partition may increase the length of our size prefix.
		lastPartitionsLen := uvarlen(len(partitions))
		newPartitionsLen := uvarlen(len(partitions) + 1)
		batchWireLength += (newPartitionsLen - lastPartitionsLen)
	}
	// If we are flexible but do not know it yet, adding partitions may
	// increase our length prefix. Since we are pessimistically assuming
	// non-flexible, we have 200mil partitions to add before we have to
	// worry about hitting 5 bytes vs. the non-flexible 4. We do not worry.

	if r.wireLength+batchWireLength > r.wireLengthLimit {
		return false
	}

	if recBuf.needSeqReset && recBuf.batches[0] == batch {
		recBuf.needSeqReset = false
		recBuf.seq = 0
		recBuf.batch0Seq = 0
	}

	batch.tries++
	batch.canFailFromLoadErrs = false
	r.wireLength += batchWireLength
	r.batches.addBatch(
		recBuf.topic,
		recBuf.partition,
		recBuf.seq,
		batch,
	)
	return true
}

// seqRecBatch: a recBatch with a sequence number.
type seqRecBatch struct {
	seq int32
	*recBatch
}

type seqRecBatches map[string]map[int32]seqRecBatch

func (rbs *seqRecBatches) addBatch(topic string, part int32, seq int32, batch *recBatch) {
	if *rbs == nil {
		*rbs = make(seqRecBatches, 5)
	}
	topicBatches, exists := (*rbs)[topic]
	if !exists {
		topicBatches = make(map[int32]seqRecBatch, 1)
		(*rbs)[topic] = topicBatches
	}
	topicBatches[part] = seqRecBatch{seq, batch}
}

func (rbs *seqRecBatches) addSeqBatch(topic string, part int32, batch seqRecBatch) {
	if *rbs == nil {
		*rbs = make(seqRecBatches, 5)
	}
	topicBatches, exists := (*rbs)[topic]
	if !exists {
		topicBatches = make(map[int32]seqRecBatch, 1)
		(*rbs)[topic] = topicBatches
	}
	topicBatches[part] = batch
}

// Resets the drain index for any batch that is the first in its record buffer.
//
// If idempotency is disabled, if a batch is timed out or hit the retry limit,
// we fail it and anything after it.
func (rbs seqRecBatches) tryResetFailingBatchesWith(cfg *cfg, canFail bool, fn func(seqRecBatch)) {
	for _, partitions := range rbs {
		for _, batch := range partitions {
			batch.owner.mu.Lock()
			if batch.isOwnersFirstBatch() {
				if canFail || cfg.disableIdempotency {
					var err error
					if batch.isTimedOut(cfg.recordTimeout) {
						err = errRecordTimeout
					} else if batch.tries >= cfg.produceRetries {
						err = errors.New("record failed after being retried too many times")
					}
					if err != nil {
						batch.owner.failAllRecords(err)
						batch.owner.mu.Unlock()
						continue
					}
				}
				batch.owner.resetBatchDrainIdx()
				fn(batch)
			}
			batch.owner.mu.Unlock()
		}
	}
}

//////////////
// COUNTING // - this section is all about counting how bytes lay out on the wire
//////////////

// Returns the non-flexible base produce request length (the request header and
// the request itself with no topics).
//
// See the large comment on maxRecordBatchBytesForTopic for why we always use
// non-flexible (in short: it is strictly larger).
func (cl *Client) baseProduceRequestLength() int32 {
	const messageRequestOverhead int32 = 4 + // int32 length prefix
		2 + // int16 key
		2 + // int16 version
		4 + // int32 correlation ID
		2 // int16 client ID len (always non flexible)
		// empty tag section skipped; see below

	const produceRequestBaseOverhead int32 = 2 + // int16 transactional ID len (flexible or not, since we cap at 16382)
		2 + // int16 acks
		4 + // int32 timeout
		4 // int32 topics non-flexible array length
		// empty tag section skipped; see below

	baseLength := messageRequestOverhead + produceRequestBaseOverhead
	if cl.cfg.id != nil {
		baseLength += int32(len(*cl.cfg.id))
	}
	if cl.cfg.txnID != nil {
		baseLength += int32(len(*cl.cfg.txnID))
	}
	return baseLength
}

// Returns the maximum size a record batch can be for this given topic, such
// that if just a **single partition** is fully stuffed with records and we
// only encode that one partition, we will not overflow our configured limits.
//
// The maximum topic length is 249, which has a 2 byte prefix for flexible or
// non-flexible.
//
// Non-flexible versions will have a 4 byte length topic array prefix, a 4 byte
// length partition array prefix. and a 4 byte records array length prefix.
//
// Flexible versions would have a 1 byte length topic array prefix, a 1 byte
// length partition array prefix, up to 5 bytes for the records array length
// prefix, and three empty tag sections resulting in 3 bytes (produce request
// struct, topic struct, partition struct). As well, for the request header
// itself, we have an additional 1 byte tag section (that we currently keep
// empty).
//
// Thus in the worst case, we have 14 bytes of prefixes for non-flexible vs.
// 11 bytes for flexible. We default to the more limiting size: non-flexible.
func (cl *Client) maxRecordBatchBytesForTopic(topic string) int32 {
	minOnePartitionBatchLength := cl.baseProduceRequestLength() +
		2 + // int16 topic string length prefix length
		int32(len(topic)) +
		4 + // int32 partitions array length
		4 + // partition int32 encoding length
		4 // int32 record bytes array length

	wireLengthLimit := cl.cfg.maxBrokerWriteBytes

	recordBatchLimit := wireLengthLimit - minOnePartitionBatchLength
	if cfgLimit := cl.cfg.maxRecordBatchBytes; cfgLimit < recordBatchLimit {
		recordBatchLimit = cfgLimit
	}
	return recordBatchLimit
}

func messageSet0Length(r *Record) int32 {
	const length = 4 + // array len
		8 + // offset
		4 + // size
		4 + // crc
		1 + // magic
		1 + // attributes
		4 + // key array bytes len
		4 // value array bytes len
	return length + int32(len(r.Key)) + int32(len(r.Value))
}

func messageSet1Length(r *Record) int32 {
	return messageSet0Length(r) + 8 // timestamp
}

// Returns the numbers for a record if it were added to the record batch.
func (b *recBatch) calculateRecordNumbers(r *Record) recordNumbers {
	tsMillis := r.Timestamp.UnixNano() / 1e6
	tsDelta := int32(tsMillis - b.firstTimestamp)
	offsetDelta := int32(len(b.records)) // since called before adding record, delta is the current end

	l := 1 + // attributes, int8 unused
		kbin.VarintLen(tsDelta) +
		kbin.VarintLen(offsetDelta) +
		kbin.VarintLen(int32(len(r.Key))) +
		len(r.Key) +
		kbin.VarintLen(int32(len(r.Value))) +
		len(r.Value) +
		kbin.VarintLen(int32(len(r.Headers))) // varint array len headers

	for _, h := range r.Headers {
		l += kbin.VarintLen(int32(len(h.Key))) +
			len(h.Key) +
			kbin.VarintLen(int32(len(h.Value))) +
			len(h.Value)
	}

	return recordNumbers{
		lengthField:    int32(l),
		timestampDelta: tsDelta,
	}
}

func uvar32(l int32) uint32 { return 1 + uint32(l) }
func uvarlen(l int) int32   { return int32(kbin.UvarintLen(uvar32(int32(l)))) }

// recordNumbers tracks a few numbers for a record that is buffered.
type recordNumbers struct {
	lengthField    int32 // the length field prefix of a record encoded on the wire
	timestampDelta int32 // the ms delta of when the record was added against the first timestamp
}

// wireLength is the wire length of a record including its length field prefix.
func (n recordNumbers) wireLength() int32 {
	return int32(kbin.VarintLen(n.lengthField)) + n.lengthField
}

func (batch *recBatch) wireLengthForProduceVersion(v int32) (batchWireLength int32, flexible bool) {
	batchWireLength = batch.wireLength

	// If we do not yet know the produce version, we default to the largest
	// size. Our request building sizes will always be an overestimate.
	if v < 0 {
		v1BatchWireLength := batch.v1wireLength
		if v1BatchWireLength > batchWireLength {
			batchWireLength = v1BatchWireLength
		}
		flexibleBatchWireLength := batch.flexibleWireLength()
		if flexibleBatchWireLength > batchWireLength {
			batchWireLength = flexibleBatchWireLength
		}

	} else {
		switch v {
		case 0, 1:
			batchWireLength = batch.v0wireLength()
		case 2:
			batchWireLength = batch.v1wireLength
		case 3, 4, 5, 6, 7, 8:
			batchWireLength = batch.wireLength
		default:
			batchWireLength = batch.flexibleWireLength()
			flexible = true
		}
	}

	return
}

func (batch *recBatch) tryBuffer(pr promisedRec, produceVersion, maxBatchBytes int32, abortOnNewBatch bool) (appended, aborted bool) {
	recordNumbers := batch.calculateRecordNumbers(pr.Record)

	batchWireLength, _ := batch.wireLengthForProduceVersion(produceVersion)
	newBatchLength := batchWireLength + recordNumbers.wireLength()

	if batch.tries == 0 && newBatchLength <= maxBatchBytes {
		if abortOnNewBatch {
			return false, true
		}
		batch.appendRecord(pr, recordNumbers)
		return true, false
	}
	return false, false
}

//////////////
// ENCODING // - this section is all about actually writing a produce request
//////////////

func (*produceRequest) Key() int16           { return 0 }
func (*produceRequest) MaxVersion() int16    { return 9 }
func (p *produceRequest) SetVersion(v int16) { p.version = v }
func (p *produceRequest) GetVersion() int16  { return p.version }
func (p *produceRequest) IsFlexible() bool   { return p.version >= 9 }
func (p *produceRequest) AppendTo(dst []byte) []byte {
	flexible := p.IsFlexible()

	if p.version >= 3 {
		if flexible {
			dst = kbin.AppendCompactNullableString(dst, p.txnID)
		} else {
			dst = kbin.AppendNullableString(dst, p.txnID)
		}
	}

	dst = kbin.AppendInt16(dst, p.acks)
	dst = kbin.AppendInt32(dst, p.timeout)
	if flexible {
		dst = kbin.AppendCompactArrayLen(dst, len(p.batches))
	} else {
		dst = kbin.AppendArrayLen(dst, len(p.batches))
	}

	for topic, partitions := range p.batches {
		if flexible {
			dst = kbin.AppendCompactString(dst, topic)
			dst = kbin.AppendCompactArrayLen(dst, len(partitions))
		} else {
			dst = kbin.AppendString(dst, topic)
			dst = kbin.AppendArrayLen(dst, len(partitions))
		}
		for partition, batch := range partitions {
			dst = kbin.AppendInt32(dst, partition)
			batch.mu.Lock()
			if batch.records == nil { // concurrent failAllRecords
				if flexible {
					dst = kbin.AppendCompactNullableBytes(dst, nil)
				} else {
					dst = kbin.AppendNullableBytes(dst, nil)
				}
				batch.mu.Unlock()
				continue
			}
			if p.version < 3 {
				dst = batch.appendToAsMessageSet(dst, uint8(p.version), p.compressor)
			} else {
				dst = batch.appendTo(dst, p.version, p.producerID, p.producerEpoch, p.idempotent, p.txnID != nil, p.compressor)
			}
			batch.mu.Unlock()
			if flexible {
				dst = append(dst, 0)
			}
		}
		if flexible {
			dst = append(dst, 0)
		}
	}
	if flexible {
		dst = append(dst, 0)
	}
	return dst
}
func (*produceRequest) ReadFrom([]byte) error {
	panic("unreachable -- the client never uses ReadFrom on its internal produceRequest")
}

func (p *produceRequest) ResponseKind() kmsg.Response {
	return &kmsg.ProduceResponse{Version: p.version}
}

func (r seqRecBatch) appendTo(
	in []byte,
	version int16,
	producerID int64,
	producerEpoch int16,
	idempotent bool,
	transactional bool,
	compressor *compressor,
) (dst []byte) { // named return so that our defer for flexible versions can modify it
	flexible := version >= 9
	dst = in
	nullableBytesLen := r.wireLength - 4 // NULLABLE_BYTES leading length, minus itself
	nullableBytesLenAt := len(dst)       // in case compression adjusting
	dst = kbin.AppendInt32(dst, nullableBytesLen)

	// With flexible versions, the array length prefix can be anywhere from
	// 1 byte long to 5 bytes long (covering up to 268MB).
	//
	// We have to add our initial understanding of the array length as a
	// uvarint, but if compressing shrinks what that length would encode
	// as, we have to shift everything down.
	if flexible {
		dst = dst[:nullableBytesLenAt]
		batchLength := r.batchLength()
		dst = kbin.AppendUvarint(dst, uvar32(batchLength)) // compact array non-null prefix
		batchAt := len(dst)
		defer func() {
			batch := dst[batchAt:]
			if int32(len(batch)) == batchLength { // we did not compress: simply return
				return
			}

			// We could have only shrunk the batch bytes, so our
			// append here will not overwrite anything.
			newDst := kbin.AppendUvarint(dst[:nullableBytesLenAt], uvar32(int32(len(batch))))

			// If our append did not shorten the length prefix, we
			// can just return the prior dst, otherwise we have to
			// shift the batch itself down on newDst.
			if len(newDst) != batchAt {
				dst = append(newDst, batch...)
			}
		}()
	}

	// Below here, we append the actual record batch, which cannot be
	// flexible. Everything encodes properly; flexible adjusting is done in
	// the defer just above.

	dst = kbin.AppendInt64(dst, 0) // firstOffset, defined as zero for producing

	batchLen := nullableBytesLen - 8 - 4 // length of what follows this field (so, minus what came before and ourself)
	batchLenAt := len(dst)               // in case compression adjusting
	dst = kbin.AppendInt32(dst, batchLen)

	dst = kbin.AppendInt32(dst, -1) // partitionLeaderEpoch, unused in clients
	dst = kbin.AppendInt8(dst, 2)   // magic, defined as 2 for records v0.11.0.0+

	crcStart := len(dst)           // fill at end
	dst = kbin.AppendInt32(dst, 0) // reserved crc

	attrsAt := len(dst) // in case compression adjusting
	r.attrs = 0
	if transactional {
		r.attrs |= 0x0010 // bit 5 is the "is transactional" bit
	}
	dst = kbin.AppendInt16(dst, r.attrs)
	dst = kbin.AppendInt32(dst, int32(len(r.records)-1)) // lastOffsetDelta
	dst = kbin.AppendInt64(dst, r.firstTimestamp)

	// maxTimestamp is the timestamp of the last record in a batch.
	lastRecord := r.records[len(r.records)-1]
	dst = kbin.AppendInt64(dst, r.firstTimestamp+int64(lastRecord.timestampDelta))

	seq := r.seq
	if !idempotent {
		producerID = -1
		producerEpoch = -1
		seq = 0
	}
	dst = kbin.AppendInt64(dst, producerID)
	dst = kbin.AppendInt16(dst, producerEpoch)
	dst = kbin.AppendInt32(dst, seq)

	dst = kbin.AppendArrayLen(dst, len(r.records))
	recordsAt := len(dst)
	for i, pnr := range r.records {
		dst = pnr.appendTo(dst, int32(i))
	}

	if compressor != nil {
		toCompress := dst[recordsAt:]
		w := sliceWriters.Get().(*sliceWriter)
		defer sliceWriters.Put(w)

		compressed, codec := compressor.compress(w, toCompress, version)
		if compressed != nil && // nil would be from an error
			len(compressed) < len(toCompress) {

			// our compressed was shorter: copy over
			copy(dst[recordsAt:], compressed)
			dst = dst[:recordsAt+len(compressed)]

			// update the few record batch fields we already wrote
			savings := int32(len(toCompress) - len(compressed))
			nullableBytesLen -= savings
			batchLen -= savings
			r.attrs |= int16(codec)
			if !flexible {
				kbin.AppendInt32(dst[:nullableBytesLenAt], nullableBytesLen)
			}
			kbin.AppendInt32(dst[:batchLenAt], batchLen)
			kbin.AppendInt16(dst[:attrsAt], r.attrs)
		}
	}

	kbin.AppendInt32(dst[:crcStart], int32(crc32.Checksum(dst[crcStart+4:], crc32c)))

	return dst
}

func (pnr promisedNumberedRecord) appendTo(dst []byte, offsetDelta int32) []byte {
	dst = kbin.AppendVarint(dst, pnr.lengthField)
	dst = kbin.AppendInt8(dst, 0) // attributes, currently unused
	dst = kbin.AppendVarint(dst, pnr.timestampDelta)
	dst = kbin.AppendVarint(dst, offsetDelta)
	dst = kbin.AppendVarintBytes(dst, pnr.Key)
	dst = kbin.AppendVarintBytes(dst, pnr.Value)
	dst = kbin.AppendVarint(dst, int32(len(pnr.Headers)))
	for _, h := range pnr.Headers {
		dst = kbin.AppendVarintString(dst, h.Key)
		dst = kbin.AppendVarintBytes(dst, h.Value)
	}
	return dst
}

func (r seqRecBatch) appendToAsMessageSet(dst []byte, version uint8, compressor *compressor) []byte {
	nullableBytesLenAt := len(dst)
	dst = append(dst, 0, 0, 0, 0) // nullable bytes len
	for i, pnr := range r.records {
		dst = appendMessageTo(
			dst,
			version,
			0,
			int64(i),
			r.firstTimestamp+int64(pnr.timestampDelta),
			pnr.Record,
		)
	}

	r.attrs = 0

	// Produce request v0 and v1 uses message set v0, which does not have
	// timestamps. We set bit 8 in our attrs which corresponds with our own
	// kgo.RecordAttrs's bit. The attrs field is unused in a sink / recBuf
	// outside of the appending functions or finishing records; if we use
	// more bits in our internal RecordAttrs, the below will need to
	// change.
	if version == 0 || version == 1 {
		r.attrs |= 0b1000_0000
	}

	if compressor != nil {
		toCompress := dst[nullableBytesLenAt+4:] // skip nullable bytes leading prefix
		w := sliceWriters.Get().(*sliceWriter)
		defer sliceWriters.Put(w)

		compressed, codec := compressor.compress(w, toCompress, int16(version))
		inner := &Record{Value: compressed}
		wrappedLength := messageSet0Length(inner)
		if version == 2 {
			wrappedLength += 8 // timestamp
		}

		if compressed != nil &&
			int(wrappedLength) < len(toCompress) {

			r.attrs |= int16(codec)

			dst = appendMessageTo(
				dst[:nullableBytesLenAt+4],
				version,
				codec,
				int64(len(r.records)-1),
				r.firstTimestamp,
				inner,
			)
		}
	}

	kbin.AppendInt32(dst[:nullableBytesLenAt], int32(len(dst[nullableBytesLenAt+4:])))
	return dst
}

func appendMessageTo(
	dst []byte,
	version uint8,
	attributes int8,
	offset int64,
	timestamp int64,
	r *Record,
) []byte {
	magic := version >> 1
	dst = kbin.AppendInt64(dst, offset)
	msgSizeStart := len(dst)
	dst = append(dst, 0, 0, 0, 0)
	crc32Start := len(dst)
	dst = append(dst, 0, 0, 0, 0)
	dst = append(dst, magic)
	dst = append(dst, byte(attributes))
	if magic == 1 {
		dst = kbin.AppendInt64(dst, timestamp)
	}
	dst = kbin.AppendNullableBytes(dst, r.Key)
	dst = kbin.AppendNullableBytes(dst, r.Value)
	kbin.AppendInt32(dst[:crc32Start], int32(crc32.ChecksumIEEE(dst[crc32Start+4:])))
	kbin.AppendInt32(dst[:msgSizeStart], int32(len(dst[msgSizeStart+4:])))
	return dst
}
