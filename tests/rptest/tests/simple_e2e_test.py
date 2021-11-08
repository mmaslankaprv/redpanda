import random
import time

from ducktape.mark.resource import cluster
from ducktape.utils.util import wait_until
import requests
from rptest.clients.kafka_cat import KafkaCat

from rptest.clients.types import TopicSpec
from rptest.services.admin import Admin
from rptest.tests.end_to_end import EndToEndTest


class SimpleEndToEndTest(EndToEndTest):
    @cluster(num_nodes=6)
    def test_correctness_while_evicitng_log(self):
        '''
        Validate that all the records will be delivered to consumers when there 
        are multiple producers and log is evicted
        '''
        # use small segment size to enable log eviction
        self.start_redpanda(num_nodes=3,
                            extra_rp_conf={
                                "log_segment_size": 1048576,
                                "retention_bytes": 5242880,
                                "default_topic_replications": 3,
                            })

        spec = TopicSpec(name="topic", partition_count=1, replication_factor=1)
        self.redpanda.create_topic(spec)
        self.topic = spec.name

        self.start_producer(2, throughput=10000)
        self.start_consumer(1)
        self.await_startup()

        self.run_validation(min_records=100000,
                            producer_timeout_sec=300,
                            consumer_timeout_sec=300)

    @cluster(num_nodes=6)
    def test_leadership_transfer(self):
        '''
        Validate that all the records will be delivered to consumers when there 
        are multiple producers and log is evicted
        '''
        # use small segment size to enable log eviction
        self.start_redpanda(num_nodes=3,
                            extra_rp_conf={
                                "log_segment_size": 10485760,
                                "retention_bytes": 52428800,
                                "default_topic_replications": 3,
                            })

        partitions = 6
        spec = TopicSpec(name="topic",
                         partition_count=partitions,
                         replication_factor=3)
        self.redpanda.create_topic(spec)
        self.topic = spec.name

        self.start_producer(2, throughput=10000)
        self.start_consumer(1)
        self.await_startup()

        def _get_leader_and_replicas(p_id):
            return KafkaCat(self.redpanda).get_partition_leader(
                self.topic, p_id)

        def _wait_for_leader(p_id, expected=None):
            def predicate():
                leader, _ = _get_leader_and_replicas(p_id)
                if expected is not None:
                    return leader == expected
                else:
                    return leader != -1

            wait_until(lambda: predicate, timeout_sec=5)

            return _get_leader_and_replicas(p_id)

        transfers = 100
        admin = Admin(self.redpanda)

        for i in range(0, transfers):
            to_move = random.randint(0, partitions - 1)
            current_leader, replicas = _wait_for_leader(to_move)
            target = current_leader
            while current_leader == target:
                target = random.choice(replicas)

            self.redpanda.logger.info(
                f'starting {i} transfer, partition: {to_move}, from: {current_leader}, to: {target}'
            )

            admin.partition_transfer_leadership("kafka", self.topic, to_move,
                                                target)

            _wait_for_leader(to_move, target)

        self.run_validation(min_records=100000,
                            producer_timeout_sec=300,
                            consumer_timeout_sec=300)
