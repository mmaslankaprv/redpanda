// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "bytes/details/io_iterator_consumer.h"
#include "bytes/iobuf_parser.h"
#include "bytes/iostream.h"
#include "kafka/protocol/batch_consumer.h"
#include "kafka/protocol/batch_reader.h"
#include "kafka/protocol/exceptions.h"
#include "kafka/protocol/kafka_batch_adapter.h"
#include "kafka/protocol/produce.h"
#include "kafka/protocol/request_reader.h"
#include "kafka/protocol/types.h"
#include "model/fundamental.h"
#include "model/tests/random_batch.h"
#include "redpanda/tests/fixture.h"

#include <seastar/core/circular_buffer.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/sstring.hh>

#include <boost/test/tools/old/interface.hpp>

#include <iterator>
#include <type_traits>
constexpr auto request_header_size = sizeof(int16_t) + sizeof(int16_t)
                                     + sizeof(kafka::correlation_id::type)
                                     + sizeof(int16_t);

SEASTAR_THREAD_TEST_CASE(batch_reader_last_offset) {
    auto file = ss::open_file_dma(
                  "/home/mmaslanka/dev/malformed.packet", ss::open_flags::ro)
                  .get();
    auto sz = file.size().get();
    auto deffered = ss::defer([file]() mutable { file.close().get(); });
    auto stream = ss::make_file_input_stream(file);

    auto r_sz = *(kafka::parse_size(stream).get());
    auto r_hdr = kafka::parse_header(stream).get();
    fmt::print("request_sz: {}, request_hdr: {}\n", r_sz, r_hdr);
    auto remaining = r_sz - request_header_size - r_hdr->client_id_buffer.size()
                     - r_hdr->tags_size_bytes;

    auto data_buffer = read_iobuf_exactly(stream, remaining).get();
    kafka::produce_request request;
    auto req_reader = kafka::request_reader(std::move(data_buffer));
    request.decode(req_reader, r_hdr->version);
    fmt::print("request: {}", request);
    // auto buffer = read_iobuf_exactly(stream, sz).get();
    // fmt::print("buffer_size: {}\n", buffer.size_bytes());

    // kafka::kafka_batch_adapter adapter;
    // adapter.adapt_with_version(std::move(buffer), kafka::api_version(7));

    // fmt::print("header: {}\n", adapter.batch->header());
}
