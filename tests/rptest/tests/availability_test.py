# Copyright 2020 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from datetime import datetime
import random
import time

from ducktape.mark.resource import cluster
from rptest.clients.kafka_cli_tools import KafkaCliTools
from rptest.clients.types import TopicSpec
from rptest.services.failure_injector import FailureInjector, FailureSpec
from rptest.services.redpanda import RedpandaService
from rptest.tests.end_to_end import EndToEndTest


class AvailabilityTests(EndToEndTest):
    """
    Basic nodes decommissioning test.
    """
    @cluster(num_nodes=5)
    def test_availability_when_one_node_failed(self):

        # allocate 5 nodes for the cluster
        self.redpanda = RedpandaService(
            self.test_context,
            3,
            KafkaCliTools,
            extra_rp_conf={
                "enable_auto_rebalance_on_node_add": True,
                "group_topic_partitions": 1,
                "default_topic_replications": 3,
            })

        self.redpanda.start()
        spec = TopicSpec(name="test-topic",
                         partition_count=6,
                         replication_factor=3)

        self.redpanda.create_topic(spec)
        self.topic = spec.name

        self.start_producer(1)
        self.start_consumer(1)
        self.await_startup()

        # inject random failure
        f_injector = FailureInjector(self.redpanda)
        f_spec = FailureSpec(random.choice(FailureSpec.FAILURE_TYPES),
                             random.choice(self.redpanda.nodes))

        f_injector.inject_failure(f_spec)

        self.run_validation(min_records=40000,
                            enable_idempotence=False,
                            producer_timeout_sec=60,
                            consumer_timeout_sec=60)

    @cluster(num_nodes=5)
    def test_recovery_after_catastrophic_failure(self):

        # allocate 5 nodes for the cluster
        self.redpanda = RedpandaService(
            self.test_context,
            3,
            KafkaCliTools,
            extra_rp_conf={
                "enable_auto_rebalance_on_node_add": True,
                "group_topic_partitions": 1,
                "default_topic_replications": 3,
            })

        self.redpanda.start()
        spec = TopicSpec(name="test-topic",
                         partition_count=6,
                         replication_factor=3)

        self.redpanda.create_topic(spec)
        self.topic = spec.name

        self.start_producer(1)
        self.start_consumer(1)
        self.await_startup()

        # inject permanent random failure
        f_injector = FailureInjector(self.redpanda)
        f_spec = FailureSpec(random.choice(FailureSpec.FAILURE_TYPES),
                             random.choice(self.redpanda.nodes[0:1]))

        f_injector.inject_failure(f_spec)

        # inject transient (2 seconds) failure on other node
        f_spec = FailureSpec(random.choice(FailureSpec.FAILURE_TYPES),
                             self.redpanda.nodes[2],
                             length=2.0)

        f_injector.inject_failure(f_spec)

        self.run_validation(min_records=40000,
                            enable_idempotence=False,
                            producer_timeout_sec=60,
                            consumer_timeout_sec=60)
