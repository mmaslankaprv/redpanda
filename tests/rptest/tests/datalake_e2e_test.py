# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.services.cluster import cluster
from rptest.services.kgo_verifier_services import KgoVerifierProducer
from rptest.services.polaris_catalog import PolarisCatalog

from ducktape.utils.util import wait_until
from rptest.clients.types import TopicSpec
from rptest.services.redpanda import SISettings
from rptest.tests.polaris_catalog_test import PolarisCatalogTest
from polaris.management.api.polaris_default_api import PolarisDefaultApi
from polaris.management.models.create_catalog_request import CreateCatalogRequest
from polaris.management.models.catalog_properties import CatalogProperties
from polaris.management.models.create_principal_request import CreatePrincipalRequest
from polaris.management.models.catalog import Catalog
from polaris.management.models.storage_config_info import StorageConfigInfo
from polaris.management.models.grant_principal_role_request import GrantPrincipalRoleRequest
from polaris.management.models.create_principal_role_request import CreatePrincipalRoleRequest
from polaris.management.models.create_catalog_role_request import CreateCatalogRoleRequest
from polaris.management.models.grant_catalog_role_request import GrantCatalogRoleRequest
from polaris.management.models.catalog_role import CatalogRole
from polaris.management.models.catalog_grant import CatalogGrant
from polaris.management.models.add_grant_request import AddGrantRequest
from polaris.management.models.principal_role import PrincipalRole
from polaris.management.models.catalog_privilege import CatalogPrivilege
from polaris.management.models.principal import Principal
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, IntegerType, StringType, LongType, TimestampType


class DatalakeEndToEndTest(PolarisCatalogTest):
    def __init__(self, test_context):
        self._topic = None

        super(DatalakeEndToEndTest, self).__init__(
            test_ctx=test_context,
            num_brokers=3,
            si_settings=SISettings(test_context,
                                   cloud_storage_enable_remote_read=False,
                                   cloud_storage_enable_remote_write=False),
            extra_rp_conf={
                "iceberg_enabled": True,
            })

    @property
    def msg_size(self):
        return 4096

    @property
    def msg_count(self):
        return int(100 if self.debug_mode else 60 * self.producer_throughput /
                   self.msg_size)

    @property
    def producer_throughput(self):
        return 1024 if self.debug_mode else 50 * 1024 * 1024

    def _setup_polaris_catalog(self,
                               catalog_name="redpanda-catalog",
                               principal_name="panda",
                               principal_role_name="panda-role",
                               catalog_role_name="panda-catalog-role"):
        polaris_api = PolarisDefaultApi(self.polaris.management_client())

        catalog = Catalog(
            type="INTERNAL",
            name=catalog_name,
            properties=CatalogProperties(
                default_base_location=
                f"file://{PolarisCatalog.PERSISTENT_ROOT}/catalog_data",
                additional_properties={}),
            storageConfigInfo=StorageConfigInfo(storageType="FILE"))

        # create a catalog
        polaris_api.create_catalog(CreateCatalogRequest(catalog=catalog))

        # create principal and grant the catalog role
        r = polaris_api.create_principal(
            CreatePrincipalRequest(principal=Principal(name=principal_name)))
        credentials = r.credentials

        polaris_api.create_principal_role(
            CreatePrincipalRoleRequest(principalRole=PrincipalRole(
                name=principal_role_name)))

        polaris_api.create_catalog_role(
            catalog_name=catalog_name,
            create_catalog_role_request=CreateCatalogRoleRequest(
                catalogRole=CatalogRole(name=catalog_role_name)))
        for p in [
                CatalogPrivilege.CATALOG_MANAGE_ACCESS,
                CatalogPrivilege.CATALOG_MANAGE_CONTENT,
                CatalogPrivilege.CATALOG_MANAGE_METADATA,
                CatalogPrivilege.NAMESPACE_CREATE,
        ]:
            polaris_api.add_grant_to_catalog_role(
                catalog_name=catalog_name,
                catalog_role_name=catalog_role_name,
                add_grant_request=AddGrantRequest(
                    grant=CatalogGrant(type='catalog', privilege=p)))
        polaris_api.assign_principal_role(
            principal_name=principal_name,
            grant_principal_role_request=GrantPrincipalRoleRequest(
                principalRole=PrincipalRole(name=principal_role_name)))

        polaris_api.assign_catalog_role_to_principal_role(
            catalog_name=catalog_name,
            principal_role_name=principal_role_name,
            grant_catalog_role_request=GrantCatalogRoleRequest(
                catalogRole=CatalogRole(name=catalog_role_name)))

        return credentials

    def start_producer(self, topic_name: str):
        self.logger.info(
            f"starting kgo-verifier producer with {self.msg_count} messages of size {self.msg_size} and throughput: {self.producer_throughput} bps"
        )
        producer = KgoVerifierProducer(self.test_context,
                                       self.redpanda,
                                       topic_name,
                                       self.msg_size,
                                       self.msg_count,
                                       rate_limit_bps=self.producer_throughput,
                                       debug_logs=True)

        producer.start(clean=False)

        wait_until(lambda: producer.produce_status.acked > 10,
                   timeout_sec=120,
                   backoff_sec=1)

        return producer

    def _redpanda_schemaless_schema(self):
        return Schema(
            NestedField(field_id=1,
                        name='redpanda_offset',
                        field_type=LongType(),
                        required=True),
            NestedField(field_id=2,
                        name='redpanda_timestamp',
                        field_type=TimestampType(),
                        required=True),
            NestedField(field_id=3,
                        name='redpanda_key',
                        field_type=StringType(),
                        required=False),
            NestedField(field_id=3,
                        name='redpanda_value',
                        field_type=StringType(),
                        required=False),
        )

    @cluster(num_nodes=5)
    def test_schemaless_datalake_topic(self):
        topic = TopicSpec(name='datalake-test-topic', partition_count=3)
        catalog_name = "redpanda-catalog"
        catalog_credentials = self._setup_polaris_catalog(
            catalog_name=catalog_name)
        py_catalog = load_catalog(
            catalog_name, **{
                "uri": f"{self.polaris.catalog_url}",
                'type': "rest",
                'scope': "PRINCIPAL_ROLE:ALL",
                "credential":
                f"{catalog_credentials.client_id}:{catalog_credentials.client_secret}",
                "warehouse": catalog_name,
            })

        py_catalog.create_namespace_if_not_exists("test_namespace")
        table = py_catalog.create_table(
            identifier="test_namespace.test_table",
            schema=self._redpanda_schemaless_schema(),
        )

        self.client().create_topic(topic)
        self.client().alter_topic_config(topic.name,
                                         "redpanda.iceberg.enabled", "true")

        producer = self.start_producer(topic_name=topic.name)
        # wait for the producer to finish
        producer.wait()
