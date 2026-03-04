# External Integrations

**Analysis Date:** 2026-03-04

## Data Lake / Table Formats

**Apache Iceberg:**
- What: Read/write Iceberg tables as external catalogs
- FE connector: `fe/fe-core/src/main/java/com/starrocks/connector/iceberg/`
- JNI reader: `java-extensions/iceberg-metadata-reader/`
- Sub-connectors: Hive Metastore catalog, REST catalog (`iceberg/rest/`), JDBC catalog (`iceberg/jdbc/`), Glue catalog (`iceberg/glue/`), Hadoop catalog (`iceberg/hadoop/`)
- SDK: `iceberg-api`, `iceberg-core`, `iceberg-hive-metastore`, `iceberg-aws` 1.10.0

**Apache Hudi:**
- What: Read Hudi MOR/COW tables as external catalogs
- FE connector: `fe/fe-core/src/main/java/com/starrocks/connector/hudi/`
- JNI reader: `java-extensions/hudi-reader/`
- SDK: `hudi-common`, `hudi-io`, `hudi-hadoop-mr` 1.0.2

**Delta Lake:**
- What: Read Delta Lake tables via Delta Kernel
- FE connector: `fe/fe-core/src/main/java/com/starrocks/connector/delta/`
- SDK: `delta-kernel-api`, `delta-kernel-defaults` 4.0.0rc1

**Apache Paimon:**
- What: Read Paimon tables as external catalogs
- FE connector: `fe/fe-core/src/main/java/com/starrocks/connector/paimon/`
- JNI reader: `java-extensions/paimon-reader/`
- SDK: `paimon-bundle`, `paimon-s3`, `paimon-oss` 1.3.1

## Metastore / Catalog Services

**Apache Hive Metastore (HMS):**
- What: Primary metastore for Hive, Iceberg-Hive, Hudi catalog federation
- FE connector: `fe/fe-core/src/main/java/com/starrocks/connector/hive/`
- Events: `fe/fe-core/src/main/java/com/starrocks/connector/hive/events/` (HMS event listener for metadata sync)
- SDK: `hive-apache` 3.1.2-22, `metastore-client-hive3`
- Config: `hive.metastore.uris` property in catalog DDL

**AWS Glue Data Catalog:**
- What: AWS-managed Hive-compatible metastore
- FE connectors:
  - Iceberg/Glue: `fe/fe-core/src/main/java/com/starrocks/connector/iceberg/glue/`
  - Hive/Glue: `fe/fe-core/src/main/java/com/starrocks/connector/hive/glue/`
- SDK: AWS SDK v2 (`aws-v2-sdk` 2.42.1)
- Config: `aws.glue.region`, `aws.glue.catalog-id` in catalog DDL

**Alibaba DLF (Data Lake Formation):**
- What: Alibaba Cloud managed metastore
- SDK: `dlf-metastore-client` 0.2.14
- Config: DLF endpoint/credentials in catalog DDL

## Object Storage

**Amazon S3 (and S3-compatible):**
- BE implementation: `be/src/fs/fs_s3.cpp`, `be/src/io/s3_input_stream.cpp`, `be/src/io/s3_output_stream.cpp`
- Compatible prefixes (BE config): `s3://`, `s3a://`, `s3n://`, `oss://`, `cos://`, `cosn://`, `obs://`, `ks3://`, `tos://`
- SDK (BE): AWS SDK C++ 1.11.267 (`be/CMakeLists.txt`)
- SDK (FE): `hadoop-aws` 3.4.3, `iceberg-aws` 1.10.0
- FE config keys: `aws_s3_path`, `aws_s3_region`, `aws_s3_endpoint`, `aws_s3_access_key`, `aws_s3_secret_key`, `aws_s3_iam_role_arn`, `aws_s3_use_instance_profile`
- Shared-data (lake) mode requires S3-compatible storage

**Azure Blob Storage / ADLS Gen2:**
- BE implementation: `be/src/fs/azure/fs_azblob.cpp`, `be/src/fs/azure/azblob_uri.cpp`
- FE SDK: `hadoop-azure`, `hadoop-azure-datalake` 3.4.3, `azure-storage-blob`, `azure-identity` (azure SDK 1.2.34)
- FE config keys: `azure_blob_endpoint`, `azure_blob_path`, `azure_blob_shared_key`, `azure_blob_sas_token`, `azure_adls2_endpoint`, `azure_adls2_oauth2_tenant_id`, `azure_adls2_oauth2_client_id`, `azure_adls2_oauth2_client_secret`

**Google Cloud Storage (GCS):**
- FE SDK: `gcs-connector` hadoop3-2.2.26 (shaded)
- BE: GCS connector C++ via thirdparty (`thirdparty/vars-x86_64.sh`: `GCS_CONNECTOR_DOWNLOAD`)
- FE config keys: `gcp_gcs_endpoint`, `gcp_gcs_path`, `gcp_gcs_service_account_email`, `gcp_gcs_service_account_private_key`

**Alibaba OSS:**
- FE SDK: `hadoop-aliyun` 3.4.3
- BE: Configurable via S3-compatible URI (`oss://`)
- Integrated via JindoSDK 4.6.8 (thirdparty, x86_64): provides optimized OSS access

**Tencent COS / Huawei OBS / Kingsoft KS3 / ByteDance TOS:**
- Supported via S3-compatible URI prefixes in BE config `s3_compatible_fs_list`

## HDFS

- What: Distributed filesystem access for data lake tables
- BE implementation: `be/src/fs/hdfs/fs_hdfs.cpp`, `be/src/exec/hdfs_scanner/`
- Transport: JNI via embedded JVM (`hdfs jvm` linked into BE binary)
- FE: `hadoop-hdfs`, `hadoop-hdfs-client` 3.4.3, `java-extensions/hadoop-ext/`
- Auth: Kerberos (KRB5 1.19.4, Cyrus SASL 2.1.28 in BE thirdparty)
- BE config: `max_hdfs_file_handle`, `hdfs_client_enable_hedged_read`, `hdfs_client_max_cache_size`

## Streaming / Message Queues

**Apache Kafka:**
- What: Routine Load streaming ingestion from Kafka topics
- BE: `be/src/runtime/routine_load/` (consumer pipe and task executor)
- C++ client: librdkafka 2.11.0
- FE client: `kafka-clients` 3.9.1
- Config: `routine_load_kafka_timeout_second` (FE), `max_pulsar_consumer_num_per_group` (BE)
- Auth: SASL/SCRAM supported via Cyrus SASL

**Apache Pulsar:**
- What: Routine Load streaming ingestion from Pulsar topics
- BE: `be/src/service/internal_service.h` (Pulsar consumer referenced)
- C++ client: Pulsar client C++ 3.3.0
- Config: `max_pulsar_consumer_num_per_group`, `routine_load_pulsar_timeout_second` (BE config)

## JDBC External Tables

**Connector:** `fe/fe-core/src/main/java/com/starrocks/connector/jdbc/`
**BE connector:** `be/src/connector/jdbc_connector.cpp`
**JNI bridge:** `java-extensions/jdbc-bridge/`
**Connection pool:** HikariCP 3.4.5

**Supported databases (via JDBC drivers):**
- MySQL: `mariadb-java-client` driver
- PostgreSQL: `postgresql` driver
- Oracle: `ojdbc10` + `orai18n`
- Microsoft SQL Server: `mssql-jdbc`
- ClickHouse: `clickhouse-jdbc`
- Schema resolvers: `fe/fe-core/src/main/java/com/starrocks/connector/jdbc/` (`MysqlSchemaResolver`, `PostgresSchemaResolver`, `OracleSchemaResolver`, `SqlServerSchemaResolver`, `ClickhouseSchemaResolver`)

**BE MySQL direct connector** (MySQL external tables without JDBC):
- `be/src/connector/mysql_connector.cpp`
- Uses `mariadb-connector-c` 3.1.14 (C client)

## Authentication & Identity

**Mechanism:** Multi-provider, pluggable via `AuthenticationProvider` SPI
**SPI:** `fe/fe-spi/src/main/java/com/starrocks/authentication/AuthenticationProvider.java`
**Factory:** `fe/fe-core/src/main/java/com/starrocks/authentication/AuthenticationProviderFactory.java`

**Native (password):**
- `PlainPasswordAuthenticationProvider.java` - username/password hashed with SHA-256
- MySQL protocol authentication (mysql wire protocol on port 9030)

**LDAP:**
- `LDAPAuthProvider.java`, `SimpleLDAPSecurityIntegration.java`
- Config: `authentication_ldap_simple_server_host`, `authentication_ldap_simple_server_port`, `authentication_ldap_simple_bind_base_dn`, `authentication_ldap_simple_user_search_attr`
- SSL support: `LdapSslSocketFactory.java`, trust store configurable

**OAuth2 / OIDC:**
- `OAuth2AuthenticationProvider.java`, `OAuth2SecurityIntegration.java`, `OpenIdConnectVerifier.java`
- JWK key management: `JwkMgr.java`
- JWT auth: `JWTAuthenticationProvider.java`, `JWTSecurityIntegration.java`
- Config: `oauth2_auth_server_url`, `oauth2_token_server_url`, `oauth2_client_id`, `oauth2_client_secret`, `oauth2_redirect_url`, `oauth2_jwks_url`
- JWT library: nimbus-jose-jwt 9.37.2

**Kerberos:**
- Supported via `KRB5` and Cyrus SASL in C++ thirdparty (primarily for HDFS/Kafka auth)

## Authorization

**Built-in RBAC:**
- Custom role-based access control in `fe/fe-core/src/main/java/com/starrocks/authorization/`
- Privilege model: databases, tables, views, functions, resources, warehouses

**Apache Ranger:**
- `fe/fe-core/src/main/java/com/starrocks/authorization/ranger/`
- Implementations: `RangerHiveAccessController.java`, `RangerStarRocksAccessController.java`
- Config: `ranger_user_ugi` toggle for UGI-based group resolution
- SDK: `ranger-plugins-common`

## Tracing / Observability

**OpenTelemetry (FE):**
- SDK: `opentelemetry-api`, `opentelemetry-sdk`, `opentelemetry-exporter-jaeger`, `opentelemetry-exporter-otlp`
- Implementation: `fe/fe-core/src/main/java/com/starrocks/common/TraceManager.java`
- Service name: `starrocks-fe`
- Config: `jaeger_grpc_endpoint` (enables Jaeger export)
- Supports OTLP gRPC export as alternative

**Jaeger (BE):**
- Config: `jaeger_endpoint = localhost:6831` in `conf/be.conf` (UDP agent endpoint)

**Metrics (FE):**
- Internal Prometheus-compatible metrics at `http://fe-host:8030/metrics`
- Java implementation: `fe/fe-core/src/main/java/com/starrocks/metric/MetricRepo.java`
- Uses Dropwizard Metrics (`codahale/metrics`) for histograms

**Metrics (BE):**
- HTTP metrics endpoint: `be/src/http/action/metrics_action.cpp`
- Prometheus-compatible scrape endpoint at `http://be-host:8040/metrics`

**Profiling:**
- BE: Breakpad 2024.02.16 for crash dump (`be/src/common/util/minidump.h`)
- BE: pprof endpoint for CPU profiling (`be/src/http/action/pprof_actions.h`)
- FE: Async-profiler 4.0 integration

## Spark Integration

**Spark DPP (Data Pre-Processing):**
- Used for bulk load data preparation
- Module: `fe/plugin/spark-dpp/`
- Spark versions: 3.5.5 (default in Gradle build)
- SDK: `spark-core_2.12`, `spark-sql_2.12`, `spark-catalyst_2.12`, `spark-launcher_2.12`

**Spark Connector:**
- External component (separate repo); FE provides StarRocks External Service API
- Thrift service: `gensrc/thrift/StarrocksExternalService.thrift`

## Alibaba MaxCompute (ODPS)

- What: Read ODPS/MaxCompute tables as external catalogs
- FE connector: `fe/fe-core/src/main/java/com/starrocks/connector/odps/`
- JNI reader: `java-extensions/odps-reader/`
- SDK: `odps-sdk-core`, `odps-sdk-table-api` 0.48.7-public

## Apache Kudu

- What: Read Kudu tables as external catalogs
- FE connector: `fe/fe-core/src/main/java/com/starrocks/connector/kudu/`
- JNI reader: `java-extensions/kudu-reader/`
- SDK: `kudu-client` 1.17.1

## Elasticsearch

- What: Read Elasticsearch indices as external tables
- BE connector: `be/src/connector/es_connector.cpp`
- BE scanner: `be/src/exec/es/` (scroll API, predicate push-down)
- Transport: HTTP via libcurl from BE
- FE connector: `fe/fe-core/src/main/java/com/starrocks/connector/elasticsearch/`

## StarOS / Shared-Data (Lake Mode)

- What: Proprietary StarOS tablet management service for shared-data deployments
- FE integration: `fe/fe-core/src/main/java/com/starrocks/staros/`
- BE integration: `be/src/service/staros_worker.cpp`
- Storage: `starcache` (binary thirdparty artifact) - local disk cache for shared-data
- SDK (FE): `starclient`, `starmanager` (from `staros` artifact group)
- Config: `USE_STAROS=ON` CMake option to enable shared-data BE build

## Arrow Flight SQL

- What: High-performance data transfer protocol (ADBC-compatible)
- BE C++: `arrow_flight`, `arrow_flight_sql` (Apache Arrow 19.0.1)
- FE Java: `flight-core`, `flight-sql`, `flight-sql-jdbc-driver`, `arrow-jdbc` (Arrow 18.0.0)
- Transport: gRPC (`grpc-netty-shaded` 1.63.0)

## CI/CD & Deployment

**Hosting:**
- Self-managed Linux deployment (no managed cloud service target)
- Docker: `starrocks/dev-env-ubuntu:latest` dev image, `docker/dockerfiles/` for production images

**CI Pipeline:**
- GitHub Actions (`.github/` workflows not inspected but referenced in AGENTS.md)
- Checks: PR CHECKER, FE UT, BE UT, Build, Checkstyle, Clang-format
- Code coverage: JaCoCo (FE), gcov (BE, `WITH_GCOV=ON`)
- SonarCloud: `sonar.organization=starrocks`, `sonar.host.url=https://sonarcloud.io` (in `fe/pom.xml`)

## Load Ingestion Endpoints

**Stream Load (HTTP):**
- FE: `fe/fe-core/src/main/java/com/starrocks/http/rest/LoadAction.java`
- BE: `be/src/http/action/transaction_stream_load.cpp`
- Protocol: HTTP PUT to `http://fe:8030/api/{db}/{table}/_stream_load`

**Broker Load:**
- FE orchestrates; BE pulls from external storage via file brokers
- Broker service: `gensrc/thrift/FileBrokerService.thrift`

**Spark Load:**
- FE orchestrates via `StarrocksExternalService.thrift`

## Webhooks & Callbacks

**Incoming:**
- MySQL wire protocol on port 9030 (FE query endpoint)
- HTTP REST API on FE port 8030 (Stream Load, admin APIs, metrics)
- HTTP REST API on BE port 8040 (tablet management, metrics, compaction)
- BRPC on BE port 8060 (query execution RPCs)
- Thrift RPC on BE port 9060 (agent tasks)
- Heartbeat on port 9050 (FE→BE health checks)

**Outgoing:**
- FE polls HMS for metadata changes (via HMS event listener)
- FE contacts Kafka brokers to fetch offsets for routine load management
- BE contacts Kafka brokers and Pulsar brokers for routine load consumption
- FE sends traces to Jaeger via gRPC (`jaeger_grpc_endpoint`)

## Environment Configuration

**Key FE config parameters (in `conf/fe.conf` or `Config.java`):**
- `aws_s3_access_key`, `aws_s3_secret_key`, `aws_s3_region`, `aws_s3_endpoint`
- `azure_blob_shared_key`, `azure_blob_sas_token`, `azure_adls2_oauth2_client_secret`
- `authentication_ldap_simple_server_host`, `authentication_ldap_simple_bind_root_pwd`
- `oauth2_client_id`, `oauth2_client_secret`, `oauth2_jwks_url`
- `jaeger_grpc_endpoint`

**Key BE config parameters (in `conf/be.conf` or `config.h`):**
- `aws_sdk_logging_trace_enabled`, `s3_compatible_fs_list`
- `max_hdfs_file_handle`, `hdfs_client_max_cache_size`
- `max_pulsar_consumer_num_per_group`, `routine_load_pulsar_timeout_second`
- `jaeger_endpoint`

**Secrets location:**
- Config files (`conf/fe.conf`, `conf/be.conf`) for self-managed deployments
- No secrets management system detected (no Vault, AWS Secrets Manager integration found)

---

*Integration audit: 2026-03-04*
