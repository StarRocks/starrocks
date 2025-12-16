// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>

#include "common/statusor.h"
#include "gen_cpp/FrontendService_types.h"
#include "storage/lake/tablet_metadata.h"
#include "storage/lake/types_fwd.h"
#include "util/bthreads/single_flight.h"

namespace starrocks::lake {

class TabletManager;

/**
 * @brief Service for managing and retrieving table schemas in the shared-data mode.
 * 
 * This service handles schema retrieval requests for both LOAD and SCAN operations, supporting
 * Fast Schema Evolution scenarios where schema changes don't propagate schema metas to backends.
 *
 * Core capabilities:
 * 1. Two-level Local Cache:
 *    - Schema cached in tablet manager.
 *    - Tablet Metadata Lookup: Checks the in-memory metadata of the specific tablet.
 *
 * 2. Remote Fetch via Frontend (FE):
 *    - If a schema is missing locally, it fetches explicitly from the FE via RPC.
 *
 * 3. Request Deduplication (SingleFlight):
 *    - Uses a SingleFlight mechanism to merge concurrent requests for the same schema, reducing pressure on the FE.
 *
 * Terminology & Relationships:
 *
 * 1. Table Schema (Logical Concept):
 *    Represents the schema of a table in the catalog. As schema evolves (Schema Evolution),
 *    a table generates multiple schema versions, each identified by a unique `schema_id`.
 *
 * 2. @ref starrocks::TabletSchema (In-Memory Representation):
 *    Despite the name "Tablet"Schema, this C++ class represents a specific VERSION of a
 *    Table Schema.
 *    - In Shared-Data mode, we maintain a "Schema Cache" (schema_id -> TabletSchemaPtr)
 *      via @ref starrocks::lake::TabletManager::cache_schema() to allow thousands of tablets
 *      belonging to the same table to share the same underlying TabletSchema object, saving memory.
 *
 * 3. TabletMetadataPB::schema (Persistence/Metadata):
 *    A field in the tablet's metadata recording the "latest" schema version known to this tablet.
 *    - After Fast Schema Evolution, new imports use the new schema to generate data.
 *      This field is updated during publish, while historical versions might be referenced in
 *      `TabletMetadataPB::historical_schemas`.
 */
class TableSchemaService {
public:
    TableSchemaService(TabletManager* tablet_mgr) : _tablet_mgr(tablet_mgr) {}
    ~TableSchemaService() = default;

    /**
     * @brief Retrieves the table schema for a LOAD operation.
     * 
     * @param schema_key Basic information to identify the schema (db_id/table_id/schema_id).
     * @param tablet_id The tablet that sends the request.
     * @param txn_id The transaction ID associated with the load.
     * @param tablet_meta Optional pointer to tablet metadata for local lookup.
     * @return A shared pointer to the TabletSchema on success, or an error status.
     */
    StatusOr<TabletSchemaPtr> get_schema_for_load(const TableSchemaKeyPB& schema_key, int64_t tablet_id, int64_t txn_id,
                                                  const TabletMetadataPtr& tablet_meta = nullptr);

    /**
     * @brief Retrieves the table schema for a SCAN operation.
     *
     * @param schema_key Identifiers for the target schema (db_id/table_id/schema_id).
     * @param tablet_id The tablet that sends the request.
     * @param query_id Query ID for the scan.
     * @param coordinator_fe Address of the coordinator FE.
     * @param tablet_meta Optional pointer to tablet metadata for local lookup.
     * @return A shared pointer to the TabletSchema on success, or an error status.
     */
    StatusOr<TabletSchemaPtr> get_schema_for_scan(const TableSchemaKeyPB& schema_key, int64_t tablet_id,
                                                  const TUniqueId& query_id, const TNetworkAddress& coordinator_fe,
                                                  const TabletMetadataPtr& tablet_meta = nullptr);

private:
    /**
     * @brief Context of the actual RPC executed by the SingleFlight leader.
     *
     * When multiple callers request the same schema, one "leader" performs the RPC.
     * This struct records the context (Query ID or Txn ID) of that leader request.
     * It allows followers to determine if the result is valid for their context or if they
     * need to retry (e.g., for stricter isolation).
     */
    struct SingleFlightExecutionContext {
        TNetworkAddress target_fe;
        TTableSchemaRequestSource::type request_source;
        // valid if request_source is SCAN
        TUniqueId query_id;
        // valid if request_source is LOAD
        int64_t txn_id;

        std::string to_string() const;
    };

    /**
     * @brief Result of a SingleFlight execution, shared among all waiters.
     */
    struct SingleFlightResult {
        SingleFlightExecutionContext execution_ctx;
        StatusOr<TabletSchemaPtr> rpc_result;
    };
    using SingleFlightResultPtr = std::shared_ptr<const SingleFlightResult>;

    /**
     * @brief Grouping strategies for SingleFlight.
     */
    enum class GroupStrategy {
        SCHEMA_AND_FE,    ///< Group by Schema ID and Frontend.
        SCHEMA_AND_QUERY, ///< Group by Schema ID and Query ID.
        SCHEMA_AND_TXN    ///< Group by Schema ID and Transaction ID.
    };

    /**
     * @brief Attempts to retrieve the schema from local sources.
     * 
     * Checks the schema cache and the provided tablet metadata (if any).
     * 
     * @param schema_id The ID of the schema to retrieve.
     * @param tablet_meta Optional pointer to tablet metadata.
     * @return The TabletSchema if found locally, otherwise nullptr.
     */
    TabletSchemaPtr _get_local_schema(int64_t schema_id, const TabletMetadataPtr& tablet_meta = nullptr);

    /**
     * @brief Retrieves the schema from a remote Frontend via RPC.
     * 
     * Uses SingleFlight to merge duplicate requests and handles retries.
     * 
     * @param request The Thrift request to send.
     * @param fe The address of the target Frontend.
     * @return The retrieved TabletSchema or an error status.
     */
    StatusOr<TabletSchemaPtr> _get_remote_schema(const TGetTableSchemaRequest& request, const TNetworkAddress& fe);

    /**
     * @brief Executes the actual RPC call to the Frontend.
     * 
     * This method is called by the SingleFlight mechanism.
     * 
     * @param request The Thrift request to send.
     * @param fe The address of the target Frontend.
     * @return A SingleFlightResultPtr containing the response and execution context.
     */
    SingleFlightResultPtr _fetch_schema_via_rpc(const TGetTableSchemaRequest& request, const TNetworkAddress& fe);

    /**
     * @brief Generates a unique string key for SingleFlight grouping.
     * 
     * @param strategy The grouping strategy to use.
     * @param request The request containing schema and ID information.
     * @param fe The target Frontend address.
     * @return A string key uniquely identifying the request group.
     */
    std::string _group_key(GroupStrategy strategy, const TGetTableSchemaRequest& request, const TNetworkAddress& fe);

    /**
     * @brief Formats the request information for logging.
     * 
     * @param request The Thrift request containing schema info.
     * @return A string containing key fields from the request.
     */
    static std::string _print_request_info(const TGetTableSchemaRequest& request);

    /**
     * @brief LOAD-only compatibility fallback to read schema from schema file.
     *
     * Triggered when FE doesn't support getTableSchema (NotSupported). This is intentionally LOAD-only.
     *
     * @param schema_id The ID of the schema to retrieve.
     * @param tablet_id The ID of the tablet to search in.
     * @return The TabletSchema if found, otherwise an error status.
     */
    StatusOr<TabletSchemaPtr> _fallback_load_to_schema_file(int64_t schema_id, int64_t tablet_id);

    using SingleFlightGroup = bthreads::singleflight::Group<std::string, SingleFlightResultPtr>;
    static constexpr size_t kSingleFlightGroupShards = 128;
    static_assert((kSingleFlightGroupShards & (kSingleFlightGroupShards - 1)) == 0,
                  "kSingleFlightGroupShards must be power of two");

    SingleFlightGroup& _select_single_flight_group(const std::string& group_key) {
        const size_t h = std::hash<std::string>{}(group_key);
        return _single_flight_groups[h & (kSingleFlightGroupShards - 1)];
    }

    TabletManager* _tablet_mgr;
    std::array<SingleFlightGroup, kSingleFlightGroupShards> _single_flight_groups;
};

} // namespace starrocks::lake