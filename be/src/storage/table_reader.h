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

#include <utility>
#include <vector>

#include "gen_cpp/Descriptors_types.h"
#include "storage/chunk_iterator.h"
#include "storage/column_predicate.h"
#include "storage/tablet.h"

namespace doris {
class PBackendService_Stub;
}

namespace starrocks {

class OlapTablePartitionParam;
class OlapTableLocationParam;
class StarRocksNodesInfo;
class LocalTabletReader;

namespace serde {
class ProtobufChunkMeta;
}

// Parameters used to create a TableReader supporting only single local tablet access
struct LocalTableReaderParams {
    int64_t tablet_id;
    int64_t version;
};

// Parameters used to create a TableReader supporting remote full table access
struct TableReaderParams {
    // table schema
    TOlapTableSchemaParam schema;
    // table and partition info, used to find the tablet that a key belongs to
    TOlapTablePartitionParam partition_param;
    // tablet id -> { node id list }, used to find BE nodes that a tablet locates on
    TOlapTableLocationParam location_param;
    // node id -> host address map, used to find the RPC port of BE nodes
    TNodesInfo nodes_info;
    std::map<int64_t, int64_t> partition_versions;
    int64_t timeout_ms{-1};
};

// Table reader provides storage interfaces for multi_get and scan. It can read from local
// or remote tablets according to the location of the data.
class TableReader {
public:
    TableReader();
    ~TableReader();

    Status init(const LocalTableReaderParams& local_params);

    Status init(const TableReaderParams& params);

    /**
     * Batch get of multi-rows by multiple keys
     * @param keys input, keys of the rows to read, the Chunk storing keys must has all the primary key columns with the
     *                    same order as the schema
     * @param value_columns input, names of the columns to read
     * @param found output, same size as keys, each element is true if the corresponding row is found
     * @param values output, a chunk with columns in the same order as `value_columns`, and append the column values of
     *                    each founded row to corresponding column
     * @return Status::OK() if no error, otherwise return error status
     * @note not thread-safe, concurrent calls not supported
     * Example:
     *     table schema:
     *         k1 int primary key, v1 int, v2 int, v3 int
     *     table data:
     *         k1 | v1 | v2 | v3
     *         -----------------
     *         1  | 1  | 1  | 1
     *         3  | 3  | 3  | 3
     *         5  | 5  | 5  | 5
     *     multi_get([3, 4, 5], [v1, v2]] will get:
     *     status: ok
     *     found: [true, false, true]
     *     values:
     *         v1 | v2
     *         ---------
     *         3  | 3
     *         5  | 5
     */
    Status multi_get(Chunk& keys, const std::vector<std::string>& value_columns, std::vector<bool>& found,
                     Chunk& values);

    /**
     * Scan the table, return the rows that match the predicates
     * @param value_columns the columns to read
     * @param predicates the predicates to match, only simple predicates are supported(e.g. >,<,=,in)
     *                   contents of predicates must remain valid when using the returned ChunkIteratorPtr
     * @return A ChunkIterator which can be used to iterate over the rows of the table satisfying the predicates, or
     *         error status
     * @note not thread-safe, concurrent calls not supported
     * @note specifying ordering is not supported, user cannot assume the order of the returned rows,
     *       its complex/inefficient to merge data from multiple remote sources and maintain some ordering requirements,
     *       it's better to let execution engine to do the ordering(rather then storage engine)
     */
    StatusOr<ChunkIteratorPtr> scan(const std::vector<std::string>& value_columns,
                                    const std::vector<const ColumnPredicate*>& predicates);

private:
    Status _tablet_multi_get(int64_t tablet_id, int64_t version, Chunk& keys,
                             const std::vector<std::string>& value_columns, std::vector<bool>& found, Chunk& values,
                             SchemaPtr& value_schema);

    Status _tablet_multi_get_remote(int64_t tablet_id, int64_t version, Chunk& keys,
                                    const std::vector<std::string>& value_columns, std::vector<bool>& found,
                                    Chunk& values, SchemaPtr& value_schema);

    Status _tablet_multi_get_rpc(doris::PBackendService_Stub* stub, int64_t tablet_id, int64_t version, Chunk& keys,
                                 const std::vector<std::string>& value_columns, std::vector<bool>& found, Chunk& values,
                                 SchemaPtr& value_schema);
    // fields for local tablet reader
    std::unique_ptr<LocalTableReaderParams> _local_params;
    std::unique_ptr<LocalTabletReader> _local_tablet_reader;

    // fields for remote tablet reader
    std::unique_ptr<TableReaderParams> _params;
    std::shared_ptr<OlapTableSchemaParam> _schema_param;
    std::unique_ptr<OlapTablePartitionParam> _partition_param;
    std::unique_ptr<OlapTableLocationParam> _location_param;
    std::unique_ptr<StarRocksNodesInfo> _nodes_info;
    std::unique_ptr<RowDescriptor> _row_desc;
};

} // namespace starrocks