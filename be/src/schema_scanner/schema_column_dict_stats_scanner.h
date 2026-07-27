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

#include <cstdint>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include "exec/schema_scanner.h"
#include "gen_cpp/FrontendService_types.h"

namespace starrocks {

class Tablet;
class TabletSchema;
class ColumnMetaPB;
using TabletSharedPtr = std::shared_ptr<Tablet>;

// information_schema.column_dict_stats
//
// E4 (column-level shared ZSTD dictionary) observability. Produces one row per
// (tablet, segment, column / flat-JSON sub-column). For every matching tablet
// the scanner opens each segment footer and reads the per-column ColumnMetaPB
// to surface the encoding, compression and the E4 shared-dictionary state
// (ColumnMetaPB.shared_dict_page, field 35).
//
// Because opening one footer per segment is expensive, the scanner REQUIRES a
// scope predicate: either a TABLE_NAME (optionally with TABLE_SCHEMA) equality
// filter, or a TABLET_ID equality filter. A full-database scan is rejected in
// start().
//
// When a segment footer cannot be read (shared-data / lake tablets, primary-key
// tablets whose applied rowsets are not exposed via the version tracker, or
// encrypted / bundled segments), the scanner degrades to one row per top-level
// tablet-schema column with the footer-derived columns emitted as NULL, so that
// no subtly-wrong value is ever surfaced.
class SchemaColumnDictStatsScanner : public SchemaScanner {
public:
    SchemaColumnDictStatsScanner();
    ~SchemaColumnDictStatsScanner() override;

    Status start(RuntimeState* state) override;
    Status get_next(ChunkPtr* chunk, bool* eos) override;

private:
    // One materialized output row. Optional fields carry the "unknown -> NULL"
    // semantics for the footer-derived columns.
    struct DictStatsRow {
        std::string table_schema;
        std::string table_name;
        int64_t partition_id{0};
        int64_t tablet_id{0};
        std::optional<int64_t> segment_id;
        std::string column_name;
        bool use_shared_dict{false}; // from the tablet schema (always known)
        std::optional<std::string> encoding;
        std::optional<std::string> compression;
        std::optional<bool> has_shared_dict;
        std::optional<int64_t> shared_dict_size;
        std::optional<int64_t> data_size;
        std::optional<int64_t> uncompressed_size;
        std::optional<double> compression_ratio;
    };

    Status fill_chunk(ChunkPtr* chunk);

    // Resolve the set of table ids that the TABLE_NAME(+TABLE_SCHEMA) predicate
    // refers to, restricted to the tables the current user is authorized on.
    std::set<int64_t> _resolve_requested_table_ids();

    // Expand one matched local (shared-nothing) tablet. Non-PK tablets are
    // footer-read; PK tablets or any read failure fall back to schema rows.
    void _expand_local_tablet(int64_t table_id, int64_t partition_id, int64_t tablet_id);

    // Expand one matched shared-data (lake) tablet. Footer read is not yet
    // implemented for lake (needs bundle-offset aware segment loading), so this
    // always emits schema-derived rows with the footer columns as NULL.
    void _expand_lake_tablet(int64_t table_id, int64_t partition_id, int64_t tablet_id);

    // Best-effort footer read of a local tablet. Returns true only if at least
    // one segment footer was read; otherwise the caller emits a schema fallback.
    bool _try_expand_local_footers(const TabletSharedPtr& tablet, int64_t table_id, int64_t partition_id,
                                   int64_t tablet_id);

    // Emit one row per top-level tablet-schema column (footer columns = NULL).
    void _emit_schema_fallback_rows(const TabletSchema& schema, int64_t table_id, int64_t partition_id,
                                    int64_t tablet_id);

    // Recurse a footer ColumnMetaPB, emitting one row per leaf (flat-JSON
    // sub-columns become leaves named by ColumnMetaPB.name, field 33).
    void _expand_footer_column(const ColumnMetaPB& meta, int64_t segment_id, const std::string& node_name,
                               bool node_use_shared_dict, int64_t table_id, int64_t partition_id, int64_t tablet_id);

    void _append_footer_leaf_row(const ColumnMetaPB& meta, int64_t segment_id, const std::string& column_name,
                                 bool use_shared_dict, int64_t table_id, int64_t partition_id, int64_t tablet_id);

    // table_id -> (table_schema, table_name), built from the tables-config RPC.
    std::string _table_schema_of(int64_t table_id) const;
    std::string _table_name_of(int64_t table_id) const;

    std::vector<DictStatsRow> _rows;
    size_t _cur_idx{0};

    std::unordered_map<int64_t, std::string> _table_id_to_schema;
    std::unordered_map<int64_t, std::string> _table_id_to_name;

    static SchemaScanner::ColumnDesc _s_columns[];

    TGetTablesConfigResponse _tables_config_response;
};

} // namespace starrocks
