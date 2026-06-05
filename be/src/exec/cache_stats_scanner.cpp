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

#include "exec/cache_stats_scanner.h"

#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/datum.h"
#include "fs/fs.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "storage/lake/tablet_manager.h"
#include "util/string_parser.hpp"

namespace starrocks {

static constexpr std::string_view kCacheStatsTabletIdColumnName = "tablet_id";
static constexpr std::string_view kCacheStatsCachedBytesColumnName = "cached_bytes";
static constexpr std::string_view kCacheStatsTotalBytesColumnName = "total_bytes";

CacheStatsScanner::CacheStatsScanner(const TupleDescriptor* tuple_desc) : _tuple_desc(tuple_desc) {}

Status CacheStatsScanner::init(RuntimeState* state, const TInternalScanRange& scan_range) {
    (void)state;
    _tablet_id = scan_range.tablet_id;
    if (!scan_range.version.empty()) {
        StringParser::ParseResult result = StringParser::PARSE_SUCCESS;
        _version = StringParser::string_to_int<int64_t>(scan_range.version.data(),
                                                        static_cast<int>(scan_range.version.size()), &result);
        if (result != StringParser::PARSE_SUCCESS) {
            return Status::InvalidArgument("Invalid cache stats scan range version: " + scan_range.version);
        }
    }
    return Status::OK();
}

Status CacheStatsScanner::open(RuntimeState* state) {
    (void)state;
    return Status::OK();
}

void CacheStatsScanner::close(RuntimeState* state) {
    (void)state;
}

Status CacheStatsScanner::get_chunk(RuntimeState* state, ChunkPtr* chunk, bool* eos) {
    (void)state;
    if (_is_finished) {
        *eos = true;
        return Status::OK();
    }
    RETURN_IF_ERROR(_collect_cache_stats(chunk));
    _is_finished = true;
    *eos = false;
    return Status::OK();
}

Status CacheStatsScanner::_collect_cache_stats(ChunkPtr* chunk) {
    int64_t cached_bytes = 0;
    int64_t total_bytes = 0;
    RETURN_IF_ERROR(_get_tablet_cache_stats(&cached_bytes, &total_bytes));

    ChunkPtr result = std::make_shared<Chunk>();
    for (auto* slot : _tuple_desc->slots()) {
        auto column = ColumnHelper::create_column(slot->type(), slot->is_nullable());
        if (slot->col_name() == kCacheStatsTabletIdColumnName) {
            column->append_datum(Datum(_tablet_id));
        } else if (slot->col_name() == kCacheStatsCachedBytesColumnName) {
            column->append_datum(Datum(cached_bytes));
        } else if (slot->col_name() == kCacheStatsTotalBytesColumnName) {
            column->append_datum(Datum(total_bytes));
        } else {
            column->append_datum(kNullDatum);
        }
        result->append_column(std::move(column), slot->id());
    }

    *chunk = std::move(result);
    return Status::OK();
}

Status CacheStatsScanner::_get_tablet_cache_stats(int64_t* cached_bytes, int64_t* total_bytes) {
    *cached_bytes = 0;
    *total_bytes = 0;

    auto* tablet_mgr = ExecEnv::GetInstance()->lake_tablet_manager();
    if (tablet_mgr == nullptr) {
        return Status::InvalidArgument("lake tablet manager is nullptr.");
    }

    if (_version <= 0) {
        return Status::InvalidArgument("Invalid cache stats scan range version: " + std::to_string(_version));
    }

    ASSIGN_OR_RETURN(auto metadata, tablet_mgr->get_tablet_metadata(_tablet_id, _version, false, false));

    auto collect_file_cache_stats = [&](const std::string& file_path, int64_t offset, int64_t size) -> Status {
        ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(file_path));
        ASSIGN_OR_RETURN(auto cache_stats, fs->get_cache_stats(file_path, offset, size));
        *cached_bytes += static_cast<int64_t>(cache_stats.first);
        *total_bytes += static_cast<int64_t>(cache_stats.second);
        return Status::OK();
    };

    for (const auto& rowset : metadata->rowsets()) {
        for (const auto& segment_meta : rowset.segment_metas()) {
            std::string segment_path = tablet_mgr->segment_location(_tablet_id, segment_meta.filename());
            int64_t offset = segment_meta.has_bundle_file_offset() ? segment_meta.bundle_file_offset() : 0;
            int64_t size = segment_meta.has_size() ? segment_meta.size() : -1;
            RETURN_IF_ERROR(collect_file_cache_stats(segment_path, offset, size));
        }
    }

    if (metadata->has_delvec_meta()) {
        for (const auto& [_, file] : metadata->delvec_meta().version_to_file()) {
            std::string delvec_path = tablet_mgr->delvec_location(_tablet_id, file.name());
            RETURN_IF_ERROR(collect_file_cache_stats(delvec_path, 0, file.size()));
        }
    }

    if (metadata->has_sstable_meta()) {
        for (const auto& sst : metadata->sstable_meta().sstables()) {
            std::string sst_path = tablet_mgr->sst_location(_tablet_id, sst.filename());
            RETURN_IF_ERROR(collect_file_cache_stats(sst_path, 0, sst.filesize()));
        }
    }

    if (metadata->has_dcg_meta()) {
        for (const auto& [_, dcg_ver] : metadata->dcg_meta().dcgs()) {
            for (const auto& filename : dcg_ver.column_files()) {
                std::string dcg_path = tablet_mgr->segment_location(_tablet_id, filename);
                RETURN_IF_ERROR(collect_file_cache_stats(dcg_path, 0, -1));
            }
        }
    }

    return Status::OK();
}

} // namespace starrocks
