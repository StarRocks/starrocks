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

#include <unordered_map>
#include <vector>

#include "column/column_access_path.h"
#include "column/datum.h"
#include "fs/fs.h"
#include "runtime/global_dict/types.h"
#include "storage/del_vector.h"
#include "storage/disjunctive_predicates.h"
#include "storage/olap_runtime_range_pruner.h"
#include "storage/seek_range.h"

namespace starrocks {
class Condition;
struct OlapReaderStatistics;
class RuntimeProfile;
class TabletSchema;
class DeltaColumnGroupLoader;
} // namespace starrocks

namespace starrocks {

class ColumnAccessPath;
class ColumnPredicate;
struct RowidRangeOption;
using RowidRangeOptionPtr = std::shared_ptr<RowidRangeOption>;
struct ShortKeyRangeOption;
using ShortKeyRangeOptionPtr = std::shared_ptr<ShortKeyRangeOption>;

class SegmentReadOptions {
public:
    using PredicateList = std::vector<const ColumnPredicate*>;

    std::shared_ptr<FileSystem> fs;

    std::vector<SeekRange> ranges;

    std::unordered_map<ColumnId, PredicateList> predicates;
    std::unordered_map<ColumnId, PredicateList> predicates_for_zone_map;

    DisjunctivePredicates delete_predicates;

    // used for updatable tablet to get delvec
    std::shared_ptr<DelvecLoader> delvec_loader;
    bool is_primary_keys = false;
    uint64_t tablet_id = 0;
    uint32_t rowset_id = 0;
    int64_t version = 0;
    // used for primary key tablet to get delta column group
    std::shared_ptr<DeltaColumnGroupLoader> dcg_loader;

    // REQUIRED (null is not allowed)
    OlapReaderStatistics* stats = nullptr;

    RuntimeProfile* profile = nullptr;

    bool use_page_cache = false;
    bool fill_data_cache = true;

    ReaderType reader_type = READER_QUERY;
    int chunk_size = DEFAULT_CHUNK_SIZE;

    const ColumnIdToGlobalDictMap* global_dictmaps = &EMPTY_GLOBAL_DICTMAPS;
    const std::unordered_set<uint32_t>* unused_output_column_ids = nullptr;

    bool has_delete_pred = false;

    SparseRangePtr rowid_range_option = nullptr;
    std::vector<ShortKeyRangeOptionPtr> short_key_ranges;

    OlapRuntimeScanRangePruner runtime_range_pruner;

    const std::atomic<bool>* is_cancelled = nullptr;

    std::vector<ColumnAccessPathPtr>* column_access_paths = nullptr;

    RowsetId rowsetid;

public:
    Status convert_to(SegmentReadOptions* dst, const std::vector<LogicalType>& new_types, ObjectPool* obj_pool) const;

    // Only used for debugging
    std::string debug_string() const;
};

} // namespace starrocks
