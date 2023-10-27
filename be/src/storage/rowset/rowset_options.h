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

#include <memory>
#include <unordered_map>
#include <vector>

#include "column/column_access_path.h"
#include "runtime/global_dict/types.h"
#include "storage/olap_common.h"
#include "storage/olap_runtime_range_pruner.h"
#include "storage/seek_range.h"

namespace starrocks {
class Conditions;
class KVStore;
struct OlapReaderStatistics;
class RuntimeProfile;
class RowCursor;
class RuntimeState;
class TabletSchema;

class ColumnPredicate;
class DeletePredicates;
struct RowidRangeOption;
struct ShortKeyRangeOption;

class RowsetReadOptions {
    using RowidRangeOptionPtr = std::shared_ptr<RowidRangeOption>;
    using ShortKeyRangeOptionPtr = std::shared_ptr<ShortKeyRangeOption>;
    using PredicateList = std::vector<const ColumnPredicate*>;

public:
    ReaderType reader_type = READER_QUERY;
    int chunk_size = DEFAULT_CHUNK_SIZE;

    std::vector<SeekRange> ranges;

    std::unordered_map<ColumnId, PredicateList> predicates;
    std::unordered_map<ColumnId, PredicateList> predicates_for_zone_map;

    // whether rowset should return rows in sorted order.
    bool sorted = true;

    const DeletePredicates* delete_predicates = nullptr;

    const TabletSchema* tablet_schema = nullptr;

    bool is_primary_keys = false;
    int64_t version = 0;
    KVStore* meta = nullptr;

    OlapReaderStatistics* stats = nullptr;
    RuntimeState* runtime_state = nullptr;
    RuntimeProfile* profile = nullptr;
    bool use_page_cache = false;
    bool fill_data_cache = true;

    ColumnIdToGlobalDictMap* global_dictmaps = &EMPTY_GLOBAL_DICTMAPS;
    const std::unordered_set<uint32_t>* unused_output_column_ids = nullptr;

    RowidRangeOptionPtr rowid_range_option = nullptr;
    std::vector<ShortKeyRangeOptionPtr> short_key_ranges;

    OlapRuntimeScanRangePruner runtime_range_pruner;

    std::vector<ColumnAccessPathPtr>* column_access_paths = nullptr;
    bool asc_hint = true;
};

} // namespace starrocks
