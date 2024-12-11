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
<<<<<<< HEAD
#include "storage/seek_range.h"
=======
#include "storage/options.h"
#include "storage/predicate_tree/predicate_tree.hpp"
#include "storage/seek_range.h"
#include "storage/tablet_schema.h"
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))

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
<<<<<<< HEAD
struct RowidRangeOption;
struct ShortKeyRangeOption;

class RowsetReadOptions {
    using RowidRangeOptionPtr = std::shared_ptr<RowidRangeOption>;
    using ShortKeyRangeOptionPtr = std::shared_ptr<ShortKeyRangeOption>;
=======
class ChunkPredicate;
struct RowidRangeOption;
struct ShortKeyRangesOption;
struct VectorSearchOption;
using VectorSearchOptionPtr = std::shared_ptr<VectorSearchOption>;

class RowsetReadOptions {
    using RowidRangeOptionPtr = std::shared_ptr<RowidRangeOption>;
    using ShortKeyRangesOptionPtr = std::shared_ptr<ShortKeyRangesOption>;
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    using PredicateList = std::vector<const ColumnPredicate*>;

public:
    ReaderType reader_type = READER_QUERY;
    int chunk_size = DEFAULT_CHUNK_SIZE;

    std::vector<SeekRange> ranges;

<<<<<<< HEAD
    std::unordered_map<ColumnId, PredicateList> predicates;
    std::unordered_map<ColumnId, PredicateList> predicates_for_zone_map;
=======
    PredicateTree pred_tree;
    PredicateTree pred_tree_for_zone_map;
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))

    // whether rowset should return rows in sorted order.
    bool sorted = true;

    const DeletePredicates* delete_predicates = nullptr;

<<<<<<< HEAD
    const TabletSchema* tablet_schema = nullptr;
=======
    TabletSchemaCSPtr tablet_schema = nullptr;
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))

    bool is_primary_keys = false;
    int64_t version = 0;
    KVStore* meta = nullptr;

    OlapReaderStatistics* stats = nullptr;
    RuntimeState* runtime_state = nullptr;
    RuntimeProfile* profile = nullptr;
    bool use_page_cache = false;
<<<<<<< HEAD
    bool fill_data_cache = true;
=======
    LakeIOOptions lake_io_opts;
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))

    ColumnIdToGlobalDictMap* global_dictmaps = &EMPTY_GLOBAL_DICTMAPS;
    const std::unordered_set<uint32_t>* unused_output_column_ids = nullptr;

    RowidRangeOptionPtr rowid_range_option = nullptr;
<<<<<<< HEAD
    std::vector<ShortKeyRangeOptionPtr> short_key_ranges;
=======
    ShortKeyRangesOptionPtr short_key_ranges_option = nullptr;
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))

    OlapRuntimeScanRangePruner runtime_range_pruner;

    std::vector<ColumnAccessPathPtr>* column_access_paths = nullptr;
<<<<<<< HEAD
    bool asc_hint = true;
=======

    bool asc_hint = true;

    bool prune_column_after_index_filter = false;
    bool enable_gin_filter = false;
    bool has_preaggregation = true;

    bool use_vector_index = false;

    VectorSearchOptionPtr vector_search_option = nullptr;

    TTableSampleOptions sample_options;
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
};

} // namespace starrocks
