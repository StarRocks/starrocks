// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <memory>
#include <unordered_map>
#include <vector>

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

namespace vectorized {

class ColumnPredicate;
class DeletePredicates;
struct RowidRangeOption;
struct ShortKeyRangeOption;

} // namespace vectorized

class RowsetReadOptions {
    using RowidRangeOptionPtr = std::shared_ptr<vectorized::RowidRangeOption>;
    using ShortKeyRangeOptionPtr = std::shared_ptr<vectorized::ShortKeyRangeOption>;
    using PredicateList = std::vector<const vectorized::ColumnPredicate*>;

public:
    ReaderType reader_type = READER_QUERY;
    int chunk_size = DEFAULT_CHUNK_SIZE;

    std::vector<vectorized::SeekRange> ranges;

    std::unordered_map<ColumnId, PredicateList> predicates;
    std::unordered_map<ColumnId, PredicateList> predicates_for_zone_map;

    // whether rowset should return rows in sorted order.
    bool sorted = true;

    const vectorized::DeletePredicates* delete_predicates = nullptr;

    const TabletSchema* tablet_schema = nullptr;

    bool is_primary_keys = false;
    int64_t version = 0;
    KVStore* meta = nullptr;

    OlapReaderStatistics* stats = nullptr;
    RuntimeState* runtime_state = nullptr;
    RuntimeProfile* profile = nullptr;
    bool use_page_cache = false;

    vectorized::ColumnIdToGlobalDictMap* global_dictmaps = &vectorized::EMPTY_GLOBAL_DICTMAPS;
    const std::unordered_set<uint32_t>* unused_output_column_ids = nullptr;

    RowidRangeOptionPtr rowid_range_option = nullptr;
    std::vector<ShortKeyRangeOptionPtr> short_key_ranges;

    vectorized::OlapRuntimeScanRangePruner runtime_range_pruner;
};

} // namespace starrocks
