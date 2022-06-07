// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <memory>
#include <unordered_map>
#include <vector>

#include "runtime/global_dicts.h"
#include "storage/olap_common.h"
#include "storage/seek_range.h"

namespace starrocks {
class Conditions;
class KVStore;
class OlapReaderStatistics;
class RuntimeProfile;
class RowCursor;
class RuntimeState;
class TabletSchema;
} // namespace starrocks

namespace starrocks::vectorized {

class ColumnPredicate;
class DeletePredicates;
class Schema;
struct RowidRangeOption;
using RowidRangeOptionPtr = std::shared_ptr<RowidRangeOption>;
struct ShortKeyRangeOption;
using ShortKeyRangeOptionPtr = std::shared_ptr<ShortKeyRangeOption>;

class RowsetReadOptions {
public:
    using PredicateList = std::vector<const ColumnPredicate*>;

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

    starrocks::OlapReaderStatistics* stats = nullptr;
    starrocks::RuntimeState* runtime_state = nullptr;
    starrocks::RuntimeProfile* profile = nullptr;
    bool use_page_cache = false;

    ColumnIdToGlobalDictMap* global_dictmaps = &EMPTY_GLOBAL_DICTMAPS;
    const std::unordered_set<uint32_t>* unused_output_column_ids = nullptr;

    RowidRangeOptionPtr rowid_range_option = nullptr;
    std::vector<ShortKeyRangeOptionPtr> short_key_ranges;
};

} // namespace starrocks::vectorized
