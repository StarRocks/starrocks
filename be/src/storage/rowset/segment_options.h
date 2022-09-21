// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <unordered_map>
#include <vector>

#include "column/datum.h"
#include "exprs/vectorized/olap_runtime_ranger.h"
#include "fs/fs.h"
#include "runtime/global_dict/types.h"
#include "storage/disjunctive_predicates.h"
#include "storage/seek_range.h"

namespace starrocks {
class Condition;
struct OlapReaderStatistics;
class RuntimeProfile;
class TabletSchema;
class KVStore;
} // namespace starrocks

namespace starrocks::vectorized {

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
    bool is_primary_keys = false;
    uint64_t tablet_id = 0;
    uint32_t rowset_id = 0;
    int64_t version = 0;
    KVStore* meta = nullptr;

    // REQUIRED (null is not allowed)
    OlapReaderStatistics* stats = nullptr;

    RuntimeProfile* profile = nullptr;

    bool use_page_cache = false;

    ReaderType reader_type = READER_QUERY;
    int chunk_size = DEFAULT_CHUNK_SIZE;

    const ColumnIdToGlobalDictMap* global_dictmaps = &EMPTY_GLOBAL_DICTMAPS;
    const std::unordered_set<uint32_t>* unused_output_column_ids = nullptr;

    bool has_delete_pred = false;

    RowidRangeOptionPtr rowid_range_option = nullptr;
    std::vector<ShortKeyRangeOptionPtr> short_key_ranges;

    RuntimeRangerContext runtime_ranger_ctx;

public:
    Status convert_to(SegmentReadOptions* dst, const std::vector<FieldType>& new_types, ObjectPool* obj_pool) const;

    // Only used for debugging
    std::string debug_string() const;
};

} // namespace starrocks::vectorized
