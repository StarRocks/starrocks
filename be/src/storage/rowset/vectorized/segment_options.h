// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <unordered_map>
#include <vector>

#include "column/datum.h"
#include "runtime/global_dicts.h"
#include "storage/fs/fs_util.h"
#include "storage/vectorized/disjunctive_predicates.h"
#include "storage/vectorized/seek_range.h"

namespace starrocks {
class Condition;
struct OlapReaderStatistics;
class RuntimeProfile;
class TabletSchema;
class KVStore;
} // namespace starrocks

namespace starrocks::vectorized {

class ColumnPredicate;

class SegmentReadOptions {
public:
    using PredicateList = std::vector<const ColumnPredicate*>;

    fs::BlockManager* block_mgr = fs::fs_util::block_manager();

    std::vector<SeekRange> ranges;

    std::unordered_map<ColumnId, PredicateList> predicates;

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

    Status convert_to(SegmentReadOptions* dst, const std::vector<FieldType>& new_types, ObjectPool* obj_pool) const;

    // Only used for debugging
    std::string debug_string() const;

    ReaderType reader_type = READER_QUERY;
    int chunk_size = DEFAULT_CHUNK_SIZE;

    const ColumnIdToGlobalDictMap* global_dictmaps = &EMPTY_GLOBAL_DICTMAPS;
};

} // namespace starrocks::vectorized
