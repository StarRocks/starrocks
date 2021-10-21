// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <memory>
#include <unordered_map>
#include <vector>

#include "runtime/global_dicts.h"
#include "storage/fs/fs_util.h"
#include "storage/olap_common.h"
#include "storage/vectorized/seek_range.h"

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

class RowsetReadOptions {
public:
    using PredicateList = std::vector<const ColumnPredicate*>;

    ReaderType reader_type = READER_QUERY;
    int chunk_size = DEFAULT_CHUNK_SIZE;

    fs::BlockManager* block_mgr = fs::fs_util::block_manager();

    std::vector<SeekRange> ranges;

    std::unordered_map<ColumnId, PredicateList> predicates;

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

    std::unordered_map<ColumnId, GlobalDictMap*>* global_dictmaps = &EMPTY_GLOBAL_DICTMAPS;
};

} // namespace starrocks::vectorized
