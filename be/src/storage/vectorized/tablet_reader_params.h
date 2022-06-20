// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <string>
#include <unordered_map>
#include <vector>

#include "runtime/global_dicts.h"
#include "storage/olap_common.h"
#include "storage/tuple.h"
#include "storage/vectorized/chunk_iterator.h"

namespace starrocks {

class RuntimeProfile;
class RuntimeState;

namespace vectorized {

class ColumnPredicate;

static inline std::unordered_set<uint32_t> EMPTY_FILTERED_COLUMN_IDS;

// Params for TabletReader
struct TabletReaderParams {
    TabletReaderParams();

    ReaderType reader_type = READER_QUERY;

    bool is_pipeline = false;
    bool skip_aggregation = false;
    bool need_agg_finalize = true;
    // 1. when read column data page:
    //     for compaction, schema_change, check_sum: we don't use page cache
    //     for query and config::disable_storage_page_cache is false, we use page cache
    // 2. when read column index page
    //     if config::disable_storage_page_cache is false, we use page cache
    bool use_page_cache = false;

    // possible values are "gt", "ge", "eq"
    std::string range;
    // possible values are "lt", "le"
    std::string end_range;
    std::vector<OlapTuple> start_key;
    std::vector<OlapTuple> end_key;
    std::vector<const ColumnPredicate*> predicates;

    RuntimeState* runtime_state = nullptr;

    RuntimeProfile* profile = nullptr;

    std::string to_string() const;
    int chunk_size = 1024;

    ColumnIdToGlobalDictMap* global_dictmaps = &EMPTY_GLOBAL_DICTMAPS;
    const std::unordered_set<uint32_t>* unused_output_column_ids = &EMPTY_FILTERED_COLUMN_IDS;
};

} // namespace vectorized
} // namespace starrocks
