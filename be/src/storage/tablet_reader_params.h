// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <string>
#include <unordered_map>
#include <vector>

#include "runtime/global_dict/types.h"
#include "storage/chunk_iterator.h"
#include "storage/olap_common.h"
#include "storage/olap_runtime_range_pruner.h"
#include "storage/tuple.h"

namespace starrocks {

class RuntimeProfile;
class RuntimeState;

namespace vectorized {

class ColumnPredicate;
struct RowidRangeOption;
using RowidRangeOptionPtr = std::shared_ptr<RowidRangeOption>;
struct ShortKeyRangeOption;
using ShortKeyRangeOptionPtr = std::shared_ptr<ShortKeyRangeOption>;

static inline std::unordered_set<uint32_t> EMPTY_FILTERED_COLUMN_IDS;

// Params for TabletReader
struct TabletReaderParams {
    enum class RangeStartOperation { GT = 0, GE, EQ };
    enum class RangeEndOperation { LT = 0, LE, EQ };

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

    RangeStartOperation range = RangeStartOperation::GT;
    RangeEndOperation end_range = RangeEndOperation::LT;
    std::vector<OlapTuple> start_key;
    std::vector<OlapTuple> end_key;
    std::vector<const ColumnPredicate*> predicates;

    RuntimeState* runtime_state = nullptr;

    RuntimeProfile* profile = nullptr;

    int chunk_size = 1024;

    ColumnIdToGlobalDictMap* global_dictmaps = &EMPTY_GLOBAL_DICTMAPS;
    const std::unordered_set<uint32_t>* unused_output_column_ids = &EMPTY_FILTERED_COLUMN_IDS;

    RowidRangeOptionPtr rowid_range_option = nullptr;
    std::vector<ShortKeyRangeOptionPtr> short_key_ranges;

    bool sorted_by_keys_per_tablet = false;
    OlapRuntimeScanRangePruner runtime_range_pruner;

    bool use_pk_index = false;

public:
    std::string to_string() const;
};

std::string to_string(TabletReaderParams::RangeStartOperation range_start_op);
std::string to_string(TabletReaderParams::RangeEndOperation range_end_op);
std::ostream& operator<<(std::ostream& os, TabletReaderParams::RangeStartOperation range_start_op);
std::ostream& operator<<(std::ostream& os, TabletReaderParams::RangeEndOperation range_end_op);

} // namespace vectorized
} // namespace starrocks
