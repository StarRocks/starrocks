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

#include <string>
#include <unordered_map>
#include <vector>

#include "column/column_access_path.h"
#include "options.h"
#include "runtime/global_dict/types.h"
#include "storage/chunk_iterator.h"
#include "storage/olap_common.h"
#include "storage/olap_runtime_range_pruner.h"
#include "storage/tuple.h"

namespace starrocks {

class RuntimeProfile;
class RuntimeState;

class ColumnPredicate;
struct RowidRangeOption;
using RowidRangeOptionPtr = std::shared_ptr<RowidRangeOption>;
struct ShortKeyRangesOption;
using ShortKeyRangesOptionPtr = std::shared_ptr<ShortKeyRangesOption>;

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

    // Options only applies to cloud-native table r/w IO
    LakeIOOptions lake_io_opts{.fill_data_cache = true};

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
    ShortKeyRangesOptionPtr short_key_ranges_option = nullptr;

    bool sorted_by_keys_per_tablet = false;
    OlapRuntimeScanRangePruner runtime_range_pruner;

    std::vector<ColumnAccessPathPtr>* column_access_paths = nullptr;
    bool use_pk_index = false;

public:
    std::string to_string() const;
};

std::string to_string(TabletReaderParams::RangeStartOperation range_start_op);
std::string to_string(TabletReaderParams::RangeEndOperation range_end_op);
std::ostream& operator<<(std::ostream& os, TabletReaderParams::RangeStartOperation range_start_op);
std::ostream& operator<<(std::ostream& os, TabletReaderParams::RangeEndOperation range_end_op);

} // namespace starrocks
