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

#include "column/chunk.h"
#include "column/column_access_path.h"
#include "common/status.h"
#include "exec/olap_utils.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "gen_cpp/InternalService_types.h"
#include "runtime/runtime_state.h"
#include "storage/conjunctive_predicates.h"
#include "storage/tablet.h"
#include "storage/tablet_reader.h"

namespace starrocks {

class OlapScanNode;

struct TabletScannerParams {
    const TInternalScanRange* scan_range = nullptr;
    const std::vector<OlapScanRange*>* key_ranges = nullptr;
    const std::vector<ExprContext*>* conjunct_ctxs = nullptr;

    const std::vector<std::string>* unused_output_columns = nullptr;
    const std::vector<ColumnAccessPathPtr>* column_access_paths = nullptr;

    bool skip_aggregation = false;
    bool need_agg_finalize = true;
    bool update_num_scan_range = false;
};

class TabletScanner {
public:
    explicit TabletScanner(OlapScanNode* parent);
    ~TabletScanner();

    TabletScanner(const TabletScanner&) = delete;
    TabletScanner(TabletScanner&&) = delete;
    void operator=(const TabletScanner&) = delete;
    void operator=(TabletScanner&&) = delete;

    Status init(RuntimeState* runtime_state, const TabletScannerParams& params);
    Status open([[maybe_unused]] RuntimeState* runtime_state);
    Status get_chunk([[maybe_unused]] RuntimeState* state, Chunk* chunk);
    Status close(RuntimeState* state);

    RuntimeState* runtime_state() { return _runtime_state; }
    int64_t raw_rows_read() const { return _raw_rows_read; }
    int64_t num_rows_read() const { return _num_rows_read; }

    // REQUIRES: `init(RuntimeState*, const TabletScannerParams&)` has been called.
    const Schema& chunk_schema() const { return _prj_iter->output_schema(); }

    void set_keep_priority(bool v) { _keep_priority = v; }
    bool keep_priority() const { return _keep_priority; }

private:
    Status _get_tablet(const TInternalScanRange* scan_range);
    Status _init_reader_params(const std::vector<OlapScanRange*>* key_ranges);
    Status _init_return_columns();
    Status _init_global_dicts();
    Status _init_unused_output_columns(const std::vector<std::string>& unused_output_columns);
    void _update_realtime_counter(Chunk* chunk);
    void update_counter();

    RuntimeState* _runtime_state = nullptr;
    OlapScanNode* _parent = nullptr;

    using PredicatePtr = std::unique_ptr<ColumnPredicate>;
    ObjectPool _pool;
    std::vector<ExprContext*> _conjunct_ctxs;
    ConjunctivePredicates _predicates;
    std::vector<uint8_t> _selection;

    // for release memory.
    std::vector<PredicatePtr> _predicate_free_pool;

    bool _is_open = false;
    bool _is_closed = false;
    bool _skip_aggregation = false;
    bool _need_agg_finalize = false;
    bool _has_update_counter = false;
    bool _update_num_scan_range = false;

    TabletReaderParams _params;
    std::shared_ptr<TabletReader> _reader;

    TabletSharedPtr _tablet;
    int64_t _version = 0;

    // output columns of `this` TabletScanner, i.e, the final output columns of `get_chunk`.
    std::vector<uint32_t> _scanner_columns;
    // columns fetched from |_reader|.
    std::vector<uint32_t> _reader_columns;

    // unused ouput columns
    std::unordered_set<uint32_t> _unused_output_column_ids;

    // projection iterator, doing the job of choosing |_scanner_columns| from |_reader_columns|.
    std::shared_ptr<ChunkIterator> _prj_iter;
    // slot descriptors for each one of |_scanner_columns|.
    std::vector<SlotDescriptor*> _query_slots;

    int64_t _num_rows_read = 0;
    int64_t _raw_rows_read = 0;
    int64_t _compressed_bytes_read = 0;

    // non-pushed-down predicates filter time.
    RuntimeProfile::Counter* _expr_filter_timer = nullptr;

    bool _keep_priority = false;
};
} // namespace starrocks
