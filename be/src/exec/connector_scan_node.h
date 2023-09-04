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

#include <atomic>
#include <memory>

#include "column/vectorized_fwd.h"
#include "connector/connector.h"
#include "exec/scan_node.h"
#include "fs/fs.h"

namespace starrocks {

class ConnectorScanner;

class ConnectorScanNode final : public starrocks::ScanNode {
public:
    ConnectorScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    ~ConnectorScanNode() override;

    Status init(const TPlanNode& tnode, RuntimeState* state) override;

    // for non-pipeline APIs.
    Status prepare(RuntimeState* state) override;
    Status open(RuntimeState* state) override;
    Status get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos) override;
    void close(RuntimeState* state) override;
    Status set_scan_ranges(const std::vector<TScanRangeParams>& scan_ranges) override;
    bool accept_empty_scan_ranges() const override;

    // for pipline APIs
    std::vector<std::shared_ptr<pipeline::OperatorFactory>> decompose_to_pipeline(
            pipeline::PipelineBuilderContext* context) override;

    connector::DataSourceProvider* data_source_provider() { return _data_source_provider.get(); }
    connector::ConnectorType connector_type() { return _connector_type; }
    bool always_shared_scan() const override;
    std::atomic<int32_t>* get_lazy_column_coalesce_counter() { return &_lazy_column_coalesce_counter; }

private:
    RuntimeState* _runtime_state = nullptr;
    connector::DataSourceProviderPtr _data_source_provider = nullptr;
    connector::ConnectorType _connector_type;

    // non-pipeline methods.
    void _init_counter();
    Status _start_scan_thread(RuntimeState* state);
    Status _create_and_init_scanner(RuntimeState* state, TScanRange& scan_range);
    bool _submit_scanner(ConnectorScanner* scanner, bool blockable);
    void _scanner_thread(ConnectorScanner* scanner);
    void _release_scanner(ConnectorScanner* scanner);
    void _update_status(const Status& status);
    Status _get_status();
    void _fill_chunk_pool(int count);
    void _close_pending_scanners();
    void _push_pending_scanner(ConnectorScanner* scanner);
    ConnectorScanner* _pop_pending_scanner();

    // non-pipeline fields.
    std::vector<TScanRangeParams> _scan_ranges;
    bool _closed = false;

    int _num_scanners = 0;
    int _chunks_per_scanner = 0;
    bool _start = false;
    mutable SpinLock _status_mtx;
    Status _status = Status::OK();

    std::atomic<int32_t> _scanner_submit_count = 0;
    std::atomic<int32_t> _running_threads = 0;
    std::atomic<int32_t> _closed_scanners = 0;
    std::atomic<int32_t> _lazy_column_coalesce_counter = 0;

private:
    template <typename T>
    class Stack {
    public:
        void reserve(size_t n) { _items.reserve(n); }

        void push(const T& p) { _items.push_back(p); }
        void push(T&& v) { _items.emplace_back(std::move(v)); }

        // REQUIRES: not empty.
        T pop() {
            DCHECK(!_items.empty());
            T v = _items.back();
            _items.pop_back();
            return v;
        }

        size_t size() const { return _items.size(); }
        bool empty() const { return _items.empty(); }
        void reverse() { std::reverse(_items.begin(), _items.end()); }

    private:
        std::vector<T> _items;
    };

    struct Profile {
        RuntimeProfile::Counter* scanner_queue_counter = nullptr;
        RuntimeProfile::Counter* scanner_queue_timer = nullptr;
    };

    std::mutex _mtx;
    Stack<ChunkPtr> _chunk_pool;
    std::atomic_bool _pending_token = true;
    Stack<ConnectorScanner*> _pending_scanners;
    UnboundedBlockingQueue<ChunkPtr> _result_chunks;
    Profile _profile;

    void _estimate_scan_row_bytes();
    void _estimate_mem_usage_per_chunk_source();
    int _estimated_max_concurrent_chunks() const;
    int64_t _mem_limit = 0;
    size_t _estimated_scan_row_bytes = 0;
    size_t _estimated_mem_usage_per_chunk_source = 0;
};
} // namespace starrocks
