// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <unordered_map>

#include "exec/pipeline/scan/balanced_chunk_buffer.h"
#include "exec/vectorized/meta_scan_node.h"
#include "exec/vectorized/meta_scanner.h"
#include "gen_cpp/Types_types.h"

namespace starrocks {
namespace vectorized {
class MetaScanner;
class MetaScanNode;
} // namespace vectorized

namespace pipeline {
class MetaScanContext;
using MetaScanContextPtr = std::shared_ptr<MetaScanContext>;
using MetaScannerPtr = std::shared_ptr<vectorized::MetaScanner>;

class MetaScanContext {
public:
    MetaScanContext(BalancedChunkBuffer& chunk_buffer) : _chunk_buffer(chunk_buffer) {}
    ~MetaScanContext() = default;

    MetaScannerPtr get_scanner(TTabletId tablet_id) {
        auto iter = _scanners.find(tablet_id);
        DCHECK(iter != _scanners.end());
        return iter->second;
    }

    void add_scanner(TTabletId tablet_id, MetaScannerPtr scanner) {
        DCHECK(_scanners.find(tablet_id) == _scanners.end());
        _scanners[tablet_id] = std::move(scanner);
    }

    BalancedChunkBuffer& get_chunk_buffer() { return _chunk_buffer; }

    void set_prepare_finished() { _is_prepare_finished = true; }

    bool is_prepare_finished() const { return _is_prepare_finished; }

private:
    // tablet_id => olap_meta_scanner
    std::unordered_map<TTabletId, MetaScannerPtr> _scanners;
    BalancedChunkBuffer& _chunk_buffer;
    std::atomic_bool _is_prepare_finished = false;
};

class MetaScanContextFactory {
public:
    MetaScanContextFactory(vectorized::MetaScanNode* const scan_node, int32_t dop, bool shared_morsel_queue,
                           ChunkBufferLimiterPtr chunk_buffer_limiter)
            : _dop(dop),
              _shared_morsel_queue(shared_morsel_queue),
              _chunk_buffer(BalanceStrategy::kDirect, dop, std::move(chunk_buffer_limiter)),
              _contexts(shared_morsel_queue ? 1 : dop) {}

    MetaScanContextPtr get_or_create(int32_t driver_sequence) {
        DCHECK_LT(driver_sequence, _dop);

        int32_t idx = _shared_morsel_queue ? 0 : driver_sequence;
        if (_contexts[idx] == nullptr) {
            _contexts[idx] = std::make_shared<MetaScanContext>(_chunk_buffer);
        }
        return _contexts[idx];
    }

private:
    const int32_t _dop;
    const bool _shared_morsel_queue;
    BalancedChunkBuffer _chunk_buffer;

    std::vector<MetaScanContextPtr> _contexts;
};

} // namespace pipeline
} // namespace starrocks