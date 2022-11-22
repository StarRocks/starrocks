// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <gen_cpp/InternalService_types.h>

#include "common/status.h"
#include "exec/olap_utils.h"
#include "exec/vectorized/meta_scan_node.h"
#include "runtime/runtime_state.h"
#include "storage/meta_reader.h"

namespace starrocks::vectorized {

struct MetaScannerParams {
    const TInternalScanRange* scan_range = nullptr;
};

class MetaScanner {
public:
    MetaScanner() = default;
    virtual ~MetaScanner() = default;

    MetaScanner(const MetaScanner&) = delete;
    MetaScanner(MetaScanner&) = delete;
    void operator=(const MetaScanner&) = delete;
    void operator=(MetaScanner&) = delete;

    virtual Status init(RuntimeState* runtime_state, const MetaScannerParams& params) = 0;

    virtual Status open(RuntimeState* state) = 0;

    virtual void close(RuntimeState* state) = 0;

    virtual Status get_chunk(RuntimeState* state, ChunkPtr* chunk) = 0;

    RuntimeState* runtime_state() { return _runtime_state; }

    virtual bool has_more() = 0;

protected:
    virtual Status _get_tablet(const TInternalScanRange* scan_range) = 0;
    virtual Status _init_meta_reader_params() = 0;

    RuntimeState* _runtime_state{nullptr};

    bool _is_open = false;
    bool _is_closed = false;
    int64_t _version = 0;
};

} // namespace starrocks::vectorized
