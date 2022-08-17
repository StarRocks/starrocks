// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
#pragma once

#include <gen_cpp/InternalService_types.h>

#include "common/status.h"
#include "exec/olap_utils.h"
#include "runtime/runtime_state.h"
#include "storage/meta_reader.h"

namespace starrocks {
namespace vectorized {

class OlapMetaScanNode;

struct OlapMetaScannerParams {
    const TInternalScanRange* scan_range = nullptr;
};

class OlapMetaScanner {
public:
    OlapMetaScanner(OlapMetaScanNode* parent);
    ~OlapMetaScanner() = default;

    OlapMetaScanner(const OlapMetaScanner&) = delete;
    OlapMetaScanner(OlapMetaScanner&) = delete;
    void operator=(const OlapMetaScanner&) = delete;
    void operator=(OlapMetaScanner&) = delete;

    Status init(RuntimeState* runtime_state, const OlapMetaScannerParams& params);

    Status open(RuntimeState* state);

    void close(RuntimeState* state);

    Status get_chunk(RuntimeState* state, ChunkPtr* chunk);

    RuntimeState* runtime_state() { return _runtime_state; }

    bool has_more();

private:
    Status _get_tablet(const TInternalScanRange* scan_range);
    Status _init_meta_reader_params();
    Status _init_return_columns();

    OlapMetaScanNode* _parent;
    RuntimeState* _runtime_state;
    TabletSharedPtr _tablet;

    MetaReaderParams _reader_params;
    std::shared_ptr<MetaReader> _reader;

    bool _is_open = false;
    bool _is_closed = false;
    int64_t _version = 0;
};

} // namespace vectorized

} // namespace starrocks
