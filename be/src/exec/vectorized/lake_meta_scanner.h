// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <gen_cpp/InternalService_types.h>

#include "common/status.h"
#include "exec/olap_utils.h"
#include "runtime/runtime_state.h"
#include "storage/lake_meta_reader.h"


namespace starrocks {
namespace vectorized {

class LakeMetaScanNode;

struct LakeMetaScannerParams {
    const TInternalScanRange* scan_range = nullptr;
};

class LakeMetaScanner {
public:
    LakeMetaScanner(LakeMetaScanNode* parent);
    ~LakeMetaScanner() = default;

    LakeMetaScanner(const LakeMetaScanner&) = delete;
    LakeMetaScanner(LakeMetaScanner&) = delete;
    void operator=(const LakeMetaScanner&) = delete;
    void operator=(LakeMetaScanner&) = delete;

    Status init(RuntimeState* runtime_state, const LakeMetaScannerParams& params);

    Status open(RuntimeState* state);

    void close(RuntimeState* state);

    Status get_chunk(RuntimeState* state, ChunkPtr* chunk);

    RuntimeState* runtime_state() { return _runtime_state; }

    bool has_more();

private:
    Status _get_tablet(const TInternalScanRange* scan_range);
    Status _init_meta_reader_params();
    Status _init_return_columns();

    LakeMetaScanNode* _parent;
    RuntimeState* _runtime_state;
    StatusOr<lake::Tablet> _tablet;
    std::shared_ptr<const TabletSchema> _tablet_schema;

    LakeMetaReaderParams _reader_params;
    std::shared_ptr<LakeMetaReader> _reader;

    bool _is_open = false;
    bool _is_closed = false;
    int64_t _version = 0;
};

} // namespace vectorized

} // namespace starrocks
