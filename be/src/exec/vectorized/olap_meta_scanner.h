// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
#pragma once

#include <gen_cpp/InternalService_types.h>

#include "common/status.h"
#include "exec/olap_utils.h"
#include "runtime/runtime_state.h"
#include "storage/olap_meta_reader.h"
#include "exec/vectorized/meta_scanner.h"

namespace starrocks::vectorized {

class OlapMetaScanNode;

class OlapMetaScanner : public MetaScanner {
public:
    OlapMetaScanner(OlapMetaScanNode* parent);
    ~OlapMetaScanner() = default;

    OlapMetaScanner(const OlapMetaScanner&) = delete;
    OlapMetaScanner(OlapMetaScanner&) = delete;
    void operator=(const OlapMetaScanner&) = delete;
    void operator=(OlapMetaScanner&) = delete;

    Status init(RuntimeState* runtime_state, const MetaScannerParams& params) override;

    Status open(RuntimeState* state) override;

    void close(RuntimeState* state) override;

    Status get_chunk(RuntimeState* state, ChunkPtr* chunk) override;

    bool has_more() override;

private:
    Status _get_tablet(const TInternalScanRange* scan_range) override;
    Status _init_meta_reader_params() override;

    OlapMetaScanNode* _parent;
    TabletSharedPtr _tablet;

    OlapMetaReaderParams _reader_params;
    std::shared_ptr<OlapMetaReader> _reader;
};

} // namespace starrocks::vectorized
