// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <gen_cpp/InternalService_types.h>

#include "common/status.h"
#include "exec/olap_utils.h"
#include "runtime/runtime_state.h"
#include "storage/lake_meta_reader.h"
#include "exec/vectorized/meta_scanner.h"


namespace starrocks {
namespace vectorized {

class LakeMetaScanNode;

class LakeMetaScanner final : public MetaScanner{
public:
    LakeMetaScanner(LakeMetaScanNode* parent);
    virtual ~LakeMetaScanner() = default;

    LakeMetaScanner(const LakeMetaScanner&) = delete;
    LakeMetaScanner(LakeMetaScanner&) = delete;
    void operator=(const LakeMetaScanner&) = delete;
    void operator=(LakeMetaScanner&) = delete;

    Status init(RuntimeState* runtime_state, const MetaScannerParams& params) override;

    Status open(RuntimeState* state) override;

    void close(RuntimeState* state) override;

    Status get_chunk(RuntimeState* state, ChunkPtr* chunk) override;

    bool has_more() override;

private:
    Status _get_tablet(const TInternalScanRange* scan_range) override;
    Status _init_meta_reader_params() override;

    LakeMetaScanNode* _parent;
    StatusOr<lake::Tablet> _tablet;
    std::shared_ptr<const TabletSchema> _tablet_schema;

    LakeMetaReaderParams _reader_params;
    std::shared_ptr<LakeMetaReader> _reader;
};

} // namespace vectorized

} // namespace starrocks
