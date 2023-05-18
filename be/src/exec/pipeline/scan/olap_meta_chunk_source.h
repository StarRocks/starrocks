// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "column/vectorized_fwd.h"
#include "exec/pipeline/scan/chunk_source.h"
#include "exec/pipeline/scan/olap_meta_scan_context.h"
#include "exec/vectorized/olap_meta_scan_node.h"
#include "runtime/runtime_state.h"

namespace starrocks {

namespace pipeline {

class OlapMetaChunkSource final : public ChunkSource {
public:
    OlapMetaChunkSource(ScanOperator* op, RuntimeProfile* runtime_profile, MorselPtr&& morsel,
                        const OlapMetaScanContextPtr& scan_ctx);

    ~OlapMetaChunkSource() override;

    Status prepare(RuntimeState* state) override;

    void close(RuntimeState* state) override;

private:
    Status _read_chunk(RuntimeState* state, ChunkPtr* chunk) override;

    const workgroup::WorkGroupScanSchedEntity* _scan_sched_entity(const workgroup::WorkGroup* wg) const override;

    OlapMetaScanContextPtr _scan_ctx;

    std::shared_ptr<vectorized::OlapMetaScanner> _scanner;
};

} // namespace pipeline
} // namespace starrocks
