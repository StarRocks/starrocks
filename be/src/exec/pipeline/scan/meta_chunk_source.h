// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "column/vectorized_fwd.h"
#include "exec/pipeline/scan/chunk_source.h"
#include "exec/pipeline/scan/meta_scan_context.h"
#include "exec/vectorized/meta_scan_node.h"
#include "runtime/runtime_state.h"

namespace starrocks::pipeline {

class MetaChunkSource final : public ChunkSource {
public:
    MetaChunkSource(int32_t scan_operator_id, RuntimeProfile* runtime_profile, MorselPtr&& morsel,
                        MetaScanContextPtr scan_ctx);

    ~MetaChunkSource() override;

    Status prepare(RuntimeState* state) override;

    void close(RuntimeState* state) override;

private:
    Status _read_chunk(RuntimeState* state, ChunkPtr* chunk) override;

    const workgroup::WorkGroupScanSchedEntity* _scan_sched_entity(const workgroup::WorkGroup* wg) const override;

    MetaScanContextPtr _scan_ctx;

    std::shared_ptr<vectorized::MetaScanner> _scanner;
};

} // namespace starrocks::pipeline
