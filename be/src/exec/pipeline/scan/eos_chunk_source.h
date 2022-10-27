// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "exec/pipeline/scan/chunk_source.h"
#include "exec/pipeline/scan/morsel.h"
#include "exec/pipeline/scan/olap_scan_context.h"
namespace starrocks::pipeline {
class EOSChunkSource : public ChunkSource {
public:
    EOSChunkSource(int32_t scan_operator_id, RuntimeProfile* runtime_profile, MorselPtr&& morsel,
                   vectorized::OlapScanNode* scan_node, OlapScanContext* scan_ct)
            : ChunkSource(scan_operator_id, runtime_profile, std::move(morsel), scan_ct->get_chunk_buffer()) {}
    ~EOSChunkSource() override = default;
    Status prepare(starrocks::RuntimeState* state) override { return Status::OK(); }
    void close(starrocks::RuntimeState* state) override {}

protected:
    Status _read_chunk(RuntimeState* state, vectorized::ChunkPtr* chunk) override;
    const workgroup::WorkGroupScanSchedEntity* _scan_sched_entity(const workgroup::WorkGroup* wg) const override;
};
} // namespace starrocks::pipeline