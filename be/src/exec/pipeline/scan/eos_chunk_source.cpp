// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exec/pipeline/scan/eos_chunk_source.h"

#include "column/chunk.h"
#include "exec/workgroup/work_group.h"
namespace starrocks::pipeline {

Status EOSChunkSource::_read_chunk(starrocks::RuntimeState* state, vectorized::ChunkPtr* chunk) {
    DCHECK(chunk != nullptr);
    auto chk = std::make_shared<vectorized::Chunk>();
    auto [tablet_id, _] = _morsel->get_lane_owner_and_version();
    chk->owner_info().set_owner_id(tablet_id, true);
    *chunk = std::move(chk);
    return Status::EndOfFile("EOS");
}

const workgroup::WorkGroupScanSchedEntity* EOSChunkSource::_scan_sched_entity(const workgroup::WorkGroup* wg) const {
    DCHECK(wg != nullptr);
    return wg->scan_sched_entity();
}
} // namespace starrocks::pipeline
