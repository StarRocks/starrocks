// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exec/pipeline/scan/meta_chunk_source.h"

#include "exec/pipeline/scan/meta_scan_operator.h"
#include "exec/workgroup/work_group.h"

namespace starrocks::pipeline {

MetaChunkSource::MetaChunkSource(int32_t scan_operator_id, RuntimeProfile* runtime_profile, MorselPtr&& morsel,
                                 MetaScanContextPtr scan_ctx)
        : ChunkSource(scan_operator_id, runtime_profile, std::move(morsel), scan_ctx->get_chunk_buffer()),
          _scan_ctx(scan_ctx) {}

MetaChunkSource::~MetaChunkSource() {}

Status MetaChunkSource::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(ChunkSource::prepare(state));
    // use tablet id in morsel to get olap meta scanner
    auto scan_morsel = dynamic_cast<ScanMorsel*>(_morsel.get());
    DCHECK(scan_morsel != nullptr);
    auto scan_range = scan_morsel->get_olap_scan_range();
    _scanner = _scan_ctx->get_scanner(scan_range->tablet_id);
    RETURN_IF_ERROR(_scanner->open(state));
    return Status::OK();
}

void MetaChunkSource::close(RuntimeState* state) {
    _scanner->close(state);
}

Status MetaChunkSource::_read_chunk(RuntimeState* state, ChunkPtr* chunk) {
    if (!_scanner->has_more()) {
        return Status::EndOfFile("end of file");
    }
    return _scanner->get_chunk(state, chunk);
}

const workgroup::WorkGroupScanSchedEntity* MetaChunkSource::_scan_sched_entity(const workgroup::WorkGroup* wg) const {
    DCHECK(wg != nullptr);
    return wg->scan_sched_entity();
}

} // namespace starrocks::pipeline
