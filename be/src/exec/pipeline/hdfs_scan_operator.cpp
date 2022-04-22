// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exec/pipeline/hdfs_scan_operator.h"

#include "column/chunk.h"
#include "exec/pipeline/hdfs_chunk_source.h"
#include "exec/vectorized/hdfs_scan_node.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"

namespace starrocks::pipeline {

// ==================== HdfsScanOperatorFactory ====================

HdfsScanOperatorFactory::HdfsScanOperatorFactory(int32_t id, ScanNode* scan_node)
        : ScanOperatorFactory(id, scan_node) {}

Status HdfsScanOperatorFactory::do_prepare(RuntimeState* state) {
    return Status::OK();
}

void HdfsScanOperatorFactory::do_close(RuntimeState*) {}

OperatorPtr HdfsScanOperatorFactory::do_create(int32_t dop, int32_t driver_sequence) {
    return std::make_shared<HdfsScanOperator>(this, _id, _scan_node);
}

// ==================== HdfsScanOperator ====================

HdfsScanOperator::HdfsScanOperator(OperatorFactory* factory, int32_t id, ScanNode* scan_node)
        : ScanOperator(factory, id, scan_node) {}

Status HdfsScanOperator::do_prepare(RuntimeState*) {
    return Status::OK();
}

void HdfsScanOperator::do_close(RuntimeState*) {}

ChunkSourcePtr HdfsScanOperator::create_chunk_source(MorselPtr morsel, int32_t chunk_source_index) {
    vectorized::HdfsScanNode* hdfs_scan_node = down_cast<vectorized::HdfsScanNode*>(_scan_node);
    return std::make_shared<HdfsChunkSource>(_chunk_source_profiles[chunk_source_index].get(), std::move(morsel), this,
                                             hdfs_scan_node);
}

} // namespace starrocks::pipeline
