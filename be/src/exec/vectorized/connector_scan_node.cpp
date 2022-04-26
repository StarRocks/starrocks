// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include <atomic>
#include <memory>

// #include "env/env.h"
// #include "exec/pipeline/hdfs_scan_operator.h"
// #include "exec/vectorized/hdfs_scan_node.h"
// #include "exec/vectorized/hdfs_scanner.h"
// #include "exec/vectorized/hdfs_scanner_orc.h"
// #include "exec/vectorized/hdfs_scanner_parquet.h"
// #include "exec/vectorized/hdfs_scanner_text.h"
// #include "exprs/expr.h"
// #include "exprs/expr_context.h"
// #include "exprs/vectorized/runtime_filter.h"
// #include "fmt/core.h"
// #include "glog/logging.h"
// #include "gutil/map_util.h"
// #include "runtime/current_thread.h"
// #include "runtime/hdfs/hdfs_fs_cache.h"
// #include "runtime/runtime_state.h"
// #include "storage/chunk_helper.h"
// #include "util/defer_op.h"
// #include "util/hdfs_util.h"
// #include "util/priority_thread_pool.hpp"

#include "exec/pipeline/connector_scan_operator.h"
#include "exec/vectorized/connector_scan_node.h"

namespace starrocks::vectorized {

ConnectorScanNode::ConnectorScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : ScanNode(pool, tnode, descs) {
    _name = "connector_scan";
    auto c = connector::ConnectorManager::default_instance()->get(tnode.connector_scan_node.connector_name);
    _data_source_provider = c->create_data_source_provider(this, tnode);
}

ConnectorScanNode::~ConnectorScanNode() {}

Status ConnectorScanNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ScanNode::init(tnode, state));
    _data_source_provider->init(state);
    return Status::OK();
}

pipeline::OpFactories ConnectorScanNode::decompose_to_pipeline(pipeline::PipelineBuilderContext* context) {
    auto factory = std::make_shared<pipeline::ConnectorScanOperatorFactory>(context->next_operator_id(), this);
    return pipeline::decompose_scan_node_to_pipeline(factory, this, context);
}

} // namespace starrocks::vectorized
