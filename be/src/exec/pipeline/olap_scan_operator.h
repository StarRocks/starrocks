// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "exec/pipeline/pipeline_builder.h"
#include "exec/pipeline/scan_operator.h"
#include "exec/scan_node.h"

namespace starrocks::pipeline {

OpFactories decompose_olap_scan_node_to_pipeline(ScanNode* scan_node, PipelineBuilderContext* context);

} // namespace starrocks::pipeline
