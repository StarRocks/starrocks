// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/exec/exec_node.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "exec/exec_factory.h"

#include <fmt/format.h>
#include <thrift/protocol/TDebugProtocol.h>

#include <sstream>
#include <string>
#include <vector>

#include "common/logging.h"
#include "connector/connector.h"
#include "exec/aggregate/aggregate_blocking_node.h"
#include "exec/aggregate/aggregate_streaming_node.h"
#include "exec/aggregate/distinct_blocking_node.h"
#include "exec/aggregate/distinct_streaming_node.h"
#include "exec/analytic_node.h"
#include "exec/assert_num_rows_node.h"
#include "exec/capture_version_node.h"
#include "exec/connector_scan_node.h"
#include "exec/cross_join_node.h"
#include "exec/dict_decode_node.h"
#include "exec/empty_set_node.h"
#include "exec/except_node.h"
#include "exec/exchange_node.h"
#include "exec/fetch_node.h"
#include "exec/file_scan_node.h"
#include "exec/hash_join_node.h"
#include "exec/intersect_node.h"
#include "exec/lake_meta_scan_node.h"
#include "exec/lookup_node.h"
#include "exec/olap_meta_scan_node.h"
#include "exec/olap_scan_node.h"
#include "exec/project_node.h"
#include "exec/raw_values_node.h"
#include "exec/repeat_node.h"
#include "exec/schema_scan_node.h"
#include "exec/select_node.h"
#include "exec/stream/stream_aggregate_node.h"
#include "exec/table_function_node.h"
#include "exec/topn_node.h"
#include "exec/union_node.h"
#include "gen_cpp/PlanNodes_types.h"
#include "gutil/strings/substitute.h"

namespace starrocks {

namespace {

Status check_tuple_ids_in_descs(const DescriptorTbl& descs, const TPlanNode& plan_node) {
    for (auto id : plan_node.row_tuples) {
        if (descs.get_tuple_descriptor(id) == nullptr) {
            std::stringstream ss;
            ss << "Plan node id: " << plan_node.node_id << ", Tuple ids: ";
            for (auto tuple_id : plan_node.row_tuples) {
                ss << tuple_id << ", ";
            }
            LOG(ERROR) << ss.str();
            ss.str("");
            ss << "DescriptorTbl: " << descs.debug_string();
            LOG(ERROR) << ss.str();
            ss.str("");
            ss << "TPlanNode: " << apache::thrift::ThriftDebugString(plan_node);
            LOG(ERROR) << ss.str();
            return Status::InternalError("Tuple ids are not in descs");
        }
    }

    return Status::OK();
}

Status create_tree_helper(RuntimeState* state, ObjectPool* pool, const std::vector<TPlanNode>& tnodes,
                          const DescriptorTbl& descs, int* node_idx, ExecNode** root) {
    // propagate error case
    if (*node_idx >= tnodes.size()) {
        return Status::InternalError("Failed to reconstruct plan tree from thrift.");
    }
    const TPlanNode& tnode = tnodes[*node_idx];

    ExecNode* node = nullptr;
    RETURN_IF_ERROR(check_tuple_ids_in_descs(descs, tnode));
    RETURN_IF_ERROR(ExecFactory::create_vectorized_node(state, pool, tnode, descs, &node));

    std::vector<ExecNode*> children;
    children.reserve(tnode.num_children);
    for (int i = 0; i < tnode.num_children; i++) {
        ++*node_idx;
        ExecNode* child = nullptr;
        RETURN_IF_ERROR(create_tree_helper(state, pool, tnodes, descs, node_idx, &child));
        children.emplace_back(child);

        // we are expecting a child, but have used all nodes
        // this means we have been given a bad tree and must fail
        if (*node_idx >= tnodes.size()) {
            // TODO: print thrift msg
            return Status::InternalError("Failed to reconstruct plan tree from thrift.");
        }
    }
    node->set_children(std::move(children));

    RETURN_IF_ERROR(node->init(tnode, state));

    // build up tree of profiles; add children >0 first, so that when we print
    // the profile, child 0 is printed last (makes the output more readable)
    const auto& node_children = node->children();
    for (size_t i = 1; i < node_children.size(); ++i) {
        node->runtime_profile()->add_child(node_children[i]->runtime_profile(), true, nullptr);
    }

    if (!node_children.empty()) {
        node->runtime_profile()->add_child(node_children[0]->runtime_profile(), true, nullptr);
    }

    *root = node;
    return Status::OK();
}

} // namespace

Status ExecFactory::create_tree(RuntimeState* state, ObjectPool* pool, const TPlan& plan, const DescriptorTbl& descs,
                                ExecNode** root) {
    if (plan.nodes.empty()) {
        *root = nullptr;
        return Status::OK();
    }

    int node_idx = 0;
    RETURN_IF_ERROR(create_tree_helper(state, pool, plan.nodes, descs, &node_idx, root));

    if (node_idx + 1 != plan.nodes.size()) {
        return Status::InternalError("Plan tree only partially reconstructed. Not all thrift nodes were used.");
    }

    return Status::OK();
}

Status ExecFactory::create_vectorized_node(RuntimeState* state, ObjectPool* pool, const TPlanNode& tnode,
                                           const DescriptorTbl& descs, ExecNode** node) {
    (void)state;
    switch (tnode.node_type) {
    case TPlanNodeType::OLAP_SCAN_NODE:
        *node = pool->add(new OlapScanNode(pool, tnode, descs));
        return Status::OK();
    case TPlanNodeType::META_SCAN_NODE:
        *node = pool->add(new OlapMetaScanNode(pool, tnode, descs));
        return Status::OK();
    case TPlanNodeType::LAKE_META_SCAN_NODE:
        *node = pool->add(new LakeMetaScanNode(pool, tnode, descs));
        return Status::OK();
    case TPlanNodeType::AGGREGATION_NODE:
        if (tnode.agg_node.__isset.use_streaming_preaggregation && tnode.agg_node.use_streaming_preaggregation) {
            if (tnode.agg_node.aggregate_functions.size() == 0) {
                *node = pool->add(new DistinctStreamingNode(pool, tnode, descs));
            } else {
                *node = pool->add(new AggregateStreamingNode(pool, tnode, descs));
            }
        } else {
            if (tnode.agg_node.aggregate_functions.size() == 0) {
                *node = pool->add(new DistinctBlockingNode(pool, tnode, descs));
            } else {
                *node = pool->add(new AggregateBlockingNode(pool, tnode, descs));
            }
        }
        return Status::OK();
    case TPlanNodeType::EMPTY_SET_NODE:
        *node = pool->add(new EmptySetNode(pool, tnode, descs));
        return Status::OK();
    case TPlanNodeType::EXCHANGE_NODE:
        *node = pool->add(new ExchangeNode(pool, tnode, descs));
        return Status::OK();
    case TPlanNodeType::HASH_JOIN_NODE:
        *node = pool->add(new HashJoinNode(pool, tnode, descs));
        return Status::OK();
    case TPlanNodeType::ANALYTIC_EVAL_NODE:
        *node = pool->add(new AnalyticNode(pool, tnode, descs));
        return Status::OK();
    case TPlanNodeType::SORT_NODE:
        *node = pool->add(new TopNNode(pool, tnode, descs));
        return Status::OK();
    case TPlanNodeType::CROSS_JOIN_NODE:
    case TPlanNodeType::NESTLOOP_JOIN_NODE:
        *node = pool->add(new CrossJoinNode(pool, tnode, descs));
        return Status::OK();
    case TPlanNodeType::UNION_NODE:
        *node = pool->add(new UnionNode(pool, tnode, descs));
        return Status::OK();
    case TPlanNodeType::RAW_VALUES_NODE:
        *node = pool->add(new RawValuesNode(pool, tnode, descs));
        return Status::OK();
    case TPlanNodeType::INTERSECT_NODE:
        *node = pool->add(new IntersectNode(pool, tnode, descs));
        return Status::OK();
    case TPlanNodeType::EXCEPT_NODE:
        *node = pool->add(new ExceptNode(pool, tnode, descs));
        return Status::OK();
    case TPlanNodeType::SELECT_NODE:
        *node = pool->add(new SelectNode(pool, tnode, descs));
        return Status::OK();
    case TPlanNodeType::FILE_SCAN_NODE: {
        if (tnode.file_scan_node.__isset.enable_pipeline_load && tnode.file_scan_node.enable_pipeline_load) {
            TPlanNode new_node = tnode;
            TConnectorScanNode connector_scan_node;
            connector_scan_node.connector_name = connector::Connector::FILE;
            new_node.connector_scan_node = connector_scan_node;
            *node = pool->add(new ConnectorScanNode(pool, new_node, descs));
        } else {
            *node = pool->add(new FileScanNode(pool, tnode, descs));
        }
    }
        return Status::OK();
    case TPlanNodeType::REPEAT_NODE:
        *node = pool->add(new RepeatNode(pool, tnode, descs));
        return Status::OK();
    case TPlanNodeType::ASSERT_NUM_ROWS_NODE:
        *node = pool->add(new AssertNumRowsNode(pool, tnode, descs));
        return Status::OK();
    case TPlanNodeType::PROJECT_NODE:
        *node = pool->add(new ProjectNode(pool, tnode, descs));
        return Status::OK();
    case TPlanNodeType::TABLE_FUNCTION_NODE:
        *node = pool->add(new TableFunctionNode(pool, tnode, descs));
        return Status::OK();
    case TPlanNodeType::HDFS_SCAN_NODE:
    case TPlanNodeType::KUDU_SCAN_NODE: {
        TPlanNode new_node = tnode;
        TConnectorScanNode connector_scan_node;
        connector_scan_node.connector_name = connector::Connector::HIVE;
        new_node.connector_scan_node = connector_scan_node;
        *node = pool->add(new ConnectorScanNode(pool, new_node, descs));
        return Status::OK();
    }
    case TPlanNodeType::MYSQL_SCAN_NODE: {
        TPlanNode new_node = tnode;
        TConnectorScanNode connector_scan_node;
        connector_scan_node.connector_name = connector::Connector::MYSQL;
        new_node.connector_scan_node = connector_scan_node;
        *node = pool->add(new ConnectorScanNode(pool, new_node, descs));
        return Status::OK();
    }
    case TPlanNodeType::BENCHMARK_SCAN_NODE: {
        TPlanNode new_node = tnode;
        TConnectorScanNode connector_scan_node;
        connector_scan_node.connector_name = connector::Connector::BENCHMARK;
        new_node.connector_scan_node = connector_scan_node;
        *node = pool->add(new ConnectorScanNode(pool, new_node, descs));
        return Status::OK();
    }
    case TPlanNodeType::ES_HTTP_SCAN_NODE: {
        TPlanNode new_node = tnode;
        TConnectorScanNode connector_scan_node;
        connector_scan_node.connector_name = connector::Connector::ES;
        new_node.connector_scan_node = connector_scan_node;
        *node = pool->add(new ConnectorScanNode(pool, new_node, descs));
        return Status::OK();
    }
    case TPlanNodeType::SCHEMA_SCAN_NODE:
        *node = pool->add(new SchemaScanNode(pool, tnode, descs));
        return Status::OK();
    case TPlanNodeType::DECODE_NODE:
        *node = pool->add(new DictDecodeNode(pool, tnode, descs));
        return Status::OK();
    case TPlanNodeType::JDBC_SCAN_NODE: {
        TPlanNode new_node = tnode;
        TConnectorScanNode connector_scan_node;
        connector_scan_node.connector_name = connector::Connector::JDBC;
        new_node.connector_scan_node = connector_scan_node;
        *node = pool->add(new ConnectorScanNode(pool, new_node, descs));
        return Status::OK();
    }
    case TPlanNodeType::LAKE_SCAN_NODE: {
        TPlanNode new_node = tnode;
        TConnectorScanNode connector_scan_node;
        connector_scan_node.connector_name = connector::Connector::LAKE;
        new_node.connector_scan_node = connector_scan_node;
        *node = pool->add(new ConnectorScanNode(pool, new_node, descs));
        return Status::OK();
    }
    case TPlanNodeType::STREAM_SCAN_NODE: {
        TPlanNode new_node = tnode;
        std::string connector_name;
        StreamSourceType::type source_type = new_node.stream_scan_node.source_type;
        switch (source_type) {
        case StreamSourceType::BINLOG: {
            connector_name = connector::Connector::BINLOG;
            break;
        }
        default:
            return Status::InternalError(fmt::format("Stream scan node does not support source type {}", source_type));
        }
        TConnectorScanNode connector_scan_node;
        connector_scan_node.connector_name = connector_name;
        new_node.connector_scan_node = connector_scan_node;
        *node = pool->add(new ConnectorScanNode(pool, new_node, descs));
        return Status::OK();
    }
    case TPlanNodeType::STREAM_AGG_NODE: {
        *node = pool->add(new StreamAggregateNode(pool, tnode, descs));
        return Status::OK();
    }
    case TPlanNodeType::CAPTURE_VERSION_NODE: {
        *node = pool->add(new CaptureVersionNode(pool, tnode, descs));
        return Status::OK();
    }
    case TPlanNodeType::FETCH_NODE: {
        *node = pool->add(new FetchNode(pool, tnode, descs));
        return Status::OK();
    }
    case TPlanNodeType::LOOKUP_NODE: {
        *node = pool->add(new LookUpNode(pool, tnode, descs));
        return Status::OK();
    }
    default:
        return Status::InternalError(strings::Substitute("Vectorized engine not support node: $0", tnode.node_type));
    }
}

} // namespace starrocks
