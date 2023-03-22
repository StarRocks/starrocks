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

#include "exec/stream/stream_operators_test.h"

#include <random>

#include "exec/connector_scan_node.h"
#include "exec/pipeline/pipeline_builder.h"
#include "exec/pipeline/pipeline_driver_executor.h"
#include "exec/pipeline/scan/morsel.h"
#include "exec/stream/aggregate/stream_aggregate_operator.h"
#include "exec/stream/aggregate/stream_aggregator.h"
#include "exec/stream/stream_pipeline_test.h"
#include "exec/stream/stream_test.h"

namespace starrocks::stream {

bool GeneratorStreamSourceOperator::is_trigger_finished(const EpochInfo& epoch_info) {
    auto trigger_mode = epoch_info.trigger_mode;
    switch (trigger_mode) {
    case TriggerMode::MANUAL: {
        return (--_processed_chunks) == 0;
    }
    default:
        VLOG_ROW << "Unsupported trigger_mode: " + std::to_string((int)(trigger_mode));
    }
    return false;
}

StatusOr<ChunkPtr> GeneratorStreamSourceOperator::pull_chunk(starrocks::RuntimeState* state) {
    VLOG_ROW << "[GeneratorStreamSourceOperator] pull_chunk: has output";
    auto chunk = std::make_shared<Chunk>();
    for (auto idx = 0; idx < _param.num_column; idx++) {
        auto column = Int64Column::create();
        for (int64_t i = 0; i < _param.chunk_size; i++) {
            _param.start += _param.step;
            VLOG_ROW << "Append col:" << idx << ", row:" << _param.start;
            column->append(_param.start % _param.ndv_count);
        }
        chunk->append_column(column, SlotId(idx));
    }

    // ops
    auto ops = Int8Column::create();
    for (int64_t i = 0; i < _param.chunk_size; i++) {
        ops->append(0);
    }
    _is_epoch_finished = is_trigger_finished(_current_epoch_info);
    return StreamChunkConverter::make_stream_chunk(std::move(chunk), std::move(ops));
}

Status PrinterStreamSinkOperator::push_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    std::cout << "<<<<<<<<< Sink Result: " << chunk->debug_columns() << std::endl;
    for (auto& col : chunk->columns()) {
        std::cout << col->debug_string() << std::endl;
    }
    this->_output_chunks.push_back(chunk);
    return Status::OK();
}

class StreamOperatorsTest : public StreamPipelineTest, public StreamTestBase {
public:
    void SetUp() override { StreamTestBase::SetUp(); }
    void TearDown() override {}

    void CheckResult(std::vector<ChunkPtr> epoch_results,
                     std::vector<std::vector<std::vector<int64_t>>> expect_results) {
        DCHECK(!epoch_results.empty());
        for (size_t i = 0; i < epoch_results.size(); i++) {
            auto result = epoch_results[i];
            auto columns = result->columns();
            auto expect = expect_results[i];
            DCHECK_EQ(columns.size(), expect.size());
            for (size_t j = 0; j < expect.size(); j++) {
                CheckColumn<int64_t>(columns[j], expect[j]);
            }
        }
    }

protected:
    DescriptorTbl* _tbl;
    std::vector<std::vector<SlotTypeInfo>> _slot_infos;
    std::vector<GroupByKeyInfo> _group_by_infos;
    std::vector<AggInfo> _agg_infos;
    std::shared_ptr<StreamAggregator> _stream_aggregator;

protected:
    std::shared_ptr<TPlanNode> _create_tplan_node(int node_id, int tuple_id);
    DescriptorTbl* _create_table_desc(int num_columns, int chunk_size);
    std::vector<TScanRangeParams> _create_binlog_scan_ranges(size_t degree_of_parallelism);
    void _generate_morse_queue(ConnectorScanNode* scan_nodes, const std::vector<TScanRangeParams>& scan_ranges,
                               int degree_of_parallelism);
};

std::shared_ptr<TPlanNode> StreamOperatorsTest::_create_tplan_node(int node_id, int tuple_id) {
    std::vector<::starrocks::TTupleId> tuple_ids{tuple_id};
    std::vector<bool> nullable_tuples{true};

    auto tnode = std::make_shared<TPlanNode>();

    tnode->__set_node_id(node_id);
    tnode->__set_node_type(TPlanNodeType::STREAM_SCAN_NODE);
    tnode->__set_row_tuples(tuple_ids);
    tnode->__set_nullable_tuples(nullable_tuples);
    tnode->__set_use_vectorized(true);
    tnode->__set_limit(-1);

    TConnectorScanNode connector_scan_node;
    connector_scan_node.connector_name = connector::Connector::BINLOG;
    tnode->__set_connector_scan_node(connector_scan_node);

    return tnode;
}

DescriptorTbl* StreamOperatorsTest::_create_table_desc(int num_columns, int chunk_size) {
    TDescriptorTableBuilder desc_tbl_builder;
    TTupleDescriptorBuilder tuple_desc_builder;
    for (int i = 0; i < num_columns; i++) {
        TSlotDescriptorBuilder slot_desc_builder;
        slot_desc_builder.type(TYPE_BIGINT).length(8).nullable(false);
        tuple_desc_builder.add_slot(slot_desc_builder.build());
    }
    tuple_desc_builder.build(&desc_tbl_builder);

    DescriptorTbl* tbl = nullptr;
    DescriptorTbl::create(_runtime_state, _obj_pool, desc_tbl_builder.desc_tbl(), &tbl, chunk_size);
    _runtime_state->set_desc_tbl(tbl);
    return tbl;
}

std::vector<TScanRangeParams> StreamOperatorsTest::_create_binlog_scan_ranges(size_t degree_of_parallelism) {
    std::vector<TScanRangeParams> range_params;

    for (int i = 0; i < degree_of_parallelism; i++) {
        TBinlogOffset binlog_offset;
        binlog_offset.__set_tablet_id(i);
        binlog_offset.__set_version(-1);
        binlog_offset.__set_lsn(-1);

        TBinlogScanRange binlog_scan_range;
        binlog_scan_range.__set_db_name("test");
        binlog_scan_range.__set_table_id(0);
        binlog_scan_range.__set_partition_id(0);
        binlog_scan_range.__set_tablet_id(i);
        binlog_scan_range.__set_offset(binlog_offset);

        TScanRange scan_range;
        scan_range.__set_binlog_scan_range(binlog_scan_range);

        TScanRangeParams param;
        param.__set_scan_range(scan_range);
        range_params.push_back(param);
    }

    return range_params;
}

void StreamOperatorsTest::_generate_morse_queue(ConnectorScanNode* scan_node,
                                                const std::vector<TScanRangeParams>& scan_ranges,
                                                int degree_of_parallelism) {
    std::vector<TScanRangeParams> no_scan_ranges;
    pipeline::MorselQueueFactoryMap& morsel_queue_factories = _fragment_ctx->morsel_queue_factories();

    std::map<int32_t, std::vector<TScanRangeParams>> no_scan_ranges_per_driver_seq;
    auto morsel_queue_factory = scan_node->convert_scan_range_to_morsel_queue_factory(
            scan_ranges, no_scan_ranges_per_driver_seq, scan_node->id(), degree_of_parallelism, true,
            TTabletInternalParallelMode::type::AUTO);
    DCHECK(morsel_queue_factory.ok());
    morsel_queue_factories.emplace(scan_node->id(), std::move(morsel_queue_factory).value());
}

TEST_F(StreamOperatorsTest, Dop_1) {
    DCHECK_IF_ERROR(start_mv([&]() {
        _degree_of_parallelism = 1;
        _pipeline_builder = [&](RuntimeState* state) {
            OpFactories op_factories{
                    std::make_shared<GeneratorStreamSourceOperatorFactory>(
                            next_operator_id(), next_plan_node_id(),
                            GeneratorStreamSourceParam{.num_column = 2, .start = 0, .step = 1, .chunk_size = 4}),
                    std::make_shared<PrinterStreamSinkOperatorFactory>(next_operator_id(), next_plan_node_id()),
            };
            _pipelines.push_back(std::make_shared<pipeline::Pipeline>(next_pipeline_id(), op_factories));
        };
        return Status::OK();
    }));

    EpochInfo epoch_info{.epoch_id = 0, .trigger_mode = TriggerMode::MANUAL};
    DCHECK_IF_ERROR(start_epoch(_tablet_ids, epoch_info));
    DCHECK_IF_ERROR(wait_until_epoch_finished(epoch_info));
    CheckResult(fetch_results<PrinterStreamSinkOperator>(epoch_info), {{{1, 2, 3, 4}, {5, 6, 7, 8}}});

    stop_mv();
}

TEST_F(StreamOperatorsTest, MultiDop_4) {
    DCHECK_IF_ERROR(start_mv([&]() {
        _degree_of_parallelism = 4;
        _pipeline_builder = [&](RuntimeState* state) {
            OpFactories op_factories;
            auto source_factory = std::make_shared<GeneratorStreamSourceOperatorFactory>(
                    next_operator_id(), next_plan_node_id(),
                    GeneratorStreamSourceParam{
                            .num_column = 2, .start = 0, .step = 1, .chunk_size = 4, .ndv_count = 8});
            source_factory->set_degree_of_parallelism(_degree_of_parallelism);
            op_factories.emplace_back(std::move(source_factory));
            // add exchange node to gather multi source operator to one sink operator
            op_factories = maybe_interpolate_local_passthrough_exchange(op_factories);
            op_factories.emplace_back(
                    std::make_shared<PrinterStreamSinkOperatorFactory>(next_operator_id(), next_plan_node_id()));
            auto pipeline = std::make_shared<pipeline::Pipeline>(next_pipeline_id(), op_factories);
            _pipelines.push_back(std::move(pipeline));
        };
        return Status::OK();
    }));

    EpochInfo epoch_info{.epoch_id = 0, .trigger_mode = TriggerMode::MANUAL};
    DCHECK_IF_ERROR(start_epoch(_tablet_ids, epoch_info));
    DCHECK_IF_ERROR(wait_until_epoch_finished(epoch_info));
    CheckResult(fetch_results<PrinterStreamSinkOperator>(epoch_info), {{{1, 2, 3, 4}, {5, 6, 7, 0}}, // chunk 0
                                                                       {{1, 2, 3, 4}, {5, 6, 7, 0}}, // chunk 1
                                                                       {{1, 2, 3, 4}, {5, 6, 7, 0}},
                                                                       {{1, 2, 3, 4}, {5, 6, 7, 0}}});

    stop_mv();
}

TEST_F(StreamOperatorsTest, Test_StreamAggregator_Dop1) {
    DCHECK_IF_ERROR(start_mv([&]() {
        _degree_of_parallelism = 1;
        _pipeline_builder = [&](RuntimeState* state) {
            _slot_infos = std::vector<std::vector<SlotTypeInfo>>{
                    // input slots
                    {
                            {"col1", TYPE_BIGINT, false},
                            {"col2", TYPE_BIGINT, false},
                    },
                    // intermediate slots
                    {
                            {"col1", TYPE_BIGINT, false},
                            {"count_agg", TYPE_BIGINT, false},
                    },
                    // result slots
                    {
                            {"col1", TYPE_BIGINT, false},
                            {"count_agg", TYPE_BIGINT, false},
                    },
            };
            _group_by_infos = {0};
            _agg_infos = std::vector<AggInfo>{// slot_index, agg_name, agg_intermediate_type, agg_result_type
                                              {1, "count", TYPE_BIGINT, TYPE_BIGINT}};

            _tbl = GenerateDescTbl(_runtime_state, (*_obj_pool), _slot_infos);
            _runtime_state->set_desc_tbl(_tbl);
            _stream_aggregator = _create_stream_aggregator(_slot_infos, _group_by_infos, _agg_infos, false, 0);
            OpFactories op_factories{
                    std::make_shared<GeneratorStreamSourceOperatorFactory>(
                            next_operator_id(), next_plan_node_id(),
                            GeneratorStreamSourceParam{
                                    .num_column = 2, .start = 0, .step = 1, .chunk_size = 4, .ndv_count = 4}),
                    std::make_shared<StreamAggregateOperatorFactory>(next_operator_id(), next_plan_node_id(),
                                                                     _stream_aggregator),
                    std::make_shared<PrinterStreamSinkOperatorFactory>(next_operator_id(), next_plan_node_id()),
            };
            _pipelines.push_back(std::make_shared<pipeline::Pipeline>(next_pipeline_id(), op_factories));
        };
        return Status::OK();
    }));

    for (auto i = 0; i < 3; i++) {
        EpochInfo epoch_info{.epoch_id = i, .trigger_mode = TriggerMode::MANUAL};
        DCHECK_IF_ERROR(start_epoch(_tablet_ids, epoch_info));
        DCHECK_IF_ERROR(wait_until_epoch_finished(epoch_info));
        CheckResult(fetch_results<PrinterStreamSinkOperator>(epoch_info),
                    {{{1, 2, 3, 0}, {i + 1, i + 1, i + 1, i + 1}}});
    }

    stop_mv();
}

TEST_F(StreamOperatorsTest, Test_StreamAggregator_MultiDop) {
    DCHECK_IF_ERROR(start_mv([&]() {
        _degree_of_parallelism = 4;
        _pipeline_builder = [&](RuntimeState* state) {
            _slot_infos = std::vector<std::vector<SlotTypeInfo>>{
                    // input slots
                    {
                            {"col1", TYPE_BIGINT, false},
                            {"col2", TYPE_BIGINT, false},
                    },
                    // intermediate slots
                    {
                            {"col1", TYPE_BIGINT, false},
                            {"count_agg", TYPE_BIGINT, false},
                    },
                    // result slots
                    {
                            {"col1", TYPE_BIGINT, false},
                            {"count_agg", TYPE_BIGINT, false},
                    },
            };
            _group_by_infos = {0};
            _agg_infos = std::vector<AggInfo>{// slot_index, agg_name, agg_intermediate_type, agg_result_type
                                              {1, "count", TYPE_BIGINT, TYPE_BIGINT}};

            _tbl = GenerateDescTbl(_runtime_state, (*_obj_pool), _slot_infos);
            _runtime_state->set_desc_tbl(_tbl);
            _stream_aggregator = _create_stream_aggregator(_slot_infos, _group_by_infos, _agg_infos, false, 0);
            OpFactories op_factories;
            auto source_factory = std::make_shared<GeneratorStreamSourceOperatorFactory>(
                    next_operator_id(), next_plan_node_id(),
                    GeneratorStreamSourceParam{
                            .num_column = 2, .start = 0, .step = 1, .chunk_size = 4, .ndv_count = 8});
            source_factory->set_degree_of_parallelism(_degree_of_parallelism);
            op_factories.emplace_back(std::move(source_factory));
            // add exchange node to gather multi source operator to one sink operator
            op_factories = maybe_interpolate_local_passthrough_exchange(op_factories);
            op_factories.emplace_back(std::make_shared<StreamAggregateOperatorFactory>(
                    next_operator_id(), next_plan_node_id(), _stream_aggregator));
            op_factories.emplace_back(
                    std::make_shared<PrinterStreamSinkOperatorFactory>(next_operator_id(), next_plan_node_id()));
            _pipelines.push_back(std::make_shared<pipeline::Pipeline>(next_pipeline_id(), op_factories));
        };
        return Status::OK();
    }));

    for (auto i = 0; i < 10; i++) {
        EpochInfo epoch_info{.epoch_id = i, .trigger_mode = TriggerMode::MANUAL};
        DCHECK_IF_ERROR(start_epoch(_tablet_ids, epoch_info));
        DCHECK_IF_ERROR(wait_until_epoch_finished(epoch_info));
        CheckResult(fetch_results<PrinterStreamSinkOperator>(epoch_info),
                    {{{1, 2, 3, 4}, {(i + 1) * 4, (i + 1) * 4, (i + 1) * 4, (i + 1) * 4}}});
        sleep(0.5);
    }
    stop_mv();
}

TEST_F(StreamOperatorsTest, binlog_dop_1) {
    DCHECK_IF_ERROR(start_mv([&]() {
        _degree_of_parallelism = 1;
        _pipeline_builder = [&](RuntimeState* state) {
            auto* descs = _create_table_desc(2, 4);
            auto tnode = _create_tplan_node(next_plan_node_id(), 0);
            auto binlog_scan_node = std::make_shared<starrocks::ConnectorScanNode>(_obj_pool, *tnode, *descs);
            _connector_node = binlog_scan_node;
            Status status = binlog_scan_node->init(*tnode, _runtime_state);
            auto scan_ranges = _create_binlog_scan_ranges(_degree_of_parallelism);
            _generate_morse_queue(binlog_scan_node.get(), scan_ranges, _degree_of_parallelism);
            OpFactories op_factories = binlog_scan_node->decompose_to_pipeline(_pipeline_context);
            for (int i = 0; i < _degree_of_parallelism; i++) {
                _tablet_ids.push_back(i);
            }

            op_factories.push_back(
                    std::make_shared<PrinterStreamSinkOperatorFactory>(next_operator_id(), next_plan_node_id()));
            _pipelines.push_back(std::make_shared<pipeline::Pipeline>(next_pipeline_id(), op_factories));
        };
        return Status::OK();
    }));

    EpochInfo epoch_info{
            .epoch_id = 0, .max_exec_millis = -1, .max_scan_rows = -1, .trigger_mode = TriggerMode::MANUAL};
    DCHECK_IF_ERROR(start_epoch(_tablet_ids, epoch_info));
    DCHECK_IF_ERROR(wait_until_epoch_finished(epoch_info));
    CheckResult(fetch_results<PrinterStreamSinkOperator>(epoch_info), {{{1, 2, 3, 4}, {5, 6, 7, 8}}});
    stop_mv();
}

TEST_F(StreamOperatorsTest, binlog_dop_1_multi_epoch) {
    DCHECK_IF_ERROR(start_mv([&]() {
        _degree_of_parallelism = 1;
        _pipeline_builder = [&](RuntimeState* state) {
            auto* descs = _create_table_desc(2, 4);
            auto tnode = _create_tplan_node(next_plan_node_id(), 0);
            auto binlog_scan_node = std::make_shared<starrocks::ConnectorScanNode>(_obj_pool, *tnode, *descs);
            _connector_node = binlog_scan_node;
            Status status = binlog_scan_node->init(*tnode, _runtime_state);
            auto scan_ranges = _create_binlog_scan_ranges(_degree_of_parallelism);
            _generate_morse_queue(binlog_scan_node.get(), scan_ranges, _degree_of_parallelism);
            OpFactories op_factories = binlog_scan_node->decompose_to_pipeline(_pipeline_context);
            for (int i = 0; i < _degree_of_parallelism; i++) {
                _tablet_ids.push_back(i);
            }

            op_factories.push_back(
                    std::make_shared<PrinterStreamSinkOperatorFactory>(next_operator_id(), next_plan_node_id()));
            _pipelines.push_back(std::make_shared<pipeline::Pipeline>(next_pipeline_id(), op_factories));
        };
        return Status::OK();
    }));

    for (auto i = 0; i < 3; i++) {
        EpochInfo epoch_info{
                .epoch_id = 0, .max_exec_millis = -1, .max_scan_rows = -1, .trigger_mode = TriggerMode::MANUAL};
        DCHECK_IF_ERROR(start_epoch(_tablet_ids, epoch_info));
        DCHECK_IF_ERROR(wait_until_epoch_finished(epoch_info));
        CheckResult(fetch_results<PrinterStreamSinkOperator>(epoch_info), {{{1, 2, 3, 4}, {5, 6, 7, 8}}});
    }

    stop_mv();
}

} // namespace starrocks::stream