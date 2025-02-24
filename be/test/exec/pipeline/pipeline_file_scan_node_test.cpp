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

// #include "exec/file_scan_node.h"
#include <gtest/gtest.h>

#include <memory>
#include <mutex>
#include <random>
#include <utility>

#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/vectorized_fwd.h"
#include "exec/connector_scan_node.h"
#include "exec/pipeline/exchange/local_exchange.h"
#include "exec/pipeline/exchange/local_exchange_sink_operator.h"
#include "exec/pipeline/exchange/local_exchange_source_operator.h"
#include "exec/pipeline/group_execution/execution_group_fwd.h"
#include "exec/pipeline/pipeline.h"
#include "exec/pipeline/pipeline_builder.h"
#include "exec/pipeline/pipeline_driver_executor.h"
#include "exec/pipeline/scan/connector_scan_operator.h"
#include "gen_cpp/InternalService_types.h"
#include "gtest/gtest.h"
#include "gutil/map_util.h"
#include "pipeline_test_base.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "storage/storage_engine.h"
#include "testutil/assert.h"
#include "util/defer_op.h"
#include "util/disk_info.h"
#include "util/mem_info.h"
#include "util/thrift_util.h"

// TODO: test multi thread
// TODO: test runtime filter
namespace starrocks::pipeline {

static const size_t degree_of_parallelism = 1;

class PipeLineFileScanNodeTest : public ::testing::Test {
public:
    void SetUp() override {
        config::enable_system_metrics = false;
        config::enable_metric_calculator = false;

        _exec_env = ExecEnv::GetInstance();

        const auto& params = _request.params;
        const auto& query_id = params.query_id;
        const auto& fragment_id = params.fragment_instance_id;

        ASSIGN_OR_ASSERT_FAIL(_query_ctx, _exec_env->query_context_mgr()->get_or_register(query_id));
        _query_ctx->set_total_fragments(1);
        _query_ctx->set_delivery_expire_seconds(60);
        _query_ctx->set_query_expire_seconds(60);
        _query_ctx->extend_delivery_lifetime();
        _query_ctx->extend_query_lifetime();
        _query_ctx->init_mem_tracker(GlobalEnv::GetInstance()->query_pool_mem_tracker()->limit(),
                                     GlobalEnv::GetInstance()->query_pool_mem_tracker());
        _query_ctx->set_query_trace(std::make_shared<starrocks::debug::QueryTrace>(query_id, false));

        _fragment_ctx = _query_ctx->fragment_mgr()->get_or_register(fragment_id);
        _fragment_ctx->set_query_id(query_id);
        _fragment_ctx->set_fragment_instance_id(fragment_id);
        _fragment_ctx->set_runtime_state(
                std::make_unique<RuntimeState>(_request.params.query_id, _request.params.fragment_instance_id,
                                               _request.query_options, _request.query_globals, _exec_env));

        _fragment_future = _fragment_ctx->finish_future();
        _runtime_state = _fragment_ctx->runtime_state();

        _runtime_state->set_chunk_size(config::vector_chunk_size);
        _runtime_state->init_mem_trackers(_query_ctx->mem_tracker());
        _runtime_state->set_be_number(_request.backend_num);
        _runtime_state->set_query_ctx(_query_ctx);
        _runtime_state->set_fragment_ctx(_fragment_ctx);
        _pool = _runtime_state->obj_pool();
        auto sink_dop = degree_of_parallelism;
        _context = _pool->add(new PipelineBuilderContext(_fragment_ctx, degree_of_parallelism, sink_dop, false));
        _builder = _pool->add(new PipelineBuilder(*_context));
    }

    void TearDown() override {}

private:
    std::shared_ptr<TPlanNode> _create_tplan_node();

    DescriptorTbl* _create_table_desc(const std::vector<TypeDescriptor>& types);

    std::vector<TScanRangeParams> _create_csv_scan_ranges(const std::vector<TypeDescriptor>& types,
                                                          const string& multi_row_delimiter = "\n",
                                                          const string& multi_column_separator = "|");

    static ChunkPtr _create_chunk(const std::vector<TypeDescriptor>& types);

    void prepare_pipeline();

    void execute_pipeline();

    void generate_morse_queue(const std::vector<starrocks::ConnectorScanNode*>& scan_nodes,
                              const std::vector<TScanRangeParams>& scan_ranges);

    RuntimeState* _runtime_state = nullptr;
    OlapTableDescriptor* _table_desc = nullptr;
    ObjectPool* _pool = nullptr;
    std::shared_ptr<MemTracker> _mem_tracker = nullptr;
    ExecEnv* _exec_env = nullptr;

    QueryContext* _query_ctx = nullptr;
    FragmentContext* _fragment_ctx = nullptr;
    FragmentFuture _fragment_future;
    TExecPlanFragmentParams _request;

    PipelineBuilderContext* _context;
    PipelineBuilder* _builder;

    std::string _file = "./be/test/exec/test_data/csv_scanner/csv_file1";
    Pipelines _pipelines;
    ExecutionGroupPtr exec_group;
};

ChunkPtr PipeLineFileScanNodeTest::_create_chunk(const std::vector<TypeDescriptor>& types) {
    ChunkPtr chunk = std::make_shared<Chunk>();
    for (int i = 0; i < types.size(); i++) {
        chunk->append_column(ColumnHelper::create_column(types[i], true), i);
    }
    return chunk;
}

std::vector<TScanRangeParams> PipeLineFileScanNodeTest::_create_csv_scan_ranges(
        const std::vector<TypeDescriptor>& types, const string& multi_row_delimiter,
        const string& multi_column_separator) {
    /// TBrokerScanRangeParams
    TBrokerScanRangeParams* params = _pool->add(new TBrokerScanRangeParams());
    params->__set_multi_row_delimiter(multi_row_delimiter);
    params->__set_multi_column_separator(multi_column_separator);
    params->strict_mode = true;
    params->dest_tuple_id = 0;
    params->src_tuple_id = 0;
    for (int i = 0; i < types.size(); i++) {
        params->expr_of_dest_slot[i] = TExpr();
        params->expr_of_dest_slot[i].nodes.emplace_back(TExprNode());
        params->expr_of_dest_slot[i].nodes[0].__set_type(types[i].to_thrift());
        params->expr_of_dest_slot[i].nodes[0].__set_node_type(TExprNodeType::SLOT_REF);
        params->expr_of_dest_slot[i].nodes[0].__set_is_nullable(true);
        params->expr_of_dest_slot[i].nodes[0].__set_slot_ref(TSlotRef());
        params->expr_of_dest_slot[i].nodes[0].slot_ref.__set_slot_id(i);
    }

    for (int i = 0; i < types.size(); i++) {
        params->src_slot_ids.emplace_back(i);
    }

    std::vector<TBrokerRangeDesc>* ranges = _pool->add(new vector<TBrokerRangeDesc>());

    TBrokerRangeDesc* range = _pool->add(new TBrokerRangeDesc());
    range->__set_path(_file);
    range->__set_start_offset(0);
    range->__set_num_of_columns_from_file(types.size());
    ranges->push_back(*range);

    TBrokerScanRange* broker_scan_range = _pool->add(new TBrokerScanRange());
    broker_scan_range->params = *params;
    broker_scan_range->ranges = *ranges;

    TScanRange scan_range;
    scan_range.__set_broker_scan_range(*broker_scan_range);

    TScanRangeParams param;
    param.__set_scan_range(scan_range);

    return std::vector<TScanRangeParams>{param};
}

std::shared_ptr<TPlanNode> PipeLineFileScanNodeTest::_create_tplan_node() {
    std::vector<::starrocks::TTupleId> tuple_ids{0};

    auto tnode = std::make_shared<TPlanNode>();

    tnode->__set_node_id(1);
    tnode->__set_node_type(TPlanNodeType::FILE_SCAN_NODE);
    tnode->__set_row_tuples(tuple_ids);
    tnode->__set_limit(-1);

    TConnectorScanNode connector_scan_node;
    connector_scan_node.connector_name = connector::Connector::FILE;
    tnode->__set_connector_scan_node(connector_scan_node);

    return tnode;
}

DescriptorTbl* PipeLineFileScanNodeTest::_create_table_desc(const std::vector<TypeDescriptor>& types) {
    /// Init DescriptorTable
    TDescriptorTableBuilder desc_tbl_builder;
    TTupleDescriptorBuilder tuple_desc_builder;
    for (auto& t : types) {
        TSlotDescriptorBuilder slot_desc_builder;
        slot_desc_builder.type(t).length(t.len).precision(t.precision).scale(t.scale).nullable(true);
        tuple_desc_builder.add_slot(slot_desc_builder.build());
    }
    tuple_desc_builder.build(&desc_tbl_builder);

    DescriptorTbl* tbl = nullptr;
    CHECK(DescriptorTbl::create(_runtime_state, _pool, desc_tbl_builder.desc_tbl(), &tbl, config::vector_chunk_size)
                  .ok());

    _runtime_state->set_desc_tbl(tbl);
    return tbl;
}

void PipeLineFileScanNodeTest::prepare_pipeline() {
    // const auto& params = _request.params;
    ASSERT_TRUE(_fragment_ctx->prepare_all_pipelines().ok());

    MorselQueueFactoryMap& morsel_queues = _fragment_ctx->morsel_queue_factories();

    _fragment_ctx->iterate_pipeline([&morsel_queues](auto pipeline) {
        if (pipeline->source_operator_factory()->with_morsels()) {
            auto source_id = pipeline->source_operator_factory()->plan_node_id();
            DCHECK(morsel_queues.count(source_id));
            auto& morsel_queue_factory = morsel_queues[source_id];

            pipeline->source_operator_factory()->set_morsel_queue_factory(morsel_queue_factory.get());
        }
    });

    _fragment_ctx->iterate_pipeline(
            [this](auto pipeline) { pipeline->instantiate_drivers(_fragment_ctx->runtime_state()); });
}

void PipeLineFileScanNodeTest::execute_pipeline() {
    _fragment_ctx->iterate_drivers(
            [state = _fragment_ctx->runtime_state()](const DriverPtr& driver) { return driver->prepare(state); });

    _fragment_ctx->iterate_drivers([exec_env = _exec_env](const DriverPtr& driver) {
        LOG(WARNING) << driver->to_readable_string();
        exec_env->wg_driver_executor()->submit(driver.get());
    });
}

void PipeLineFileScanNodeTest::generate_morse_queue(const std::vector<starrocks::ConnectorScanNode*>& scan_nodes,
                                                    const std::vector<TScanRangeParams>& scan_ranges) {
    std::vector<TScanRangeParams> no_scan_ranges;
    MorselQueueFactoryMap& morsel_queue_factories = _fragment_ctx->morsel_queue_factories();

    std::map<int32_t, std::vector<TScanRangeParams>> no_scan_ranges_per_driver_seq;
    for (auto& i : scan_nodes) {
        auto* scan_node = (ScanNode*)(i);
        auto morsel_queue_factory = scan_node->convert_scan_range_to_morsel_queue_factory(
                scan_ranges, no_scan_ranges_per_driver_seq, scan_node->id(), degree_of_parallelism, false, true,
                TTabletInternalParallelMode::type::AUTO);
        DCHECK(morsel_queue_factory.ok());
        morsel_queue_factories.emplace(scan_node->id(), std::move(morsel_queue_factory).value());
    }
}

#define ASSERT_COUNTER_CHUNK_NUM(counter, expected_push_chunk_num, expected_pull_chunk_num) \
    do {                                                                                    \
        ASSERT_EQ(expected_push_chunk_num, counter->push_chunk_num());                      \
        ASSERT_EQ(expected_pull_chunk_num, counter->pull_chunk_num());                      \
    } while (false)

#define ASSERT_COUNTER_CHUNK_ROW_NUM(counter, expected_push_chunk_row_num, expected_pull_chunk_row_num) \
    do {                                                                                                \
        ASSERT_EQ(expected_push_chunk_row_num, counter->push_chunk_row_num());                          \
        ASSERT_EQ(expected_pull_chunk_row_num, counter->pull_chunk_row_num());                          \
    } while (false)

class FileScanCounter {
public:
    void process_push(const ChunkPtr& chunk) {
        std::lock_guard<std::mutex> l(_mutex);
        ++_push_chunk_num;
        _push_chunk_row_num += chunk->num_rows();
    }

    void process_pull(const ChunkPtr& chunk) {
        std::lock_guard<std::mutex> l(_mutex);
        ++_pull_chunk_num;
        _pull_chunk_row_num += chunk->num_rows();
    }

    size_t push_chunk_num() {
        std::lock_guard<std::mutex> l(_mutex);
        return _push_chunk_num;
    }

    size_t pull_chunk_num() {
        std::lock_guard<std::mutex> l(_mutex);
        return _pull_chunk_num;
    }

    size_t push_chunk_row_num() {
        std::lock_guard<std::mutex> l(_mutex);
        return _push_chunk_row_num;
    }

    size_t pull_chunk_row_num() {
        std::lock_guard<std::mutex> l(_mutex);
        return _pull_chunk_row_num;
    }

private:
    std::mutex _mutex;
    size_t _push_chunk_num = 0;
    size_t _pull_chunk_num = 0;
    size_t _push_chunk_row_num = 0;
    size_t _pull_chunk_row_num = 0;
};

using CounterPtr = std::shared_ptr<FileScanCounter>;

class TestFileScanSinkOperator : public Operator {
public:
    TestFileScanSinkOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence,
                             CounterPtr counter)
            : Operator(factory, id, "test_sink", plan_node_id, false, driver_sequence), _counter(std::move(counter)) {}
    ~TestFileScanSinkOperator() override = default;

    Status prepare(RuntimeState* state) override {
        RETURN_IF_ERROR(Operator::prepare(state));
        return Status::OK();
    }

    Status set_finishing(RuntimeState* state) override {
        _is_finished = true;
        return Status::OK();
    }

    void close(RuntimeState* state) override { return Operator::close(state); }

    bool need_input() const override { return true; }

    bool has_output() const override { return false; }

    bool is_finished() const override { return _is_finished; }

    Status push_chunk(RuntimeState* state, const ChunkPtr& chunk) override;

    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override;

private:
    CounterPtr _counter;
    bool _is_finished = false;
};

Status TestFileScanSinkOperator::push_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    _counter->process_push(chunk);
    return Status::OK();
}

StatusOr<ChunkPtr> TestFileScanSinkOperator::pull_chunk(RuntimeState* state) {
    return Status::InternalError("Shouldn't pull chunk to sink operator");
}

class TestFileScanSinkOperatorFactory final : public OperatorFactory {
public:
    TestFileScanSinkOperatorFactory(int32_t id, int32_t plan_node_id, CounterPtr counter)
            : OperatorFactory(id, "test_sink", plan_node_id), _counter(std::move(counter)) {}

    ~TestFileScanSinkOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<TestFileScanSinkOperator>(this, _id, _plan_node_id, driver_sequence, _counter);
    }

private:
    CounterPtr _counter;
};

TEST_F(PipeLineFileScanNodeTest, CSVBasic) {
    std::vector<TypeDescriptor> types;
    types.emplace_back(TYPE_INT);
    types.emplace_back(TYPE_DOUBLE);
    types.emplace_back(TYPE_VARCHAR);
    types.emplace_back(TYPE_DATE);
    types.emplace_back(TYPE_VARCHAR);

    auto tnode = _create_tplan_node();
    auto* descs = _create_table_desc(types);
    auto file_scan_node = _pool->add(new starrocks::ConnectorScanNode(_pool, *tnode, *descs));

    Status status = file_scan_node->init(*tnode, _runtime_state);
    ASSERT_TRUE(status.ok());

    auto scan_ranges = _create_csv_scan_ranges(types);
    generate_morse_queue({file_scan_node}, scan_ranges);

    starrocks::pipeline::CounterPtr sinkCounter = std::make_shared<starrocks::pipeline::FileScanCounter>();

    exec_group = ExecutionGroupBuilder::create_normal_exec_group();

    OpFactories op_factories = file_scan_node->decompose_to_pipeline(_context);

    op_factories.push_back(std::make_shared<starrocks::pipeline::TestFileScanSinkOperatorFactory>(
            _context->next_operator_id(), 0, sinkCounter));

    _pipelines.push_back(
            std::make_shared<starrocks::pipeline::Pipeline>(_context->next_pipe_id(), op_factories, exec_group.get()));
    exec_group->add_pipeline(_pipelines.back().get());
    auto pipelines = _pipelines;
    _fragment_ctx->set_pipelines({exec_group}, std::move(pipelines));

    prepare_pipeline();

    execute_pipeline();

    ASSERT_EQ(std::future_status::ready, _fragment_future.wait_for(std::chrono::seconds(15)));

    ASSERT_COUNTER_CHUNK_ROW_NUM(sinkCounter, 3, 0);
}
} // namespace starrocks::pipeline
