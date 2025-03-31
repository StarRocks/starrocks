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

#include "pipeline_test_base.h"

#include <random>

#include "column/nullable_column.h"
#include "common/config.h"
#include "exec/pipeline/fragment_context.h"
#include "exec/pipeline/group_execution/execution_group_builder.h"
#include "exec/pipeline/pipeline_driver_executor.h"
#include "exec/workgroup/work_group.h"
#include "exprs/function_context.h"
#include "storage/chunk_helper.h"
#include "testutil/assert.h"
#include "types/date_value.h"
#include "types/timestamp_value.h"
#include "util/thrift_util.h"

namespace starrocks::pipeline {

void PipelineTestBase::SetUp() {}

void PipelineTestBase::TearDown() {}

void PipelineTestBase::start_test() {
    _prepare();
    _execute();
}

OpFactories PipelineTestBase::maybe_interpolate_local_passthrough_exchange(OpFactories& pred_operators) {
    DCHECK(!pred_operators.empty() && pred_operators[0]->is_source());
    auto* source_operator = down_cast<SourceOperatorFactory*>(pred_operators[0].get());
    if (source_operator->degree_of_parallelism() > 1) {
        auto pseudo_plan_node_id = -200;
        auto mem_mgr = std::make_shared<ChunkBufferMemoryManager>(_vector_chunk_size,
                                                                  config::local_exchange_buffer_mem_limit_per_driver);
        auto local_exchange_source =
                std::make_shared<LocalExchangeSourceOperatorFactory>(next_operator_id(), pseudo_plan_node_id, mem_mgr);
        auto local_exchange = std::make_shared<PassthroughExchanger>(mem_mgr, local_exchange_source.get());
        auto local_exchange_sink = std::make_shared<LocalExchangeSinkOperatorFactory>(
                next_operator_id(), pseudo_plan_node_id, local_exchange);
        // Add LocalExchangeSinkOperator to predecessor pipeline.
        pred_operators.emplace_back(std::move(local_exchange_sink));
        // predecessor pipeline comes to end.
        _pipelines.emplace_back(std::make_unique<Pipeline>(next_pipeline_id(), pred_operators,
                                                           _fragment_ctx->_execution_groups[0].get()));

        OpFactories operators_source_with_local_exchange;
        // Multiple LocalChangeSinkOperators pipe into one LocalChangeSourceOperator.
        local_exchange_source->set_degree_of_parallelism(1);
        // A new pipeline is created, LocalExchangeSourceOperator is added as the head of the pipeline.
        operators_source_with_local_exchange.emplace_back(std::move(local_exchange_source));
        return operators_source_with_local_exchange;
    } else {
        return pred_operators;
    }
}

void PipelineTestBase::_prepare() {
    _exec_env = ExecEnv::GetInstance();

    _prepare_request();

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
    _fragment_ctx->set_workgroup(ExecEnv::GetInstance()->workgroup_manager()->get_default_workgroup());

    _fragment_future = _fragment_ctx->finish_future();
    _runtime_state = _fragment_ctx->runtime_state();

    _runtime_state->set_chunk_size(_vector_chunk_size);
    _runtime_state->init_mem_trackers(_query_ctx->mem_tracker());
    _runtime_state->set_be_number(_request.backend_num);
    _runtime_state->set_query_ctx(_query_ctx);
    _runtime_state->set_fragment_ctx(_fragment_ctx);

    _obj_pool = _runtime_state->obj_pool();

    ASSERT_TRUE(_pipeline_builder != nullptr);
    exec_group = ExecutionGroupBuilder::create_normal_exec_group();
    _pipeline_builder(_fragment_ctx->runtime_state());
    for (auto pipeline : _pipelines) {
        exec_group->add_pipeline(std::move(pipeline.get()));
    }
    _fragment_ctx->set_pipelines({exec_group}, std::move(_pipelines));
    _pipelines.clear();
    ASSERT_TRUE(_fragment_ctx->prepare_all_pipelines().ok());
    _fragment_ctx->iterate_pipeline([this](Pipeline* pipeline) { pipeline->instantiate_drivers(_runtime_state); });
}

void PipelineTestBase::_execute() {
    _fragment_ctx->iterate_drivers(
            [state = _fragment_ctx->runtime_state()](const DriverPtr& driver) { CHECK_OK(driver->prepare(state)); });

    _fragment_ctx->iterate_drivers(
            [exec_env = _exec_env](const DriverPtr& driver) { exec_env->wg_driver_executor()->submit(driver.get()); });
}

ChunkPtr PipelineTestBase::_create_and_fill_chunk(const std::vector<SlotDescriptor*>& slots, size_t row_num) {
    static std::default_random_engine e;
    static std::uniform_int_distribution<int8_t> u8;
    static std::uniform_int_distribution<int16_t> u16;
    static std::uniform_int_distribution<int32_t> u32;
    static std::uniform_int_distribution<int64_t> u64;
    static std::uniform_int_distribution<__int128_t> u128;
    static std::uniform_real_distribution<float> uf;
    static std::uniform_real_distribution<double> ud;

    auto chunk = ChunkHelper::new_chunk(slots, row_num);

    // add data
    for (size_t i = 0; i < slots.size(); ++i) {
        auto* slot = slots[i];
        auto& column = chunk->columns()[i];

        Column* data_column = column.get();
        if (data_column->is_nullable()) {
            auto* nullable_column = down_cast<NullableColumn*>(data_column);
            data_column = nullable_column->data_column().get();
        }

        for (size_t j = 0; j < row_num; ++j) {
            switch (slot->type().type) {
            case TYPE_VARCHAR:
            case TYPE_CHAR:
                data_column->append_datum("var");
                break;
            case TYPE_BOOLEAN:
                data_column->append_datum(true);
                break;
            case TYPE_TINYINT:
                data_column->append_datum(u8(e));
                break;
            case TYPE_SMALLINT:
                data_column->append_datum(u16(e));
                break;
            case TYPE_INT:
                data_column->append_datum(u32(e));
                break;
            case TYPE_BIGINT:
                data_column->append_datum(u64(e));
                break;
            case TYPE_LARGEINT:
                data_column->append_datum(u128(e));
                break;
            case TYPE_FLOAT:
                data_column->append_datum(uf(e));
                break;
            case TYPE_DOUBLE:
                data_column->append_datum(ud(e));
                break;
            case TYPE_DATE: {
                DateValue value;
                data_column->append_datum(value);
                break;
            }
            case TYPE_DATETIME: {
                TimestampValue value;
                data_column->append_datum(value);
                break;
            }
            case TYPE_DECIMALV2: {
                DecimalV2Value value;
                data_column->append_datum(value);
                break;
            }
            case TYPE_DECIMAL32:
                data_column->append_datum(u32(e));
                break;
            case TYPE_DECIMAL64:
                data_column->append_datum(u64(e));
                break;
            case TYPE_DECIMAL128:
                data_column->append_datum(u128(e));
                break;
            default:
                break;
            }
        }
    }

    return chunk;
}

ChunkPtr PipelineTestBase::_create_and_fill_chunk(size_t row_num) {
    // the following content is TDescriptorTable serialized in json format
    // CREATE TABLE IF NOT EXISTS `test_aggregate` (
    //   `id_int` INT(11) NOT NULL,
    //   `id_tinyint` TINYINT NOT NULL,
    //   `id_smallint` SMALLINT NOT NULL,
    //   `id_bigint` BIGINT NOT NULL,
    //   `id_largeint` LARGEINT NOT NULL,
    //   `id_float` FLOAT NOT NULL,
    //   `id_double` DOUBLE NOT NULL,
    //   `id_char` CHAR(10) NOT NULL,
    //   `id_varchar` VARCHAR(100) NOT NULL,
    //   `id_date` DATE NOT NULL,
    //   `id_datetime` DATETIME NOT NULL,
    //   `id_decimal` DECIMAL(9,0) NOT NULL
    // );
    std::string content =
            "{\"1\":{\"lst\":[\"rec\",21,{\"1\":{\"i32\":1},\"2\":{\"i32\":0},\"3\":{\"rec\":{\"1\":{\"lst\":["
            "\"rec\",1,{\"1\":{\"i32\":0},\"2\":{\"rec\":{\"1\":{\"i32\":6}}}}]}}},\"4\":{\"i32\":-1},\"5\":{"
            "\"i32\":0},\"6\":{\"i32\":0},\"7\":{\"i32\":-1},\"8\":{\"str\":\"table_id\"},\"9\":{\"i32\":0},\"10\":"
            "{\"tf\":1}},{\"1\":{\"i32\":2},\"2\":{\"i32\":0},\"3\":{\"rec\":{\"1\":{\"lst\":[\"rec\",1,{\"1\":{"
            "\"i32\":0},\"2\":{\"rec\":{\"1\":{\"i32\":15},\"2\":{\"i32\":65530}}}}]}}},\"4\":{\"i32\":-1},\"5\":{"
            "\"i32\":48},\"6\":{\"i32\":0},\"7\":{\"i32\":-1},\"8\":{\"str\":\"column_name\"},\"9\":{\"i32\":6},"
            "\"10\":{\"tf\":1}},{\"1\":{\"i32\":3},\"2\":{\"i32\":0},\"3\":{\"rec\":{\"1\":{\"lst\":[\"rec\",1,{"
            "\"1\":{\"i32\":0},\"2\":{\"rec\":{\"1\":{\"i32\":6}}}}]}}},\"4\":{\"i32\":-1},\"5\":{\"i32\":8},\"6\":"
            "{\"i32\":0},\"7\":{\"i32\":-1},\"8\":{\"str\":\"db_id\"},\"9\":{\"i32\":1},\"10\":{\"tf\":1}},{\"1\":{"
            "\"i32\":6},\"2\":{\"i32\":0},\"3\":{\"rec\":{\"1\":{\"lst\":[\"rec\",1,{\"1\":{\"i32\":0},\"2\":{"
            "\"rec\":{\"1\":{\"i32\":6}}}}]}}},\"4\":{\"i32\":-1},\"5\":{\"i32\":16},\"6\":{\"i32\":0},\"7\":{"
            "\"i32\":-1},\"8\":{\"str\":\"row_count\"},\"9\":{\"i32\":2},\"10\":{\"tf\":1}},{\"1\":{\"i32\":7},"
            "\"2\":{\"i32\":0},\"3\":{\"rec\":{\"1\":{\"lst\":[\"rec\",1,{\"1\":{\"i32\":0},\"2\":{\"rec\":{\"1\":{"
            "\"i32\":6}}}}]}}},\"4\":{\"i32\":-1},\"5\":{\"i32\":24},\"6\":{\"i32\":0},\"7\":{\"i32\":-1},\"8\":{"
            "\"str\":\"data_size\"},\"9\":{\"i32\":3},\"10\":{\"tf\":1}},{\"1\":{\"i32\":8},\"2\":{\"i32\":0},"
            "\"3\":{\"rec\":{\"1\":{\"lst\":[\"rec\",1,{\"1\":{\"i32\":0},\"2\":{\"rec\":{\"1\":{\"i32\":6}}}}]}}},"
            "\"4\":{\"i32\":-1},\"5\":{\"i32\":32},\"6\":{\"i32\":0},\"7\":{\"i32\":-1},\"8\":{\"str\":\"distinct_"
            "count\"},\"9\":{\"i32\":4},\"10\":{\"tf\":1}},{\"1\":{\"i32\":9},\"2\":{\"i32\":0},\"3\":{\"rec\":{"
            "\"1\":{\"lst\":[\"rec\",1,{\"1\":{\"i32\":0},\"2\":{\"rec\":{\"1\":{\"i32\":6}}}}]}}},\"4\":{\"i32\":-"
            "1},\"5\":{\"i32\":40},\"6\":{\"i32\":0},\"7\":{\"i32\":-1},\"8\":{\"str\":\"null_count\"},\"9\":{"
            "\"i32\":5},\"10\":{\"tf\":1}},{\"1\":{\"i32\":10},\"2\":{\"i32\":0},\"3\":{\"rec\":{\"1\":{\"lst\":["
            "\"rec\",1,{\"1\":{\"i32\":0},\"2\":{\"rec\":{\"1\":{\"i32\":15},\"2\":{\"i32\":65530}}}}]}}},\"4\":{"
            "\"i32\":-1},\"5\":{\"i32\":64},\"6\":{\"i32\":0},\"7\":{\"i32\":-1},\"8\":{\"str\":\"max\"},\"9\":{"
            "\"i32\":7},\"10\":{\"tf\":1}},{\"1\":{\"i32\":11},\"2\":{\"i32\":0},\"3\":{\"rec\":{\"1\":{\"lst\":["
            "\"rec\",1,{\"1\":{\"i32\":0},\"2\":{\"rec\":{\"1\":{\"i32\":15},\"2\":{\"i32\":65530}}}}]}}},\"4\":{"
            "\"i32\":-1},\"5\":{\"i32\":80},\"6\":{\"i32\":0},\"7\":{\"i32\":-1},\"8\":{\"str\":\"min\"},\"9\":{"
            "\"i32\":8},\"10\":{\"tf\":1}},{\"1\":{\"i32\":12},\"2\":{\"i32\":0},\"3\":{\"rec\":{\"1\":{\"lst\":["
            "\"rec\",1,{\"1\":{\"i32\":0},\"2\":{\"rec\":{\"1\":{\"i32\":10}}}}]}}},\"4\":{\"i32\":-1},\"5\":{"
            "\"i32\":96},\"6\":{\"i32\":0},\"7\":{\"i32\":-1},\"8\":{\"str\":\"update_time\"},\"9\":{\"i32\":9},"
            "\"10\":{\"tf\":1}},{\"1\":{\"i32\":1},\"2\":{\"i32\":1},\"3\":{\"rec\":{\"1\":{\"lst\":[\"rec\",1,{"
            "\"1\":{\"i32\":0},\"2\":{\"rec\":{\"1\":{\"i32\":6}}}}]}}},\"4\":{\"i32\":-1},\"5\":{\"i32\":8},\"6\":"
            "{\"i32\":0},\"7\":{\"i32\":-1},\"8\":{\"str\":\"\"},\"9\":{\"i32\":1},\"10\":{\"tf\":1}},{\"1\":{"
            "\"i32\":2},\"2\":{\"i32\":1},\"3\":{\"rec\":{\"1\":{\"lst\":[\"rec\",1,{\"1\":{\"i32\":0},\"2\":{"
            "\"rec\":{\"1\":{\"i32\":15},\"2\":{\"i32\":-1}}}}]}}},\"4\":{\"i32\":-1},\"5\":{\"i32\":64},\"6\":{"
            "\"i32\":0},\"7\":{\"i32\":-1},\"8\":{\"str\":\"\"},\"9\":{\"i32\":7},\"10\":{\"tf\":1}},{\"1\":{"
            "\"i32\":3},\"2\":{\"i32\":1},\"3\":{\"rec\":{\"1\":{\"lst\":[\"rec\",1,{\"1\":{\"i32\":0},\"2\":{"
            "\"rec\":{\"1\":{\"i32\":6}}}}]}}},\"4\":{\"i32\":-1},\"5\":{\"i32\":16},\"6\":{\"i32\":0},\"7\":{"
            "\"i32\":-1},\"8\":{\"str\":\"\"},\"9\":{\"i32\":2},\"10\":{\"tf\":1}},{\"1\":{\"i32\":6},\"2\":{"
            "\"i32\":1},\"3\":{\"rec\":{\"1\":{\"lst\":[\"rec\",1,{\"1\":{\"i32\":0},\"2\":{\"rec\":{\"1\":{"
            "\"i32\":6}}}}]}}},\"4\":{\"i32\":-1},\"5\":{\"i32\":24},\"6\":{\"i32\":0},\"7\":{\"i32\":-1},\"8\":{"
            "\"str\":\"\"},\"9\":{\"i32\":3},\"10\":{\"tf\":1}},{\"1\":{\"i32\":7},\"2\":{\"i32\":1},\"3\":{"
            "\"rec\":{\"1\":{\"lst\":[\"rec\",1,{\"1\":{\"i32\":0},\"2\":{\"rec\":{\"1\":{\"i32\":6}}}}]}}},\"4\":{"
            "\"i32\":-1},\"5\":{\"i32\":32},\"6\":{\"i32\":0},\"7\":{\"i32\":-1},\"8\":{\"str\":\"\"},\"9\":{"
            "\"i32\":4},\"10\":{\"tf\":1}},{\"1\":{\"i32\":8},\"2\":{\"i32\":1},\"3\":{\"rec\":{\"1\":{\"lst\":["
            "\"rec\",1,{\"1\":{\"i32\":0},\"2\":{\"rec\":{\"1\":{\"i32\":6}}}}]}}},\"4\":{\"i32\":-1},\"5\":{"
            "\"i32\":40},\"6\":{\"i32\":0},\"7\":{\"i32\":-1},\"8\":{\"str\":\"\"},\"9\":{\"i32\":5},\"10\":{"
            "\"tf\":1}},{\"1\":{\"i32\":9},\"2\":{\"i32\":1},\"3\":{\"rec\":{\"1\":{\"lst\":[\"rec\",1,{\"1\":{"
            "\"i32\":0},\"2\":{\"rec\":{\"1\":{\"i32\":6}}}}]}}},\"4\":{\"i32\":-1},\"5\":{\"i32\":48},\"6\":{"
            "\"i32\":0},\"7\":{\"i32\":-1},\"8\":{\"str\":\"\"},\"9\":{\"i32\":6},\"10\":{\"tf\":1}},{\"1\":{"
            "\"i32\":10},\"2\":{\"i32\":1},\"3\":{\"rec\":{\"1\":{\"lst\":[\"rec\",1,{\"1\":{\"i32\":0},\"2\":{"
            "\"rec\":{\"1\":{\"i32\":15},\"2\":{\"i32\":-1}}}}]}}},\"4\":{\"i32\":-1},\"5\":{\"i32\":80},\"6\":{"
            "\"i32\":0},\"7\":{\"i32\":-1},\"8\":{\"str\":\"\"},\"9\":{\"i32\":8},\"10\":{\"tf\":1}},{\"1\":{"
            "\"i32\":11},\"2\":{\"i32\":1},\"3\":{\"rec\":{\"1\":{\"lst\":[\"rec\",1,{\"1\":{\"i32\":0},\"2\":{"
            "\"rec\":{\"1\":{\"i32\":15},\"2\":{\"i32\":-1}}}}]}}},\"4\":{\"i32\":-1},\"5\":{\"i32\":96},\"6\":{"
            "\"i32\":0},\"7\":{\"i32\":-1},\"8\":{\"str\":\"\"},\"9\":{\"i32\":9},\"10\":{\"tf\":1}},{\"1\":{"
            "\"i32\":12},\"2\":{\"i32\":1},\"3\":{\"rec\":{\"1\":{\"lst\":[\"rec\",1,{\"1\":{\"i32\":0},\"2\":{"
            "\"rec\":{\"1\":{\"i32\":10}}}}]}}},\"4\":{\"i32\":-1},\"5\":{\"i32\":112},\"6\":{\"i32\":0},\"7\":{"
            "\"i32\":-1},\"8\":{\"str\":\"\"},\"9\":{\"i32\":10},\"10\":{\"tf\":1}},{\"1\":{\"i32\":13},\"2\":{"
            "\"i32\":1},\"3\":{\"rec\":{\"1\":{\"lst\":[\"rec\",1,{\"1\":{\"i32\":0},\"2\":{\"rec\":{\"1\":{"
            "\"i32\":5}}}}]}}},\"4\":{\"i32\":-1},\"5\":{\"i32\":0},\"6\":{\"i32\":0},\"7\":{\"i32\":-1},\"8\":{"
            "\"str\":\"\"},\"9\":{\"i32\":0},\"10\":{\"tf\":1}}]},\"2\":{\"lst\":[\"rec\",2,{\"1\":{\"i32\":0},"
            "\"2\":{\"i32\":112},\"3\":{\"i32\":0},\"4\":{\"i64\":10030},\"5\":{\"i32\":0}},{\"1\":{\"i32\":1},"
            "\"2\":{\"i32\":128},\"3\":{\"i32\":0},\"5\":{\"i32\":0}}]},\"3\":{\"lst\":[\"rec\",1,{\"1\":{\"i64\":"
            "10030},\"2\":{\"i32\":1},\"3\":{\"i32\":12},\"4\":{\"i32\":0},\"7\":{\"str\":\"table_statistic_v1\"},"
            "\"8\":{\"str\":\"\"},\"11\":{\"rec\":{\"1\":{\"str\":\"table_statistic_v1\"}}}}]}}";

    TDescriptorTable tbl;
    const uint8_t* buf = reinterpret_cast<uint8_t*>(content.data());
    uint32_t len = content.size();
    CHECK(deserialize_thrift_msg(buf, &len, TProtocolType::JSON, &tbl).ok());

    std::vector<SlotDescriptor> slots;
    phmap::flat_hash_set<SlotId> seen_slots;
    for (auto& t_slot : tbl.slotDescriptors) {
        if (auto [_, is_new] = seen_slots.insert(t_slot.id); is_new) {
            slots.emplace_back(t_slot);
        }
    }

    std::vector<SlotDescriptor*> p_slots;
    for (auto& slot : slots) {
        p_slots.push_back(&slot);
    }

    return _create_and_fill_chunk(p_slots, row_num);
}

}; // namespace starrocks::pipeline
