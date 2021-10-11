// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "pipeline_test_base.h"

#include "column/nullable_column.h"
#include "runtime/date_value.h"
#include "runtime/timestamp_value.h"
#include "storage/vectorized/chunk_helper.h"
#include "udf/udf.h"
#include "util/thrift_util.h"

namespace starrocks::pipeline {

void PipelineTestBase::SetUp() {}

void PipelineTestBase::TearDown() {}

void PipelineTestBase::start_test() {
    _prepare();
    _execute();
}

void PipelineTestBase::_prepare() {
    _exec_env = ExecEnv::GetInstance();

    _prepare_request();

    const auto& params = _request.params;
    const auto& query_id = params.query_id;
    const auto& fragment_id = params.fragment_instance_id;

    _query_ctx = QueryContextManager::instance()->get_or_register(query_id);
    _query_ctx->set_total_fragments(1);
    _query_ctx->set_expire_seconds(60);

    _fragment_ctx = _query_ctx->fragment_mgr()->get_or_register(fragment_id);
    _fragment_ctx->set_query_id(query_id);
    _fragment_ctx->set_fragment_instance_id(fragment_id);
    _fragment_ctx->set_runtime_state(
            std::make_unique<RuntimeState>(_request, _request.query_options, _request.query_globals, _exec_env));

    _fragment_future = _fragment_ctx->finish_future();
    _runtime_state = _fragment_ctx->runtime_state();

    int64_t bytes_limit = _request.query_options.mem_limit;
    _fragment_ctx->set_mem_tracker(std::make_unique<MemTracker>(bytes_limit, "pipeline test mem-limit",
                                                                _exec_env->query_pool_mem_tracker(), true));

    auto mem_tracker = _fragment_ctx->mem_tracker();

    _runtime_state->set_batch_size(config::vector_chunk_size);
    ASSERT_TRUE(_runtime_state->init_mem_trackers(query_id).ok());
    _runtime_state->set_be_number(_request.backend_num);
    _runtime_state->set_fragment_mem_tracker(mem_tracker);

    _obj_pool = _runtime_state->obj_pool();

    ASSERT_TRUE(_pipeline_builder != nullptr);
    _pipeline_builder();
    _fragment_ctx->set_pipelines(std::move(_pipelines));
    ASSERT_TRUE(_fragment_ctx->prepare_all_pipelines().ok());

    Drivers drivers;
    const auto& pipelines = _fragment_ctx->pipelines();
    const size_t num_pipelines = pipelines.size();
    for (auto n = 0; n < num_pipelines; ++n) {
        const auto& pipeline = pipelines[n];

        const auto degree_of_parallelism = pipeline->source_operator_factory()->degree_of_parallelism();
        const bool is_root = (n == num_pipelines - 1);

        if (pipeline->source_operator_factory()->with_morsels()) {
            // TODO(hcf) missing branch of with_morsels()
        } else {
            if (is_root) {
                _fragment_ctx->set_num_root_drivers(degree_of_parallelism);
            }

            for (size_t i = 0; i < degree_of_parallelism; ++i) {
                auto&& operators = pipeline->create_operators(degree_of_parallelism, i);
                DriverPtr driver =
                        std::make_shared<PipelineDriver>(std::move(operators), _query_ctx, _fragment_ctx, i, is_root);
                drivers.emplace_back(driver);
            }
        }
    }

    _fragment_ctx->set_drivers(std::move(drivers));
}

void PipelineTestBase::_execute() {
    for (const auto& driver : _fragment_ctx->drivers()) {
        ASSERT_TRUE(driver->prepare(_fragment_ctx->runtime_state()).ok());
    }
    for (const auto& driver : _fragment_ctx->drivers()) {
        _exec_env->driver_dispatcher()->dispatch(driver.get());
    }
}

vectorized::ChunkPtr PipelineTestBase::_create_and_fill_chunk(const std::vector<SlotDescriptor*>& slots,
                                                              size_t row_num) {
    auto chunk = vectorized::ChunkHelper::new_chunk(slots, row_num);

    // add data
    for (size_t i = 0; i < slots.size(); ++i) {
        auto* slot = slots[i];
        auto& column = chunk->columns()[i];

        vectorized::Column* data_column = column.get();
        if (data_column->is_nullable()) {
            auto* nullable_column = down_cast<vectorized::NullableColumn*>(data_column);
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
                data_column->append_datum(std::numeric_limits<int8_t>::max());
                break;
            case TYPE_SMALLINT:
                data_column->append_datum(std::numeric_limits<int16_t>::max());
                break;
            case TYPE_INT:
                data_column->append_datum(std::numeric_limits<int32_t>::max());
                break;
            case TYPE_BIGINT:
                data_column->append_datum(std::numeric_limits<int64_t>::max());
                break;
            case TYPE_LARGEINT:
                data_column->append_datum(std::numeric_limits<__int128_t>::max());
                break;
            case TYPE_FLOAT:
                data_column->append_datum(std::numeric_limits<float>::max());
                break;
            case TYPE_DOUBLE:
                data_column->append_datum(std::numeric_limits<double>::max());
                break;
            case TYPE_DATE: {
                vectorized::DateValue value;
                data_column->append_datum(value);
                break;
            }
            case TYPE_DATETIME: {
                vectorized::TimestampValue value;
                data_column->append_datum(value);
                break;
            }
            case TYPE_DECIMALV2: {
                DecimalV2Value value;
                data_column->append_datum(value);
                break;
            }
            case TYPE_DECIMAL32:
                data_column->append_datum(std::numeric_limits<int32_t>::max());
                break;
            case TYPE_DECIMAL64:
                data_column->append_datum(std::numeric_limits<int64_t>::max());
                break;
            case TYPE_DECIMAL128:
                data_column->append_datum(std::numeric_limits<int128_t>::max());
                break;
            default:
                break;
            }
        }
    }

    return chunk;
}

vectorized::ChunkPtr PipelineTestBase::_create_and_fill_chunk(size_t row_num) {
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
    deserialize_thrift_msg(buf, &len, TProtocolType::JSON, &tbl);

    std::vector<SlotDescriptor> slots;
    for (auto& t_slot : tbl.slotDescriptors) {
        slots.emplace_back(t_slot);
    }

    std::vector<SlotDescriptor*> p_slots;
    for (auto& slot : slots) {
        p_slots.push_back(&slot);
    }

    return _create_and_fill_chunk(p_slots, row_num);
}

}; // namespace starrocks::pipeline
