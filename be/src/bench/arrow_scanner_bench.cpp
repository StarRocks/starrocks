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

#include <arrow/builder.h>
#include <arrow/io/file.h>
#include <arrow/ipc/writer.h>
#include <benchmark/benchmark.h>
#include <glog/logging.h>

#include <filesystem>
#include <fstream>
#include <memory>
#include <utility>
#include <vector>

#include "column/chunk.h"
#include "exec/file_scanner/arrow_scanner.h"
#include "exec/file_scanner/json_scanner.h"
#include "gen_cpp/Descriptors_types.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/mem_tracker.h"
#include "runtime/runtime_state.h"
#include "types/type_descriptor.h"

namespace starrocks {

static void generate_test_files(const std::string& arrow_path, const std::string& json_path, int64_t num_rows) {
    // 1. Generate Arrow IPC Stream File
    arrow::Int32Builder int_builder;
    arrow::StringBuilder str_builder;
    arrow::DoubleBuilder double_builder;

    for (int64_t i = 0; i < num_rows; ++i) {
        (void)int_builder.Append(static_cast<int32_t>(i));
        (void)str_builder.Append("varchar_str_" + std::to_string(i % 100));
        (void)double_builder.Append(static_cast<double>(i) * 1.5);
    }

    std::shared_ptr<arrow::Array> int_array;
    std::shared_ptr<arrow::Array> str_array;
    std::shared_ptr<arrow::Array> double_array;

    (void)int_builder.Finish(&int_array);
    (void)str_builder.Finish(&str_array);
    (void)double_builder.Finish(&double_array);

    auto schema = arrow::schema({arrow::field("c0_int", arrow::int32()), arrow::field("c1_str", arrow::utf8()),
                                 arrow::field("c2_double", arrow::float64())});

    auto batch = arrow::RecordBatch::Make(schema, num_rows, {int_array, str_array, double_array});

    auto out_file_res = arrow::io::FileOutputStream::Open(arrow_path);
    if (!out_file_res.ok()) {
        LOG(FATAL) << "Failed to open Arrow stream file: " << out_file_res.status().ToString();
    }
    auto out_file = out_file_res.ValueOrDie();

    auto writer_res = arrow::ipc::MakeStreamWriter(out_file, schema);
    if (!writer_res.ok()) {
        LOG(FATAL) << "Failed to create Arrow stream writer: " << writer_res.status().ToString();
    }
    auto writer = writer_res.ValueOrDie();

    (void)writer->WriteRecordBatch(*batch);
    (void)writer->Close();
    (void)out_file->Close();

    // 2. Generate Line-delimited JSON (NDJSON) File
    std::ofstream json_file(json_path);
    if (!json_file.is_open()) {
        LOG(FATAL) << "Failed to open JSON file for writing: " << json_path;
    }
    for (int64_t i = 0; i < num_rows; ++i) {
        json_file << "{\"c0_int\":" << i << ",\"c1_str\":\"varchar_str_" << (i % 100)
                  << "\",\"c2_double\":" << (static_cast<double>(i) * 1.5) << "}\n";
    }
    json_file.close();
}

static DescriptorTbl* create_descriptor_table(RuntimeState* state, ObjectPool* pool) {
    TDescriptorTable thrift_tbl;

    // Source Tuple (id = 0)
    TTupleDescriptor src_tuple;
    src_tuple.id = 0;
    src_tuple.byteSize = 0;
    src_tuple.numNullBytes = 0;
    thrift_tbl.tupleDescriptors.push_back(src_tuple);

    // Dest Tuple (id = 1)
    TTupleDescriptor dest_tuple;
    dest_tuple.id = 1;
    dest_tuple.byteSize = 0;
    dest_tuple.numNullBytes = 0;
    thrift_tbl.tupleDescriptors.push_back(dest_tuple);

    std::vector<std::pair<std::string, TypeDescriptor>> columns = {
            {"c0_int", TypeDescriptor::from_logical_type(TYPE_INT)},
            {"c1_str", TypeDescriptor::from_logical_type(TYPE_VARCHAR)},
            {"c2_double", TypeDescriptor::from_logical_type(TYPE_DOUBLE)}};

    int slot_id = 0;
    for (int i = 0; i < columns.size(); ++i) {
        // Source Slot
        TSlotDescriptor src_slot;
        src_slot.id = slot_id++;
        src_slot.parent = 0;
        src_slot.slotType = columns[i].second.to_thrift();
        src_slot.columnPos = i;
        src_slot.byteOffset = 0;
        src_slot.nullIndicatorByte = 0;
        src_slot.nullIndicatorBit = 0;
        src_slot.colName = columns[i].first;
        src_slot.slotIdx = i;
        src_slot.isMaterialized = true;
        thrift_tbl.slotDescriptors.push_back(src_slot);

        // Dest Slot
        TSlotDescriptor dest_slot;
        dest_slot.id = slot_id++;
        dest_slot.parent = 1;
        dest_slot.slotType = columns[i].second.to_thrift();
        dest_slot.columnPos = i;
        dest_slot.byteOffset = 0;
        dest_slot.nullIndicatorByte = 0;
        dest_slot.nullIndicatorBit = 0;
        dest_slot.colName = columns[i].first;
        dest_slot.slotIdx = i;
        dest_slot.isMaterialized = true;
        thrift_tbl.slotDescriptors.push_back(dest_slot);
    }

    DescriptorTbl* desc_tbl = nullptr;
    Status st = DescriptorTbl::create(state, pool, thrift_tbl, &desc_tbl, 4096);
    if (!st.ok()) {
        LOG(FATAL) << "Failed to create DescriptorTable: " << st.to_string();
    }
    return desc_tbl;
}

static TBrokerScanRange* create_scan_range(ObjectPool* pool, const std::string& path, TFileFormatType::type format_type,
                                           DescriptorTbl* desc_tbl) {
    TBrokerScanRangeParams* params = pool->add(new TBrokerScanRangeParams());
    params->strict_mode = true;
    params->src_tuple_id = 0;
    params->dest_tuple_id = 1;
    params->json_file_size_limit = 1024 * 1024 * 1024;

    const auto* src_tuple = desc_tbl->get_tuple_descriptor(0);
    const auto* dst_tuple = desc_tbl->get_tuple_descriptor(1);

    for (int i = 0; i < src_tuple->slots().size(); ++i) {
        auto* src_slot = src_tuple->slots()[i];
        auto* dst_slot = dst_tuple->slots()[i];

        TExpr expr;
        expr.nodes.emplace_back(TExprNode());
        expr.nodes[0].__set_type(src_slot->type().to_thrift());
        expr.nodes[0].__set_node_type(TExprNodeType::SLOT_REF);
        expr.nodes[0].__set_is_nullable(true);
        expr.nodes[0].__set_slot_ref(TSlotRef());
        expr.nodes[0].slot_ref.__set_slot_id(src_slot->id());

        params->expr_of_dest_slot[dst_slot->id()] = expr;
        params->src_slot_ids.emplace_back(src_slot->id());
    }

    TBrokerRangeDesc range;
    range.__set_path(path);
    range.start_offset = 0;
    range.size = LONG_MAX;
    range.file_type = TFileType::FILE_LOCAL;
    range.__set_format_type(format_type);
    range.__set_num_of_columns_from_file(src_tuple->slots().size());

    TBrokerScanRange* scan_range = pool->add(new TBrokerScanRange());
    scan_range->params = *params;
    scan_range->ranges.push_back(range);

    return scan_range;
}

static std::string g_arrow_file_path;
static std::string g_json_file_path;
static constexpr int64_t kNumRows = 100000;

static void setup_global_files() {
    char arrow_pattern[] = "/tmp/arrow_bench_XXXXXX.arrow";
    int fd_arrow = mkstemps(arrow_pattern, 6);
    if (fd_arrow != -1) {
        close(fd_arrow);
        g_arrow_file_path = arrow_pattern;
    } else {
        g_arrow_file_path = "/tmp/arrow_bench.arrow";
    }

    char json_pattern[] = "/tmp/json_bench_XXXXXX.json";
    int fd_json = mkstemps(json_pattern, 5);
    if (fd_json != -1) {
        close(fd_json);
        g_json_file_path = json_pattern;
    } else {
        g_json_file_path = "/tmp/json_bench.json";
    }

    generate_test_files(g_arrow_file_path, g_json_file_path, kNumRows);
}

static void cleanup_global_files() {
    std::error_code ec;
    std::filesystem::remove(g_arrow_file_path, ec);
    std::filesystem::remove(g_json_file_path, ec);
}

static void BM_ArrowScanner(benchmark::State& state) {
    ObjectPool pool;
    TQueryOptions query_options;
    TQueryGlobals query_globals;
    auto* runtime_state = pool.add(new RuntimeState(TUniqueId(), query_options, query_globals, nullptr));
    runtime_state->init_instance_mem_tracker();

    auto* desc_tbl = create_descriptor_table(runtime_state, &pool);
    runtime_state->set_desc_tbl(desc_tbl);

    auto* scan_range = create_scan_range(&pool, g_arrow_file_path, TFileFormatType::FORMAT_ARROW, desc_tbl);

    for (auto _ : state) {
        auto profile = pool.add(new RuntimeProfile("arrow_bench_prof"));
        auto counter = pool.add(new ScannerCounter());
        auto scanner = std::make_unique<ArrowScanner>(runtime_state, profile, *scan_range, counter);

        Status st = scanner->open();
        if (!st.ok()) {
            state.SkipWithError(("Failed to open ArrowScanner: " + st.to_string()).c_str());
            break;
        }

        int64_t total_rows = 0;
        while (true) {
            auto res = scanner->get_next();
            if (res.status().is_end_of_file()) {
                break;
            }
            if (!res.ok()) {
                state.SkipWithError(("Failed to scan chunk: " + res.status().to_string()).c_str());
                break;
            }
            auto chunk = res.value();
            if (chunk) {
                total_rows += chunk->num_rows();
            }
        }
        scanner->close();
        benchmark::DoNotOptimize(total_rows);
    }

    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * kNumRows);
}

static void BM_JsonScanner(benchmark::State& state) {
    ObjectPool pool;
    TQueryOptions query_options;
    TQueryGlobals query_globals;
    auto* runtime_state = pool.add(new RuntimeState(TUniqueId(), query_options, query_globals, nullptr));
    runtime_state->init_instance_mem_tracker();

    auto* desc_tbl = create_descriptor_table(runtime_state, &pool);
    runtime_state->set_desc_tbl(desc_tbl);

    auto* scan_range = create_scan_range(&pool, g_json_file_path, TFileFormatType::FORMAT_JSON, desc_tbl);

    for (auto _ : state) {
        auto profile = pool.add(new RuntimeProfile("json_bench_prof"));
        auto counter = pool.add(new ScannerCounter());
        auto scanner = std::make_unique<JsonScanner>(runtime_state, profile, *scan_range, counter);

        Status st = scanner->open();
        if (!st.ok()) {
            state.SkipWithError(("Failed to open JsonScanner: " + st.to_string()).c_str());
            break;
        }

        int64_t total_rows = 0;
        while (true) {
            auto res = scanner->get_next();
            if (res.status().is_end_of_file()) {
                break;
            }
            if (!res.ok()) {
                state.SkipWithError(("Failed to scan chunk: " + res.status().to_string()).c_str());
                break;
            }
            auto chunk = res.value();
            if (chunk) {
                total_rows += chunk->num_rows();
            }
        }
        scanner->close();
        benchmark::DoNotOptimize(total_rows);
    }

    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * kNumRows);
}

BENCHMARK(BM_JsonScanner)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_ArrowScanner)->Unit(benchmark::kMillisecond);

} // namespace starrocks

int main(int argc, char** argv) {
    starrocks::setup_global_files();
    benchmark::Initialize(&argc, argv);
    if (benchmark::ReportUnrecognizedArguments(argc, argv)) {
        starrocks::cleanup_global_files();
        return 1;
    }
    benchmark::RunSpecifiedBenchmarks();
    starrocks::cleanup_global_files();
    return 0;
}
