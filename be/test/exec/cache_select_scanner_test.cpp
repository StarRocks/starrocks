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

#include "exec/cache_select_scanner.h"

#include <gtest/gtest.h>

#include <memory>

#include "cache/block_cache/block_cache.h"
#include "column/column_helper.h"
#include "exec/hdfs_scanner_orc.h"
#include "exec/hdfs_scanner_parquet.h"
#include "exec/pipeline/fragment_context.h"
#include "runtime/descriptor_helper.h"
#include "runtime/runtime_state.h"
#include "storage/chunk_helper.h"
#include "testutil/assert.h"

namespace starrocks {

namespace {
struct SlotDesc {
    string name;
    TypeDescriptor type;
};
} // namespace

class CacheSelectScannerTest : public ::testing::Test {
public:
    void SetUp() override { _create_runtime_state(""); }
    void TearDown() override {}

protected:
    void _create_runtime_state(const std::string& timezone);
    HdfsScannerParams* _create_param(const std::string& file, THdfsScanRange* range, TupleDescriptor* tuple_desc);
    THdfsScanRange* _create_scan_range(const std::string& file, uint64_t offset, uint64_t length,
                                       const THdfsFileFormat::type& type);
    TupleDescriptor* _create_tuple_desc(SlotDesc* descs);

    ObjectPool _pool;
    RuntimeState* _runtime_state = nullptr;
};

void CacheSelectScannerTest::_create_runtime_state(const std::string& timezone) {
    TUniqueId fragment_id;
    TQueryOptions query_options;
    TQueryGlobals query_globals;
    if (timezone != "") {
        query_globals.__set_time_zone(timezone);
    }
    _runtime_state = _pool.add(new RuntimeState(fragment_id, query_options, query_globals, nullptr));
    _runtime_state->init_instance_mem_tracker();
    pipeline::FragmentContext* fragment_context = _pool.add(new pipeline::FragmentContext());
    fragment_context->set_pred_tree_params({true, true});
    _runtime_state->set_fragment_ctx(fragment_context);
}

THdfsScanRange* CacheSelectScannerTest::_create_scan_range(const std::string& file, uint64_t offset, uint64_t length,
                                                           const THdfsFileFormat::type& type) {
    auto* scan_range = _pool.add(new THdfsScanRange());
    uint64_t file_size = 10;
    scan_range->relative_path = file;
    scan_range->offset = offset;
    scan_range->length = length == 0 ? file_size : length;
    scan_range->file_length = file_size;
    scan_range->file_format = type;
    return scan_range;
}

HdfsScannerParams* CacheSelectScannerTest::_create_param(const std::string& file, THdfsScanRange* range,
                                                         TupleDescriptor* tuple_desc) {
    auto* param = _pool.add(new HdfsScannerParams());
    auto* lazy_column_coalesce_counter = _pool.add(new std::atomic<int32_t>(0));
    param->fs = FileSystem::Default();
    param->path = file;
    param->file_size = range->file_length;
    param->scan_range = range;
    param->tuple_desc = tuple_desc;
    param->runtime_filter_collector = _pool.add(new RuntimeFilterProbeCollector());
    std::vector<int> materialize_index_in_chunk;
    std::vector<int> partition_index_in_chunk;
    std::vector<SlotDescriptor*> mat_slots;
    std::vector<SlotDescriptor*> part_slots;

    for (int i = 0; i < tuple_desc->slots().size(); i++) {
        SlotDescriptor* slot = tuple_desc->slots()[i];
        if (slot->col_name().find("PART_") != std::string::npos) {
            partition_index_in_chunk.push_back(i);
            part_slots.push_back(slot);
        } else {
            materialize_index_in_chunk.push_back(i);
            mat_slots.push_back(slot);
        }
    }

    param->partition_index_in_chunk = partition_index_in_chunk;
    param->materialize_index_in_chunk = materialize_index_in_chunk;
    param->materialize_slots = mat_slots;
    param->partition_slots = part_slots;
    param->lazy_column_coalesce_counter = lazy_column_coalesce_counter;
    return param;
}

TupleDescriptor* CacheSelectScannerTest::_create_tuple_desc(SlotDesc* descs) {
    TDescriptorTableBuilder table_desc_builder;
    TSlotDescriptorBuilder slot_desc_builder;
    TTupleDescriptorBuilder tuple_desc_builder;
    int slot_id = 0;
    while (descs->name != "") {
        slot_desc_builder.column_name(descs->name).type(descs->type).id(slot_id).nullable(true);
        tuple_desc_builder.add_slot(slot_desc_builder.build());
        descs += 1;
        slot_id += 1;
    }
    tuple_desc_builder.build(&table_desc_builder);
    std::vector<TTupleId> row_tuples = std::vector<TTupleId>{0};
    DescriptorTbl* tbl = nullptr;
    CHECK(DescriptorTbl::create(_runtime_state, &_pool, table_desc_builder.desc_tbl(), &tbl, config::vector_chunk_size)
                  .ok());
    auto* row_desc = _pool.add(new RowDescriptor(*tbl, row_tuples));
    auto* tuple_desc = row_desc->tuple_descriptors()[0];
    return tuple_desc;
}

TEST_F(CacheSelectScannerTest, TestUnknowFormat) {
    SlotDesc slot_desc[] = {{"Id", TypeDescriptor::from_logical_type(LogicalType::TYPE_INT)}, {""}};
    auto scanner = std::make_shared<CacheSelectScanner>();
    auto* range = _create_scan_range("jni_scan_range", 0, 0, THdfsFileFormat::UNKNOWN);
    auto* tuple_desc = _create_tuple_desc(slot_desc);
    auto* param = _create_param("fake_file", range, tuple_desc);

    Status status = scanner->init(_runtime_state, *param);
    EXPECT_TRUE(status.ok());

    status = scanner->open(_runtime_state);
    EXPECT_TRUE(status.ok());

    ChunkPtr chunk = ChunkHelper::new_chunk(*tuple_desc, 0);
    status = scanner->get_next(_runtime_state, &chunk);
    ASSERT_TRUE(status.is_end_of_file());
}
} // namespace starrocks