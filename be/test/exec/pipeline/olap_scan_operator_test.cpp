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

#include "exec/pipeline/scan/olap_scan_operator.h"

#include "common/util/table_metrics.h"
#include "compute_env/global_dict/fragment_dict_state.h"
#include "compute_env/query/fragment_runtime_state.h"
#include "exec/exec_env.h"
#include "exec/olap_scan_node.h"
#include "exec/pipeline/query_context.h"
#include "exec/pipeline/scan/olap_chunk_source.h"
#include "exec/pipeline/scan/olap_scan_prepare_operator.h"
#include "exec_primitive/pipeline/scan/scan_morsel.h"
#include "gtest/gtest.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "storage/query/olap_fixed_morsel_queue.h"
#include "storage/tablet_schema_helper.h"

namespace starrocks::pipeline {

namespace {

void expect_vector_index_counter(RuntimeProfile* profile, const char* name, const char* parent) {
    auto it = profile->_counter_map.find(name);
    ASSERT_NE(it, profile->_counter_map.end()) << name;
    EXPECT_EQ(it->second.second, parent) << name;
    EXPECT_EQ(it->second.first->value(), 0) << name;
}

void expect_vector_index_counters(RuntimeProfile* profile) {
    ASSERT_NE(profile, nullptr);
    expect_vector_index_counter(profile, "VectorIndex", "SegmentInit");
    expect_vector_index_counter(profile, "VectorIndexLoad", "VectorIndex");
    expect_vector_index_counter(profile, "VectorIndexCacheLookup", "VectorIndexLoad");
    expect_vector_index_counter(profile, "VectorIndexFileOpenAndGetSize", "VectorIndexLoad");
    expect_vector_index_counter(profile, "VectorIndexFileRead", "VectorIndexLoad");
    expect_vector_index_counter(profile, "VectorIndexDeserialize", "VectorIndexLoad");
    expect_vector_index_counter(profile, "VectorIndexSearcherCreate", "VectorIndexLoad");
    expect_vector_index_counter(profile, "VectorIndexCacheHit", "VectorIndexCacheLookup");
    expect_vector_index_counter(profile, "VectorIndexCacheMiss", "VectorIndexCacheLookup");
    expect_vector_index_counter(profile, "VectorIndexSearch", "VectorIndex");
    expect_vector_index_counter(profile, "VectorANNSearch", "VectorIndexSearch");
    expect_vector_index_counter(profile, "VectorResultProcess", "VectorIndexSearch");
    expect_vector_index_counter(profile, "VectorIndexFilterRows", "VectorIndexSearch");
}

void expect_sample_counter(RuntimeProfile* profile, const char* name, TUnit::type unit, int64_t value) {
    auto it = profile->_counter_map.find(name);
    ASSERT_NE(it, profile->_counter_map.end()) << name;
    EXPECT_EQ(it->second.second, "SegmentRead") << name;
    EXPECT_EQ(it->second.first->type(), unit) << name;
    EXPECT_EQ(it->second.first->value(), value) << name;
}

} // namespace

class OlapScanOperatorTest : public ::testing::Test {
public:
    void SetUp() override;

protected:
    ObjectPool _object_pool;
    RuntimeState _runtime_state;
    TDescriptorTable _thrift_tbl;
    const int64_t _chunk_size = 4096;
    DescriptorTbl* _tbl = nullptr;
    TPlanNode _tnode;
    ChunkBufferLimiterPtr _chunk_buffer_limiter;
    QueryContext _query_ctx;
    std::unique_ptr<FragmentDictState> _fragment_dict_state;
};

void OlapScanOperatorTest::SetUp() {
    TTableDescriptor t_table_desc;
    t_table_desc.id = 1;
    t_table_desc.tableType = TTableType::OLAP_TABLE;
    _thrift_tbl.tableDescriptors.emplace_back(t_table_desc);

    TTupleDescriptor t_tuple_desc;
    t_tuple_desc.id = 1;
    t_tuple_desc.tableId = 1;
    _thrift_tbl.tupleDescriptors.emplace_back(t_tuple_desc);

    _tnode.row_tuples.emplace_back(1);

    Status st = DescriptorTbl::create(&_runtime_state, &_object_pool, _thrift_tbl, &_tbl, _chunk_size);
    ASSERT_TRUE(st.ok());

    _runtime_state.set_desc_tbl(_tbl);
    _fragment_dict_state = std::make_unique<FragmentDictState>();
    _runtime_state.set_fragment_dict_state(_fragment_dict_state.get());
    _chunk_buffer_limiter = std::make_unique<UnlimitedChunkBufferLimiter>();

    _query_ctx.init_mem_tracker(-1, RuntimeEnv::GetInstance()->process_mem_tracker());
    _runtime_state.set_query_ctx(&_query_ctx, &_query_ctx.query_runtime_state(), _query_ctx.object_pool());
}

TEST_F(OlapScanOperatorTest, test_finish_sequence) {
    SyncPoint::GetInstance()->EnableProcessing();
    SyncPoint::GetInstance()->SetCallBack("OlapScanPrepareOperator::prepare",
                                          [](void* arg) { *(Status*)arg = Status::OK(); });
    SyncPoint::GetInstance()->SetCallBack("ScanOperatorFactory::prepare",
                                          [](void* arg) { *(Status*)arg = Status::OK(); });
    SyncPoint::GetInstance()->SetCallBack("OlapScanContext::parse_conjuncts",
                                          [](void* arg) { *(Status*)arg = Status::EndOfFile(""); });

    Morsels morsels;
    OlapFixedMorselQueue morsel_queue(std::move(morsels));

    OlapScanNode scan_node(&_object_pool, _tnode, *_tbl);
    auto scan_ctx_factory =
            std::make_shared<OlapScanContextFactory>(&scan_node, 1, false, false, std::move(_chunk_buffer_limiter));

    // create operator factory
    OlapScanPrepareOperatorFactory scan_prepare_operator_factory(1, 1, &scan_node, scan_ctx_factory);
    Status st = scan_prepare_operator_factory.prepare(&_runtime_state);
    ASSERT_TRUE(st.ok());

    OlapScanOperatorFactory scan_operator_factory(1, &scan_node, scan_ctx_factory);
    st = scan_operator_factory.prepare(&_runtime_state);
    ASSERT_TRUE(st.ok());

    // create operator
    auto scan_prepare_operator = scan_prepare_operator_factory.create(1, 0);
    ASSERT_TRUE(scan_prepare_operator != nullptr);
    down_cast<OlapScanPrepareOperator*>(scan_prepare_operator.get())->add_morsel_queue(&morsel_queue);

    auto scan_operator = scan_operator_factory.create(1, 0);
    ASSERT_TRUE(scan_operator != nullptr);

    // operator prepare
    st = scan_prepare_operator->prepare(&_runtime_state);
    ASSERT_TRUE(st.ok());

    // pull chunk
    SyncPoint::GetInstance()->SetCallBack("OlapScnPrepareOperator::pull_chunk::before_set_finished",
                                          [&scan_operator](void* arg) { ASSERT_FALSE(scan_operator->has_output()); });
    SyncPoint::GetInstance()->SetCallBack("OlapScnPrepareOperator::pull_chunk::after_set_finished",
                                          [&scan_operator](void* arg) { ASSERT_FALSE(scan_operator->has_output()); });
    SyncPoint::GetInstance()->SetCallBack("OlapScnPrepareOperator::pull_chunk::after_set_prepare_finished",
                                          [&scan_operator](void* arg) { ASSERT_FALSE(scan_operator->has_output()); });

    auto ret = scan_prepare_operator->pull_chunk(&_runtime_state);
    ASSERT_TRUE(ret.status().is_end_of_file());

    scan_node.close(&_runtime_state);

    SyncPoint::GetInstance()->DisableProcessing();
}

TEST_F(OlapScanOperatorTest, legacy_scan_registers_vector_index_counters) {
    OlapScanNode scan_node(&_object_pool, _tnode, *_tbl);
    // Legacy Gin counters still attach to the node profile instead of the scan profile.
    ADD_TIMER(scan_node._runtime_profile, "SegmentInit");

    scan_node._init_counter(&_runtime_state);

    expect_vector_index_counters(scan_node._scan_profile);
    scan_node.close(&_runtime_state);
}

TEST_F(OlapScanOperatorTest, pipeline_chunk_source_registers_vector_index_counters) {
    OlapScanNode scan_node(&_object_pool, _tnode, *_tbl);
    auto scan_ctx_factory =
            std::make_shared<OlapScanContextFactory>(&scan_node, 1, false, false, std::move(_chunk_buffer_limiter));
    OlapScanOperatorFactory scan_operator_factory(1, &scan_node, scan_ctx_factory);
    auto scan_operator = std::make_shared<OlapScanOperator>(&scan_operator_factory, 1, 0, 1, &scan_node,
                                                            scan_ctx_factory->get_or_create(0));
    TScanRange scan_range;
    auto chunk_source = scan_operator->create_chunk_source(std::make_unique<ScanMorsel>(1, scan_range), 0);
    auto* olap_chunk_source = down_cast<OlapChunkSource*>(chunk_source.get());

    ASSERT_TRUE(olap_chunk_source->ChunkSource::prepare(&_runtime_state).ok());
    olap_chunk_source->_init_counter(&_runtime_state);

    expect_vector_index_counters(olap_chunk_source->_runtime_profile);
    scan_node.close(&_runtime_state);
}

// Each sample counter must report its own statistic. SampleTime used to be fed sample_population_size,
// so a block/page count was rendered as a duration in the profile.
TEST_F(OlapScanOperatorTest, sample_counters_report_their_own_statistic) {
    OlapScanNode scan_node(&_object_pool, _tnode, *_tbl);
    auto scan_ctx_factory =
            std::make_shared<OlapScanContextFactory>(&scan_node, 1, false, false, std::move(_chunk_buffer_limiter));
    OlapScanOperatorFactory scan_operator_factory(1, &scan_node, scan_ctx_factory);
    auto scan_operator = std::make_shared<OlapScanOperator>(&scan_operator_factory, 1, 0, 1, &scan_node,
                                                            scan_ctx_factory->get_or_create(0));
    TScanRange scan_range;
    auto chunk_source = scan_operator->create_chunk_source(std::make_unique<ScanMorsel>(1, scan_range), 0);
    auto* olap_chunk_source = down_cast<OlapChunkSource*>(chunk_source.get());

    ASSERT_TRUE(olap_chunk_source->ChunkSource::prepare(&_runtime_state).ok());
    olap_chunk_source->_runtime_state = &_runtime_state;
    olap_chunk_source->_init_counter(&_runtime_state);

    FragmentRuntimeState fragment_runtime_state;
    _runtime_state.set_fragment_runtime_state(&fragment_runtime_state);

    // _update_counter() only reads the reader statistics and the table metrics, so a reader over an empty
    // schema is enough to check how the sample statistics are mapped onto the profile counters.
    olap_chunk_source->_table_metrics = std::make_shared<TableMetrics>(1, false);
    olap_chunk_source->_reader = std::make_shared<TabletReader>(nullptr, Version(0, 1), Schema(),
                                                                TabletSchemaHelper::create_tablet_schema());

    olap_chunk_source->_params.sample_options.__set_enable_sampling(true);
    olap_chunk_source->_params.sample_options.__set_sample_method(SampleMethod::BY_BLOCK);
    olap_chunk_source->_params.sample_options.__set_probability_percent(10);

    // Distinct values so that a counter fed from the wrong statistic is unambiguous.
    auto* stats = olap_chunk_source->_reader->mutable_stats();
    stats->sample_time_ns = 111;
    stats->sample_build_histogram_time_ns = 222;
    stats->sample_size = 333;
    stats->sample_population_size = 444;
    stats->sample_build_histogram_count = 555;

    olap_chunk_source->_update_counter();

    auto* profile = olap_chunk_source->_runtime_profile;
    expect_sample_counter(profile, "SampleTime", TUnit::TIME_NS, 111);
    expect_sample_counter(profile, "SampleBuildHistogramTime", TUnit::TIME_NS, 222);
    expect_sample_counter(profile, "SampleSize", TUnit::UNIT, 333);
    expect_sample_counter(profile, "SamplePopulationSize", TUnit::UNIT, 444);
    expect_sample_counter(profile, "SampleBuildHistogramCount", TUnit::UNIT, 555);

    scan_node.close(&_runtime_state);
}

} // namespace starrocks::pipeline
