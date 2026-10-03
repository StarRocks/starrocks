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

#include "base/testutil/assert.h"
#include "column/chunk_factory.h"
#include "column/column_helper.h"
#include "common/util/table_metrics.h"
#include "compute_env/global_dict/fragment_dict_state.h"
#include "compute_env/query/fragment_runtime_state.h"
#include "exec/exec_env.h"
#include "exec/olap_scan_node.h"
#include "exec/pipeline/query_context.h"
#include "exec/pipeline/scan/olap_chunk_source.h"
#include "exec/pipeline/scan/olap_scan_prepare_operator.h"
#include "exec_primitive/pipeline/scan/scan_morsel.h"
#include "exprs/column_ref.h"
#include "exprs/in_const_predicate.hpp"
#include "gtest/gtest.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_filter.h"
#include "runtime/runtime_state.h"
#include "storage/query/olap_fixed_morsel_queue.h"
#include "storage/tablet_schema_helper.h"
#include "testutil/exprs_test_helper.h"

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

namespace {

TExpr make_int_literal(int32_t value) {
    TExprNode node;
    node.__set_node_type(TExprNodeType::INT_LITERAL);
    node.__set_type(TypeDescriptor(TYPE_INT).to_thrift());
    node.__set_num_children(0);
    TIntLiteral literal;
    literal.__set_value(value);
    node.__set_int_literal(literal);
    TExpr expr;
    expr.nodes.emplace_back(std::move(node));
    return expr;
}

TPartitionBoundary make_range_boundary(TSlotId slot_id, int64_t physical_id, int32_t lower, int32_t upper) {
    TPartitionBoundary boundary;
    boundary.__set_physical_partition_ids({physical_id});
    boundary.__set_slot_id(slot_id);
    boundary.__set_range_lower(make_int_literal(lower));
    boundary.__set_range_upper(make_int_literal(upper));
    return boundary;
}

// A join runtime IN filter `slot IN (value)` built the way HashJoiner builds it.
ExprContext* make_runtime_in_filter(RuntimeState* state, TSlotId slot_id, int32_t value) {
    auto* pool = state->obj_pool();
    auto column = ColumnHelper::create_column(TypeDescriptor(TYPE_INT), true);
    column->append_datum(Datum(value));
    VectorizedInConstPredicateBuilder builder(state, pool, pool->add(new ColumnRef(TypeDescriptor(TYPE_INT), slot_id)));
    builder.use_as_join_runtime_filter();
    CHECK_OK(builder.create());
    builder.add_values(column, 0);
    ExprContext* filter = builder.get_in_const_predicate();
    CHECK_OK(filter->prepare(state));
    CHECK_OK(filter->open(state));
    return filter;
}

void expect_counter(RuntimeProfile* profile, const char* name, int64_t value) {
    auto* counter = profile->get_counter(name);
    ASSERT_NE(counter, nullptr) << name;
    EXPECT_EQ(counter->value(), value) << name;
}

} // namespace

// A chunk source whose partition was pruned by a runtime filter must never open the tablet reader: prepare()
// returns before _init_olap_reader, _read_chunk reports EOF, and the reader-based hooks tolerate the missing
// reader. A chunk source on a surviving partition passes the checkpoint untouched.
TEST_F(OlapScanOperatorTest, runtime_filter_pruned_chunk_source_skips_reader) {
    // One INT slot so the partition boundary can resolve its slot, then rebuild the descriptor table.
    const TSlotId slot_id = 7;
    TSlotDescriptor slot =
            TSlotDescriptorBuilder().type(TYPE_INT).column_name("p").column_pos(0).nullable(true).id(slot_id).build();
    slot.__set_parent(1);
    _thrift_tbl.slotDescriptors.emplace_back(slot);
    // The fixture assigns tableId without the isset flag, so the tuple is not linked to its table;
    // OlapChunkSource::prepare reads the table name through that link.
    _thrift_tbl.tupleDescriptors[0].__set_tableId(1);
    ASSERT_TRUE(DescriptorTbl::create(&_runtime_state, &_object_pool, _thrift_tbl, &_tbl, _chunk_size).ok());
    _runtime_state.set_desc_tbl(_tbl);

    _tnode.olap_scan_node.__set_tuple_id(1);
    _tnode.olap_scan_node.__set_partition_boundaries(
            {make_range_boundary(slot_id, 100, 10, 20), make_range_boundary(slot_id, 200, 20, 30)});

    OlapScanNode scan_node(&_object_pool, _tnode, *_tbl);
    ASSERT_TRUE(scan_node.init(_tnode, &_runtime_state).ok());
    auto* pruner = scan_node.runtime_filter_partition_pruner();
    ASSERT_NE(pruner, nullptr);
    ASSERT_EQ(pruner->candidate_partition_count(), 2);

    // p IN (25) only intersects [20, 30): partition 100 is pruned.
    ExprContext* in_filter = make_runtime_in_filter(&_runtime_state, slot_id, 25);
    pruner->prune_by_in_filters({in_filter});
    ASSERT_EQ(1, pruner->pruned_partition_count());
    ASSERT_TRUE(pruner->is_partition_pruned(100));
    ASSERT_FALSE(pruner->is_partition_pruned(200));

    auto scan_ctx_factory =
            std::make_shared<OlapScanContextFactory>(&scan_node, 1, false, false, std::move(_chunk_buffer_limiter));
    OlapScanOperatorFactory scan_operator_factory(1, &scan_node, scan_ctx_factory);
    // The bloom checkpoint reads the factory's probe collector, which decompose_to_pipeline always installs.
    auto rf_probe_collector = std::make_shared<RcRfProbeCollector>(1, RuntimeFilterProbeCollector());
    scan_operator_factory.init_runtime_filter(nullptr, {1}, LocalRFWaitingSet(), scan_node.record_desc(),
                                              rf_probe_collector, {}, {});
    auto scan_operator = std::make_shared<OlapScanOperator>(&scan_operator_factory, 1, 0, 1, &scan_node,
                                                            scan_ctx_factory->get_or_create(0));

    auto create_chunk_source = [&](int64_t partition_id) {
        TInternalScanRange internal_range;
        internal_range.__set_partition_id(partition_id);
        TScanRange scan_range;
        scan_range.__set_internal_scan_range(internal_range);
        return scan_operator->create_chunk_source(std::make_unique<ScanMorsel>(1, scan_range), 0);
    };

    // Pruned partition: the real prepare() finishes without a tablet because the reader is never opened.
    auto pruned = create_chunk_source(100);
    auto* pruned_source = down_cast<OlapChunkSource*>(pruned.get());
    ASSERT_TRUE(pruned_source->prepare(&_runtime_state).ok());
    EXPECT_TRUE(pruned_source->_partition_pruned);
    EXPECT_EQ(pruned_source->_reader, nullptr);
    ChunkPtr chunk;
    EXPECT_TRUE(pruned_source->_read_chunk(&_runtime_state, &chunk).is_end_of_file());
    pruned_source->update_chunk_exec_stats(&_runtime_state);
    // A second checkpoint on a pruned task is a no-op: the task is not counted twice.
    pruned_source->update_runtime_filter_partition_pruning(&_runtime_state);
    pruned_source->close(&_runtime_state);

    // Surviving partition: the checkpoint leaves it to the reader (stop before prepare(), which needs a tablet).
    auto kept = create_chunk_source(200);
    auto* kept_source = down_cast<OlapChunkSource*>(kept.get());
    ASSERT_TRUE(kept_source->ChunkSource::prepare(&_runtime_state).ok());
    kept_source->_runtime_state = &_runtime_state;
    kept_source->_init_counter(&_runtime_state);
    kept_source->update_runtime_filter_partition_pruning(&_runtime_state);
    EXPECT_FALSE(kept_source->_partition_pruned);

    // Every checkpoint snapshots the shared pruned set, so the IN verdict shows up here as well.
    auto* profile = scan_operator->unique_metrics();
    expect_counter(profile, "RuntimeFilterPartitionsTotal", 2);
    expect_counter(profile, "RuntimeFilterPartitionsPruned", 1);
    expect_counter(profile, "RuntimeFilterPrunedScanTasks", 1);

    in_filter->close(&_runtime_state);
    scan_node.close(&_runtime_state);
}

TEST_F(OlapScanOperatorTest, late_runtime_filter_stops_started_chunk_source) {
    _runtime_state.init_instance_mem_tracker();
    _tnode.__set_limit(-1);
    const TSlotId slot_id = 7;
    auto slot =
            TSlotDescriptorBuilder().type(TYPE_INT).column_name("p").column_pos(0).nullable(true).id(slot_id).build();
    slot.__set_parent(1);
    _thrift_tbl.slotDescriptors.emplace_back(slot);
    ASSERT_OK(DescriptorTbl::create(&_runtime_state, &_object_pool, _thrift_tbl, &_tbl, _chunk_size));
    _runtime_state.set_desc_tbl(_tbl);
    _tnode.olap_scan_node.__set_tuple_id(1);
    _tnode.olap_scan_node.__set_partition_boundaries({make_range_boundary(slot_id, 100, 10, 20)});
    OlapScanNode scan_node(&_object_pool, _tnode, *_tbl);
    ASSERT_OK(scan_node.init(_tnode, &_runtime_state));

    TRuntimeFilterDescription thrift;
    thrift.__set_filter_id(1);
    thrift.__set_has_remote_targets(false);
    thrift.__set_build_plan_node_id(0);
    thrift.__set_build_join_mode(TRuntimeFilterBuildJoinMode::BROADCAST);
    thrift.__set_plan_node_id_to_target_expr({{0, ExprsTestHelper::create_column_ref_t_expr<TYPE_INT>(slot_id, true)}});
    RuntimeFilterProbeDescriptor descriptor;
    ASSERT_OK(descriptor.init(&_object_pool, thrift, 0, &_runtime_state));
    RuntimeFilterProbeCollector collector;
    collector.add_descriptor(&descriptor);
    auto scan_ctx_factory =
            std::make_shared<OlapScanContextFactory>(&scan_node, 1, false, false, std::move(_chunk_buffer_limiter));
    OlapScanOperatorFactory factory(1, &scan_node, scan_ctx_factory);
    factory.init_runtime_filter(nullptr, {1}, LocalRFWaitingSet(), scan_node.record_desc(),
                                std::make_shared<RcRfProbeCollector>(1, std::move(collector)), {}, {});
    auto scan_ctx = scan_ctx_factory->get_or_create(0);
    auto scan_operator = std::make_shared<OlapScanOperator>(&factory, 1, 0, 1, &scan_node, scan_ctx);
    TInternalScanRange internal_range;
    internal_range.__set_partition_id(100);
    TScanRange scan_range;
    scan_range.__set_internal_scan_range(internal_range);
    auto source = scan_operator->create_chunk_source(std::make_unique<ScanMorsel>(1, scan_range), 0);
    auto* olap_source = down_cast<OlapChunkSource*>(source.get());
    ASSERT_OK(olap_source->ChunkSource::prepare(&_runtime_state));
    olap_source->_runtime_state = &_runtime_state;
    olap_source->_init_counter(&_runtime_state);
    olap_source->_table_metrics = std::make_shared<TableMetrics>(1, false);
    FragmentRuntimeState fragment_runtime_state;
    _runtime_state.set_fragment_runtime_state(&fragment_runtime_state);

    Schema schema({std::make_shared<Field>(0, "p", get_type_info(TYPE_INT), true)});
    olap_source->_reader =
            std::make_shared<TabletReader>(nullptr, Version(0, 1), schema, TabletSchemaHelper::create_tablet_schema());
    // Keep storage deterministic while exercising the real chunk source read and EOF paths.
    class CountingIterator final : public ChunkIterator {
    public:
        CountingIterator(Schema schema, OlapReaderStatistics* stats)
                : ChunkIterator(std::move(schema)), _stats(stats) {}
        Status do_get_next(Chunk* chunk) override {
            ++reads;
            ++_stats->raw_rows_read;
            chunk->get_column_by_index(0)->as_mutable_ptr()->append_datum(Datum(int32_t{15}));
            return Status::OK();
        }
        void close() override { closed = true; }
        int reads = 0;
        bool closed = false;

    private:
        OlapReaderStatistics* _stats;
    };
    auto iter = std::make_shared<CountingIterator>(schema, olap_source->_reader->mutable_stats());
    olap_source->_prj_iter = iter;
    olap_source->update_runtime_filter_partition_pruning(&_runtime_state);
    ASSERT_FALSE(olap_source->_partition_pruned);
    auto first_chunk = ChunkFactory::new_chunk(schema, _chunk_size);
    ASSERT_OK(olap_source->_read_chunk_from_storage(&_runtime_state, first_chunk.get()));
    ASSERT_EQ(1, first_chunk->num_rows());
    EXPECT_EQ(15, first_chunk->get_column_by_index(0)->get(0).get_int32());
    EXPECT_EQ(1, iter->reads);

    ComposedRuntimeBloomFilter<TYPE_INT> filter;
    filter.membership_filter().init(16);
    filter.insert(25);
    descriptor.set_runtime_filter(&filter);
    olap_source->update_runtime_filter_partition_pruning(&_runtime_state);
    ASSERT_TRUE(olap_source->_partition_pruned);
    ASSERT_TRUE(olap_source->buffer_next_batch_chunks_blocking(&_runtime_state, 1, nullptr).is_end_of_file());
    ChunkPtr last_chunk;
    ASSERT_TRUE(scan_ctx->get_chunk_buffer().try_get(0, &last_chunk));
    EXPECT_TRUE(last_chunk->is_empty());
    EXPECT_TRUE(last_chunk->owner_info().is_last_chunk());
    EXPECT_FALSE(olap_source->has_next_chunk());
    EXPECT_EQ(1, iter->reads);
    olap_source->update_runtime_filter_partition_pruning(&_runtime_state);
    expect_counter(scan_operator->unique_metrics(), "RuntimeFilterPrunedScanTasks", 1);
    expect_counter(scan_operator->unique_metrics(), "RuntimeFilterPartitionsPruned", 1);
    olap_source->update_chunk_exec_stats(&_runtime_state);
    olap_source->close(&_runtime_state);
    EXPECT_TRUE(iter->closed);
    EXPECT_EQ(nullptr, olap_source->_reader);
    scan_node.close(&_runtime_state);
    _runtime_state.set_fragment_runtime_state(nullptr);
}

} // namespace starrocks::pipeline
