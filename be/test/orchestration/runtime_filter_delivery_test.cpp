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

// Every planned global runtime filter must reach its probes in a final state: a bloom filter, a degraded min/max
// filter, or a filter that passes every row. These tests cover the pass-all state and the paths that used to lose
// the filter instead.

#include "orchestration/runtime_filter_delivery.h"

#include <gtest/gtest.h>

#include <limits>
#include <vector>

#include "base/testutil/assert.h"
#include "column/column_helper.h"
#include "column/fixed_length_column.h"
#include "common/brpc/brpc_stub_cache.h"
#include "common/bthread_timer.h"
#include "common/object_pool.h"
#include "exec/pipeline/runtime_filter_types.h"
#include "exec/runtime_filter_compat/runtime_filter_merger.h"
#include "exec/runtime_filter_compat/runtime_filter_port.h"
#include "exec/runtime_filter_compat/runtime_filter_serde.h"
#include "exec_primitive/runtime_filter/runtime_filter_descriptor.h"
#include "exec_primitive/runtime_filter/runtime_filter_probe.h"
#include "runtime/runtime_filter.h"
#include "runtime/runtime_filter_cache.h"
#include "runtime/runtime_filter_factory.h"
#include "runtime/runtime_filter_sender.h"
#include "runtime/runtime_state.h"
#include "runtime/service_contexts.h"
#include "storage_primitive/runtime_filter_predicate.h"
#include "testutil/exprs_test_helper.h"

namespace starrocks {

static std::string serialize_filter(RuntimeFilter* rf) {
    std::string data(RuntimeFilterSerde::max_size(RF_VERSION_V3, rf), '\0');
    data.resize(RuntimeFilterSerde::serialize(RF_VERSION_V3, rf, reinterpret_cast<uint8_t*>(data.data())));
    return data;
}

// A pass-all filter goes through the wire and the merger's concat, the way a global runtime filter does, and still
// passes every row, including the extreme values and null.
template <LogicalType LT>
static void check_pass_all_filter(const std::vector<RunTimeCppType<LT>>& build_values,
                                  const std::vector<RunTimeCppType<LT>>& probe_values) {
    ObjectPool pool;
    RuntimeFilter* pass_all =
            RuntimeFilterFactory::create_pass_all_filter(&pool, LT, TRuntimeFilterBuildJoinMode::PARTITIONED);
    ASSERT_TRUE(pass_all != nullptr);
    ASSERT_EQ(RuntimeFilterSerializeType::EMPTY_FILTER, pass_all->type());
    ASSERT_FALSE(pass_all->get_membership_filter()->can_use_bf());
    ASSERT_TRUE(pass_all->has_null());

    std::vector<uint8_t> buffer(RuntimeFilterSerde::max_size(RF_VERSION_V3, pass_all), 0);
    buffer.resize(RuntimeFilterSerde::serialize(RF_VERSION_V3, pass_all, buffer.data()));
    RuntimeFilter* received = nullptr;
    RuntimeFilterSerde::deserialize(&pool, &received, buffer.data(), buffer.size());
    ASSERT_TRUE(received != nullptr);
    ASSERT_TRUE(received->has_null());

    // a partial filter of another build instance, degraded to an empty filter by the merger.
    auto* partial = pool.add(new ComposedRuntimeBloomFilter<LT>());
    partial->membership_filter().init(build_values.size());
    for (const auto& v : build_values) {
        partial->insert(v);
    }
    RuntimeFilter* out = RuntimeFilterFactory::to_empty_filter(&pool, partial->create_empty(&pool));
    out->concat(RuntimeFilterFactory::to_empty_filter(&pool, partial));
    out->concat(received);
    // the probes evaluate the merged filter that they receive.
    std::vector<uint8_t> total(RuntimeFilterSerde::max_size(RF_VERSION_V3, out), 0);
    total.resize(RuntimeFilterSerde::serialize(RF_VERSION_V3, out, total.data()));
    RuntimeFilter* probe_filter = nullptr;
    RuntimeFilterSerde::deserialize(&pool, &probe_filter, total.data(), total.size());
    ASSERT_TRUE(probe_filter != nullptr);
    // the receiver recognizes it, and marks it always true for the probes to skip it.
    ASSERT_TRUE(received->is_pass_all());
    ASSERT_TRUE(probe_filter->is_pass_all());

    auto col = RunTimeColumnType<LT>::create();
    for (const auto& v : probe_values) {
        col->append(v);
    }
    auto nullable_col = ColumnHelper::cast_to_nullable_column(col->clone());
    nullable_col->append_nulls(1);

    RuntimeFilter::RunningContext ctx;
    probe_filter->evaluate(nullable_col.get(), &ctx);
    ASSERT_EQ(probe_values.size() + 1, ctx.selection.size());
    for (size_t i = 0; i < ctx.selection.size(); i++) {
        EXPECT_EQ(1, ctx.selection[i]) << "row " << i;
    }
}

// A filter is taken for one that passes every row by its content: a degraded filter whose min/max happens to cover
// every value and null does pass every row, and a narrower one does not.
TEST(RuntimeFilterDeliveryTest, is_pass_all_by_content) {
    ObjectPool pool;
    auto* bloom = pool.add(new ComposedRuntimeBloomFilter<TYPE_INT>());
    bloom->membership_filter().init(2);
    bloom->insert(std::numeric_limits<int32_t>::lowest());
    bloom->insert(std::numeric_limits<int32_t>::max());
    bloom->insert_null();
    ASSERT_FALSE(bloom->is_pass_all());
    auto* degraded = RuntimeFilterFactory::to_empty_filter(&pool, bloom);
    std::vector<uint8_t> buffer(RuntimeFilterSerde::max_size(RF_VERSION_V3, degraded), 0);
    buffer.resize(RuntimeFilterSerde::serialize(RF_VERSION_V3, degraded, buffer.data()));
    RuntimeFilter* received = nullptr;
    RuntimeFilterSerde::deserialize(&pool, &received, buffer.data(), buffer.size());
    // the same bounds and null: it does pass every row.
    ASSERT_TRUE(received->is_pass_all());

    auto* narrow = pool.add(new ComposedRuntimeBloomFilter<TYPE_INT>());
    narrow->membership_filter().init(1);
    narrow->insert(5);
    narrow->insert_null();
    ASSERT_FALSE(RuntimeFilterFactory::to_empty_filter(&pool, narrow)->is_pass_all());
}

// No min/max of a floating type passes NaN, so there is no pass-all filter of it; and a filter degraded from real keys
// whose bounds are +-infinity, which still filters NaN out, is not taken for one.
TEST(RuntimeFilterDeliveryTest, no_pass_all_filter_of_floating_type) {
    ObjectPool pool;
    ASSERT_EQ(nullptr, RuntimeFilterFactory::create_pass_all_filter(&pool, TYPE_DOUBLE,
                                                                    TRuntimeFilterBuildJoinMode::PARTITIONED));
    ASSERT_EQ(nullptr, RuntimeFilterFactory::create_pass_all_filter(&pool, TYPE_FLOAT,
                                                                    TRuntimeFilterBuildJoinMode::PARTITIONED));

    auto* bloom = pool.add(new ComposedRuntimeBloomFilter<TYPE_DOUBLE>());
    bloom->membership_filter().init(2);
    bloom->insert(-std::numeric_limits<double>::infinity());
    bloom->insert(std::numeric_limits<double>::infinity());
    bloom->insert_null();
    auto* degraded = RuntimeFilterFactory::to_empty_filter(&pool, bloom);
    std::string data = serialize_filter(degraded);
    RuntimeFilter* received = nullptr;
    RuntimeFilterSerde::deserialize(&pool, &received, reinterpret_cast<const uint8_t*>(data.data()), data.size());
    ASSERT_TRUE(received != nullptr);
    ASSERT_FALSE(received->is_pass_all());
}

TEST(RuntimeFilterDeliveryTest, pass_all_filter) {
    check_pass_all_filter<TYPE_INT>(
            {5, 10}, {std::numeric_limits<int32_t>::lowest(), -1, 5, 7, 200, std::numeric_limits<int32_t>::max()});
    check_pass_all_filter<TYPE_BIGINT>(
            {5, 10}, {std::numeric_limits<int64_t>::lowest(), 0, std::numeric_limits<int64_t>::max()});
    check_pass_all_filter<TYPE_DATE>(
            {DateValue::create(2020, 1, 1)},
            {DateValue::MIN_DATE_VALUE, DateValue::create(2000, 1, 1), DateValue::MAX_DATE_VALUE});
    check_pass_all_filter<TYPE_VARCHAR>({Slice("bb"), Slice("cc")}, {Slice(""), Slice("aa"), Slice("zz")});
}

namespace {

TRuntimeFilterDescription make_build_desc(TRuntimeFilterBuildJoinMode::type join_mode) {
    TRuntimeFilterLayout layout;
    layout.__set_filter_id(1);
    layout.__set_local_layout(TRuntimeFilterLayoutMode::SINGLETON);
    layout.__set_global_layout(TRuntimeFilterLayoutMode::SINGLETON);
    layout.__set_pipeline_level_multi_partitioned(false);
    layout.__set_num_instances(2);
    layout.__set_num_drivers_per_instance(1);

    TNetworkAddress merge_node;
    merge_node.__set_hostname("127.0.0.1");
    merge_node.__set_port(8060);

    TRuntimeFilterDescription desc;
    desc.__set_filter_id(1);
    desc.__set_expr_order(0);
    desc.__set_has_remote_targets(true);
    desc.__set_build_join_mode(join_mode);
    desc.__set_filter_type(TRuntimeFilterBuildType::JOIN_FILTER);
    desc.__set_build_expr(ExprsTestHelper::create_column_ref_t_expr<TYPE_INT>(1, false));
    desc.__set_layout(layout);
    desc.__set_plan_node_id_to_target_expr({{2, ExprsTestHelper::create_column_ref_t_expr<TYPE_INT>(3, false)}});
    desc.__set_runtime_filter_merge_nodes({merge_node});
    return desc;
}

class CapturingRuntimeFilterSender final : public RuntimeFilterSender {
public:
    void send_part_runtime_filter(PTransmitRuntimeFilterParams&& params, const std::vector<TNetworkAddress>& addrs,
                                  int timeout_ms, int64_t rpc_http_min_size, EventType type) override {
        parts.emplace_back(std::move(params));
    }
    void send_broadcast_runtime_filter(PTransmitRuntimeFilterParams&& params,
                                       const std::vector<TRuntimeFilterDestination>& destinations, int timeout_ms,
                                       int64_t rpc_http_min_size) override {
        broadcasts.emplace_back(std::move(params));
    }

    std::vector<PTransmitRuntimeFilterParams> parts;
    std::vector<PTransmitRuntimeFilterParams> broadcasts;
};

} // namespace

class RuntimeFilterPublishTest : public ::testing::Test {
protected:
    void SetUp() override {
        _runtime_services.runtime_filter_sender = &_sender;
        _runtime_services.runtime_filter_cache = &_rf_cache;
        _query_execution_services.runtime = &_runtime_services;
        _state = std::make_unique<RuntimeState>(TUniqueId(), TUniqueId(), TQueryOptions(), TQueryGlobals(),
                                                &_query_execution_services, nullptr);
        _state->set_func_version(TFunctionVersion::RUNTIME_FILTER_SERIALIZE_VERSION_3);
    }

    RuntimeFilterBuildDescriptor* make_desc(TRuntimeFilterBuildJoinMode::type join_mode) {
        auto* desc = _pool.add(new RuntimeFilterBuildDescriptor());
        EXPECT_OK(desc->init(&_pool, make_build_desc(join_mode), _state.get()));
        return desc;
    }

    void publish(RuntimeFilterBuildDescriptor* desc) {
        RuntimeFilterPort port(_state.get());
        port.publish_runtime_filters({desc});
    }

    ObjectPool _pool;
    CapturingRuntimeFilterSender _sender;
    RuntimeFilterCache _rf_cache{2};
    RuntimeServices _runtime_services;
    QueryExecutionServices _query_execution_services;
    std::unique_ptr<RuntimeState> _state;
};

// A build instance of a shuffle join without a filter, e.g. one that spilled, still sends its partial filter to the
// merge node, as a filter that passes every row.
TEST_F(RuntimeFilterPublishTest, partitioned_join_without_filter_sends_pass_all) {
    auto* desc = make_desc(TRuntimeFilterBuildJoinMode::PARTITIONED);
    ASSERT_EQ(nullptr, desc->runtime_filter());
    publish(desc);

    ASSERT_EQ(1, _sender.parts.size());
    const auto& params = _sender.parts[0];
    ASSERT_TRUE(params.is_partial());
    ASSERT_EQ(1, params.filter_id());

    ObjectPool pool;
    RuntimeFilter* rf = nullptr;
    RuntimeFilterSerde::deserialize(&pool, &rf, reinterpret_cast<const uint8_t*>(params.data().data()),
                                    params.data().size());
    ASSERT_TRUE(rf != nullptr);
    ASSERT_EQ(RuntimeFilterSerializeType::EMPTY_FILTER, rf->type());
    ASSERT_TRUE(rf->has_null());

    auto col = Int32Column::create();
    col->append(std::numeric_limits<int32_t>::lowest());
    col->append(0);
    col->append(std::numeric_limits<int32_t>::max());
    RuntimeFilter::RunningContext ctx;
    rf->evaluate(col.get(), &ctx);
    for (auto selected : ctx.selection) {
        EXPECT_EQ(1, selected);
    }
}

// The other senders of a broadcast join may still send the real filter, so nothing is sent.
TEST_F(RuntimeFilterPublishTest, broadcast_join_without_filter_sends_nothing) {
    publish(make_desc(TRuntimeFilterBuildJoinMode::BROADCAST));
    ASSERT_TRUE(_sender.parts.empty());
    ASSERT_TRUE(_sender.broadcasts.empty());
}

// An empty filter can not be serialized before RUNTIME_FILTER_SERIALIZE_VERSION_3.
TEST_F(RuntimeFilterPublishTest, old_serialize_version_sends_nothing) {
    _state->set_func_version(TFunctionVersion::RUNTIME_FILTER_SERIALIZE_VERSION_2);
    publish(make_desc(TRuntimeFilterBuildJoinMode::PARTITIONED));
    ASSERT_TRUE(_sender.parts.empty());
}

// A pass-all join filter, marked always true by the receiver, is skipped by the storage layer too.
TEST_F(RuntimeFilterPublishTest, storage_skips_pass_all_filter) {
    TRuntimeFilterDescription tdesc;
    tdesc.__set_filter_id(1);
    tdesc.__set_has_remote_targets(true);
    tdesc.__set_build_plan_node_id(1);
    tdesc.__set_build_join_mode(TRuntimeFilterBuildJoinMode::PARTITIONED);
    tdesc.__set_filter_type(TRuntimeFilterBuildType::JOIN_FILTER);
    tdesc.__set_plan_node_id_to_target_expr({{1, ExprsTestHelper::create_column_ref_t_expr<TYPE_INT>(0, false)}});
    auto* probe_desc = _pool.add(new RuntimeFilterProbeDescriptor());
    ASSERT_OK(probe_desc->init(&_pool, tdesc, 1, _state.get()));

    std::string data = serialize_filter(
            RuntimeFilterFactory::create_pass_all_filter(&_pool, TYPE_INT, TRuntimeFilterBuildJoinMode::PARTITIONED));
    RuntimeFilter* rf = nullptr;
    RuntimeFilterSerde::deserialize(nullptr, &rf, reinterpret_cast<const uint8_t*>(data.data()), data.size());
    ASSERT_TRUE(rf != nullptr && rf->is_pass_all());
    rf->set_always_true();
    probe_desc->set_shared_runtime_filter(std::shared_ptr<RuntimeFilter>(rf));

    auto col = Int32Column::create();
    col->append(std::numeric_limits<int32_t>::lowest());
    col->append(0);
    col->append(std::numeric_limits<int32_t>::max());
    Chunk chunk;
    chunk.append_column(ColumnPtr(std::move(col)), 0, true);

    RuntimeFilterPredicate pred(probe_desc, 0);
    ASSERT_TRUE(pred.init(0));
    std::vector<uint8_t> selection(3, 1);
    ASSERT_OK(pred.evaluate(&chunk, selection.data(), 0, 3));
    ASSERT_EQ(std::vector<uint8_t>({1, 1, 1}), selection);

    std::vector<uint16_t> sel{0, 1, 2};
    std::vector<uint16_t> target_sel(3);
    auto size = pred.evaluate(&chunk, sel.data(), 3, target_sel.data());
    ASSERT_OK(size.status());
    ASSERT_EQ(3, size.value());
    ASSERT_EQ(sel, target_sel);
}

// When some build driver spills, the merged filter is always true, but its descriptors are still handed out without
// runtime filters, so that they are published as pass-all filters.
TEST_F(RuntimeFilterPublishTest, always_true_keeps_descriptors) {
    for (bool spill_last : {true, false}) {
        auto* desc = make_desc(TRuntimeFilterBuildJoinMode::PARTITIONED);
        pipeline::PartialRuntimeFilterMerger merger(&_pool, 100, 1 << 20,
                                                    TFunctionVersion::RUNTIME_FILTER_SERIALIZE_VERSION_3, true);
        merger.incr_builder();
        merger.incr_builder();

        auto add_built = [&]() {
            auto column = Int32Column::create();
            column->append(0);
            column->append(7);
            pipeline::OpTRuntimeBloomFilterBuildParams params;
            params.emplace_back(pipeline::RuntimeMembershipFilterBuildParam(false, false, false, {std::move(column)},
                                                                            nullptr, TypeDescriptor(TYPE_INT)));
            return merger.add_partial_filters(0, 1, {}, std::move(params), {desc});
        };
        auto add_spilled = [&]() { return merger.set_always_true({desc}); };

        auto first = spill_last ? add_built() : add_spilled();
        ASSERT_OK(first.status());
        ASSERT_FALSE(first.value());
        auto last = spill_last ? add_spilled() : add_built();
        ASSERT_OK(last.status());
        ASSERT_TRUE(last.value());

        auto descs = merger.get_total_bloom_filters();
        ASSERT_EQ(1, descs.size());
        ASSERT_EQ(desc, descs.front());
        ASSERT_EQ(nullptr, desc->runtime_filter());
    }
}

namespace {

std::string serialize_bloom_filter(ObjectPool* pool, int32_t num_values) {
    auto* rf = pool->add(new ComposedRuntimeBloomFilter<TYPE_INT>());
    rf->membership_filter().init(num_values);
    for (int32_t i = 0; i < num_values; i++) {
        rf->insert(i * 3);
    }
    return serialize_filter(rf);
}

PTransmitRuntimeFilterParams make_partial(int32_t be_number, std::string data) {
    PTransmitRuntimeFilterParams params;
    params.set_is_partial(true);
    params.set_is_pipeline(false);
    params.set_filter_id(1);
    params.set_build_be_number(be_number);
    params.mutable_query_id()->set_hi(1);
    params.mutable_query_id()->set_lo(2);
    params.set_data(std::move(data));
    return params;
}

TNetworkAddress unreachable_address() {
    // nothing listens on it, so the runtime filter rpc fails, which does not matter to the tests.
    TNetworkAddress address;
    address.__set_hostname("127.0.0.1");
    address.__set_port(1);
    return address;
}

} // namespace

class RuntimeFilterMergerDeliveryTest : public ::testing::Test {
protected:
    void SetUp() override {
        _timer = std::make_unique<BthreadTimer>();
        ASSERT_OK(_timer->start());
        _stub_cache = std::make_unique<BrpcStubCache>(_timer.get());
        _rpc_services.brpc_stub_cache = _stub_cache.get();
        _runtime_services.runtime_filter_cache = &_rf_cache;
    }

    void TearDown() override {
        _stub_cache.reset();
        _timer.reset();
    }

    std::unique_ptr<RuntimeFilterMerger> make_merger(int32_t num_builders) {
        TRuntimeFilterProberParams prober;
        prober.__set_fragment_instance_id(TUniqueId());
        prober.__set_fragment_instance_address(unreachable_address());
        TRuntimeFilterParams params;
        params.__set_id_to_prober_params({{1, {prober}}});
        params.__set_runtime_filter_builder_number({{1, num_builders}});
        auto merger = std::make_unique<RuntimeFilterMerger>(&_runtime_services, &_rpc_services, UniqueId(1, 2),
                                                            TQueryOptions(), false);
        EXPECT_OK(merger->init(params));
        return merger;
    }

    static RuntimeFilterMergerStatus& status(RuntimeFilterMerger* merger) { return merger->_statuses.at(1); }

    std::unique_ptr<RuntimeFilterMerger> make_skew_merger(int64_t max_size) {
        TRuntimeFilterProberParams prober;
        prober.__set_fragment_instance_id(TUniqueId());
        prober.__set_fragment_instance_address(unreachable_address());
        TRuntimeFilterParams params;
        params.__set_id_to_prober_params({{1, {prober}}});
        params.__set_runtime_filter_builder_number({{1, 2}});
        params.__set_skew_join_runtime_filters({1});
        params.__set_runtime_filter_max_size(max_size);
        auto merger = std::make_unique<RuntimeFilterMerger>(&_runtime_services, &_rpc_services, UniqueId(1, 2),
                                                            TQueryOptions(), false);
        EXPECT_OK(merger->init(params));
        return merger;
    }

    static PTransmitRuntimeFilterParams make_skew_material() {
        auto mutable_column = Int32Column::create();
        mutable_column->append(0);
        mutable_column->append(100);
        ColumnPtr column = std::move(mutable_column);
        std::string data(RuntimeFilterSerde::skew_max_size(column), '\0');
        auto size = RuntimeFilterSerde::skew_serialize(column, false, reinterpret_cast<uint8_t*>(data.data()));
        EXPECT_OK(size.status());
        data.resize(size.value());
        auto params = make_partial(2, std::move(data));
        params.set_is_skew_broadcast_join(true);
        params.set_skew_shuffle_filter_id(1);
        *params.mutable_columntype() = TypeDescriptor(TYPE_INT).to_protobuf();
        return params;
    }

    ObjectPool _pool;
    std::unique_ptr<BthreadTimer> _timer;
    std::unique_ptr<BrpcStubCache> _stub_cache;
    RpcServices _rpc_services;
    RuntimeFilterCache _rf_cache{2};
    RuntimeServices _runtime_services;
};

// A partial filter that can not be deserialized still counts, and makes the merged filter pass every row, instead of
// leaving it incomplete forever.
TEST_F(RuntimeFilterMergerDeliveryTest, partial_filter_can_not_be_deserialized) {
    // empty, and of an unknown version.
    for (const std::string& bad_data : {std::string(), std::string(1, '\x01')}) {
        auto merger = make_merger(2);
        auto partial0 = make_partial(0, serialize_bloom_filter(&_pool, 100));
        merger->merge_runtime_filter(partial0);
        ASSERT_FALSE(status(merger.get()).isSent);

        auto partial1 = make_partial(1, bad_data);
        merger->merge_runtime_filter(partial1);
        ASSERT_TRUE(status(merger.get()).isSent);
        // degraded: the bloom filters are dropped, and min/max covers the pass-all filter.
        ASSERT_FALSE(status(merger.get()).exceeded);
    }
}

// A partial filter that passes every row, from a build instance that could not build its filter, degrades the merged
// filter the same way.
TEST_F(RuntimeFilterMergerDeliveryTest, pass_all_partial_filter) {
    auto merger = make_merger(2);
    auto partial0 = make_partial(0, serialize_filter(RuntimeFilterFactory::create_pass_all_filter(
                                            &_pool, TYPE_INT, TRuntimeFilterBuildJoinMode::PARTITIONED)));
    merger->merge_runtime_filter(partial0);
    ASSERT_FALSE(status(merger.get()).isSent);
    auto partial1 = make_partial(1, serialize_bloom_filter(&_pool, 100));
    merger->merge_runtime_filter(partial1);
    ASSERT_TRUE(status(merger.get()).isSent);
    ASSERT_FALSE(status(merger.get()).exceeded);
}

// Without any partial filter that can be deserialized, the type is unknown and nothing can be sent.
TEST_F(RuntimeFilterMergerDeliveryTest, no_partial_filter_can_be_deserialized) {
    auto merger = make_merger(2);
    auto partial0 = make_partial(0, "");
    auto partial1 = make_partial(1, "");
    merger->merge_runtime_filter(partial0);
    merger->merge_runtime_filter(partial1);
    ASSERT_FALSE(status(merger.get()).isSent);
}

TEST_F(RuntimeFilterMergerDeliveryTest, normal_partial_filters) {
    auto merger = make_merger(2);
    auto partial0 = make_partial(0, serialize_bloom_filter(&_pool, 100));
    auto duplicated = make_partial(0, serialize_bloom_filter(&_pool, 100));
    merger->merge_runtime_filter(partial0);
    merger->merge_runtime_filter(duplicated);
    ASSERT_FALSE(status(merger.get()).isSent);
    auto partial1 = make_partial(1, serialize_bloom_filter(&_pool, 100));
    merger->merge_runtime_filter(partial1);
    ASSERT_TRUE(status(merger.get()).isSent);
    ASSERT_TRUE(status(merger.get()).exceeded);
}

// A skew join filter that degrades to min/max drops the keys of the skew broadcast join, which the min/max of the
// partial filters does not cover, so a filter of their min/max stands in for them.
TEST_F(RuntimeFilterMergerDeliveryTest, degraded_skew_join_filter_keeps_broadcast_keys) {
    // the stand-in covers exactly the broadcast keys, and does not pass every row.
    {
        auto merger = make_skew_merger(1);
        auto partial0 = make_partial(0, serialize_bloom_filter(&_pool, 10));
        merger->merge_runtime_filter(partial0);
        auto material = make_skew_material();
        merger->store_skew_broadcast_join_runtime_filter(material);
        auto& st = status(merger.get());
        ColumnPtr keys = st.skew_broadcast_rf_material->key_column;
        ASSERT_TRUE(merger->_add_skew_broadcast_min_max_filter(&st, st.filters.begin()->second, keys));
        auto* stand_in = down_cast<ComposedRuntimeEmptyFilter<TYPE_INT>*>(st.filters.at(-1));
        ASSERT_EQ(100, stand_in->min_max_filter().min_value(&_pool));
        ASSERT_EQ(100, stand_in->min_max_filter().max_value(&_pool));
        ASSERT_FALSE(stand_in->is_pass_all());
    }
    // over max_size, the last partial filter degrades it.
    {
        auto merger = make_skew_merger(1);
        auto partial0 = make_partial(0, serialize_bloom_filter(&_pool, 10));
        auto partial1 = make_partial(1, serialize_bloom_filter(&_pool, 10));
        auto material = make_skew_material();
        merger->merge_runtime_filter(partial0);
        merger->store_skew_broadcast_join_runtime_filter(material);
        merger->merge_runtime_filter(partial1);
        ASSERT_TRUE(status(merger.get()).isSent);
        ASSERT_FALSE(status(merger.get()).exceeded);
        ASSERT_TRUE(status(merger.get()).filters.contains(-1));
    }
    // a partial filter that passes every row degrades it, also when the material arrives last.
    {
        auto merger = make_skew_merger(1L << 30);
        auto partial0 = make_partial(0, serialize_bloom_filter(&_pool, 10));
        auto partial1 = make_partial(1, serialize_filter(RuntimeFilterFactory::create_pass_all_filter(
                                                &_pool, TYPE_INT, TRuntimeFilterBuildJoinMode::PARTITIONED)));
        auto material = make_skew_material();
        merger->merge_runtime_filter(partial0);
        merger->merge_runtime_filter(partial1);
        merger->store_skew_broadcast_join_runtime_filter(material);
        ASSERT_TRUE(status(merger.get()).isSent);
        ASSERT_FALSE(status(merger.get()).exceeded);
        ASSERT_TRUE(status(merger.get()).filters.contains(-1));
    }
}

// When the material arrives last, the bloom filters are sent over max_size, as they have always been.
TEST_F(RuntimeFilterMergerDeliveryTest, skew_join_material_last_keeps_bloom_over_size_limit) {
    auto merger = make_skew_merger(1);
    auto partial0 = make_partial(0, serialize_bloom_filter(&_pool, 10));
    auto partial1 = make_partial(1, serialize_bloom_filter(&_pool, 10));
    auto material = make_skew_material();
    merger->merge_runtime_filter(partial0);
    merger->merge_runtime_filter(partial1);
    merger->store_skew_broadcast_join_runtime_filter(material);
    ASSERT_TRUE(status(merger.get()).isSent);
    ASSERT_TRUE(status(merger.get()).exceeded);
    ASSERT_FALSE(status(merger.get()).filters.contains(-1));
}

// A skew join filter within the size limit merges the skew broadcast keys into its bloom filter.
TEST_F(RuntimeFilterMergerDeliveryTest, skew_join_filter_within_limit) {
    auto merger = make_skew_merger(1L << 30);
    auto partial0 = make_partial(0, serialize_bloom_filter(&_pool, 10));
    auto partial1 = make_partial(1, serialize_bloom_filter(&_pool, 10));
    auto material = make_skew_material();
    merger->merge_runtime_filter(partial0);
    merger->merge_runtime_filter(partial1);
    ASSERT_FALSE(status(merger.get()).isSent);
    merger->store_skew_broadcast_join_runtime_filter(material);
    ASSERT_TRUE(status(merger.get()).isSent);
    ASSERT_TRUE(status(merger.get()).exceeded);
    ASSERT_FALSE(status(merger.get()).filters.contains(-1));
}

// A total filter that can not be deserialized is still forwarded to the rest of the relay chain.
TEST_F(RuntimeFilterMergerDeliveryTest, receiver_forwards_filter_that_can_not_be_deserialized) {
    _rf_cache.set_enable_trace(true);
    orchestration::RuntimeFilterDelivery delivery(&_runtime_services, &_rpc_services, nullptr);
    auto request = make_partial(0, std::string(1, '\x01'));
    request.set_is_partial(false);
    auto* target = request.add_forward_targets();
    target->set_host("127.0.0.1");
    target->set_port(1);
    delivery.receive_total_runtime_filter(request, 1000, std::numeric_limits<int64_t>::max());

    bool forwarded = false;
    for (const auto& [_, events] : _rf_cache.get_events()) {
        for (const auto& event : events) {
            forwarded |= event.find("FORWARD") != std::string::npos;
        }
    }
    ASSERT_TRUE(forwarded);
}

TEST(RuntimeFilterSerdeDeliveryTest, shrink_to_pass_all) {
    ObjectPool pool;
    // a big bloom filter is replaced with a pass-all filter.
    {
        std::string data = serialize_bloom_filter(&pool, 10000);
        const size_t size = data.size();
        ASSERT_TRUE(RuntimeFilterSerde::shrink_to_pass_all(&data));
        ASSERT_LT(data.size(), size);
        RuntimeFilter* rf = nullptr;
        RuntimeFilterSerde::deserialize(&pool, &rf, reinterpret_cast<const uint8_t*>(data.data()), data.size());
        ASSERT_TRUE(rf != nullptr);
        ASSERT_EQ(RuntimeFilterSerializeType::EMPTY_FILTER, rf->type());
        ASSERT_TRUE(rf->has_null());
    }
    // a filter that already passes every row, e.g. a degraded or resent one, still goes through.
    {
        std::string data = serialize_filter(
                RuntimeFilterFactory::create_pass_all_filter(&pool, TYPE_VARCHAR, TRuntimeFilterBuildJoinMode::NONE));
        const size_t size = data.size();
        ASSERT_TRUE(RuntimeFilterSerde::shrink_to_pass_all(&data));
        ASSERT_EQ(size, data.size());
    }
    // a pass-all string filter is bigger than a small bloom filter, which is left as is.
    {
        auto* rf = pool.add(new ComposedRuntimeBloomFilter<TYPE_VARCHAR>());
        rf->membership_filter().init(1);
        rf->insert(Slice("a"));
        std::string data = serialize_filter(rf);
        const std::string original = data;
        ASSERT_FALSE(RuntimeFilterSerde::shrink_to_pass_all(&data));
        ASSERT_EQ(original, data);
    }
    // an in filter, a truncated filter, or one of an old version can not be replaced.
    {
        auto* in_filter = RuntimeFilterFactory::create_in_filter(&pool, TYPE_INT, 0);
        std::string data = serialize_filter(in_filter);
        ASSERT_FALSE(RuntimeFilterSerde::shrink_to_pass_all(&data));
        std::string truncated(2, '\x03');
        ASSERT_FALSE(RuntimeFilterSerde::shrink_to_pass_all(&truncated));
        std::string old_version = serialize_bloom_filter(&pool, 10);
        old_version[0] = RF_VERSION_V2;
        ASSERT_FALSE(RuntimeFilterSerde::shrink_to_pass_all(&old_version));
    }
}

} // namespace starrocks
