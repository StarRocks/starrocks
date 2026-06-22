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

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <future>
#include <iterator>
#include <memory>
#include <thread>
#include <utility>
#include <vector>

#include "base/testutil/assert.h"
#include "base/uid_util.h"
#include "base/utility/defer_op.h"
#include "column/adaptive_nullable_column.h"
#include "column/array_column.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/column_visitor_adapter.h"
#include "column/map_column.h"
#include "column/nullable_column.h"
#include "column/sorting/sorting.h"
#include "column/struct_column.h"
#include "column/vectorized_fwd.h"
#include "common/config_exec_fwd.h"
#include "common/config_storage_fwd.h"
#include "common/object_pool.h"
#include "common/runtime_profile.h"
#include "common/status.h"
#include "common/statusor.h"
#include "compute_env/sorting/merge.h"
#include "compute_env/spill/log_block_manager.h"
#include "compute_env/spill/mem_table.h"
#include "compute_env/spill/mem_tracker_guard.h"
#include "compute_env/spill/spill_components.h"
#include "compute_env/spill/spill_observable.h"
#include "compute_env/spill/spiller.h"
#include "compute_env/spill/spiller.hpp"
#include "compute_env/spill/spiller_factory.h"
#include "compute_env/spill/task_executor.h"
#include "compute_env/workgroup/scan_task.h"
#include "exec/pipeline/primitives/pipeline_observer.h"
#include "exprs/column_ref.h"
#include "exprs/expr_context.h"
#include "exprs/expr_factory.h"
#include "exprs/sort_exec_exprs.h"
#include "fs/fs.h"
#include "gen_cpp/Exprs_types.h"
#include "gen_cpp/Types_types.h"
#include "runtime/descriptors.h"
#include "runtime/mem_tracker.h"
#include "runtime/query_context_lifetime.h"
#include "runtime/runtime_state.h"
#include "storage/olap_define.h"
#include "types/logical_type.h"

namespace starrocks::vectorized {

namespace {

ColumnRef* find_first_column_ref(Expr* expr) {
    if (expr->is_slotref()) {
        return down_cast<ColumnRef*>(expr);
    }
    for (Expr* child : expr->children()) {
        if (ColumnRef* ref = find_first_column_ref(child); ref != nullptr) {
            return ref;
        }
    }
    return nullptr;
}

} // namespace

class TExprBuilder {
public:
    TExprBuilder& operator<<(const LogicalType& slot_type) {
        TExpr expr;
        TExprNode node;
        node.__set_node_type(TExprNodeType::SLOT_REF);
        TTypeDesc tdesc;
        TTypeNode ttpe;
        TScalarType scalar_tp;
        scalar_tp.type = to_thrift(slot_type);
        scalar_tp.__set_len(200);
        scalar_tp.__set_precision(27);
        scalar_tp.__set_scale(9);
        ttpe.__set_scalar_type(scalar_tp);
        ttpe.type = TTypeNodeType::SCALAR;
        tdesc.types.push_back(std::move(ttpe));
        node.__set_type(tdesc);
        TSlotRef slot_ref;
        slot_ref.__set_tuple_id(tuple_id);
        slot_ref.__set_slot_id(column_id++);
        node.__set_slot_ref(slot_ref);
        expr.nodes.push_back(std::move(node));
        res.push_back(expr);
        return *this;
    }
    std::vector<TExpr> get_res() { return res; }

private:
    const int tuple_id = 0;
    int column_id = 0;
    std::vector<TExpr> res;
};

class ColumnFiller : public ColumnVisitorMutableAdapter<ColumnFiller> {
public:
    const size_t append_size = 4096;

    ColumnFiller() : ColumnVisitorMutableAdapter(this) {}

    template <typename T>
    Status do_visit(T* column) {
        column->append_default(append_size);
        return Status::OK();
    }

    Status do_visit(NullableColumn* column) {
        RETURN_IF_ERROR(fill(column->null_column_raw_ptr()));
        RETURN_IF_ERROR(column->data_column_raw_ptr()->accept_mutable(this));
        return Status::OK();
    }

    Status fill(NullColumn* column) {
        auto& container = column->get_data();
        container.resize(append_size);
        std::generate(container.begin(), container.end(), []() { return rand() % 2; });
        return Status::OK();
    }

    template <typename T>
    Status do_visit(FixedLengthColumnBase<T>* column) {
        auto& container = column->get_data();
        container.resize(append_size);
        std::generate(container.begin(), container.end(), []() { return rand(); });
        return Status::OK();
    }
};

class RandomChunkBuilder {
public:
    ColumnFiller filler;
    ChunkPtr gen(const std::vector<ExprContext*>& ctxs, const std::vector<bool>& nullable) {
        ChunkPtr chunk = std::make_shared<Chunk>();
        for (size_t i = 0; i < ctxs.size(); ++i) {
            auto ctx = ctxs[i];
            CHECK(ctx->root()->is_slotref());
            auto ref = find_first_column_ref(ctx->root());
            auto col = ColumnHelper::create_column(ctx->root()->type(), nullable[i]);
            CHECK(col->accept_mutable(&filler).ok());
            chunk->append_column(std::move(col), ref->slot_id());
        }
        return chunk;
    }
};

struct SyncExecutor {
    static Status submit(workgroup::ScanTask task) {
        do {
            task.run();
        } while (!task.is_finished());
        return Status::OK();
    }
    static void force_submit(workgroup::ScanTask task) { (void)submit(std::move(task)); }
};

// Test-only executor whose submit() always fails: the IO task is discarded, so its lambda (and every defer
// inside it) never runs. Used to drive the submit-failure compensation paths deterministically, without
// the fail-point machinery.
struct FailingSubmitExecutor {
    static Status submit(workgroup::ScanTask task) { return Status::InternalError("inject submit failure"); }
    static void force_submit(workgroup::ScanTask task) { (void)submit(std::move(task)); }
};

// Records which trigger the spiller fired so notify-on-completion (and
// notify-on-submit-failure) can be asserted without a real driver.
class CountingObserver final : public pipeline::PipelineObserver {
public:
    void source_trigger() override { source_count++; }
    void sink_trigger() override { sink_count++; }
    void cancel_trigger() override { cancel_count++; }
    void all_trigger() override {
        source_count++;
        sink_count++;
    }
    void runtime_filter_timeout_trigger() override {}
    std::string debug_string() const override { return "CountingObserver"; }

    std::atomic_int32_t source_count{0};
    std::atomic_int32_t sink_count{0};
    std::atomic_int32_t cancel_count{0};
};

struct ASyncExecutor {
    using ExecFunction = std::function<void(workgroup::YieldContext&)>;

    static std::vector<std::future<void>> _futures;
    static Status submit(workgroup::ScanTask task) {
        _futures.emplace_back(std::async([task = std::move(task)]() mutable {
            do {
                task.run();
            } while (!task.is_finished());
        }));
        return Status::OK();
    }
    static void force_submit(workgroup::ScanTask task) { (void)submit(std::move(task)); }

    static void join() {
        for (auto& future : _futures) {
            future.get();
        }
    }
};
std::vector<std::future<void>> ASyncExecutor::_futures;

class BlockHoleOutputStream final : public spill::SpillOutputDataStream {
public:
    Status append(RuntimeState* state, const std::vector<Slice>& data, size_t total_write_size,
                  size_t write_num_rows) override {
        _write_total_size += total_write_size;
        return Status::OK();
    }
    Status flush() override { return Status::OK(); }
    bool is_remote() const override { return false; }
    const size_t total_size() const { return _write_total_size; }

private:
    size_t _write_total_size{};
};

using SpillProcessMetrics = spill::SpillProcessMetrics;
using EmptyMemGuard = spill::EmptyMemGuard;
using SpilledOptions = spill::SpilledOptions;

class SpillTest : public ::testing::Test {
public:
    void SetUp() override {
        TUniqueId dummy_query_id = generate_uuid();
        auto path = config::storage_root_path + "/spill_test_data/" + print_id(dummy_query_id);
        auto fs = FileSystem::Default();
        ASSERT_OK(fs->create_dir_recursive(path));
        LOG(WARNING) << "TRACE:" << path;
        dummy_dir_mgr = std::make_unique<spill::DirManager>();
        ASSERT_OK(dummy_dir_mgr->init(path, {config::storage_root_path}));

        dummy_block_mgr = std::make_unique<spill::LogBlockManager>(dummy_query_id, dummy_dir_mgr.get());

        dummy_rt_st.set_chunk_size(config::vector_chunk_size);

        metrics = SpillProcessMetrics(&dummy_profile, &spill_bytes);
    }
    void TearDown() override {}
    std::unique_ptr<spill::DirManager> dummy_dir_mgr;
    std::unique_ptr<spill::LogBlockManager> dummy_block_mgr;
    RuntimeState dummy_rt_st;
    RuntimeProfile dummy_profile{"dummy"};
    std::vector<std::string> clean_up;
    std::atomic_int64_t spill_bytes;
    SpillProcessMetrics metrics;
};

struct SpillTestContext {
    ObjectPool pool;
    // partition nums
    size_t partition_nums = 1;
    // partition exprs
    std::vector<ExprContext*> parition_exprs;
    //
    SortExecExprs sort_exprs;
    //
    SortDescs sort_descs;
};

StatusOr<SpillTestContext*> no_partition_context(ObjectPool* pool, RuntimeState* runtime_state,
                                                 const std::vector<TExpr>& order_bys, std::vector<TExpr>& tuple) {
    auto context = pool->add(new SpillTestContext());
    context->partition_nums = 1;
    //
    if (!order_bys.empty()) {
        RETURN_IF_ERROR(context->sort_exprs.init(order_bys, &tuple, &context->pool, runtime_state));
        RETURN_IF_ERROR(context->sort_exprs.prepare(runtime_state, {}, {}));
        RETURN_IF_ERROR(context->sort_exprs.open(runtime_state));
    }

    //
    std::vector<bool> ascs(order_bys.size());
    std::fill_n(ascs.begin(), order_bys.size(), true);
    std::vector<bool> null_firsts(order_bys.size());
    std::fill_n(null_firsts.begin(), order_bys.size(), true);
    context->sort_descs = {ascs, null_firsts};

    return context;
}

template <class Writer, class Reader>
struct SpillerCaller {
    SpillerCaller(spill::Spiller* spiller) : _spiller(spiller) {}

    template <class TaskExecutor, class MemGuard>
    Status spill(RuntimeState* state, const ChunkPtr& chunk, MemGuard&& guard) {
        if (_spiller->_chunk_builder.chunk_schema()->empty()) {
            _spiller->_chunk_builder.chunk_schema()->set_schema(chunk);
            RETURN_IF_ERROR(_spiller->_serde->prepare());
        }
        auto writer = _spiller->_writer->as<Writer>();
        return writer->template spill<TaskExecutor>(state, chunk, std::forward<MemGuard>(guard));
    }

    template <class TaskExecutor, class MemGuard>
    Status flush(RuntimeState* state, MemGuard&& guard) {
        auto writer = _spiller->_writer->as<Writer>();
        return writer->template flush<TaskExecutor>(state, std::forward<MemGuard>(guard));
    }

    template <class TaskExecutor, class MemGuard>
    StatusOr<ChunkPtr> restore(RuntimeState* state, MemGuard&& guard) {
        return _spiller->_reader->restore<TaskExecutor>(state, std::forward<MemGuard>(guard));
    }

    template <class TaskExecutor, class MemGuard>
    Status trigger_restore(RuntimeState* state, MemGuard&& guard) {
        if (!acquire_once) {
            acquire_once = true;
            RETURN_IF_ERROR(_spiller->_acquire_input_stream(state));
        }
        return _spiller->_reader->trigger_restore<TaskExecutor>(state, std::forward<MemGuard>(guard));
    }

    bool acquire_once = false;
    spill::Spiller* _spiller;
};

TEST_F(SpillTest, unsorted_process) {
    ObjectPool pool;

    // order by id_int
    TExprBuilder order_by_slots_builder;
    order_by_slots_builder << TYPE_INT;
    auto order_by_slots = order_by_slots_builder.get_res();
    // full data id_int, id_smallint
    std::vector<bool> nullables = {false, false};
    TExprBuilder tuple_slots_builder;
    tuple_slots_builder << TYPE_INT << TYPE_SMALLINT;
    auto tuple_slots = tuple_slots_builder.get_res();

    auto ctx_st = no_partition_context(&pool, &dummy_rt_st, order_by_slots, tuple_slots);
    ASSERT_OK(ctx_st.status());
    auto ctx = ctx_st.value();

    auto& tuple = ctx->sort_exprs.sort_tuple_slot_expr_ctxs();

    // create chunk
    RandomChunkBuilder chunk_builder;

    // create spilled factory
    // auto factory_options = SpilledFactoryOptions(ctx->partition_nums, ctx->parition_exprs, ctx->sort_exprs, ctx->sort_descs, false);
    auto factory = spill::make_spilled_factory();

    // create spiller
    SpilledOptions spill_options;
    // 4 buffer chunk
    spill_options.mem_table_pool_size = 4;
    // file size: 1M
    spill_options.spill_mem_table_bytes_size = 1 * 1024 * 1024;
    // spill format type
    spill_options.spill_type = spill::SpillFormaterType::SPILL_BY_COLUMN;

    spill_options.block_manager = dummy_block_mgr.get();

    auto chunk_empty = chunk_builder.gen(tuple, nullables);

    auto spiller = factory->create(spill_options);
    spiller->set_metrics(metrics);
    SpillerCaller<spill::RawSpillerWriter*, spill::SpillerReader*> caller(spiller.get());
    ASSERT_OK(spiller->prepare(&dummy_rt_st));

    size_t test_loop = 1024;
    std::vector<ChunkPtr> holder;
    {
        for (size_t i = 0; i < test_loop; ++i) {
            auto chunk = chunk_builder.gen(tuple, nullables);
            ASSERT_OK(caller.spill<SyncExecutor>(&dummy_rt_st, chunk, EmptyMemGuard{}));
            ASSERT_OK(spiller->_spilled_task_status);
            holder.push_back(chunk);
        }
        ASSERT_OK(caller.flush<SyncExecutor>(&dummy_rt_st, EmptyMemGuard{}));
    }
    size_t input_rows = 0;
    for (const auto& chunk : holder) {
        input_rows += chunk->num_rows();
    }

    // test restore
    {
        std::vector<ChunkPtr> restored;
        ASSERT_OK(caller.trigger_restore<SyncExecutor>(&dummy_rt_st, EmptyMemGuard{}));
        for (size_t i = 0; i < test_loop; ++i) {
            auto chunk_st = caller.restore<SyncExecutor>(&dummy_rt_st, EmptyMemGuard{});
            ASSERT_OK(chunk_st.status());
            ASSERT_OK(spiller->_spilled_task_status);
            if (chunk_st.value() != nullptr) {
                restored.emplace_back(std::move(chunk_st.value()));
            }
        }

        auto chunk_st = caller.restore<SyncExecutor>(&dummy_rt_st, EmptyMemGuard{});
        ASSERT_TRUE(chunk_st.status().is_end_of_file());

        size_t output_rows = 0;
        for (const auto& chunk : restored) {
            output_rows += chunk->num_rows();
        }
        ASSERT_EQ(input_rows, output_rows);
    }

    // test 2
    {
        for (size_t i = 0; i < test_loop; ++i) {
            if (!spiller->is_full()) {
                auto chunk = chunk_builder.gen(tuple, nullables);
                ASSERT_OK(caller.spill<ASyncExecutor>(&dummy_rt_st, chunk, EmptyMemGuard{}));
                ASSERT_OK(spiller->_spilled_task_status);
            }
        }
        ASyncExecutor::join();
    }

    {
        // dummy_rt_st
        // test schedule_mem_table_flush
        size_t max_buffer_size = 1024 * 1024 * 1024;
        std::shared_ptr<spill::SpillableMemTable> mem_table =
                std::make_shared<spill::UnorderedMemTable>(&dummy_rt_st, max_buffer_size, nullptr, spiller.get());
        std::vector<ChunkPtr> input;
        for (size_t i = 0; i < 500; ++i) {
            auto chunk = chunk_builder.gen(tuple, nullables);
            input.emplace_back(chunk->clone_unique());
            ASSERT_OK(mem_table->append(std::move(chunk)));
        }
        ASSERT_OK(mem_table->done());
        //
        auto output = std::make_shared<BlockHoleOutputStream>();
        workgroup::YieldContext yield_ctx;
        yield_ctx.task_context_data = std::make_shared<spill::SpillIOTaskContext>();
        do {
            yield_ctx.time_spent_ns = 0;
            yield_ctx.need_yield = false;
            ASSERT_OK(mem_table->finalize(yield_ctx, output));
        } while (yield_ctx.need_yield);
    }
}

struct FailedGuard {
    bool scoped_begin() const { return false; }
    void scoped_end() const {}
};

TEST_F(SpillTest, yield_with_failed_guard) {
    ObjectPool pool;
    // order by id_int
    TExprBuilder order_by_slots_builder;
    order_by_slots_builder << TYPE_INT;
    auto order_by_slots = order_by_slots_builder.get_res();
    // full data id_int, id_smallint
    std::vector<bool> nullables = {false, false};
    TExprBuilder tuple_slots_builder;
    tuple_slots_builder << TYPE_INT << TYPE_SMALLINT;
    auto tuple_slots = tuple_slots_builder.get_res();

    auto ctx_st = no_partition_context(&pool, &dummy_rt_st, order_by_slots, tuple_slots);
    ASSERT_OK(ctx_st.status());
    auto ctx = ctx_st.value();

    auto& tuple = ctx->sort_exprs.sort_tuple_slot_expr_ctxs();

    // create chunk
    RandomChunkBuilder chunk_builder;

    // create spilled factory
    // auto factory_options = SpilledFactoryOptions(ctx->partition_nums, ctx->parition_exprs, ctx->sort_exprs, ctx->sort_descs, false);
    auto factory = spill::make_spilled_factory();

    // create spiller
    SpilledOptions spill_options;
    // 4 buffer chunk
    spill_options.mem_table_pool_size = 4;
    // file size: 1M
    spill_options.spill_mem_table_bytes_size = 1 * 1024 * 1024;
    // spill format type
    spill_options.spill_type = spill::SpillFormaterType::SPILL_BY_COLUMN;

    spill_options.block_manager = dummy_block_mgr.get();

    auto chunk_empty = chunk_builder.gen(tuple, nullables);

    auto spiller = factory->create(spill_options);
    spiller->set_metrics(metrics);
    SpillerCaller<spill::RawSpillerWriter*, spill::SpillerReader*> caller(spiller.get());
    ASSERT_OK(spiller->prepare(&dummy_rt_st));

    size_t test_loop = 1024;
    std::vector<ChunkPtr> holder;
    {
        for (size_t i = 0; i < test_loop; ++i) {
            auto chunk = chunk_builder.gen(tuple, nullables);
            ASSERT_OK(caller.spill<SyncExecutor>(&dummy_rt_st, chunk, EmptyMemGuard{}));
            ASSERT_OK(spiller->_spilled_task_status);
            holder.push_back(chunk);
        }
        ASSERT_OK(caller.flush<SyncExecutor>(&dummy_rt_st, FailedGuard{}));
    }
}

TEST_F(SpillTest, order_by_process) {
    ObjectPool pool;
    // order by id_int
    TExprBuilder order_by_slots_builder;
    order_by_slots_builder << TYPE_INT;
    auto order_by_slots = order_by_slots_builder.get_res();
    // full data id_int, id_smallint
    std::vector<bool> nullables = {false, false};
    TExprBuilder tuple_slots_builder;
    tuple_slots_builder << TYPE_INT << TYPE_SMALLINT;
    auto tuple_slots = tuple_slots_builder.get_res();

    auto ctx_st = no_partition_context(&pool, &dummy_rt_st, order_by_slots, tuple_slots);
    ASSERT_OK(ctx_st.status());
    auto ctx = ctx_st.value();

    auto& tuple = ctx->sort_exprs.sort_tuple_slot_expr_ctxs();

    // create chunk
    RandomChunkBuilder chunk_builder;

    // create spilled factory
    auto factory = spill::make_spilled_factory();

    // create spiller
    SpilledOptions spill_options(&ctx->sort_exprs, &ctx->sort_descs);
    // 4 buffer chunk
    spill_options.mem_table_pool_size = 2;
    // file size: 1M
    spill_options.spill_mem_table_bytes_size = 1 * 1024 * 1024;
    // spill format type
    spill_options.spill_type = spill::SpillFormaterType::SPILL_BY_COLUMN;
    // enable compaction for spill
    spill_options.enable_block_compaction = true;

    spill_options.block_manager = dummy_block_mgr.get();

    auto chunk_empty = chunk_builder.gen(tuple, nullables);

    // Test 1
    {
        auto spiller = factory->create(spill_options);
        spiller->set_metrics(metrics);
        SpillerCaller<spill::RawSpillerWriter*, spill::SpillerReader*> caller(spiller.get());
        ASSERT_OK(spiller->prepare(&dummy_rt_st));

        size_t test_loop = 1024;
        std::vector<ChunkPtr> holder;
        size_t contain_rows = 0;
        {
            for (size_t i = 0; i < test_loop; ++i) {
                auto chunk = chunk_builder.gen(tuple, nullables);
                ASSERT_OK(caller.spill<SyncExecutor>(&dummy_rt_st, chunk, EmptyMemGuard{}));
                ASSERT_OK(spiller->_spilled_task_status);
                holder.push_back(chunk);
                contain_rows += chunk->num_rows();
            }
            ASSERT_OK(caller.flush<SyncExecutor>(&dummy_rt_st, EmptyMemGuard{}));
        }

        std::vector<ChunkPtr> restored;
        size_t restored_rows = 0;
        {
            ASSERT_OK(caller.trigger_restore<SyncExecutor>(&dummy_rt_st, EmptyMemGuard{}));
            ASSERT_TRUE(caller._spiller->has_output_data());
            for (size_t i = 0; i < test_loop; ++i) {
                auto chunk_st = caller.restore<SyncExecutor>(&dummy_rt_st, EmptyMemGuard{});
                ASSERT_OK(chunk_st.status());
                ASSERT_OK(spiller->_spilled_task_status);
                if (chunk_st.value() != nullptr) {
                    LOG(INFO) << "restored:" << chunk_st.value()->num_rows();
                    restored_rows += chunk_st.value()->num_rows();
                    restored.emplace_back(std::move(chunk_st.value()));
                }
            }

            auto chunk_st = caller.restore<SyncExecutor>(&dummy_rt_st, EmptyMemGuard{});
            ASSERT_TRUE(chunk_st.status().is_end_of_file());
        }
        ASSERT_EQ(contain_rows, restored_rows);
        ASSERT_GT(metrics.compact_count->value(), 0);
    }
}

TEST_F(SpillTest, partition_process) {
    ObjectPool pool;

    // order by id_int
    // full data id_int, id_smallint
    std::vector<bool> nullables = {false, false};
    TExprBuilder tuple_slots_builder;
    tuple_slots_builder << TYPE_INT;
    auto tuple_slots = tuple_slots_builder.get_res();

    auto ctx_st = no_partition_context(&pool, &dummy_rt_st, {}, tuple_slots);
    ASSERT_OK(ctx_st.status());
    auto ctx = ctx_st.value();
    (void)ctx;

    std::vector<ExprContext*> tuple;
    ASSERT_OK(ExprFactory::create_expr_trees(&pool, tuple_slots, &tuple, &dummy_rt_st));

    // create chunk
    RandomChunkBuilder chunk_builder;

    // create spilled factory
    // auto factory_options = SpilledFactoryOptions(ctx->partition_nums, ctx->parition_exprs, ctx->sort_exprs, ctx->sort_descs, false);
    auto factory = spill::make_spilled_factory();

    // create spiller
    SpilledOptions spill_options(4);
    // 4 buffer chunk
    spill_options.mem_table_pool_size = 1;
    // file size: 1M
    spill_options.spill_mem_table_bytes_size = 1 * 1024 * 1024;
    // spill format type
    spill_options.spill_type = spill::SpillFormaterType::SPILL_BY_COLUMN;

    spill_options.block_manager = dummy_block_mgr.get();

    auto chunk_empty = chunk_builder.gen(tuple, nullables);

    auto spiller = factory->create(spill_options);
    spiller->set_metrics(metrics);
    SpillerCaller<spill::PartitionedSpillerWriter*, spill::SpillerReader*> caller(spiller.get());
    ASSERT_OK(spiller->prepare(&dummy_rt_st));

    size_t test_loop = 1024;
    std::vector<ChunkPtr> holder;
    {
        for (size_t i = 0; i < test_loop; ++i) {
            auto chunk = chunk_builder.gen(tuple, nullables);
            auto hash_column = spill::SpillHashColumn::create(chunk->num_rows());
            chunk->append_column(std::move(hash_column), -1);
            ASSERT_OK(spiller->spill<SyncExecutor>(&dummy_rt_st, chunk, EmptyMemGuard{}));
            ASSERT_OK(spiller->_spilled_task_status);
            holder.push_back(chunk);
        }
        ASSERT_OK(spiller->flush<SyncExecutor>(&dummy_rt_st, EmptyMemGuard{}));
    }

    {
        for (size_t i = 0; i < test_loop; ++i) {
            auto chunk = chunk_builder.gen(tuple, nullables);
            auto hash_column = spill::SpillHashColumn::create(chunk->num_rows());
            chunk->append_column(std::move(hash_column), -1);
            ASSERT_OK(spiller->spill<SyncExecutor>(&dummy_rt_st, chunk, EmptyMemGuard{}));
            ASSERT_OK(spiller->_spilled_task_status);
            holder.push_back(chunk);
        }
        ASSERT_OK(spiller->flush<SyncExecutor>(&dummy_rt_st, FailedGuard{}));
    }
}

struct PredoSyncExecutor {
    static std::function<void()> predo;
    static Status submit(workgroup::ScanTask task) {
        do {
            predo();
            task.run();
        } while (!task.is_finished());
        return Status::OK();
    }
    static void force_submit(workgroup::ScanTask task) { (void)submit(std::move(task)); }
};
std::function<void()> PredoSyncExecutor::predo;

TEST_F(SpillTest, partition_yield_with_failed) {
    ObjectPool pool;

    // order by id_int
    // full data id_int, id_smallint
    std::vector<bool> nullables = {false, false};
    TExprBuilder tuple_slots_builder;
    tuple_slots_builder << TYPE_INT;
    auto tuple_slots = tuple_slots_builder.get_res();

    auto ctx_st = no_partition_context(&pool, &dummy_rt_st, {}, tuple_slots);
    ASSERT_OK(ctx_st.status());
    auto ctx = ctx_st.value();
    (void)ctx;

    std::vector<ExprContext*> tuple;
    ASSERT_OK(ExprFactory::create_expr_trees(&pool, tuple_slots, &tuple, &dummy_rt_st));

    // create chunk
    RandomChunkBuilder chunk_builder;

    // create spilled factory
    // auto factory_options = SpilledFactoryOptions(ctx->partition_nums, ctx->parition_exprs, ctx->sort_exprs, ctx->sort_descs, false);
    auto factory = spill::make_spilled_factory();

    // create spiller
    SpilledOptions spill_options(4);
    // 4 buffer chunk
    spill_options.mem_table_pool_size = 1;
    // file size: 1M
    spill_options.spill_mem_table_bytes_size = 1 * 1024 * 1024;
    // spill format type
    spill_options.spill_type = spill::SpillFormaterType::SPILL_BY_COLUMN;

    spill_options.block_manager = dummy_block_mgr.get();

    auto chunk_empty = chunk_builder.gen(tuple, nullables);

    auto spiller = factory->create(spill_options);
    spiller->set_metrics(metrics);
    SpillerCaller<spill::PartitionedSpillerWriter*, spill::SpillerReader*> caller(spiller.get());
    ASSERT_OK(spiller->prepare(&dummy_rt_st));

    size_t test_loop = 1024;
    std::vector<ChunkPtr> holder;
    {
        for (size_t i = 0; i < test_loop; ++i) {
            auto chunk = chunk_builder.gen(tuple, nullables);
            auto hash_column = spill::SpillHashColumn::create(chunk->num_rows());
            chunk->append_column(std::move(hash_column), -1);
            ASSERT_OK(spiller->spill<SyncExecutor>(&dummy_rt_st, chunk, EmptyMemGuard{}));
            ASSERT_OK(spiller->_spilled_task_status);
            holder.push_back(chunk);
        }
        auto dummy = std::make_shared<int>();
        PredoSyncExecutor::predo = [&]() { dummy.reset(); };
        ASSERT_OK(spiller->flush<PredoSyncExecutor>(&dummy_rt_st,
                                                    spill::ResourceMemTrackerGuard(nullptr, std::weak_ptr(dummy))));
    }
}

// Submit failure on the raw-writer flush choke point must fully compensate:
// in-flight aggregate and per-writer counter back to zero, the mem-table back in
// the pool, the error published, and both observer lists woken.
TEST_F(SpillTest, submit_failure_raw_flush_compensates) {
    ObjectPool pool;
    TExprBuilder order_by_slots_builder;
    order_by_slots_builder << TYPE_INT;
    auto order_by_slots = order_by_slots_builder.get_res();
    std::vector<bool> nullables = {false, false};
    TExprBuilder tuple_slots_builder;
    tuple_slots_builder << TYPE_INT << TYPE_SMALLINT;
    auto tuple_slots = tuple_slots_builder.get_res();

    auto ctx_st = no_partition_context(&pool, &dummy_rt_st, order_by_slots, tuple_slots);
    ASSERT_OK(ctx_st.status());
    auto ctx = ctx_st.value();
    auto& tuple = ctx->sort_exprs.sort_tuple_slot_expr_ctxs();

    RandomChunkBuilder chunk_builder;
    auto factory = spill::make_spilled_factory();

    SpilledOptions spill_options;
    spill_options.mem_table_pool_size = 4;
    spill_options.spill_mem_table_bytes_size = 1 * 1024 * 1024;
    spill_options.spill_type = spill::SpillFormaterType::SPILL_BY_COLUMN;
    spill_options.block_manager = dummy_block_mgr.get();

    auto spiller = factory->create(spill_options);
    spiller->set_metrics(metrics);
    SpillerCaller<spill::RawSpillerWriter*, spill::SpillerReader*> caller(spiller.get());
    ASSERT_OK(spiller->prepare(&dummy_rt_st));

    dummy_rt_st.set_enable_event_scheduler(true);
    CountingObserver sink_obs;
    CountingObserver source_obs;
    spiller->observable().subscribe_sink(&dummy_rt_st, &sink_obs);
    spiller->observable().subscribe_source(&dummy_rt_st, &source_obs);

    auto* writer = spiller->_writer->as<spill::RawSpillerWriter*>();
    const size_t pool_capacity = spill_options.mem_table_pool_size;

    // One spill acquires a mem-table from the pool into _mem_table.
    auto chunk = chunk_builder.gen(tuple, nullables);
    ASSERT_OK(caller.spill<SyncExecutor>(&dummy_rt_st, chunk, EmptyMemGuard{}));
    ASSERT_EQ(writer->_mem_table_pool.size(), pool_capacity - 1);
    ASSERT_FALSE(spiller->has_running_io_tasks());

    auto st = caller.flush<FailingSubmitExecutor>(&dummy_rt_st, EmptyMemGuard{});
    ASSERT_FALSE(st.ok());

    // Aggregate and per-writer counter fully unwound.
    ASSERT_FALSE(spiller->has_running_io_tasks());
    ASSERT_EQ(spiller->_in_flight_io.load(), 0);
    ASSERT_EQ(writer->running_flush_tasks(), 0);
    // Mem-table returned to the pool, none lost.
    ASSERT_EQ(writer->_mem_table_pool.size(), pool_capacity);
    // Error published.
    ASSERT_FALSE(spiller->task_status().ok());
    // Both lists woken (a flush completion or failure matters to the writer and the pump).
    ASSERT_GT(sink_obs.sink_count.load(), 0);
    ASSERT_GT(source_obs.source_count.load(), 0);
}

// Submit failure on the restore choke point must compensate the aggregate and
// per-reader counter, publish the error, and wake source-side sleepers only.
TEST_F(SpillTest, submit_failure_restore_compensates) {
    ObjectPool pool;
    TExprBuilder order_by_slots_builder;
    order_by_slots_builder << TYPE_INT;
    auto order_by_slots = order_by_slots_builder.get_res();
    std::vector<bool> nullables = {false, false};
    TExprBuilder tuple_slots_builder;
    tuple_slots_builder << TYPE_INT << TYPE_SMALLINT;
    auto tuple_slots = tuple_slots_builder.get_res();

    auto ctx_st = no_partition_context(&pool, &dummy_rt_st, order_by_slots, tuple_slots);
    ASSERT_OK(ctx_st.status());
    auto ctx = ctx_st.value();
    auto& tuple = ctx->sort_exprs.sort_tuple_slot_expr_ctxs();

    RandomChunkBuilder chunk_builder;
    auto factory = spill::make_spilled_factory();

    SpilledOptions spill_options;
    spill_options.mem_table_pool_size = 4;
    spill_options.spill_mem_table_bytes_size = 1 * 1024 * 1024;
    spill_options.spill_type = spill::SpillFormaterType::SPILL_BY_COLUMN;
    spill_options.block_manager = dummy_block_mgr.get();

    auto spiller = factory->create(spill_options);
    spiller->set_metrics(metrics);
    SpillerCaller<spill::RawSpillerWriter*, spill::SpillerReader*> caller(spiller.get());
    ASSERT_OK(spiller->prepare(&dummy_rt_st));

    // Spill + flush some data so there is a non-empty stream to restore from.
    size_t test_loop = 256;
    for (size_t i = 0; i < test_loop; ++i) {
        auto chunk = chunk_builder.gen(tuple, nullables);
        ASSERT_OK(caller.spill<SyncExecutor>(&dummy_rt_st, chunk, EmptyMemGuard{}));
        ASSERT_OK(spiller->_spilled_task_status);
    }
    ASSERT_OK(caller.flush<SyncExecutor>(&dummy_rt_st, EmptyMemGuard{}));

    dummy_rt_st.set_enable_event_scheduler(true);
    CountingObserver sink_obs;
    CountingObserver source_obs;
    spiller->observable().subscribe_sink(&dummy_rt_st, &sink_obs);
    spiller->observable().subscribe_source(&dummy_rt_st, &source_obs);

    // trigger_restore acquires the input stream on first call, then submits a
    // restore task; FailingSubmitExecutor makes that submit fail.
    auto st = caller.trigger_restore<FailingSubmitExecutor>(&dummy_rt_st, EmptyMemGuard{});
    ASSERT_FALSE(st.ok());

    ASSERT_FALSE(spiller->has_running_io_tasks());
    ASSERT_EQ(spiller->_in_flight_io.load(), 0);
    ASSERT_EQ(spiller->_reader->running_restore_tasks(), 0);
    ASSERT_FALSE(spiller->task_status().ok());
    // Restore feeds only readers: source woken, sink untouched.
    ASSERT_GT(source_obs.source_count.load(), 0);
    ASSERT_EQ(sink_obs.sink_count.load(), 0);
}

// Submit failure on the partitioned-writer flush choke point must compensate the
// aggregate and per-writer counter, publish the error, and wake both lists.
TEST_F(SpillTest, submit_failure_partitioned_flush_compensates) {
    ObjectPool pool;
    std::vector<bool> nullables = {false};
    TExprBuilder tuple_slots_builder;
    tuple_slots_builder << TYPE_INT;
    auto tuple_slots = tuple_slots_builder.get_res();

    auto ctx_st = no_partition_context(&pool, &dummy_rt_st, {}, tuple_slots);
    ASSERT_OK(ctx_st.status());

    std::vector<ExprContext*> tuple;
    ASSERT_OK(ExprFactory::create_expr_trees(&pool, tuple_slots, &tuple, &dummy_rt_st));

    RandomChunkBuilder chunk_builder;
    auto factory = spill::make_spilled_factory();

    SpilledOptions spill_options(4);
    spill_options.mem_table_pool_size = 1;
    spill_options.spill_mem_table_bytes_size = 1 * 1024 * 1024;
    spill_options.spill_type = spill::SpillFormaterType::SPILL_BY_COLUMN;
    spill_options.block_manager = dummy_block_mgr.get();

    auto spiller = factory->create(spill_options);
    spiller->set_metrics(metrics);
    SpillerCaller<spill::PartitionedSpillerWriter*, spill::SpillerReader*> caller(spiller.get());
    ASSERT_OK(spiller->prepare(&dummy_rt_st));

    dummy_rt_st.set_enable_event_scheduler(true);
    CountingObserver sink_obs;
    CountingObserver source_obs;
    spiller->observable().subscribe_sink(&dummy_rt_st, &sink_obs);
    spiller->observable().subscribe_source(&dummy_rt_st, &source_obs);

    size_t test_loop = 1024;
    for (size_t i = 0; i < test_loop; ++i) {
        auto chunk = chunk_builder.gen(tuple, nullables);
        auto hash_column = spill::SpillHashColumn::create(chunk->num_rows());
        chunk->append_column(std::move(hash_column), -1);
        ASSERT_OK(spiller->spill<SyncExecutor>(&dummy_rt_st, chunk, EmptyMemGuard{}));
        ASSERT_OK(spiller->_spilled_task_status);
    }

    auto st = spiller->flush<FailingSubmitExecutor>(&dummy_rt_st, EmptyMemGuard{});
    ASSERT_FALSE(st.ok());

    ASSERT_FALSE(spiller->has_running_io_tasks());
    ASSERT_EQ(spiller->_in_flight_io.load(), 0);
    ASSERT_EQ(spiller->_writer->running_flush_tasks(), 0);
    ASSERT_FALSE(spiller->task_status().ok());
    ASSERT_GT(sink_obs.sink_count.load(), 0);
    ASSERT_GT(source_obs.source_count.load(), 0);
}

// Helper: build a prepared raw spiller with a subscribed observer pair.
struct RawSpillerFixture {
    RawSpillerFixture(SpillTest* test, ObjectPool* pool, RuntimeState* state, size_t pool_size)
            : _test(test), _state(state) {
        TExprBuilder order_by_slots_builder;
        order_by_slots_builder << TYPE_INT;
        order_by_slots = order_by_slots_builder.get_res();
        TExprBuilder tuple_slots_builder;
        tuple_slots_builder << TYPE_INT << TYPE_SMALLINT;
        tuple_slots = tuple_slots_builder.get_res();

        auto ctx_st = no_partition_context(pool, state, order_by_slots, tuple_slots);
        CHECK(ctx_st.ok());
        ctx = ctx_st.value();

        auto factory = spill::make_spilled_factory();
        SpilledOptions spill_options;
        spill_options.mem_table_pool_size = pool_size;
        spill_options.spill_mem_table_bytes_size = 1 * 1024 * 1024;
        spill_options.spill_type = spill::SpillFormaterType::SPILL_BY_COLUMN;
        spill_options.block_manager = test->dummy_block_mgr.get();
        spiller = factory->create(spill_options);
        spiller->set_metrics(test->metrics);
        CHECK(spiller->prepare(state).ok());

        state->set_enable_event_scheduler(true);
        spiller->observable().subscribe_sink(state, &sink_obs);
        spiller->observable().subscribe_source(state, &source_obs);
    }

    std::vector<TExpr> order_by_slots;
    std::vector<TExpr> tuple_slots;
    SpillTestContext* ctx = nullptr;
    std::shared_ptr<spill::Spiller> spiller;
    CountingObserver sink_obs;
    CountingObserver source_obs;
    SpillTest* _test;
    RuntimeState* _state;
};

// A raw-writer flush completion wakes both the sink and the source list, even with pool_size > 1 (the
// first mem-table return must wake).
TEST_F(SpillTest, emission_e1_raw_flush_wakes_both_lists) {
    ObjectPool pool;
    RawSpillerFixture fx(this, &pool, &dummy_rt_st, /*pool_size=*/3);
    RandomChunkBuilder chunk_builder;
    auto& tuple = fx.ctx->sort_exprs.sort_tuple_slot_expr_ctxs();
    std::vector<bool> nullables = {false, false};
    SpillerCaller<spill::RawSpillerWriter*, spill::SpillerReader*> caller(fx.spiller.get());

    auto chunk = chunk_builder.gen(tuple, nullables);
    ASSERT_OK(caller.spill<SyncExecutor>(&dummy_rt_st, chunk, EmptyMemGuard{}));

    ASSERT_EQ(fx.sink_obs.sink_count.load(), 0);
    ASSERT_EQ(fx.source_obs.source_count.load(), 0);

    ASSERT_OK(caller.flush<SyncExecutor>(&dummy_rt_st, EmptyMemGuard{}));

    // First return wakes: both lists notified, in-flight back to zero.
    ASSERT_GT(fx.sink_obs.sink_count.load(), 0);
    ASSERT_GT(fx.source_obs.source_count.load(), 0);
    ASSERT_FALSE(fx.spiller->has_running_io_tasks());
}

// The synchronous flush-all path. set_flush_all_call_back registered with
// zero running flush tasks runs the callback on the calling thread and fires the
// source wakeup (read phase may begin), with no IO thread involved.
TEST_F(SpillTest, emission_e3_sync_flush_all_wakes_source) {
    ObjectPool pool;
    RawSpillerFixture fx(this, &pool, &dummy_rt_st, /*pool_size=*/4);
    RandomChunkBuilder chunk_builder;
    auto& tuple = fx.ctx->sort_exprs.sort_tuple_slot_expr_ctxs();
    std::vector<bool> nullables = {false, false};
    SpillerCaller<spill::RawSpillerWriter*, spill::SpillerReader*> caller(fx.spiller.get());

    // Spill + flush some data so the stream is non-empty and spilled() is true.
    for (size_t i = 0; i < 64; ++i) {
        auto chunk = chunk_builder.gen(tuple, nullables);
        ASSERT_OK(caller.spill<SyncExecutor>(&dummy_rt_st, chunk, EmptyMemGuard{}));
    }
    ASSERT_OK(caller.flush<SyncExecutor>(&dummy_rt_st, EmptyMemGuard{}));
    ASSERT_FALSE(fx.spiller->has_running_io_tasks());

    int32_t source_before = fx.source_obs.source_count.load();
    bool cb_ran = false;
    // No flush task is in flight, so the wrapper runs synchronously here.
    ASSERT_OK(fx.spiller->set_flush_all_call_back<SyncExecutor>(
            [&]() {
                cb_ran = true;
                return Status::OK();
            },
            &dummy_rt_st, EmptyMemGuard{}));
    ASSERT_TRUE(cb_ran);
    ASSERT_GT(fx.source_obs.source_count.load(), source_before);
}

// A restore completion wakes the source list only.
TEST_F(SpillTest, emission_e4_restore_wakes_source) {
    ObjectPool pool;
    RawSpillerFixture fx(this, &pool, &dummy_rt_st, /*pool_size=*/4);
    RandomChunkBuilder chunk_builder;
    auto& tuple = fx.ctx->sort_exprs.sort_tuple_slot_expr_ctxs();
    std::vector<bool> nullables = {false, false};
    SpillerCaller<spill::RawSpillerWriter*, spill::SpillerReader*> caller(fx.spiller.get());

    for (size_t i = 0; i < 256; ++i) {
        auto chunk = chunk_builder.gen(tuple, nullables);
        ASSERT_OK(caller.spill<SyncExecutor>(&dummy_rt_st, chunk, EmptyMemGuard{}));
    }
    ASSERT_OK(caller.flush<SyncExecutor>(&dummy_rt_st, EmptyMemGuard{}));

    int32_t sink_before = fx.sink_obs.sink_count.load();
    int32_t source_before = fx.source_obs.source_count.load();

    ASSERT_OK(caller.trigger_restore<SyncExecutor>(&dummy_rt_st, EmptyMemGuard{}));

    // Restore completion woke the source list; the sink list is untouched.
    ASSERT_GT(fx.source_obs.source_count.load(), source_before);
    ASSERT_EQ(fx.sink_obs.sink_count.load(), sink_before);
    ASSERT_FALSE(fx.spiller->has_running_io_tasks());
}

// reset_state must refuse while IO is in flight: swapping
// writer/reader out from under a task that captured the raw pointers is unsafe.
// Drive the in-flight aggregate through the public accessors so the guard is
// tested deterministically, without thread/timing dependence.
TEST_F(SpillTest, reset_state_refuses_while_io_in_flight) {
    ObjectPool pool;
    RawSpillerFixture fx(this, &pool, &dummy_rt_st, /*pool_size=*/4);

    ASSERT_FALSE(fx.spiller->has_running_io_tasks());
    ASSERT_OK(fx.spiller->reset_state(&dummy_rt_st));

    fx.spiller->increase_in_flight_io();
    ASSERT_TRUE(fx.spiller->has_running_io_tasks());
    ASSERT_FALSE(fx.spiller->reset_state(&dummy_rt_st).ok());

    fx.spiller->decrease_in_flight_io();
    ASSERT_FALSE(fx.spiller->has_running_io_tasks());
    ASSERT_OK(fx.spiller->reset_state(&dummy_rt_st));
}

// Observer lists live on the Spiller and must survive reset_state: re-prepare
// reallocates writer/reader but must not clear or re-append observers
// (subscribe-once-per-driver-lifetime).
TEST_F(SpillTest, observers_survive_reset_state) {
    ObjectPool pool;
    RawSpillerFixture fx(this, &pool, &dummy_rt_st, /*pool_size=*/4);

    ASSERT_EQ(fx.spiller->observable().observer_count(), 2);
    ASSERT_OK(fx.spiller->reset_state(&dummy_rt_st));
    ASSERT_EQ(fx.spiller->observable().observer_count(), 2);

    // Still wired after reset: a notify reaches the same observers.
    int32_t source_before = fx.source_obs.source_count.load();
    fx.spiller->notify_source_observers();
    ASSERT_GT(fx.source_obs.source_count.load(), source_before);
}

// detach during an in-flight notify on the spiller's own observable must not
// crash (locked iteration vs exclusive clear, TSAN; jthread pattern).
TEST_F(SpillTest, detach_notify_race_on_spiller) {
    ObjectPool pool;
    RawSpillerFixture fx(this, &pool, &dummy_rt_st, /*pool_size=*/4);

    std::vector<std::jthread> threads;
    threads.emplace_back([&]() { fx.spiller->notify_source_observers(); });
    threads.emplace_back([&]() { fx.spiller->notify_sink_observers(); });
    threads.emplace_back([&]() { fx.spiller->observable().detach_observers(); });
    for (auto& thread : threads) {
        thread.join();
    }
    ASSERT_EQ(fx.spiller->observable().observer_count(), 0);
}

TEST_F(SpillTest, resource_mem_tracker_guard_should_protect_query_lifetime) {
    auto lifetime = std::make_shared<QueryContextLifetime>();
    auto guard = spill::ResourceMemTrackerGuard(nullptr, std::weak_ptr<QueryContextLifetime>(lifetime));

    ASSERT_TRUE(guard.scoped_begin());
    guard.scoped_end();

    lifetime.reset();
    ASSERT_FALSE(guard.scoped_begin());
}

TEST_F(SpillTest, resource_mem_tracker_guard_should_require_all_resources) {
    auto lifetime = std::make_shared<QueryContextLifetime>();
    auto first_resource = std::make_shared<int>();
    auto second_resource = std::make_shared<int>();

    auto guard =
            spill::ResourceMemTrackerGuard(nullptr, std::weak_ptr<QueryContextLifetime>(lifetime),
                                           std::weak_ptr<int>(first_resource), std::weak_ptr<int>(second_resource));
    ASSERT_TRUE(guard.scoped_begin());
    guard.scoped_end();

    second_resource.reset();
    ASSERT_FALSE(guard.scoped_begin());
}

TEST_F(SpillTest, aligned_buffer) {
    spill::AlignedBuffer buffer;
    ASSERT_EQ(buffer.data(), nullptr);
    auto is_aligned = [](void* ptr, std::size_t alignment) {
        return reinterpret_cast<uintptr_t>(ptr) % alignment == 0;
    };
    buffer.resize(1);
    buffer.data()[0] = '@';
    ASSERT_TRUE(is_aligned(buffer.data(), 4096));
    buffer.resize(8192);
    ASSERT_EQ(buffer.data()[0], '@');
    ASSERT_TRUE(is_aligned(buffer.data(), 4096));
    buffer.resize(1);
    ASSERT_EQ(buffer.data()[0], '@');
    ASSERT_TRUE(is_aligned(buffer.data(), 4096));
}

/*
TEST_F(SpillTest, file_group_test) {
    auto chunk = std::make_unique<Chunk>();
    chunk->append_column(Int32Column::create(), 0);
    chunk->append_column(Int64Column::create(), 1);
    auto formater_st =
            SpillFormater::create(SpillFormaterType::SPILL_BY_COLUMN, [&]() { return chunk->clone_unique(); });
    ASSERT_OK(formater_st.status());
    SpilledFileGroup file_group(*formater_st.value());

    auto fs = FileSystem::Default();
    std::string path = "/";
    std::vector<std::string> result;
    ASSERT_OK(fs->get_children(path, &result));

    for (const auto& st : result) {
        auto spill_file = std::make_shared<SpillFile>(path + "/" + st, FileSystem::Default());
        file_group.append_file(spill_file);
    }

    auto factory = make_spilled_factory();

    ObjectPool pool;
    SortExecExprs sort_exprs;
    TExprBuilder order_by_slots_builder;
    order_by_slots_builder << TYPE_INT;
    auto order_bys = order_by_slots_builder.get_res();

    ASSERT_OK(sort_exprs.init(order_bys, nullptr, &pool, &dummy_rt_st));
    ASSERT_OK(sort_exprs.prepare(&dummy_rt_st, {}, {}));
    ASSERT_OK(sort_exprs.open(&dummy_rt_st));

    SortDescs descs = SortDescs::asc_null_first(1);
    auto vst = file_group.as_sorted_stream(factory, &dummy_rt_st, &sort_exprs, &descs);
    ASSERT_OK(vst.status());

    auto [stream, tasks] = std::move(vst.value());
    stream->is_ready();

    SpillFormatContext context;

    int32_t last_value = -1;
    Status st;
    while (!stream->eof()) {
        for (auto& task : tasks) {
            auto st = task->do_read(context);
            if (!st.is_end_of_file()) {
                ASSERT_OK(st);
            }
        }
        stream->is_ready();
        auto res = stream->read(context);
        ASSERT_OK(res.status());
        auto chunk = std::move(res.value());
        auto icol = down_cast<Int32Column*>(chunk->columns()[0].get());
        auto data = icol->get_data();
        DCHECK(std::is_sorted(data.begin(), data.end()));
        DCHECK_GE(data[0], last_value);
        last_value = data[data.size() - 1];
    }
}
*/

// Death tests for the CHECK invariants added in _compact_skew_chunks
// (https://github.com/StarRocks/starrocks/issues/74074). Contract guard so a
// future refactor cannot silently re-introduce a defensive skip and let the
// OOB read sneak back in. The test target is built with -fno-access-control,
// so we can call the private method directly.
class SpillCompactSkewChunksCheckTest : public ::testing::Test {
protected:
    static ChunkPtr make_chunk_with_n_rows(size_t n) {
        auto chunk = std::make_shared<Chunk>();
        auto hash_col = spill::SpillHashColumn::create(n);
        chunk->append_column(std::move(hash_col), 0);
        return chunk;
    }
    static ChunkPtr make_empty_chunk() { return std::make_shared<Chunk>(); }
    static void invoke(std::vector<ChunkPtr> chunks, size_t num_rows) {
        RuntimeState rt_st;
        spill::PartitionedSpillerWriter writer(nullptr, &rt_st);
        (void)writer._compact_skew_chunks(num_rows, chunks);
    }
};

TEST_F(SpillCompactSkewChunksCheckTest, empty_chunks_aborts) {
    EXPECT_DEATH(invoke({}, 30), "empty chunks with num_rows");
}

TEST_F(SpillCompactSkewChunksCheckTest, null_first_chunk_aborts) {
    EXPECT_DEATH(invoke({ChunkPtr{}}, 30), "null chunk at idx 0");
}

TEST_F(SpillCompactSkewChunksCheckTest, empty_first_chunk_aborts) {
    EXPECT_DEATH(invoke({make_empty_chunk()}, 30), "empty chunk at idx 0");
}

TEST_F(SpillCompactSkewChunksCheckTest, num_rows_overshoot_aborts) {
    EXPECT_DEATH(invoke({make_chunk_with_n_rows(5)}, 100), "disagrees with sum");
}

TEST_F(SpillCompactSkewChunksCheckTest, null_mid_chunk_in_locator_aborts) {
    EXPECT_DEATH(invoke({make_chunk_with_n_rows(15), ChunkPtr{}, make_chunk_with_n_rows(30)}, 45),
                 "null chunk at idx 1");
}

TEST_F(SpillCompactSkewChunksCheckTest, empty_mid_chunk_in_locator_aborts) {
    EXPECT_DEATH(invoke({make_chunk_with_n_rows(15), make_empty_chunk(), make_chunk_with_n_rows(30)}, 45),
                 "empty chunk at idx 1");
}

TEST_F(SpillCompactSkewChunksCheckTest, null_chunk_in_frequency_pass_aborts) {
    EXPECT_DEATH(invoke({make_chunk_with_n_rows(50), ChunkPtr{}}, 50), "null chunk in frequency pass");
}

TEST_F(SpillCompactSkewChunksCheckTest, empty_chunk_in_frequency_pass_aborts) {
    EXPECT_DEATH(invoke({make_chunk_with_n_rows(50), make_empty_chunk()}, 50), "empty chunk in frequency pass");
}

} // namespace starrocks::vectorized
