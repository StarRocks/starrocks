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

#include "base/container/raw_container.h"
#include "base/testutil/assert.h"
#include "base/uid_util.h"
#include "base/utility/defer_op.h"
#include "column/adaptive_nullable_column.h"
#include "column/array_column.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/column_visitor_adapter.h"
#include "column/fixed_length_column.h"
#include "column/map_column.h"
#include "column/nullable_column.h"
#include "column/struct_column.h"
#include "column/vectorized_fwd.h"
#include "common/config_exec_fwd.h"
#include "common/config_storage_fwd.h"
#include "common/object_pool.h"
#include "common/runtime_profile.h"
#include "common/status.h"
#include "common/statusor.h"
#include "compute_env/sorting/merge.h"
#include "compute_env/sorting/sorting.h"
#include "compute_env/spill/log_block_manager.h"
#include "compute_env/spill/mem_table.h"
#include "compute_env/spill/mem_tracker_guard.h"
#include "compute_env/spill/spill_components.h"
#include "compute_env/spill/spiller.h"
#include "compute_env/spill/spiller.hpp"
#include "compute_env/spill/spiller_factory.h"
#include "compute_env/spill/task_executor.h"
#include "compute_env/workgroup/scan_task.h"
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

namespace {

spill::BlockReaderOptions make_block_read_options(RuntimeProfile* profile, bool enable_buffer_read,
                                                  size_t max_buffer_bytes) {
    spill::BlockReaderOptions opts;
    opts.enable_buffer_read = enable_buffer_read;
    opts.max_buffer_bytes = max_buffer_bytes;
    opts.read_io_timer = ADD_TIMER(profile, "read_io_timer");
    opts.read_io_count = ADD_COUNTER(profile, "read_io_count", TUnit::UNIT);
    opts.read_io_bytes = ADD_COUNTER(profile, "read_io_bytes", TUnit::BYTES);
    return opts;
}

ChunkPtr make_int_chunk(int32_t base, size_t rows) {
    auto col = Int32Column::create();
    for (size_t i = 0; i < rows; ++i) {
        col->append(base + i);
    }
    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(std::move(col), 0);
    return chunk;
}

} // namespace

TEST_F(SpillTest, block_reader_read_view) {
    spill::AcquireBlockOptions aopts;
    aopts.query_id = generate_uuid();
    aopts.fragment_instance_id = aopts.query_id;
    aopts.plan_node_id = 1;
    aopts.name = "read_view";
    aopts.block_size = 1 << 20;
    auto block_res = dummy_block_mgr->acquire_block(aopts);
    ASSERT_OK(block_res.status());
    auto block = block_res.value();

    // records of [12-byte header][body]; body sizes chosen to exercise the
    // buffered path (<= max_buffer_bytes, including the exact boundary), the
    // fallback path (> max_buffer_bytes) and refills with a partial tail
    constexpr size_t kMaxBuffer = 16384;
    const std::vector<size_t> body_sizes = {100, kMaxBuffer, 40000, 5000, 1, 12345, kMaxBuffer - 1, 7000};
    std::string master;
    for (size_t len : body_sizes) {
        for (size_t i = 0; i < 12 + len; ++i) {
            master.push_back(static_cast<char>((master.size() * 131 + 7) % 251));
        }
    }
    ASSERT_OK(block->append({Slice(master)}));
    ASSERT_OK(block->flush());

    auto verify = [&](bool enable_buffer_read) {
        auto ropts = make_block_read_options(&dummy_profile, enable_buffer_read, kMaxBuffer);
        auto reader = block->get_reader(ropts);
        raw::RawString fallback;
        size_t off = 0;
        for (size_t len : body_sizes) {
            char header[12];
            ASSERT_OK(reader->read_fully(header, 12));
            ASSERT_EQ(0, memcmp(header, master.data() + off, 12));
            off += 12;
            auto view = reader->read_view(&fallback, len);
            ASSERT_OK(view.status());
            ASSERT_EQ(len, view.value().size);
            ASSERT_EQ(0, memcmp(view.value().data, master.data() + off, len));
            off += len;
        }
        ASSERT_EQ(master.size(), off);
        // the block is fully consumed
        raw::RawString tail;
        ASSERT_TRUE(reader->read_view(&tail, 1).status().is_end_of_file());
        char dummy;
        ASSERT_TRUE(reader->read_fully(&dummy, 1).is_end_of_file());
    };
    verify(true);
    verify(false);

    {
        // a negative size (e.g. from a corrupted header) must fail instead of
        // defeating the bounds checks
        auto ropts = make_block_read_options(&dummy_profile, true, kMaxBuffer);
        auto reader = block->get_reader(ropts);
        raw::RawString fallback;
        auto view = reader->read_view(&fallback, -8);
        ASSERT_FALSE(view.ok());
        ASSERT_FALSE(view.status().is_end_of_file());
    }
}

TEST_F(SpillTest, raw_chunk_input_stream_move_or_clone) {
    workgroup::YieldContext yield_ctx;
    spill::SerdeContext serde_ctx;

    {
        // exclusively owned chunk: handed over without copying
        auto chunk = make_int_chunk(0, 16);
        const Column* raw_column = chunk->columns()[0].get();
        std::vector<ChunkPtr> chunks;
        chunks.emplace_back(std::move(chunk));
        auto stream = spill::SpillInputStream::as_stream(std::move(chunks), nullptr);
        auto res = stream->get_next(yield_ctx, serde_ctx);
        ASSERT_OK(res.status());
        ASSERT_EQ(raw_column, res.value()->columns()[0].get());
        ASSERT_EQ(16, res.value()->num_rows());
        ASSERT_TRUE(stream->get_next(yield_ctx, serde_ctx).status().is_end_of_file());
    }

    {
        // the chunk itself is shared: served as a deep copy, the source stays
        // intact
        auto chunk = make_int_chunk(100, 8);
        const Column* raw_column = chunk->columns()[0].get();
        std::vector<ChunkPtr> chunks;
        chunks.emplace_back(chunk);
        auto stream = spill::SpillInputStream::as_stream(std::move(chunks), nullptr);
        auto res = stream->get_next(yield_ctx, serde_ctx);
        ASSERT_OK(res.status());
        ASSERT_NE(raw_column, res.value()->columns()[0].get());
        ASSERT_EQ(8, chunk->num_rows());
        ASSERT_EQ(8, res.value()->num_rows());
    }

    {
        // unique top-level column wrapping a shared child: the nested sharing
        // must force a copy
        auto data = Int64Column::create();
        for (int i = 0; i < 4; ++i) {
            data->append(i);
        }
        ColumnPtr shared_data = std::move(data);
        auto nullable = NullableColumn::create(shared_data, NullColumn::create(4, 0));
        auto chunk = std::make_shared<Chunk>();
        chunk->append_column(std::move(nullable), 0);
        const Column* raw_column = chunk->columns()[0].get();
        std::vector<ChunkPtr> chunks;
        chunks.emplace_back(std::move(chunk));
        auto stream = spill::SpillInputStream::as_stream(std::move(chunks), nullptr);
        auto res = stream->get_next(yield_ctx, serde_ctx);
        ASSERT_OK(res.status());
        ASSERT_NE(raw_column, res.value()->columns()[0].get());
        ASSERT_EQ(4, shared_data->size());
        ASSERT_EQ(4, res.value()->num_rows());
    }
}

TEST_F(SpillTest, unordered_mem_table_read_modes) {
    auto factory = spill::make_spilled_factory();
    SpilledOptions spill_options;
    spill_options.mem_table_pool_size = 2;
    spill_options.spill_mem_table_bytes_size = 1 * 1024 * 1024;
    spill_options.spill_type = spill::SpillFormaterType::SPILL_BY_COLUMN;
    spill_options.block_manager = dummy_block_mgr.get();
    auto spiller = factory->create(spill_options);
    spiller->set_metrics(metrics);

    auto mem_table = std::make_shared<spill::UnorderedMemTable>(&dummy_rt_st, 1L << 30, nullptr, spiller.get());
    size_t input_rows = 0;
    for (int i = 0; i < 10; ++i) {
        auto chunk = make_int_chunk(i * 100, 64);
        input_rows += chunk->num_rows();
        ASSERT_OK(mem_table->append(std::move(chunk)));
    }
    size_t consumption = mem_table->mem_usage();
    ASSERT_GT(consumption, 0);

    workgroup::YieldContext yield_ctx;
    spill::SerdeContext serde_ctx;
    auto drain = [&](const std::shared_ptr<spill::SpillInputStream>& stream) {
        size_t rows = 0;
        while (true) {
            auto res = stream->get_next(yield_ctx, serde_ctx);
            if (res.status().is_end_of_file()) {
                break;
            }
            EXPECT_OK(res.status());
            rows += res.value()->num_rows();
        }
        return rows;
    };

    {
        // shared read keeps the mem table intact
        auto stream_st = mem_table->as_input_stream(spill::MemTableReadMode::SHARED);
        ASSERT_OK(stream_st.status());
        ASSERT_EQ(input_rows, drain(stream_st.value()));
        ASSERT_FALSE(mem_table->is_empty());
        ASSERT_FALSE(mem_table->consumed_by_exclusive_read());
        ASSERT_EQ(consumption, mem_table->mem_usage());
    }

    {
        // exclusive read consumes the mem table but keeps the accounting
        // until reset
        auto stream_st = mem_table->as_input_stream(spill::MemTableReadMode::EXCLUSIVE);
        ASSERT_OK(stream_st.status());
        ASSERT_TRUE(mem_table->is_empty());
        ASSERT_TRUE(mem_table->consumed_by_exclusive_read());
        ASSERT_EQ(consumption, mem_table->mem_usage());
        ASSERT_EQ(input_rows, drain(stream_st.value()));
    }

    // both drains were served from in-memory chunks
    ASSERT_EQ(2 * input_rows, metrics.restore_from_mem_table_rows->value());
    ASSERT_GT(metrics.restore_from_mem_table_bytes->value(), 0);

    // appending again makes the mem table reusable
    ASSERT_OK(mem_table->append(make_int_chunk(0, 8)));
    ASSERT_FALSE(mem_table->consumed_by_exclusive_read());

    mem_table->reset();
    ASSERT_FALSE(mem_table->consumed_by_exclusive_read());
    ASSERT_EQ(0, mem_table->mem_usage());
}

TEST_F(SpillTest, mem_table_double_exclusive_acquire) {
    auto factory = spill::make_spilled_factory();
    SpilledOptions spill_options;
    spill_options.mem_table_pool_size = 2;
    spill_options.spill_mem_table_bytes_size = 1 * 1024 * 1024;
    spill_options.spill_type = spill::SpillFormaterType::SPILL_BY_COLUMN;
    spill_options.block_manager = dummy_block_mgr.get();
    ASSERT_FALSE(spill_options.read_shared);

    auto spiller = factory->create(spill_options);
    spiller->set_metrics(metrics);
    SpillerCaller<spill::RawSpillerWriter*, spill::SpillerReader*> caller(spiller.get());
    ASSERT_OK(spiller->prepare(&dummy_rt_st));

    // spill some data but do not flush, so the mem table stays in memory
    ASSERT_OK(caller.spill<SyncExecutor>(&dummy_rt_st, make_int_chunk(0, 128), EmptyMemGuard{}));

    auto writer = spiller->_writer->as<spill::RawSpillerWriter*>();
    std::shared_ptr<spill::SpillInputStream> first;
    ASSERT_OK(writer->acquire_stream(&first));

    // the in-memory rows were handed over to the first stream; acquiring a
    // second exclusive stream must fail loudly instead of losing them
    std::shared_ptr<spill::SpillInputStream> second;
    auto st = writer->acquire_stream(&second);
    ASSERT_FALSE(st.ok());
}

} // namespace starrocks::vectorized
