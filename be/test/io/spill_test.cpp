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
#include <filesystem>
#include <iterator>
#include <memory>
#include <utility>
#include <vector>

#include "column/array_column.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/column_visitor_adapter.h"
#include "column/map_column.h"
#include "column/nullable_column.h"
#include "column/struct_column.h"
#include "column/vectorized_fwd.h"
#include "common/config.h"
#include "common/object_pool.h"
#include "common/status.h"
#include "common/statusor.h"
#include "exec/pipeline/pipeline_fwd.h"
#include "exec/sorting/merge.h"
#include "exec/sorting/sorting.h"
#include "exec/spill/executor.h"
#include "exec/spill/log_block_manager.h"
#include "exec/spill/spiller.h"
#include "exec/spill/spiller.hpp"
#include "exec/spill/spiller_factory.h"
#include "exprs/column_ref.h"
#include "exprs/expr_context.h"
#include "fs/fs.h"
#include "gen_cpp/Exprs_types.h"
#include "gen_cpp/Types_types.h"
#include "runtime/runtime_state.h"
#include "testutil/assert.h"
#include "types/logical_type.h"
#include "util/defer_op.h"
#include "util/runtime_profile.h"
#include "util/uid_util.h"

namespace starrocks::vectorized {
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
        RETURN_IF_ERROR(fill(column->null_column().get()));
        RETURN_IF_ERROR(column->data_column()->accept_mutable(this));
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
            DCHECK(ctx->root()->is_slotref());
            auto ref = ctx->root()->get_column_ref();
            auto col = ColumnHelper::create_column(ctx->root()->type(), nullable[i]);
            DCHECK(col->accept_mutable(&filler).ok());
            chunk->append_column(std::move(col), ref->slot_id());
        }
        return chunk;
    }
};

struct SyncExecutor {
    template <class Runnable>
    Status submit(Runnable&& runnable) {
        std::forward<Runnable>(runnable)();
        return Status::OK();
    }
};

struct ASyncExecutor {
    template <class Runnable>
    Status submit(Runnable&& runnable) {
        _threads.emplace_back(std::forward<Runnable>(runnable));
        return Status::OK();
    }
    ~ASyncExecutor() {
        for (auto& thread : _threads) {
            thread.join();
        }
    }

private:
    std::vector<std::thread> _threads;
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
        ASSERT_OK(dummy_dir_mgr->init(path));

        dummy_block_mgr = std::make_unique<spill::LogBlockManager>(dummy_query_id);
        dummy_block_mgr->set_dir_manager(dummy_dir_mgr.get());

        dummy_query_ctx = std::make_shared<pipeline::QueryContext>();
        dummy_rt_st.set_query_ctx(dummy_query_ctx.get());
        dummy_rt_st.set_chunk_size(config::vector_chunk_size);

        metrics = SpillProcessMetrics(&dummy_profile);
    }
    void TearDown() override {}
    std::unique_ptr<spill::DirManager> dummy_dir_mgr;
    std::unique_ptr<spill::LogBlockManager> dummy_block_mgr;
    pipeline::QueryContextPtr dummy_query_ctx;
    RuntimeState dummy_rt_st;
    RuntimeProfile dummy_profile{"dummy"};
    std::vector<std::string> clean_up;
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
                                                 std::vector<TExpr>& order_bys, std::vector<TExpr>& tuple) {
    auto context = pool->add(new SpillTestContext());
    context->partition_nums = 1;
    //
    RETURN_IF_ERROR(context->sort_exprs.init(order_bys, &tuple, &context->pool, runtime_state));
    RETURN_IF_ERROR(context->sort_exprs.prepare(runtime_state, {}, {}));
    RETURN_IF_ERROR(context->sort_exprs.open(runtime_state));
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
    Status spill(RuntimeState* state, const ChunkPtr& chunk, TaskExecutor&& executor, MemGuard&& guard) {
        if (_spiller->_chunk_builder.chunk_schema()->empty()) {
            _spiller->_chunk_builder.chunk_schema()->set_schema(chunk);
        }
        return _spiller->_writer->as<Writer>()->spill(state, chunk, std::forward<TaskExecutor>(executor),
                                                      std::forward<MemGuard>(guard));
    }

    template <class TaskExecutor, class MemGuard>
    Status flush(RuntimeState* state, TaskExecutor&& executor, MemGuard&& guard) {
        return _spiller->_writer->as<Writer>()->flush(state, std::forward<TaskExecutor>(executor),
                                                      std::forward<MemGuard>(guard));
    }

    template <class TaskExecutor, class MemGuard>
    StatusOr<ChunkPtr> restore(RuntimeState* state, TaskExecutor&& executor, MemGuard&& guard) {
        return _spiller->_reader->restore(state, std::forward<TaskExecutor>(executor), std::forward<MemGuard>(guard));
    }

    template <class TaskExecutor, class MemGuard>
    Status trigger_restore(RuntimeState* state, TaskExecutor&& executor, MemGuard&& guard) {
        if (!acquire_once) {
            acquire_once = true;
            RETURN_IF_ERROR(_spiller->_acquire_input_stream(state));
        }
        return _spiller->_reader->trigger_restore(state, std::forward<TaskExecutor>(executor),
                                                  std::forward<MemGuard>(guard));
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
            ASSERT_OK(caller.spill(&dummy_rt_st, chunk, SyncExecutor{}, EmptyMemGuard{}));
            ASSERT_OK(spiller->_spilled_task_status);
            holder.push_back(chunk);
        }
        ASSERT_OK(caller.flush(&dummy_rt_st, SyncExecutor{}, EmptyMemGuard{}));
    }
    size_t input_rows = 0;
    for (const auto& chunk : holder) {
        input_rows += chunk->num_rows();
    }

    // test restore
    {
        std::vector<ChunkPtr> restored;
        ASSERT_OK(caller.trigger_restore(&dummy_rt_st, SyncExecutor{}, EmptyMemGuard{}));
        for (size_t i = 0; i < test_loop; ++i) {
            auto chunk_st = caller.restore(&dummy_rt_st, SyncExecutor{}, EmptyMemGuard{});
            ASSERT_OK(chunk_st.status());
            ASSERT_OK(spiller->_spilled_task_status);
            if (chunk_st.value() != nullptr) {
                restored.emplace_back(std::move(chunk_st.value()));
            }
        }

        auto chunk_st = caller.restore(&dummy_rt_st, SyncExecutor{}, EmptyMemGuard{});
        ASSERT_TRUE(chunk_st.status().is_end_of_file());

        size_t output_rows = 0;
        for (const auto& chunk : restored) {
            output_rows += chunk->num_rows();
        }
        ASSERT_EQ(input_rows, output_rows);
    }

    // test 2
    {
        ASyncExecutor executor;
        for (size_t i = 0; i < test_loop; ++i) {
            if (!spiller->is_full()) {
                auto chunk = chunk_builder.gen(tuple, nullables);
                ASSERT_OK(caller.spill(&dummy_rt_st, chunk, executor, EmptyMemGuard{}));
                ASSERT_OK(spiller->_spilled_task_status);
            }
        }
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
                ASSERT_OK(caller.spill(&dummy_rt_st, chunk, SyncExecutor{}, EmptyMemGuard{}));
                ASSERT_OK(spiller->_spilled_task_status);
                holder.push_back(chunk);
                contain_rows += chunk->num_rows();
            }
            ASSERT_OK(caller.flush(&dummy_rt_st, SyncExecutor{}, EmptyMemGuard{}));
        }

        std::vector<ChunkPtr> restored;
        size_t restored_rows = 0;
        {
            ASSERT_OK(caller.trigger_restore(&dummy_rt_st, SyncExecutor{}, EmptyMemGuard{}));
            ASSERT_TRUE(caller._spiller->has_output_data());
            for (size_t i = 0; i < test_loop; ++i) {
                auto chunk_st = caller.restore(&dummy_rt_st, SyncExecutor{}, EmptyMemGuard{});
                ASSERT_OK(chunk_st.status());
                ASSERT_OK(spiller->_spilled_task_status);
                if (chunk_st.value() != nullptr) {
                    LOG(INFO) << "restored:" << chunk_st.value()->num_rows();
                    restored_rows += chunk_st.value()->num_rows();
                    restored.emplace_back(std::move(chunk_st.value()));
                }
            }

            auto chunk_st = caller.restore(&dummy_rt_st, SyncExecutor{}, EmptyMemGuard{});
            ASSERT_TRUE(chunk_st.status().is_end_of_file());
        }
        ASSERT_EQ(contain_rows, restored_rows);
    }
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

} // namespace starrocks::vectorized