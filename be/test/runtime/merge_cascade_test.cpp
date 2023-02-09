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

#include "common/object_pool.h"
#include "common/status.h"
#include "common/statusor.h"
#include "exec/sort_exec_exprs.h"
#include "exec/sorting/merge.h"
#include "exec/sorting/sorting.h"
#include "runtime/runtime_state.h"
#include "testutil/assert.h"
#include "testutil/exprs_test_helper.h"

namespace starrocks {
TEST(MergeCascadeTest, merge_cursor_test) {
    RuntimeState dummy_rt_st;
    auto chunk = std::make_unique<Chunk>();
    chunk->append_column(Int32Column::create(), 0);

    ObjectPool pool;
    SortExecExprs sort_exprs;
    TExprBuilder order_by_slots_builder;
    order_by_slots_builder << TYPE_INT;
    auto order_bys = order_by_slots_builder.get_res();

    ASSERT_OK(sort_exprs.init(order_bys, nullptr, &pool, &dummy_rt_st));
    ASSERT_OK(sort_exprs.prepare(&dummy_rt_st, {}, {}));
    ASSERT_OK(sort_exprs.open(&dummy_rt_st));

    struct ChannelChunkProvider {
        ChannelChunkProvider(std::queue<ChunkUniquePtr>& channel_, bool* shutdown_)
                : channel(channel_), _shutdown(shutdown_) {}
        bool operator()(ChunkUniquePtr* out_chunk, bool* eos) const {
            if (out_chunk == nullptr || eos == nullptr) {
                return !channel.empty();
            }
            if (channel.empty()) {
                if (*_shutdown) {
                    *eos = true;
                    return false;
                }
                return false;
            }

            *out_chunk = std::move(channel.front());
            channel.pop();
            return true;
        }

        std::queue<ChunkUniquePtr>& channel;
        bool* _shutdown;
    };

    // merge two stream
    // one is not ready
    {
        std::queue<ChunkUniquePtr> l_chunk_channel;
        std::queue<ChunkUniquePtr> r_chunk_channel;

        bool l_shutdown = false;
        bool r_shutdown = false;

        ChunkProvider l_provider = ChannelChunkProvider(l_chunk_channel, &l_shutdown);
        ChunkProvider r_provider = ChannelChunkProvider(r_chunk_channel, &r_shutdown);

        auto l_cursor = std::make_unique<SimpleChunkSortCursor>(l_provider, &sort_exprs.lhs_ordering_expr_ctxs());
        auto r_cursor = std::make_unique<SimpleChunkSortCursor>(r_provider, &sort_exprs.lhs_ordering_expr_ctxs());

        size_t chunk_size = 4096;
        auto l = chunk->clone_unique();
        l->columns()[0]->append_datum(Datum(1));
        l->columns()[0]->assign(chunk_size, 0);

        auto r = chunk->clone_unique();
        r->columns()[0]->append_datum(Datum(9999));
        r->columns()[0]->assign(chunk_size, 0);

        l_chunk_channel.emplace(l->clone_unique());
        r_chunk_channel.emplace(r->clone_unique());

        auto desc = SortDescs::asc_null_first(1);
        MergeTwoCursor cursor(desc, std::move(l_cursor), std::move(r_cursor));

        auto out_cursor = cursor.as_chunk_cursor();
        ASSERT_TRUE(out_cursor->is_data_ready());

        // call get next 1
        auto merge_res = out_cursor->try_get_next();
        // call get next 2
        merge_res = out_cursor->try_get_next();

        // left channel is is not eos
        ASSERT_TRUE(merge_res.first == nullptr);

        l_shutdown = true;
        merge_res = out_cursor->try_get_next();

        ASSERT_FALSE(merge_res.first->is_empty());
    }

    // merge two stream
    // all of data return in one batch
    // one is not ready the other is ready
    {
        std::queue<ChunkUniquePtr> l_chunk_channel;
        std::queue<ChunkUniquePtr> r_chunk_channel;

        bool l_shutdown = false;
        bool r_shutdown = false;

        ChunkProvider l_provider = ChannelChunkProvider(l_chunk_channel, &l_shutdown);
        ChunkProvider r_provider = ChannelChunkProvider(r_chunk_channel, &r_shutdown);

        auto l_cursor = std::make_unique<SimpleChunkSortCursor>(l_provider, &sort_exprs.lhs_ordering_expr_ctxs());
        auto r_cursor = std::make_unique<SimpleChunkSortCursor>(r_provider, &sort_exprs.lhs_ordering_expr_ctxs());

        size_t chunk_size = 4096;
        auto l = chunk->clone_unique();
        l->columns()[0]->append_datum(Datum(1));
        l->columns()[0]->assign(chunk_size, 0);

        auto r = chunk->clone_unique();
        r->columns()[0]->append_datum(Datum(1));
        r->columns()[0]->assign(chunk_size, 0);

        l_chunk_channel.emplace(l->clone_unique());
        r_chunk_channel.emplace(r->clone_unique());

        auto desc = SortDescs::asc_null_first(1);
        MergeTwoCursor cursor(desc, std::move(l_cursor), std::move(r_cursor));

        auto out_cursor = cursor.as_chunk_cursor();
        ASSERT_TRUE(out_cursor->is_data_ready());

        // call get next 1
        auto merge_res = out_cursor->try_get_next();
        ASSERT_TRUE(!merge_res.first->is_empty());

        // call get next 2
        merge_res = out_cursor->try_get_next();

        ASSERT_TRUE(l_chunk_channel.empty() && r_chunk_channel.empty());
        // both chunk has no chunk
        ASSERT_TRUE(merge_res.first == nullptr);

        // notify one channel, but the other channel is not ready
        l_chunk_channel.emplace(l->clone_unique());
        merge_res = out_cursor->try_get_next();
        ASSERT_TRUE(merge_res.first == nullptr);
    }
}
} // namespace starrocks