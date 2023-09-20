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

#include "exec/chunks_sorter_heap_sort.h"

#include <gtest/gtest.h>

#include <cstdlib>
#include <functional>
#include <memory>
#include <optional>
#include <vector>

#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "column/datum.h"
#include "column/vectorized_fwd.h"
#include "common/object_pool.h"
#include "exprs/column_ref.h"
#include "exprs/expr_context.h"
#include "runtime/types.h"
#include "testutil/assert.h"
#include "types/logical_type.h"
#include "types/logical_type_infra.h"
#include "util/value_generator.h"

namespace starrocks {

struct ChunksSorterHeapSortTest : public testing::Test {
    void SetUp() override {
        config::vector_chunk_size = 1024;
        _runtime_state = _create_runtime_state();
    }

    std::shared_ptr<RuntimeState> _create_runtime_state() {
        TUniqueId fragment_id;
        TQueryOptions query_options;
        query_options.batch_size = config::vector_chunk_size;
        TQueryGlobals query_globals;
        auto runtime_state = std::make_shared<RuntimeState>(fragment_id, query_options, query_globals, nullptr);
        runtime_state->init_instance_mem_tracker();
        return runtime_state;
    }

    std::shared_ptr<RuntimeState> _runtime_state;
    ObjectPool _pool;
};

struct BuildOptions {
    // use list value
    std::vector<Datum> use_list_values;
    // use random value
    bool use_random_value;
    // is nullable value
    bool is_nullable_value;
};

template <LogicalType TYPE>
struct ColumnRandomAppender {
    static bool append(ColumnPtr& col, int sz) {
        auto* spec_col = ColumnHelper::cast_to_raw<TYPE>(col);
        if constexpr (isArithmeticLT<TYPE>) {
            auto& container = spec_col->get_data();
            container.resize(sz);
            for (int i = 0; i < sz; ++i) {
                container[i] = RandomGenerator<RunTimeCppType<TYPE>, 10000000>::next_value();
            }
            return true;
        } else {
            return false;
        }
    }
};

template <template <LogicalType, typename... Args> typename Function, typename... Args>
void dispatch_function(LogicalType type, Args&&... args) {
    bool result = false;
    switch (type) {
#define M(NAME)                                                                    \
    case LogicalType::NAME: {                                                      \
        result = Function<LogicalType::NAME>::append(std::forward<Args>(args)...); \
        break;                                                                     \
    }
        APPLY_FOR_ALL_NUMBER_TYPE(M)
#undef M
    default:
        break;
    }
    DCHECK(result) << "not support function call for type:" << type;
}

// depends rand
struct FakeChunks {
    FakeChunks(ObjectPool* pool, std::vector<TypeDescriptor*> descs, std::vector<BuildOptions> build_options) {
        _pool = pool;
        _type_descs = std::move(descs);
        _build_options = std::move(build_options);

        for (int i = 0; i < _type_descs.size(); ++i) {
            _slot_refs.push_back(pool->add(new ColumnRef(*_type_descs[i], i)));
        }

        for (int i = 0; i < _type_descs.size(); ++i) {
            map[i] = i;
        }
    }

    ChunkPtr next_chunk(int chunk_sz) {
        Columns columns;
        for (int i = 0; i < _slot_refs.size(); ++i) {
            columns.push_back(ColumnHelper::create_column(*_type_descs[i], _build_options[i].is_nullable_value));
        }
        for (int i = 0; i < _slot_refs.size(); ++i) {
            const auto& build_option = _build_options[i];
            if (build_option.use_random_value) {
                if (!build_option.use_list_values.empty()) {
                    for (int j = 0; j < chunk_sz; ++j) {
                        int k = rand() % build_option.use_list_values.size();
                        columns[i]->append_datum(build_option.use_list_values[k]);
                    }
                } else {
                    dispatch_function<ColumnRandomAppender>(_type_descs[i]->type, columns[i], chunk_sz);
                }
            } else {
                if (build_option.use_list_values.size() >= chunk_sz) {
                    for (int j = 0; j < chunk_sz; ++j) {
                        columns[i]->append_datum(build_option.use_list_values[j]);
                    }
                } else {
                    __builtin_unreachable();
                }
            }
        }
        return std::make_shared<Chunk>(columns, map);
    }
    const std::vector<ColumnRef*>& slot_refs() { return _slot_refs; }

private:
    ObjectPool* _pool;
    std::vector<TypeDescriptor*> _type_descs;
    std::vector<BuildOptions> _build_options;
    std::vector<ColumnRef*> _slot_refs;
    Chunk::SlotHashMap map;
};

TEST_F(ChunksSorterHeapSortTest, single_column_order_by_notnull_test) {
    std::vector<bool> is_asc = {true};
    std::vector<bool> null_first = {true};

    {
        // clang-format off
        std::vector<TypeDescriptor*> type_descs = {_pool.add(new TypeDescriptor(TYPE_INT)),
                                                   _pool.add(new TypeDescriptor(TYPE_VARCHAR))};
        std::vector<BuildOptions> build_options = {
            {{}, true, false},
            {{Slice("value1"), Slice("value2"), Slice("value3")}, true, false}
        };
        // clang-format on

        srand(0);
        FakeChunks fake_chunks(&_pool, type_descs, build_options);

        // Test sort by INT less than limit
        {
            std::vector<ExprContext*> sort_exprs;
            sort_exprs.push_back(_pool.add(new ExprContext(fake_chunks.slot_refs()[0])));
            ASSERT_OK(Expr::prepare(sort_exprs, _runtime_state.get()));
            ASSERT_OK(Expr::open(sort_exprs, _runtime_state.get()));
            ChunksSorterHeapSort sorter(_runtime_state.get(), &sort_exprs, &is_asc, &null_first, "", 0, 1024);
            sorter.setup_runtime(_runtime_state.get(), _pool.add(new RuntimeProfile("")),
                                 _pool.add(new MemTracker(1L << 62, "parent", nullptr)));
            ASSERT_OK(sorter.update(nullptr, fake_chunks.next_chunk(1024)));
            ASSERT_OK(sorter.done(nullptr));

            ChunkPtr chunk;
            bool eos = false;
            ASSERT_OK(sorter.get_next(&chunk, &eos));
            ASSERT_EQ(chunk->num_rows(), 1024);
            auto column = chunk->get_column_by_slot_id(0);
            auto* i32_col = ColumnHelper::cast_to_raw<TYPE_INT>(column);
            const auto& i32_data = i32_col->get_data();
            ASSERT_TRUE(std::is_sorted(i32_data.begin(), i32_data.end()));
        }
        // Test sort by INT greater than limit
        {
            std::vector<ExprContext*> sort_exprs;
            sort_exprs.push_back(_pool.add(new ExprContext(fake_chunks.slot_refs()[0])));
            ASSERT_OK(Expr::prepare(sort_exprs, _runtime_state.get()));
            ASSERT_OK(Expr::open(sort_exprs, _runtime_state.get()));
            ChunksSorterHeapSort sorter(_runtime_state.get(), &sort_exprs, &is_asc, &null_first, "", 0, 1024);
            sorter.setup_runtime(_runtime_state.get(), _pool.add(new RuntimeProfile("")),
                                 _pool.add(new MemTracker(1L << 62, "parent", nullptr)));
            ASSERT_OK(sorter.update(nullptr, fake_chunks.next_chunk(1023)));
            ASSERT_OK(sorter.update(nullptr, fake_chunks.next_chunk(1023)));
            ASSERT_OK(sorter.done(nullptr));

            ChunkPtr chunk;
            bool eos = false;
            ASSERT_OK(sorter.get_next(&chunk, &eos));
            ASSERT_EQ(chunk->num_rows(), 1024);
            auto column = chunk->get_column_by_slot_id(0);
            auto* i32_col = ColumnHelper::cast_to_raw<TYPE_INT>(column);
            const auto& i32_data = i32_col->get_data();
            ASSERT_TRUE(std::is_sorted(i32_data.begin(), i32_data.end()));
            ASSERT_OK(sorter.get_next(&chunk, &eos));
            ASSERT_TRUE(eos);
        }
        // Test sort by VARCHAR
        {
            std::vector<ExprContext*> sort_exprs;
            sort_exprs.push_back(_pool.add(new ExprContext(fake_chunks.slot_refs()[1])));
            ASSERT_OK(Expr::prepare(sort_exprs, _runtime_state.get()));
            ASSERT_OK(Expr::open(sort_exprs, _runtime_state.get()));
            ChunksSorterHeapSort sorter(_runtime_state.get(), &sort_exprs, &is_asc, &null_first, "", 0, 1024);
            sorter.setup_runtime(_runtime_state.get(), _pool.add(new RuntimeProfile("")),
                                 _pool.add(new MemTracker(1L << 62, "parent", nullptr)));
            ASSERT_OK(sorter.update(nullptr, fake_chunks.next_chunk(1024)));
            ASSERT_OK(sorter.update(nullptr, fake_chunks.next_chunk(1023)));
            ASSERT_OK(sorter.done(nullptr));

            ChunkPtr chunk;
            bool eos = false;
            ASSERT_OK(sorter.get_next(&chunk, &eos));
            auto column = chunk->get_column_by_slot_id(1);
            auto* slice_col = ColumnHelper::cast_to_raw<TYPE_VARCHAR>(column);
            const auto& slice_data = slice_col->get_data();
            ASSERT_TRUE(std::is_sorted(slice_data.begin(), slice_data.end()));
        }
    }
}

TEST_F(ChunksSorterHeapSortTest, single_column_order_by_nullable_test) {
    {
        std::vector<bool> is_asc = {true};
        std::vector<bool> null_first = {true};

        std::vector<TypeDescriptor*> type_descs = {_pool.add(new TypeDescriptor(TYPE_INT))};
        std::vector<BuildOptions> build_options = {{{5, 2, 5, 3, 1, 4, 7, 8, 3, 9}, false, true}};
        build_options[0].use_list_values[1].set_null();
        srand(0);
        FakeChunks fake_chunks(&_pool, type_descs, build_options);

        {
            std::vector<ExprContext*> sort_exprs;
            sort_exprs.push_back(_pool.add(new ExprContext(fake_chunks.slot_refs()[0])));
            ASSERT_OK(Expr::prepare(sort_exprs, _runtime_state.get()));
            ASSERT_OK(Expr::open(sort_exprs, _runtime_state.get()));
            // limit 5
            int limit_sz = 5;
            ChunksSorterHeapSort sorter(_runtime_state.get(), &sort_exprs, &is_asc, &null_first, "", 0, limit_sz);
            sorter.setup_runtime(_runtime_state.get(), _pool.add(new RuntimeProfile("")),
                                 _pool.add(new MemTracker(1L << 62, "parent", nullptr)));
            ASSERT_OK(sorter.update(nullptr, fake_chunks.next_chunk(10)));
            ASSERT_OK(sorter.done(nullptr));

            ChunkPtr chunk;
            bool eos = false;
            ASSERT_OK(sorter.get_next(&chunk, &eos));
            auto column = chunk->get_column_by_slot_id(0);
            ColumnViewer<TYPE_INT> viewer(column);
            ASSERT_TRUE(viewer.is_null(0));
            const auto& container = viewer.column()->get_data();
            const auto& null_container = viewer.null_column()->get_data();
            ASSERT_TRUE(std::all_of(null_container.begin() + 1, null_container.end(), [](auto v) { return !v; }));
            ASSERT_TRUE(std::is_sorted(container.begin() + 1, container.end()));
        }

        null_first[0] = false;
        {
            std::vector<ExprContext*> sort_exprs;
            sort_exprs.push_back(_pool.add(new ExprContext(fake_chunks.slot_refs()[0])));
            ASSERT_OK(Expr::prepare(sort_exprs, _runtime_state.get()));
            ASSERT_OK(Expr::open(sort_exprs, _runtime_state.get()));
            // limit 5
            int limit_sz = 10;
            ChunksSorterHeapSort sorter(_runtime_state.get(), &sort_exprs, &is_asc, &null_first, "", 0, limit_sz);
            sorter.setup_runtime(_runtime_state.get(), _pool.add(new RuntimeProfile("")),
                                 _pool.add(new MemTracker(1L << 62, "parent", nullptr)));
            ASSERT_OK(sorter.update(nullptr, fake_chunks.next_chunk(10)));
            ASSERT_OK(sorter.done(nullptr));

            ChunkPtr chunk;
            bool eos = false;
            ASSERT_OK(sorter.get_next(&chunk, &eos));
            auto column = chunk->get_column_by_slot_id(0);
            ColumnViewer<TYPE_INT> viewer(column);
            ASSERT_TRUE(viewer.is_null(viewer.size() - 1));
            const auto& container = viewer.column()->get_data();
            const auto& null_container = viewer.null_column()->get_data();
            ASSERT_TRUE(std::all_of(null_container.begin(), null_container.end() - 1, [](auto v) { return !v; }));
            ASSERT_TRUE(std::is_sorted(container.begin(), container.end() - 1));
        }

        is_asc[0] = false;
        null_first[0] = true;
        {
            std::vector<ExprContext*> sort_exprs;
            sort_exprs.push_back(_pool.add(new ExprContext(fake_chunks.slot_refs()[0])));
            ASSERT_OK(Expr::prepare(sort_exprs, _runtime_state.get()));
            ASSERT_OK(Expr::open(sort_exprs, _runtime_state.get()));
            // limit 5
            int limit_sz = 5;
            ChunksSorterHeapSort sorter(_runtime_state.get(), &sort_exprs, &is_asc, &null_first, "", 0, limit_sz);
            sorter.setup_runtime(_runtime_state.get(), _pool.add(new RuntimeProfile("")),
                                 _pool.add(new MemTracker(1L << 62, "parent", nullptr)));
            ASSERT_OK(sorter.update(nullptr, fake_chunks.next_chunk(10)));
            ASSERT_OK(sorter.done(nullptr));

            ChunkPtr chunk;
            bool eos = false;
            ASSERT_OK(sorter.get_next(&chunk, &eos));
            auto column = chunk->get_column_by_slot_id(0);
            ColumnViewer<TYPE_INT> viewer(column);
            ASSERT_TRUE(viewer.is_null(0));
            const auto& container = viewer.column()->get_data();
            const auto& null_container = viewer.null_column()->get_data();
            ASSERT_TRUE(std::all_of(null_container.begin() + 1, null_container.end(), [](auto v) { return !v; }));
            ASSERT_TRUE(std::is_sorted(container.begin() + 1, container.end(), std::greater<int32_t>()));
        }
    }
}

} // namespace starrocks
