// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exec/vectorized/sorting/sorting.h"

#include <gtest/gtest.h>

#include <random>

#include "column/chunk.h"
#include "column/column_helper.h"
#include "exec/vectorized/sorting/sort_helper.h"
#include "exprs/vectorized/column_ref.h"
#include "runtime/vectorized/chunk_cursor.h"

namespace starrocks::vectorized {

TEST(MergeTest, merge_sorter_chunks_two_way) {
    TypeDescriptor type_desc = TypeDescriptor(TYPE_INT);
    ColumnPtr col1 = ColumnHelper::create_column(type_desc, false);
    ColumnPtr col2 = ColumnHelper::create_column(type_desc, false);
    ColumnPtr col3 = ColumnHelper::create_column(type_desc, false);
    ColumnPtr col4 = ColumnHelper::create_column(type_desc, false);
    std::vector<int32_t> elements_col1{1, 1, 1, 2, 2, 3, 4, 5, 6};
    std::vector<int32_t> elements_col2{1, 2, 2, 2, 3, 3, 6, 7, 8};
    std::vector<int32_t> elements_col3{1, 2, 2, 2, 3, 3, 6, 7, 8};
    std::vector<int32_t> elements_col4{2, 3, 4, 4, 4, 8, 9, 7, 11};

    for (int i = 0; i < elements_col1.size(); i++) {
        col1->append_datum(Datum(elements_col1[i]));
        col2->append_datum(Datum(elements_col2[i]));
        col3->append_datum(Datum(elements_col3[i]));
        col4->append_datum(Datum(elements_col4[i]));
    }
    Chunk::SlotHashMap map;
    map[0] = 0;
    map[1] = 1;

    ChunkPtr chunk1 = std::make_shared<Chunk>(Columns{col1, col3}, map);
    ChunkPtr chunk2 = std::make_shared<Chunk>(Columns{col2, col4}, map);
    Permutation perm;
    merge_sorted_chunks_two_way(chunk1, chunk2, &perm);

    size_t expected_size = col1->size() + col2->size();
    std::unique_ptr<Chunk> output = chunk1->clone_empty();
    append_by_permutation(output.get(), std::vector<ChunkPtr>{chunk1, chunk2}, perm);
    ASSERT_EQ(expected_size, perm.size());
    Int32Column* output_column1 = down_cast<Int32Column*>(output->get_column_by_index(0).get());
    Int32Column* output_column2 = down_cast<Int32Column*>(output->get_column_by_index(0).get());
    Int32Column::Container& data1 = output_column1->get_data();
    Int32Column::Container& data2 = output_column2->get_data();
    std::vector<std::tuple<int32_t, int32_t>> rows;
    for (int i = 0; i < data1.size(); i++) {
        rows.emplace_back(data1[i], data2[i]);
    }

    ASSERT_EQ(expected_size, output_column1->size());
    ASSERT_EQ(expected_size, data1.size());
    ASSERT_TRUE(std::is_sorted(rows.begin(), rows.end(),
                               [](auto x, auto y) {
                                   if (std::get<0>(x) != std::get<0>(y)) {
                                       return std::get<0>(x) < std::get<0>(y);
                                   }
                                   return std::get<1>(x) < std::get<1>(y);
                               }))
            << "merged data: " << fmt::format("{}", fmt::join(data1, ", "));
}

static ColumnPtr build_sorted_column(TypeDescriptor type_desc, int slot_index, int32_t start, int32_t count,
                                     int32_t step) {
    DCHECK_EQ(TYPE_INT, type_desc.type);

    ColumnPtr column = ColumnHelper::create_column(type_desc, false);
    for (int i = 0; i < count; i++) {
        column->append_datum(Datum(start + step * i));
    }
    return column;
}

TEST(MergeTest, merge_sorted_cursor_two_way) {
    constexpr int num_columns = 3;
    constexpr int num_chunks = 3;
    constexpr int chunk_size = 10;

    std::vector<std::unique_ptr<ColumnRef>> exprs;
    std::vector<ExprContext*> sort_exprs;
    std::vector<bool> asc_arr;
    std::vector<bool> null_first;
    Chunk::SlotHashMap map;
    TypeDescriptor type_desc = TypeDescriptor(TYPE_INT);

    for (int i = 0; i < num_columns; i++) {
        auto expr = std::make_unique<ColumnRef>(type_desc, i);
        exprs.emplace_back(std::move(expr));
        sort_exprs.push_back(new ExprContext(exprs.back().get()));
        asc_arr.push_back(true);
        null_first.push_back(true);
        map[i] = i;
    }

    std::vector<std::unique_ptr<Chunk>> left_chunks;
    std::vector<std::unique_ptr<Chunk>> right_chunks;
    int left_index = 0;
    int right_index = 0;
    for (int i = 0; i < num_chunks; i++) {
        Columns left_columns, right_columns;
        for (int k = 0; k < num_columns; k++) {
            left_columns.push_back(build_sorted_column(type_desc, k, i * chunk_size * (k + 1), chunk_size, k + 1));
            right_columns.push_back(
                    build_sorted_column(type_desc, k, (i + 1) * chunk_size * (k + 1), chunk_size, k + 1));
        }

        left_chunks.push_back(std::make_unique<Chunk>(left_columns, map));
        right_chunks.push_back(std::make_unique<Chunk>(right_columns, map));
    }
    for (auto& chunk : left_chunks) {
        for (int i = 0; i < chunk->num_rows(); i++) {
            fmt::print("left_chunk row: {}\n", chunk->debug_row(i));
        }
    }
    for (auto& chunk : right_chunks) {
        for (int i = 0; i < chunk->num_rows(); i++) {
            fmt::print("right_chunk row: {}\n", chunk->debug_row(i));
        }
    }

    ChunkSupplier left_chunk_supplier = [&](Chunk** chunk) { return Status::OK(); };
    ChunkHasSupplier left_chunk_has_supplier = [&]() { return true; };
    ChunkProbeSupplier left_chunk_probe_supplier = [&](Chunk** chunk) {
        if (left_index >= left_chunks.size()) return false;
        *chunk = left_chunks[left_index++].release();
        return true;
    };

    ChunkSupplier right_chunk_supplier = [&](Chunk** chunk) { return Status::OK(); };
    ChunkHasSupplier right_chunk_has_supplier = [&]() { return true; };
    ChunkProbeSupplier right_chunk_probe_supplier = [&](Chunk** chunk) {
        if (right_index >= right_chunks.size()) return false;
        *chunk = right_chunks[right_index++].release();
        return true;
    };

    ChunkCursor left_cursor(left_chunk_supplier, left_chunk_probe_supplier, left_chunk_has_supplier, &sort_exprs,
                            &asc_arr, &null_first, true);
    ChunkCursor right_cursor(right_chunk_supplier, right_chunk_probe_supplier, right_chunk_has_supplier, &sort_exprs,
                             &asc_arr, &null_first, true);
    std::vector<ChunkPtr> output_chunks;
    ChunkConsumer consumer = [&](Chunk* chunk) {
        output_chunks.push_back(ChunkPtr(chunk));
        return Status::OK();
    };
    merge_sorted_cursor_two_way(left_cursor, right_cursor, consumer);

    for (auto& chunk : output_chunks) {
        for (int i = 0; i < chunk->num_rows(); i++) {
            fmt::print("row: {}\n", chunk->debug_row(i));
            if (i > 0) {
                int x = compare_chunk_row(*chunk, *chunk, i - 1, i);
                ASSERT_LE(x, 0);
            }
        }
    }
}

TEST(MergeTest, merge_sorted_stream) {
    constexpr int num_columns = 3;
    constexpr int num_runs = 4;
    constexpr int num_chunks_per_run = 4;
    std::vector<std::unique_ptr<ColumnRef>> exprs;
    std::vector<ExprContext*> sort_exprs;
    std::vector<bool> asc_arr;
    std::vector<bool> null_first;
    Chunk::SlotHashMap map;
    TypeDescriptor type_desc = TypeDescriptor(TYPE_INT);

    for (int i = 0; i < num_columns; i++) {
        auto expr = std::make_unique<ColumnRef>(type_desc, i);
        exprs.emplace_back(std::move(expr));
        sort_exprs.push_back(new ExprContext(exprs.back().get()));
        asc_arr.push_back(true);
        null_first.push_back(true);
        map[i] = i;
    }

    ChunkSuppliers chunk_suppliers;
    ChunkProbeSuppliers chunk_probe_suppliers;
    ChunkHasSuppliers chunk_has_suppliers;
    std::vector<int> chunk_probe_index(num_runs, 0);
    std::vector<int> chunk_run_max(num_runs, 0);
    for (int run = 0; run < num_runs; run++) {
        ChunkSupplier chunk_supplier = [&](Chunk** output) -> Status {
            CHECK(false) << "unreachable";
            return Status::OK();
        };
        ChunkProbeSupplier chunk_probe_supplier = [&, run](Chunk** output) -> bool {
            if (chunk_probe_index[run]++ > num_chunks_per_run) {
                *output = nullptr;
                return false;
            } else {
                Columns columns;
                for (int col_idx = 0; col_idx < num_columns; col_idx++) {
                    auto column =
                            build_sorted_column(type_desc, col_idx, col_idx * 10 * chunk_probe_index[run], 10, col_idx);
                    columns.push_back(column);
                }
                ChunkUniquePtr chunk = std::make_unique<Chunk>(columns, map);
                *output = chunk.release();

                return true;
            }
        };
        ChunkHasSupplier chunk_has_supplier = [&]() -> bool { return true; };
        chunk_suppliers.emplace_back(chunk_supplier);
        chunk_probe_suppliers.emplace_back(chunk_probe_supplier);
        chunk_has_suppliers.emplace_back(chunk_has_supplier);
    }

    std::deque<SortedChunkStream> streams;
    for (int run = 0; run < num_runs; run++) {
        SortedChunkStream stream;
        Chunk* chunk = nullptr;
        int row = 0;
        while (chunk_probe_suppliers[run](&chunk)) {
            if (chunk == nullptr) break;
            stream.chunks.push_back(ChunkPtr{chunk});
            for (int k = 0; k < chunk->num_rows(); k++) {
                row++;
                fmt::print("run {} index {} row {}: {}\n", run, k, row, chunk->debug_row(k));
            }
        }
        streams.push_back(stream);
    }

    while (streams.size() > 1) {
        SortedChunkStream left_stream = streams.front();
        streams.pop_front();
        SortedChunkStream right_stream = streams.front();
        streams.pop_front();
        CHECK(!right_stream.chunks.empty());
        ChunkCursor left(
                left_stream.get_supplier(), left_stream.get_probe_supplier(), []() { return true; }, &sort_exprs,
                &asc_arr, &null_first, true);
        ChunkCursor right(
                right_stream.get_supplier(), right_stream.get_probe_supplier(), []() { return true; }, &sort_exprs,
                &asc_arr, &null_first, true);
        SortedChunkStream merged;
        Status st = merge_sorted_cursor_two_way(left, right, [&](Chunk* chunk) {
            CHECK(!chunk->is_empty());
            merged.chunks.push_back(ChunkPtr(chunk));
            fmt::print("merge chunk {} rows\n", chunk->num_rows());
            return Status::OK();
        });
        fmt::print("generate merged stream with {} chunks and {} rows\n", merged.chunks.size(), merged.num_rows());
        CHECK(st.ok());
        CHECK(!merged.chunks.empty());
        streams.push_back(merged);
    }
    fmt::print("merge {} stream of {} rows\n", num_runs, streams[0].num_rows());

    ASSERT_EQ(1, streams.size());
    for (int c = 0; c < streams[0].chunks.size(); c++) {
        auto& chunk = streams[0].chunks[c];
        if (c > 0) {
            fmt::print("sorted row {}: {}\n", c * chunk->num_rows(), chunk->debug_row(0));
            auto& last_chunk = streams[0].chunks[c - 1];
            int x = compare_chunk_row(*last_chunk, *chunk, last_chunk->num_rows() - 1, 0);
            ASSERT_LE(x, 0);
        }
        for (int i = 1; i < chunk->num_rows(); i++) {
            fmt::print("sorted row {}: {}\n", c * chunk->num_rows() + i, chunk->debug_row(i));
            int x = compare_chunk_row(*chunk, *chunk, i - 1, i);
            ASSERT_LE(x, 0);
        }
    }
}

} // namespace starrocks::vectorized

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
