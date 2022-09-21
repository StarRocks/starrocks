// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "storage/datum_row_iterator.h"

#include <gtest/gtest.h>

#include "storage/chunk_iterator.h"
#include "column/datum.h"
#include "column/column_pool.h"
#include "column/vectorized_fwd.h"

namespace starrocks {

using Schema = vectorized::Schema;

// refer to projection_iterator_test.cpp
class VectorIterator final : public vectorized::ChunkIterator {
public:
    explicit VectorIterator(std::vector<int32_t> c1, std::vector<string> c2, std::vector<int32_t> c3)
            : ChunkIterator(schema()), _c1(std::move(c1)), _c2(std::move(c2)), _c3(std::move(c3)) {}

    // at most 10 elements every time.
    Status do_get_next(vectorized::Chunk* chunk) override {
        size_t n = std::min(10LU, _c1.size() - _idx);
        for (size_t i = 0; i < n; i++) {
            chunk->get_column_by_index(0)->append_datum(vectorized::Datum(_c1[_idx]));
            chunk->get_column_by_index(1)->append_datum(vectorized::Datum(Slice(_c2[_idx])));
            chunk->get_column_by_index(2)->append_datum(vectorized::Datum(_c3[_idx]));
            _idx++;
        }
        return n > 0 ? Status::OK() : Status::EndOfFile("eof");
    }

    void reset() {
        _idx = 0;
        _encoded_schema.clear();
    }

    static Schema schema() {
        vectorized::FieldPtr f1 = std::make_shared<vectorized::Field>(0, "c1", get_type_info(OLAP_FIELD_TYPE_INT), false);
        vectorized::FieldPtr f2 = std::make_shared<vectorized::Field>(1, "c2", get_type_info(OLAP_FIELD_TYPE_VARCHAR), false);
        vectorized::FieldPtr f3 = std::make_shared<vectorized::Field>(2, "c3", get_type_info(OLAP_FIELD_TYPE_INT), false);
        f1->set_is_key(true);
        return Schema(std::vector<vectorized::FieldPtr>{f1, f2, f3});
    }

    void close() override {}

private:
    size_t _idx = 0;
    std::vector<int32_t> _c1;
    std::vector<std::string> _c2;
    std::vector<int32_t> _c3;
};

class DatumRowIteratorTest : public testing::Test {
public:

    void SetUp() override {}

    void TearDown() override {
        vectorized::TEST_clear_all_columns_this_thread();
    }
};

TEST_F(DatumRowIteratorTest, test_basic) {
    std::vector<int32_t> c1{1, 2, 3, 4, 5};
    std::vector<std::string> c2{"a", "b", "c", "d", "e"};
    std::vector<int32_t> c3{10, 11, 12, 13, 14};

    auto child = std::make_shared<VectorIterator>(c1, c2, c3);
    auto row_iterator = std::make_shared<DatumRowIterator>(child, 2);
    for (size_t i = 0; i < c1.size(); i++) {
        auto status_or_row = row_iterator->get_next();
        ASSERT_TRUE(status_or_row.ok());
        RowPtr row = status_or_row.value();
        ASSERT_TRUE(row != nullptr);
        ASSERT_EQ(c1[i], row->get_int32(0));
        ASSERT_EQ(c2[i], row->get_slice(1));
        ASSERT_EQ(c3[i], row->get_int32(2));
    }
    ASSERT_TRUE(row_iterator->get_next().status().is_end_of_file());
    row_iterator->close();
}

} // namespace starrocks
