#include <memory>

#include "column/binary_column.h"
#include "column/column_helper.h"
#include "exprs/string_functions.h"
#include "gtest/gtest.h"

namespace starrocks {
class SplitPartTest : public ::testing::Test {};

void split_part_test(const std::string& haystack, const std::string& delimiter, int part_number,
                     const std::string& expected) {
    auto context = std::make_unique<FunctionContext>();

    auto haystack_column = BinaryColumn::create();
    auto delimiter_column = BinaryColumn::create();
    haystack_column->append(haystack);
    delimiter_column->append(delimiter);

    size_t chunk_size = haystack_column->size();
    auto part_number_column = ColumnHelper::create_const_column<TYPE_INT>(part_number, chunk_size);

    Columns columns;
    columns.emplace_back(std::move(haystack_column));
    columns.emplace_back(std::move(delimiter_column));
    columns.emplace_back(std::move(part_number_column));

    auto result = StringFunctions::split_part(context.get(), columns);

    ASSERT_TRUE(result.ok());
    auto result_column = std::move(result).value();
    ASSERT_EQ(result_column->size(), 1);
    ASSERT_EQ(result_column->get(0).get_slice().to_string(), expected);
}

TEST_F(SplitPartTest, splitPartTest) {
    split_part_test("hello world", "|", 1, "hello world");
    split_part_test("hello world", "|", 100, "");
    split_part_test("hello world", " ", 1, "hello");
    split_part_test("hello world", " ", 2, "world");
    split_part_test("hello world", " ", -1, "world");
    split_part_test("hello world", " ", -2, "hello");
    split_part_test("abca", "a", 1, "");
    split_part_test("abca", "a", -1, "");
    split_part_test("abca", "a", -2, "bc");
    split_part_test("hello", "", 1, "h");
    split_part_test("hello", "", 0, "");
    split_part_test("hello", "", 10, "");
    split_part_test("hello world", " ", 0, "");
    split_part_test("hello world", " ", 100, "");
    split_part_test("hello world", "z", 1, "hello world");
    split_part_test("hello world", "z", 2, "");
}
} // namespace starrocks
