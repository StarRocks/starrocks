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

#include "column/column_helper.h"
#include "common/config_scan_io_fwd.h"
#include "common/storage_define.h"
#include "exprs/string_functions.h"

namespace starrocks {

constexpr int32_t kTestOlapStringMaxLength = 64 * 1024;

class StringFunctionRepeatTest : public ::testing::Test {
protected:
    void SetUp() override {
        _saved_olap_string_max_length = config::olap_string_max_length;
        config::olap_string_max_length = kTestOlapStringMaxLength;
    }

    void TearDown() override { config::olap_string_max_length = _saved_olap_string_max_length; }

private:
    int32_t _saved_olap_string_max_length = 0;
};
TEST_F(StringFunctionRepeatTest, repeatTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto str = BinaryColumn::create();
    auto times = Int32Column::create();
    for (int j = 0; j < 20; ++j) {
        str->append(std::to_string(j));
        times->append(j);
    }

    columns.emplace_back(std::move(str));
    columns.emplace_back(std::move(times));

    ColumnPtr result = StringFunctions::repeat(ctx.get(), columns).value();
    ASSERT_EQ(20, result->size());

    auto v = ColumnViewer<TYPE_VARCHAR>(result);

    for (int k = 0; k < 20; ++k) {
        std::string s;
        for (int i = 0; i < k; ++i) {
            s.append(std::to_string(k));
        }
        ASSERT_EQ(s, v.value(k).to_string());
    }
}

TEST_F(StringFunctionRepeatTest, repeatLargeTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto str = BinaryColumn::create();
    auto times = Int32Column::create();

    str->append(std::to_string(1));
    times->append(get_olap_string_max_length() + 100);

    columns.emplace_back(std::move(str));
    columns.emplace_back(std::move(times));

    ColumnPtr result = StringFunctions::repeat(ctx.get(), columns).value();
    ASSERT_EQ(1, result->size());
}

TEST_F(StringFunctionRepeatTest, repeatConstTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto str = BinaryColumn::create();
    auto times = Int32Column::create();

    for (int i = 0; i < 100; ++i) {
        str->append(std::string(i, 'x'));
        str->append(std::string(1, 'x'));
    }

    int32_t repeat_times = get_olap_string_max_length() / 100 + 10;
    times->append(repeat_times);

    columns.emplace_back(str->clone());
    columns.emplace_back(ConstColumn::create(std::move(times), 1));

    ColumnPtr result = StringFunctions::repeat(ctx.get(), columns).value();
    const auto num_rows = str->size();
    ASSERT_EQ(num_rows, result->size());

    auto v = ColumnViewer<TYPE_VARCHAR>(result);

    for (int i = 0; i < num_rows; ++i) {
        auto si = str->get_slice(i);
        auto so = v.value(i);

        if (si.size * repeat_times <= get_olap_string_max_length()) {
            ASSERT_EQ(so.size, si.size * repeat_times);
        } else {
            ASSERT_TRUE(v.is_null(i));
        }
    }
}

TEST_F(StringFunctionRepeatTest, repeatConstExactLimitWithOversizeRow) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto str = BinaryColumn::create();
    auto times = Int32Column::create();

    constexpr int32_t repeat_times = 2;
    str->append(std::string(get_olap_string_max_length() / repeat_times, 'x'));
    str->append(std::string(get_olap_string_max_length() / repeat_times + 1, 'y'));
    times->append(repeat_times);

    Columns columns;
    columns.emplace_back(std::move(str));
    columns.emplace_back(ConstColumn::create(std::move(times), 1));

    ColumnPtr result = StringFunctions::repeat(ctx.get(), columns).value();
    ASSERT_EQ(2, result->size());

    auto viewer = ColumnViewer<TYPE_VARCHAR>(result);
    ASSERT_FALSE(viewer.is_null(0));
    ASSERT_EQ(get_olap_string_max_length(), viewer.value(0).size);
    ASSERT_TRUE(viewer.is_null(1));
}

// A chunk whose bytes are dominated by rows that repeat past the string limit - and therefore
// produce nothing at all. This is the skew the reservation used to get wrong: `times *
// total_input_bytes` counts the dropped row's 700 bytes, and the recount that would have corrected
// it only ran when that product exceeded `max_length * num_rows`, which this input stays well
// under. The over-reservation itself is transient - the byte buffer is sized down to what was
// written before the column is handed back - so what is pinned here is the result, over exactly
// the shape where the old guard was silent.
TEST_F(StringFunctionRepeatTest, repeatConstOversizeRowsDoNotInflateTheOutput) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());

    constexpr int32_t repeat_times = 100;
    constexpr int kSmallRows = 9;
    // 700 * 100 > 64 KiB, so this row is dropped; it still carries most of the input bytes.
    constexpr size_t kOversizeRowLength = 700;
    ASSERT_GT(kOversizeRowLength * repeat_times, get_olap_string_max_length());

    auto str = BinaryColumn::create();
    for (int i = 0; i < kSmallRows; ++i) {
        str->append(std::string(1, 'x'));
    }
    str->append(std::string(kOversizeRowLength, 'y'));
    auto times = Int32Column::create();
    times->append(repeat_times);

    Columns columns;
    columns.emplace_back(std::move(str));
    columns.emplace_back(ConstColumn::create(std::move(times), 1));

    ColumnPtr result = StringFunctions::repeat(ctx.get(), columns).value();
    ASSERT_EQ(kSmallRows + 1, result->size());

    auto viewer = ColumnViewer<TYPE_VARCHAR>(result);
    for (int i = 0; i < kSmallRows; ++i) {
        ASSERT_FALSE(viewer.is_null(i));
        ASSERT_EQ(std::string(repeat_times, 'x'), viewer.value(i).to_string());
    }
    ASSERT_TRUE(viewer.is_null(kSmallRows));

    // 900 bytes of payload, not the 70900 the old estimate reserved: the dropped row contributes
    // nothing to the result either.
    constexpr size_t kProducedBytes = static_cast<size_t>(kSmallRows) * repeat_times;
    const auto* data_column = down_cast<const BinaryColumn*>(ColumnHelper::get_data_column(result.get()));
    ASSERT_EQ(kProducedBytes, data_column->get_immutable_bytes().size());
}

} // namespace starrocks
