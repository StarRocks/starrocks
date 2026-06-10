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

#include "exec/plain_text_builder.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>

#include "base/testutil/assert.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
#include "exprs/column_ref.h"
#include "exprs/mock_vectorized_expr.h"
#include "fs/fs.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"

namespace starrocks {

class PlainTextBuilderTest : public testing::Test {
protected:
    void SetUp() override {
        _test_dir = "./plain_text_builder_test_" + std::to_string(::getpid());
        std::filesystem::create_directories(_test_dir);
    }

    void TearDown() override {
        if (std::filesystem::exists(_test_dir)) {
            std::filesystem::remove_all(_test_dir);
        }
    }

    std::string read_file(const std::string& path) {
        std::ifstream file(path);
        std::stringstream buffer;
        buffer << file.rdbuf();
        return buffer.str();
    }

    std::string _test_dir;
};

// Test PlainTextBuilder without header
TEST_F(PlainTextBuilderTest, TestNoHeader) {
    std::string file_path = _test_dir + "/no_header.csv";

    PlainTextBuilderOptions options{.column_terminated_by = ",",
                                    .line_terminated_by = "\n",
                                    .with_header = false,
                                    .column_names = {"col1", "col2", "col3"}};

    ASSIGN_OR_ABORT(auto writable_file, FileSystem::Default()->new_writable_file(file_path));

    // Create empty expr contexts vector (for simplicity, we test init() directly)
    std::vector<ExprContext*> output_expr_ctxs;

    PlainTextBuilder builder(std::move(options), std::move(writable_file), output_expr_ctxs);

    // Call finish to flush and close file
    ASSERT_OK(builder.finish());

    // Read the file content
    std::string content = read_file(file_path);

    // Without header, file should be empty when no data is added
    EXPECT_EQ(content, "");
}

// Test PlainTextBuilder with header but empty column names
TEST_F(PlainTextBuilderTest, TestHeaderWithEmptyColumnNames) {
    std::string file_path = _test_dir + "/empty_column_names.csv";

    PlainTextBuilderOptions options{
            .column_terminated_by = ",", .line_terminated_by = "\n", .with_header = true, .column_names = {}};

    ASSIGN_OR_ABORT(auto writable_file, FileSystem::Default()->new_writable_file(file_path));

    std::vector<ExprContext*> output_expr_ctxs;

    PlainTextBuilder builder(std::move(options), std::move(writable_file), output_expr_ctxs);

    ASSERT_OK(builder.finish());

    std::string content = read_file(file_path);

    // With empty column names, no header row should be written
    EXPECT_EQ(content, "");
}

// Test PlainTextBuilder with header and column names
TEST_F(PlainTextBuilderTest, TestHeaderWithColumnNames) {
    std::string file_path = _test_dir + "/with_header.csv";

    PlainTextBuilderOptions options{.column_terminated_by = ",",
                                    .line_terminated_by = "\n",
                                    .with_header = true,
                                    .column_names = {"id", "name", "score"}};

    ASSIGN_OR_ABORT(auto writable_file, FileSystem::Default()->new_writable_file(file_path));

    std::vector<ExprContext*> output_expr_ctxs;

    PlainTextBuilder builder(std::move(options), std::move(writable_file), output_expr_ctxs);

    ASSERT_OK(builder.finish());

    std::string content = read_file(file_path);

    // Header row should be written with column names
    EXPECT_EQ(content, "id,name,score\n");
}

// Test PlainTextBuilder with custom delimiters
TEST_F(PlainTextBuilderTest, TestHeaderWithCustomDelimiters) {
    std::string file_path = _test_dir + "/custom_delimiters.csv";

    PlainTextBuilderOptions options{.column_terminated_by = "\t",
                                    .line_terminated_by = "\r\n",
                                    .with_header = true,
                                    .column_names = {"col_a", "col_b"}};

    ASSIGN_OR_ABORT(auto writable_file, FileSystem::Default()->new_writable_file(file_path));

    std::vector<ExprContext*> output_expr_ctxs;

    PlainTextBuilder builder(std::move(options), std::move(writable_file), output_expr_ctxs);

    ASSERT_OK(builder.finish());

    std::string content = read_file(file_path);

    // Header row should use custom delimiters
    EXPECT_EQ(content, "col_a\tcol_b\r\n");
}

// Test PlainTextBuilder with single column header
TEST_F(PlainTextBuilderTest, TestHeaderWithSingleColumn) {
    std::string file_path = _test_dir + "/single_column.csv";

    PlainTextBuilderOptions options{.column_terminated_by = ",",
                                    .line_terminated_by = "\n",
                                    .with_header = true,
                                    .column_names = {"only_column"}};

    ASSIGN_OR_ABORT(auto writable_file, FileSystem::Default()->new_writable_file(file_path));

    std::vector<ExprContext*> output_expr_ctxs;

    PlainTextBuilder builder(std::move(options), std::move(writable_file), output_expr_ctxs);

    ASSERT_OK(builder.finish());

    std::string content = read_file(file_path);

    EXPECT_EQ(content, "only_column\n");
}

// Test PlainTextBuilder with many columns
TEST_F(PlainTextBuilderTest, TestHeaderWithManyColumns) {
    std::string file_path = _test_dir + "/many_columns.csv";

    std::vector<std::string> column_names;
    for (int i = 0; i < 100; i++) {
        column_names.push_back("col_" + std::to_string(i));
    }

    PlainTextBuilderOptions options{
            .column_terminated_by = ",", .line_terminated_by = "\n", .with_header = true, .column_names = column_names};

    ASSIGN_OR_ABORT(auto writable_file, FileSystem::Default()->new_writable_file(file_path));

    std::vector<ExprContext*> output_expr_ctxs;

    PlainTextBuilder builder(std::move(options), std::move(writable_file), output_expr_ctxs);

    ASSERT_OK(builder.finish());

    std::string content = read_file(file_path);

    // Build expected header
    std::string expected_header;
    for (int i = 0; i < 100; i++) {
        if (i > 0) expected_header += ",";
        expected_header += "col_" + std::to_string(i);
    }
    expected_header += "\n";

    EXPECT_EQ(content, expected_header);
}

// Test PlainTextBuilder with special characters in column names (space and @)
TEST_F(PlainTextBuilderTest, TestHeaderWithSpecialChars) {
    std::string file_path = _test_dir + "/special_chars.csv";

    PlainTextBuilderOptions options{.column_terminated_by = ",",
                                    .line_terminated_by = "\n",
                                    .with_header = true,
                                    .column_names = {"user_id", "user name", "user@email"}};

    ASSIGN_OR_ABORT(auto writable_file, FileSystem::Default()->new_writable_file(file_path));

    std::vector<ExprContext*> output_expr_ctxs;

    PlainTextBuilder builder(std::move(options), std::move(writable_file), output_expr_ctxs);

    ASSERT_OK(builder.finish());

    std::string content = read_file(file_path);

    EXPECT_EQ(content, "user_id,user name,user@email\n");
}

// Test PlainTextBuilder with column names containing comma (delimiter)
TEST_F(PlainTextBuilderTest, TestHeaderWithComma) {
    std::string file_path = _test_dir + "/comma_in_name.csv";

    PlainTextBuilderOptions options{.column_terminated_by = ",",
                                    .line_terminated_by = "\n",
                                    .with_header = true,
                                    .column_names = {"user,id", "name"}};

    ASSIGN_OR_ABORT(auto writable_file, FileSystem::Default()->new_writable_file(file_path));

    std::vector<ExprContext*> output_expr_ctxs;

    PlainTextBuilder builder(std::move(options), std::move(writable_file), output_expr_ctxs);

    ASSERT_OK(builder.finish());

    std::string content = read_file(file_path);

    // Column name with comma should be quoted
    EXPECT_EQ(content, "\"user,id\",name\n");
}

// Test PlainTextBuilder with column names containing double quotes
TEST_F(PlainTextBuilderTest, TestHeaderWithQuotes) {
    std::string file_path = _test_dir + "/quotes_in_name.csv";

    PlainTextBuilderOptions options{.column_terminated_by = ",",
                                    .line_terminated_by = "\n",
                                    .with_header = true,
                                    .column_names = {"user\"name", "id"}};

    ASSIGN_OR_ABORT(auto writable_file, FileSystem::Default()->new_writable_file(file_path));

    std::vector<ExprContext*> output_expr_ctxs;

    PlainTextBuilder builder(std::move(options), std::move(writable_file), output_expr_ctxs);

    ASSERT_OK(builder.finish());

    std::string content = read_file(file_path);

    // Column name with quotes should be quoted and internal quotes doubled
    EXPECT_EQ(content, "\"user\"\"name\",id\n");
}

// Test PlainTextBuilder with column names containing newline
TEST_F(PlainTextBuilderTest, TestHeaderWithNewline) {
    std::string file_path = _test_dir + "/newline_in_name.csv";

    PlainTextBuilderOptions options{.column_terminated_by = ",",
                                    .line_terminated_by = "\n",
                                    .with_header = true,
                                    .column_names = {"user\ninfo", "id"}};

    ASSIGN_OR_ABORT(auto writable_file, FileSystem::Default()->new_writable_file(file_path));

    std::vector<ExprContext*> output_expr_ctxs;

    PlainTextBuilder builder(std::move(options), std::move(writable_file), output_expr_ctxs);

    ASSERT_OK(builder.finish());

    std::string content = read_file(file_path);

    // Column name with newline should be quoted
    EXPECT_EQ(content, "\"user\ninfo\",id\n");
}

// Test PlainTextBuilder with column names containing carriage return
TEST_F(PlainTextBuilderTest, TestHeaderWithCarriageReturn) {
    std::string file_path = _test_dir + "/cr_in_name.csv";

    PlainTextBuilderOptions options{.column_terminated_by = ",",
                                    .line_terminated_by = "\n",
                                    .with_header = true,
                                    .column_names = {"user\rinfo", "id"}};

    ASSIGN_OR_ABORT(auto writable_file, FileSystem::Default()->new_writable_file(file_path));

    std::vector<ExprContext*> output_expr_ctxs;

    PlainTextBuilder builder(std::move(options), std::move(writable_file), output_expr_ctxs);

    ASSERT_OK(builder.finish());

    std::string content = read_file(file_path);

    // Column name with carriage return should be quoted
    EXPECT_EQ(content, "\"user\rinfo\",id\n");
}

// Test PlainTextBuilder with column names containing multiple special characters
TEST_F(PlainTextBuilderTest, TestHeaderWithMultipleSpecialChars) {
    std::string file_path = _test_dir + "/multiple_special.csv";

    PlainTextBuilderOptions options{.column_terminated_by = ",",
                                    .line_terminated_by = "\n",
                                    .with_header = true,
                                    .column_names = {"user,\"name\"\ninfo", "id"}};

    ASSIGN_OR_ABORT(auto writable_file, FileSystem::Default()->new_writable_file(file_path));

    std::vector<ExprContext*> output_expr_ctxs;

    PlainTextBuilder builder(std::move(options), std::move(writable_file), output_expr_ctxs);

    ASSERT_OK(builder.finish());

    std::string content = read_file(file_path);

    // Column name with comma, quotes, and newline should be properly escaped
    EXPECT_EQ(content, "\"user,\"\"name\"\"\ninfo\",id\n");
}

// Test PlainTextBuilder with custom delimiter in column name
TEST_F(PlainTextBuilderTest, TestHeaderWithCustomDelimiterInName) {
    std::string file_path = _test_dir + "/custom_delim.csv";

    PlainTextBuilderOptions options{.column_terminated_by = "\t",
                                    .line_terminated_by = "\n",
                                    .with_header = true,
                                    .column_names = {"user\tid", "name"}};

    ASSIGN_OR_ABORT(auto writable_file, FileSystem::Default()->new_writable_file(file_path));

    std::vector<ExprContext*> output_expr_ctxs;

    PlainTextBuilder builder(std::move(options), std::move(writable_file), output_expr_ctxs);

    ASSERT_OK(builder.finish());

    std::string content = read_file(file_path);

    // Column name with tab (delimiter) should be quoted
    EXPECT_EQ(content, "\"user\tid\"\tname\n");
}

// Test PlainTextBuilder header written only once
TEST_F(PlainTextBuilderTest, TestHeaderWrittenOnce) {
    std::string file_path = _test_dir + "/header_once.csv";

    PlainTextBuilderOptions options{.column_terminated_by = ",",
                                    .line_terminated_by = "\n",
                                    .with_header = true,
                                    .column_names = {"a", "b", "c"}};

    ASSIGN_OR_ABORT(auto writable_file, FileSystem::Default()->new_writable_file(file_path));

    std::vector<ExprContext*> output_expr_ctxs;

    PlainTextBuilder builder(std::move(options), std::move(writable_file), output_expr_ctxs);

    // Multiple calls to finish() should not write header multiple times
    // The init() is called lazily in add_chunk(), but we're calling finish() directly
    ASSERT_OK(builder.finish());

    std::string content = read_file(file_path);

    // Header should appear only once
    EXPECT_EQ(content, "a,b,c\n");
}

// Test file size includes header
TEST_F(PlainTextBuilderTest, TestFileSizeIncludesHeader) {
    std::string file_path = _test_dir + "/size_test.csv";

    PlainTextBuilderOptions options{
            .column_terminated_by = ",", .line_terminated_by = "\n", .with_header = true, .column_names = {"x", "y"}};

    ASSIGN_OR_ABORT(auto writable_file, FileSystem::Default()->new_writable_file(file_path));

    std::vector<ExprContext*> output_expr_ctxs;

    PlainTextBuilder builder(std::move(options), std::move(writable_file), output_expr_ctxs);

    ASSERT_OK(builder.finish());

    // After finish(), file_size() should reflect the header
    // Note: file_size() returns the size tracked by output stream
    std::string content = read_file(file_path);
    EXPECT_EQ(content.size(), 4); // "x,y\n" = 4 bytes
}

} // namespace starrocks
