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

#include "formats/csv/csv_reader.h"

#include <gtest/gtest.h>

#include "types/type_descriptor.h"

namespace starrocks::csv {

// Mock CSVReader for testing - implements the pure virtual function
class MockCSVReader : public starrocks::CSVReader {
public:
    explicit MockCSVReader(const starrocks::CSVParseOptions& parse_options) : CSVReader(parse_options) {}

protected:
    starrocks::Status _fill_buffer() override {
        // Mock implementation - not needed for split_record tests
        return starrocks::Status::OK();
    }

    char* _find_line_delimiter(starrocks::CSVBuffer& buffer, size_t pos) override {
        // Mock implementation - not needed for split_record tests
        return nullptr;
    }
};

// In-memory CSVReader used to drive the more_rows() state machine from a
// fixed string buffer. Mirrors the behavior of
// CSVScanner::ScannerCSVReader::_fill_buffer for the state-machine parser.
class StringCSVReader : public starrocks::CSVReader {
public:
    StringCSVReader(const starrocks::CSVParseOptions& parse_options, std::string data, size_t max_chunk = SIZE_MAX)
            : CSVReader(parse_options), _data(std::move(data)), _max_chunk(max_chunk) {}

    // Collects each row's fields as copied strings so tests don't have to
    // worry about the underlying buffer being reused.
    starrocks::Status read_all_rows(std::vector<std::vector<std::string>>* rows) {
        while (true) {
            starrocks::CSVRow row;
            auto st = next_record(row);
            if (st.is_end_of_file()) {
                return starrocks::Status::OK();
            }
            if (!st.ok()) {
                return st;
            }
            std::vector<std::string> fields;
            fields.reserve(row.columns.size());
            for (const auto& column : row.columns) {
                const char* base = column.is_escaped_column ? escapeDataPtr() : buffBasePtr();
                fields.emplace_back(base + column.start_pos, column.length);
            }
            rows->push_back(std::move(fields));
        }
    }

protected:
    starrocks::Status _fill_buffer() override {
        DCHECK(_buff.free_space() > 0);
        size_t free_space = _buff.free_space();
        size_t remaining = _data.size() - _pos;
        size_t to_copy = std::min({free_space, remaining, _max_chunk});
        if (to_copy > 0) {
            memcpy(_buff.limit(), _data.data() + _pos, to_copy);
            _buff.add_limit(to_copy);
            _pos += to_copy;
            return starrocks::Status::OK();
        }
        auto n = _buff.available();
        if (n < _parse_options.row_delimiter.size() ||
            _buff.find(_parse_options.row_delimiter, n - _parse_options.row_delimiter.size()) == nullptr) {
            if (_buff.free_space() < _parse_options.row_delimiter.size()) {
                return starrocks::Status::InternalError("row delimiter does not fit");
            }
            for (char ch : _parse_options.row_delimiter) {
                _buff.append(ch);
            }
        }
        if (n == 0) {
            _buff.skip(_parse_options.row_delimiter.size());
            return starrocks::Status::EndOfFile("string-csv-reader");
        }
        return starrocks::Status::OK();
    }

    char* _find_line_delimiter(starrocks::CSVBuffer& buffer, size_t pos) override {
        return buffer.find(_parse_options.row_delimiter, pos);
    }

private:
    std::string _data;
    size_t _pos = 0;
    size_t _max_chunk;
};

class CSVReaderTest : public ::testing::Test {
public:
    CSVReaderTest() = default;

protected:
    void SetUp() override {
        _parse_options.column_delimiter = ",";
        _parse_options.row_delimiter = "\n";
        _parse_options.trim_space = false;
    }

    starrocks::CSVParseOptions _parse_options;
};

// NOLINTNEXTLINE
TEST_F(CSVReaderTest, test_split_record_single_delimiter) {
    MockCSVReader reader(_parse_options);

    // Test basic splitting
    starrocks::CSVReader::Record record1{"a,b,c", 5};
    starrocks::CSVReader::Fields fields1;
    reader.split_record(record1, &fields1);

    EXPECT_EQ(3, fields1.size());
    EXPECT_EQ("a", fields1[0].to_string());
    EXPECT_EQ("b", fields1[1].to_string());
    EXPECT_EQ("c", fields1[2].to_string());
}

// NOLINTNEXTLINE
TEST_F(CSVReaderTest, test_split_record_empty_fields) {
    MockCSVReader reader(_parse_options);

    // Test empty fields
    starrocks::CSVReader::Record record1{",,", 2};
    starrocks::CSVReader::Fields fields1;
    reader.split_record(record1, &fields1);

    EXPECT_EQ(3, fields1.size());
    EXPECT_EQ("", fields1[0].to_string());
    EXPECT_EQ("", fields1[1].to_string());
    EXPECT_EQ("", fields1[2].to_string());
}

// NOLINTNEXTLINE
TEST_F(CSVReaderTest, test_split_record_ends_with_delimiter) {
    MockCSVReader reader(_parse_options);

    // Test string ending with delimiter
    starrocks::CSVReader::Record record1{"a,b,", 4};
    starrocks::CSVReader::Fields fields1;
    reader.split_record(record1, &fields1);

    EXPECT_EQ(3, fields1.size());
    EXPECT_EQ("a", fields1[0].to_string());
    EXPECT_EQ("b", fields1[1].to_string());
    EXPECT_EQ("", fields1[2].to_string());
}

// NOLINTNEXTLINE
TEST_F(CSVReaderTest, test_split_record_starts_with_delimiter) {
    MockCSVReader reader(_parse_options);

    // Test string starting with delimiter
    starrocks::CSVReader::Record record1{",a,b", 4};
    starrocks::CSVReader::Fields fields1;
    reader.split_record(record1, &fields1);

    EXPECT_EQ(3, fields1.size());
    EXPECT_EQ("", fields1[0].to_string());
    EXPECT_EQ("a", fields1[1].to_string());
    EXPECT_EQ("b", fields1[2].to_string());
}

// NOLINTNEXTLINE
TEST_F(CSVReaderTest, test_split_record_single_field) {
    MockCSVReader reader(_parse_options);

    // Test single field (no delimiters)
    starrocks::CSVReader::Record record1{"single_field", 12};
    starrocks::CSVReader::Fields fields1;
    reader.split_record(record1, &fields1);

    EXPECT_EQ(1, fields1.size());
    EXPECT_EQ("single_field", fields1[0].to_string());
}

// NOLINTNEXTLINE
TEST_F(CSVReaderTest, test_split_record_empty_string) {
    MockCSVReader reader(_parse_options);

    // Test empty string
    starrocks::CSVReader::Record record1{"", 0};
    starrocks::CSVReader::Fields fields1;
    reader.split_record(record1, &fields1);

    EXPECT_EQ(1, fields1.size());
    EXPECT_EQ("", fields1[0].to_string());
}

// NOLINTNEXTLINE
TEST_F(CSVReaderTest, test_split_record_multi_character_delimiter) {
    starrocks::CSVParseOptions options;
    options.column_delimiter = "||";
    options.row_delimiter = "\n";
    options.trim_space = false;

    MockCSVReader reader(options);

    // Test multi-character delimiter
    starrocks::CSVReader::Record record1{"a||b||c", 7};
    starrocks::CSVReader::Fields fields1;
    reader.split_record(record1, &fields1);

    EXPECT_EQ(3, fields1.size());
    EXPECT_EQ("a", fields1[0].to_string());
    EXPECT_EQ("b", fields1[1].to_string());
    EXPECT_EQ("c", fields1[2].to_string());
}

// NOLINTNEXTLINE
TEST_F(CSVReaderTest, test_split_record_with_trim_space) {
    starrocks::CSVParseOptions options;
    options.column_delimiter = ",";
    options.row_delimiter = "\n";
    options.trim_space = true;

    MockCSVReader reader(options);

    // Test with trim_space enabled
    starrocks::CSVReader::Record record1{" a , b , c ", 11};
    starrocks::CSVReader::Fields fields1;
    reader.split_record(record1, &fields1);

    EXPECT_EQ(3, fields1.size());
    EXPECT_EQ("a", fields1[0].to_string());
    EXPECT_EQ("b", fields1[1].to_string());
    EXPECT_EQ("c", fields1[2].to_string());
}

// NOLINTNEXTLINE
TEST_F(CSVReaderTest, test_split_record_large_data) {
    MockCSVReader reader(_parse_options);

    // Test with larger data to verify performance optimization
    std::string large_data;
    for (int i = 0; i < 1000; ++i) {
        if (i > 0) large_data += ",";
        large_data += "field" + std::to_string(i);
    }

    starrocks::CSVReader::Record record1{large_data.c_str(), large_data.size()};
    starrocks::CSVReader::Fields fields1;
    reader.split_record(record1, &fields1);

    EXPECT_EQ(1000, fields1.size());
    EXPECT_EQ("field0", fields1[0].to_string());
    EXPECT_EQ("field999", fields1[999].to_string());
}

// Regression test for issue #51725: when the input uses Windows-style CRLF
// line endings and the last column of each row is enclosed, the closing
// enclose byte and the preceding '\r' must both be stripped from the
// emitted value. The previous code dropped only the closing enclose, so
// the emitted value ended with "'\r".
TEST_F(CSVReaderTest, test_more_rows_enclose_crlf_last_field) {
    starrocks::CSVParseOptions options("\n", ",", 0, false, 0, '\'');
    std::string data = "a,'{\"x\":1}'\r\nb,'{\"y\":2}'\r\n";
    StringCSVReader reader(options, data);

    std::vector<std::vector<std::string>> rows;
    ASSERT_TRUE(reader.read_all_rows(&rows).ok());
    ASSERT_EQ(2u, rows.size());
    ASSERT_EQ(2u, rows[0].size());
    EXPECT_EQ("a", rows[0][0]);
    EXPECT_EQ("{\"x\":1}", rows[0][1]);
    ASSERT_EQ(2u, rows[1].size());
    EXPECT_EQ("b", rows[1][0]);
    EXPECT_EQ("{\"y\":2}", rows[1][1]);
}

// The CRLF fix targets the row-terminator transition out of ENCLOSE. An
// enclosed non-final column is followed directly by a column delimiter
// (not '\r'), so its behavior must remain unchanged.
TEST_F(CSVReaderTest, test_more_rows_enclose_crlf_non_final_field) {
    starrocks::CSVParseOptions options("\n", ",", 0, false, 0, '\'');
    std::string data = "'v1','v2','v3'\r\n";
    StringCSVReader reader(options, data);

    std::vector<std::vector<std::string>> rows;
    ASSERT_TRUE(reader.read_all_rows(&rows).ok());
    ASSERT_EQ(1u, rows.size());
    ASSERT_EQ(3u, rows[0].size());
    EXPECT_EQ("v1", rows[0][0]);
    EXPECT_EQ("v2", rows[0][1]);
    EXPECT_EQ("v3", rows[0][2]);
}

// Regression guard: LF line endings with enclose must still parse
// correctly (the pre-existing, working case).
TEST_F(CSVReaderTest, test_more_rows_enclose_lf_last_field) {
    starrocks::CSVParseOptions options("\n", ",", 0, false, 0, '\'');
    std::string data = "a,'{\"x\":1}'\nb,'{\"y\":2}'\n";
    StringCSVReader reader(options, data);

    std::vector<std::vector<std::string>> rows;
    ASSERT_TRUE(reader.read_all_rows(&rows).ok());
    ASSERT_EQ(2u, rows.size());
    EXPECT_EQ("a", rows[0][0]);
    EXPECT_EQ("{\"x\":1}", rows[0][1]);
    EXPECT_EQ("b", rows[1][0]);
    EXPECT_EQ("{\"y\":2}", rows[1][1]);
}

// Regression guard: without an enclose char the state-machine parser is
// not used, but the legacy next_record path is. This test documents the
// current behavior (trailing '\r' stays in the last column) so future
// changes to that code path trip this assertion deliberately.
TEST_F(CSVReaderTest, test_more_rows_no_enclose_crlf_preserved) {
    starrocks::CSVParseOptions options("\n", ",", 0, false, 0, 0);
    std::string data = "a,b\r\nc,d\r\n";
    StringCSVReader reader(options, data);

    starrocks::CSVReader::Record record;
    ASSERT_TRUE(reader.next_record(&record).ok());
    starrocks::CSVReader::Fields fields;
    reader.split_record(record, &fields);
    ASSERT_EQ(2u, fields.size());
    EXPECT_EQ("a", fields[0].to_string());
    // The legacy fast path leaves '\r' attached to the last column; this
    // is a separate, pre-existing behavior that the enclose CRLF fix does
    // not touch.
    EXPECT_EQ("b\r", fields[1].to_string());
}

// Regression guard: the double-enclose escape form ('') inside a quoted
// value must still work after the CRLF fix.
TEST_F(CSVReaderTest, test_more_rows_enclose_crlf_with_escaped_enclose) {
    starrocks::CSVParseOptions options("\n", ",", 0, false, 0, '\'');
    std::string data = "a,'it''s fine'\r\n";
    StringCSVReader reader(options, data);

    std::vector<std::vector<std::string>> rows;
    ASSERT_TRUE(reader.read_all_rows(&rows).ok());
    ASSERT_EQ(1u, rows.size());
    ASSERT_EQ(2u, rows[0].size());
    EXPECT_EQ("a", rows[0][0]);
    EXPECT_EQ("it's fine", rows[0][1]);
}

// Exercises the readMore() path inside the CRLF fix: feeding data one
// byte at a time forces the buffer to hold only '\r' when the closing
// enclose is consumed, so _buff.available() < 2 triggers readMore to
// pull in the '\n'. Covers the buffer-boundary scenario flagged in
// code review.
TEST_F(CSVReaderTest, test_more_rows_enclose_crlf_buffer_boundary) {
    starrocks::CSVParseOptions options("\n", ",", 0, false, 0, '\'');
    std::string data = "a,'{\"x\":1}'\r\nb,'{\"y\":2}'\r\n";
    StringCSVReader reader(options, data, 1);

    std::vector<std::vector<std::string>> rows;
    ASSERT_TRUE(reader.read_all_rows(&rows).ok());
    ASSERT_EQ(2u, rows.size());
    ASSERT_EQ(2u, rows[0].size());
    EXPECT_EQ("a", rows[0][0]);
    EXPECT_EQ("{\"x\":1}", rows[0][1]);
    ASSERT_EQ(2u, rows[1].size());
    EXPECT_EQ("b", rows[1][0]);
    EXPECT_EQ("{\"y\":2}", rows[1][1]);
}

} // namespace starrocks::csv
