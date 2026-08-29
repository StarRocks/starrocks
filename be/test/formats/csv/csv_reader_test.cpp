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

#include <algorithm>
#include <cstring>

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
    StringCSVReader(const starrocks::CSVParseOptions& parse_options, std::string data, size_t max_chunk = SIZE_MAX,
                    size_t buffer_size = 128 * 1024)
            : CSVReader(parse_options, buffer_size), _data(std::move(data)), _max_chunk(max_chunk) {}

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

// The multi-character delimiter path splits records in its own loop, so trim_space has to be
// covered there as well and not only for a single-character delimiter.
// NOLINTNEXTLINE
TEST_F(CSVReaderTest, test_split_record_multi_char_delimiter_with_trim_space) {
    starrocks::CSVParseOptions options;
    options.column_delimiter = "||";
    options.row_delimiter = "\n";
    options.trim_space = true;

    MockCSVReader reader(options);

    starrocks::CSVReader::Record record1{" a || b || c ", 13};
    starrocks::CSVReader::Fields fields1;
    reader.split_record(record1, &fields1);

    EXPECT_EQ(3, fields1.size());
    EXPECT_EQ("a", fields1[0].to_string());
    EXPECT_EQ("b", fields1[1].to_string());
    EXPECT_EQ("c", fields1[2].to_string());

    // Empty leading and trailing fields, which reach trim with a zero length -- the old helper
    // computed `end = len - 1` and underflowed here.
    starrocks::CSVReader::Record record2{"||x||", 5};
    starrocks::CSVReader::Fields fields2;
    reader.split_record(record2, &fields2);

    EXPECT_EQ(3, fields2.size());
    EXPECT_EQ("", fields2[0].to_string());
    EXPECT_EQ("x", fields2[1].to_string());
    EXPECT_EQ("", fields2[2].to_string());

    // Fields that hold nothing but spaces must be trimmed down to empty ones.
    starrocks::CSVReader::Record record3{" || x || ", 9};
    starrocks::CSVReader::Fields fields3;
    reader.split_record(record3, &fields3);

    EXPECT_EQ(3, fields3.size());
    EXPECT_EQ("", fields3[0].to_string());
    EXPECT_EQ("x", fields3[1].to_string());
    EXPECT_EQ("", fields3[2].to_string());
}

// Regression test for the multi-char column-delimiter buffer-expansion use-after-free:
// is_column_delimiter() caches base_ptr = _buff.base_ptr(), then mid-match calls readMore(),
// which can expand the buffer via _storage.resize() (reallocation frees the old storage). The
// subsequent read *(base_ptr + p) then dereferences the freed old buffer. Feed a field that fills
// the buffer so a two-char column delimiter straddles the boundary, forcing expansion during the
// match. (is_row_delimiter has the same pattern and the same fix.)
TEST_F(CSVReaderTest, test_multichar_delimiter_expand_boundary_uaf) {
    starrocks::CSVParseOptions options("\n", "||", 0, false, 0, 0);
    const size_t buf = 16;
    std::string data(buf - 1, 'a'); // first '|' lands at the last buffer byte -> 2nd '|' forces expand
    data += "||b\n";
    StringCSVReader reader(options, data, SIZE_MAX, buf);

    std::vector<std::vector<std::string>> rows;
    auto st = reader.read_all_rows(&rows);
    ASSERT_TRUE(st.ok()) << st.to_string();
    ASSERT_EQ(1u, rows.size());
    ASSERT_EQ(2u, rows[0].size());
    EXPECT_EQ(std::string(buf - 1, 'a'), rows[0][0]);
    EXPECT_EQ("b", rows[0][1]);
}

} // namespace starrocks::csv
