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

#include "formats/csv/csv_record_framer.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <cstring>
#include <random>
#include <string>
#include <vector>

#include "formats/csv/csv_reader.h"

namespace starrocks {

// Drives the production parser over an in-memory buffer, mirroring the way ScannerCSVReader tops
// the buffer up and supplies the row delimiter a file may lack at its end.
//
// Record starts are recovered by accumulating record lengths rather than by reading parsed_start
// directly, because parsed_start is an offset into a buffer that more_rows() compacts as it goes.
// Lengths are contiguous only while no record is blank, so corpora used for parity never contain a
// blank line; blank lines are covered by the explicit cases below.
class ParityCSVReader : public CSVReader {
public:
    ParityCSVReader(const CSVParseOptions& options, std::string data) : CSVReader(options), _data(std::move(data)) {}

    // Where each record the parser emits begins, as an offset into the input.
    //
    // parsed_start is an offset into the buffer, which is the same thing only while more_rows() has
    // not compacted it. That holds whenever the whole input fits in one bufferful, which is the
    // case for every corpus here except the refill one - that one uses boundaries() below instead.
    std::vector<int64_t> record_starts() {
        std::vector<int64_t> out(1, 0);
        while (true) {
            CSVRow row;
            if (!next_record(row).ok()) {
                break;
            }
            const int64_t start = static_cast<int64_t>(row.parsed_start);
            if (start > out.back() && start < static_cast<int64_t>(_data.size())) {
                out.push_back(start);
            }
        }
        return out;
    }

    // Record starts recovered by accumulating record lengths, for corpora too large to hold in one
    // bufferful, where parsed_start is no longer a file offset.
    //
    // Returns false when the lengths do not add up to the whole input. That happens when the parser
    // skipped a blank record rather than emitting one, after which the accumulated positions have
    // drifted by the skipped bytes and are no longer offsets into anything. Do not use this on a
    // corpus that can contain blank records - the drift is silent, and a run of it can still add up
    // by accident when the row delimiter invented at end of file replaces the missing bytes.
    bool boundaries(std::vector<int64_t>* out) {
        out->assign(1, 0);
        int64_t at = 0;
        while (true) {
            CSVRow row;
            Status st = next_record(row);
            if (!st.ok()) {
                break;
            }
            at += static_cast<int64_t>(row.parsed_end - row.parsed_start);
            if (at >= static_cast<int64_t>(_data.size())) {
                return at == static_cast<int64_t>(_data.size());
            }
            out->push_back(at);
        }
        return false;
    }

protected:
    Status _fill_buffer() override {
        if (_consumed < _data.size()) {
            const size_t n = std::min(_buff.free_space(), _data.size() - _consumed);
            std::memcpy(_buff.limit(), _data.data() + _consumed, n);
            _buff.add_limit(n);
            _consumed += n;
            return Status::OK();
        }
        const size_t avail = _buff.available();
        if (avail < _row_delimiter_length ||
            _buff.find(_parse_options.row_delimiter, avail - _row_delimiter_length) == nullptr) {
            if (_buff.free_space() < _row_delimiter_length) {
                return Status::InternalError("csv line length exceeds the test buffer");
            }
            // One invented delimiter is all it takes to close the last record. A second means the
            // parser is not consuming them - an enclosed field the file never closes swallows each
            // one as content - and a real reader would go on inventing until the buffer filled and
            // then fail. Reporting the end of the file instead keeps the records already parsed:
            // next_record() returns an error in place of them, and the sample would look as though
            // it had no records at all. What is being compared here is where records begin, not how
            // the reader behaves once it runs out of room.
            if (_invented_delimiter) {
                return Status::EndOfFile("eof");
            }
            _invented_delimiter = true;
            for (char ch : _parse_options.row_delimiter) {
                _buff.append(ch);
            }
        }
        if (avail == 0) {
            _buff.skip(_row_delimiter_length);
            return Status::EndOfFile("eof");
        }
        return Status::OK();
    }

    char* _find_line_delimiter(CSVBuffer& buffer, size_t pos) override {
        return buffer.find(_parse_options.row_delimiter, pos);
    }

private:
    std::string _data;
    bool _invented_delimiter = false;
    size_t _consumed = 0;
};

static std::vector<int64_t> frame(const CSVParseOptions& options, const std::string& data, int64_t min_split_size = 0,
                                  size_t chunk_size = 0) {
    CSVRecordFramer framer(options, min_split_size);
    const size_t step = chunk_size == 0 ? data.size() : chunk_size;
    for (size_t at = 0; at < data.size(); at += step) {
        framer.feed(data.data() + at, std::min(step, data.size() - at));
    }
    framer.finish();
    return framer.split_offsets();
}

static CSVParseOptions options(const std::string& row_delimiter = "\n", const std::string& column_delimiter = ",",
                               char escape = 0, char enclose = '"', bool trim_space = false) {
    return CSVParseOptions(row_delimiter, column_delimiter, 0, trim_space, escape, enclose);
}

// NOLINTNEXTLINE
TEST(CSVRecordFramerTest, test_plain_rows) {
    EXPECT_EQ(std::vector<int64_t>({0, 4, 8}), frame(options("\n", ",", 0, 0), "a,b\nc,d\ne,f\n"));
}

// NOLINTNEXTLINE
TEST(CSVRecordFramerTest, test_row_delimiter_inside_enclosed_field_is_not_a_boundary) {
    EXPECT_EQ(std::vector<int64_t>({0, 10}), frame(options(), "a,\"x\ny\",b\nc,d,e\n"));
}

// NOLINTNEXTLINE
TEST(CSVRecordFramerTest, test_doubled_enclose_stays_inside_the_field) {
    EXPECT_EQ(std::vector<int64_t>({0, 21}), frame(options(), "a,\"he said \"\"hi\"\"\",b\nc,d,e\n"));
}

// NOLINTNEXTLINE
TEST(CSVRecordFramerTest, test_multi_character_row_delimiter) {
    EXPECT_EQ(std::vector<int64_t>({0, 10}), frame(options("\r\n"), "a,\"x\r\ny\"\r\nb,c\r\n"));
}

// NOLINTNEXTLINE
// A blank line is not a record: more_rows() consumes it and carries the record start past it
// without ever producing one, so the record after two blank lines begins at 6 and nothing begins
// at 4 or 5. Reporting those would offer split points the parser does not read as record starts.
TEST(CSVRecordFramerTest, test_blank_lines_do_not_start_records) {
    EXPECT_EQ(std::vector<int64_t>({0, 6}), frame(options(), "a,b\n\n\nc,d\n"));
}

// NOLINTNEXTLINE
TEST(CSVRecordFramerTest, test_no_boundary_is_reported_at_end_of_file) {
    // The trailing delimiter closes the last record; nothing starts at offset 12.
    const std::vector<int64_t> got = frame(options("\n", ",", 0, 0), "a,b\nc,d\ne,f\n");
    EXPECT_EQ(3u, got.size());
    EXPECT_LT(got.back(), 12);
}

// NOLINTNEXTLINE
TEST(CSVRecordFramerTest, test_chunked_feeding_matches_whole_buffer_feeding) {
    const std::string data = "a,\"x\r\ny\"\r\nb,c\r\nd,\"e\r\nf\"\r\n";
    const std::vector<int64_t> expected = frame(options("\r\n"), data);
    for (size_t chunk = 1; chunk <= data.size(); chunk++) {
        EXPECT_EQ(expected, frame(options("\r\n"), data, 0, chunk)) << "chunk size " << chunk;
    }
}

// NOLINTNEXTLINE
TEST(CSVRecordFramerTest, test_min_split_size_thins_boundaries) {
    const std::string data = "a,b\nc,d\ne,f\ng,h\ni,j\nk,l\n";
    EXPECT_EQ(std::vector<int64_t>({0, 4, 8, 12, 16, 20}), frame(options(), data));
    EXPECT_EQ(std::vector<int64_t>({0, 8, 16}), frame(options(), data, 8));
}

// NOLINTNEXTLINE
TEST(CSVRecordFramerTest, test_escaped_character_inside_enclosed_field_stays_in_the_field) {
    // The escape before 'y' is taken literally and leaves the field open, so the row delimiter
    // inside it is data and the first record runs to the one after 'b'.
    EXPECT_EQ(std::vector<int64_t>({0, 13}), frame(options("\n", ",", '\\'), "a,\"x\\yz\nq\",b\nc,d,e\n"));
}

// NOLINTNEXTLINE
TEST(CSVRecordFramerTest, test_lone_enclose_outside_a_field_leaves_the_delimiter_alone) {
    // The quote at offset 2 opens nothing and is not doubled, so it is dropped from the column but
    // the row delimiter behind it still ends the record.
    EXPECT_EQ(std::vector<int64_t>({0, 4, 7}), frame(options(), "ab\"\ncd\nef\n"));
}

// NOLINTNEXTLINE
TEST(CSVRecordFramerTest, test_issue_65245_split_point_inside_a_multiline_field) {
    const std::string data =
            "id,name,note\n"
            "1,\"FIA CARD SERVICES\nNATIONAL ASSOCIATION_501330\",ok\n"
            "2,plain,fine\n";
    const std::vector<int64_t> boundaries = frame(options(), data);
    EXPECT_EQ(std::vector<int64_t>({0, 13, 66}), boundaries);

    // Seeking to the next raw row delimiter - what a range with start_offset > 0 does today -
    // lands at offset 34, inside the enclosed field, which is what splits the record and produces
    // the "Target column count: 4 doesn't match source value column count: 1" rejection.
    EXPECT_EQ(34u, data.find('\n', 30) + 1);
    EXPECT_EQ(66, *std::lower_bound(boundaries.begin(), boundaries.end(), 30));
}

// NOLINTNEXTLINE
TEST(CSVRecordFramerTest, test_agrees_with_the_parser_on_a_generated_corpus) {
    const std::vector<CSVParseOptions> dialects = {
            options("\n", ",", 0, '"'),   options("\n", ",", '\\', '"'),  options("\n", ",", '\\', 0),
            options("\r\n", ",", 0, '"'), options("||", "::", '\\', '"'),
    };
    const std::string alphabet = "ab,|:\r\n\"\\ ";

    std::mt19937 rng(65245);
    int compared = 0;
    for (const CSVParseOptions& dialect : dialects) {
        for (int iter = 0; iter < 400; iter++) {
            std::string data;
            const int rows = 1 + static_cast<int>(rng() % 12);
            for (int r = 0; r < rows; r++) {
                const int len = 1 + static_cast<int>(rng() % 24);
                for (int i = 0; i < len; i++) {
                    data += alphabet[rng() % alphabet.size()];
                }
                data += dialect.row_delimiter;
            }

            ParityCSVReader reader(dialect, data);
            compared++;
            EXPECT_EQ(reader.record_starts(), frame(dialect, data)) << "data: " << data;
        }
    }
    EXPECT_GT(compared, 200) << "corpus degenerated; the comparison is no longer meaningful";
}

// NOLINTNEXTLINE
TEST(CSVRecordFramerTest, test_agrees_with_the_parser_across_buffer_refills) {
    // Every case above fits in one bufferful, so none of them reach the parser's refill path:
    // buffInit() compacting the buffer, _fill_buffer() topping it up, and the reachBuffEnd rewind
    // that re-parses a record straddling a refill. CSVReader's buffer is 128KB under BE_TEST, so a
    // few hundred KB of input forces all three, with enclosed newlines spanning the seams.
    const CSVParseOptions dialect = options();

    std::mt19937 rng(4290);
    std::string data;
    while (data.size() < 400 * 1024) {
        switch (rng() % 4) {
        case 0:
            data += "plain,row,here\n";
            break;
        case 1:
            data += "a,\"quoted\nwith\nnewlines\",z\n";
            break;
        case 2:
            data += "b,\"doubled \"\"quotes\"\" inside\",y\n";
            break;
        default: {
            data += "c,\"";
            const size_t len = 40 + rng() % 400;
            for (size_t i = 0; i < len; i++) {
                data += (rng() % 16 == 0) ? '\n' : 'x';
            }
            data += "\",w\n";
            break;
        }
        }
    }

    ParityCSVReader reader(dialect, data);
    std::vector<int64_t> expected;
    ASSERT_TRUE(reader.boundaries(&expected));
    EXPECT_GT(expected.size(), 1000u);
    EXPECT_EQ(expected, frame(dialect, data));

    // Chunk sizes that line up with neither the parser's buffer nor each other, so quote state has
    // to survive both sets of seams.
    EXPECT_EQ(expected, frame(dialect, data, 0, 7919));
    EXPECT_EQ(expected, frame(dialect, data, 0, 131072));
}

} // namespace starrocks
