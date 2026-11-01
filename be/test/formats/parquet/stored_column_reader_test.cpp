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

#include "formats/parquet/stored_column_reader.h"

#include <gtest/gtest.h>
#include <thrift/protocol/TCompactProtocol.h>
#include <thrift/transport/TBufferTransports.h>

#include <optional>
#include <string>
#include <vector>

#include "base/bit/rle_encoding.h"
#include "base/coding.h"
#include "base/string/faststring.h"
#include "base/testutil/assert.h"
#include "column/column_helper.h"
#include "column/fixed_length_column.h"
#include "formats/parquet/column_reader.h"
#include "formats/parquet/encoding_dict.h"
#include "formats/parquet/encoding_plain.h"
#include "formats/parquet/metadata.h"
#include "formats/parquet/parquet_block_split_bloom_filter.h"
#include "formats/parquet/schema.h"
#include "formats/scan_context.h"
#include "fs/fs.h"
#include "io/string_input_stream.h"
#include "runtime/current_thread.h"
#include "runtime/mem_tracker.h"
#include "types/type_descriptor.h"

namespace starrocks::parquet {
namespace {

using Values = std::vector<std::optional<int32_t>>;

template <typename T>
std::string compact_bytes(const T& value) {
    auto buffer = std::make_shared<apache::thrift::transport::TMemoryBuffer>();
    apache::thrift::protocol::TCompactProtocol protocol(buffer);
    value.write(&protocol);
    return buffer->getBufferAsString();
}

// RLE levels of a v1 data page: 4 bytes length + RLE/bit-packed hybrid data.
std::string encode_levels(const std::vector<level_t>& levels, level_t max_level) {
    int bit_width = 0;
    while ((1 << bit_width) <= max_level) {
        ++bit_width;
    }
    faststring buf;
    RleEncoder<int32_t> encoder(&buf, bit_width);
    for (auto level : levels) {
        encoder.Put(level);
    }
    encoder.Flush();
    std::string out;
    put_fixed32_le(&out, buf.size());
    out.append(reinterpret_cast<const char*>(buf.data()), buf.size());
    return out;
}

// One column chunk with a single PLAIN INT32 data page.
struct SinglePageColumn {
    std::vector<tparquet::SchemaElement> schema;
    std::vector<level_t> rep_levels;
    std::vector<level_t> def_levels;
    std::vector<int32_t> values; // non-null values only
    size_t num_levels = 0;

    std::string file_bytes;
    tparquet::ColumnChunk chunk;
    FileMetaData file_meta;

    void build(level_t max_def_level, level_t max_rep_level) {
        std::string payload;
        if (max_rep_level > 0) payload += encode_levels(rep_levels, max_rep_level);
        if (max_def_level > 0) payload += encode_levels(def_levels, max_def_level);
        for (int32_t v : values) put_fixed32_le(&payload, static_cast<uint32_t>(v));

        tparquet::DataPageHeader data;
        data.__set_num_values(num_levels);
        data.__set_encoding(tparquet::Encoding::PLAIN);
        data.__set_definition_level_encoding(tparquet::Encoding::RLE);
        data.__set_repetition_level_encoding(tparquet::Encoding::RLE);
        tparquet::PageHeader page;
        page.__set_type(tparquet::PageType::DATA_PAGE);
        page.__set_uncompressed_page_size(payload.size());
        page.__set_compressed_page_size(payload.size());
        page.__set_data_page_header(data);
        std::string page_bytes = compact_bytes(page) + payload;

        file_bytes = "PAR1";
        tparquet::ColumnMetaData metadata;
        metadata.__set_type(tparquet::Type::INT32);
        metadata.__set_encodings({tparquet::Encoding::PLAIN, tparquet::Encoding::RLE});
        metadata.__set_codec(tparquet::CompressionCodec::UNCOMPRESSED);
        metadata.__set_num_values(num_levels);
        metadata.__set_total_uncompressed_size(page_bytes.size());
        metadata.__set_total_compressed_size(page_bytes.size());
        metadata.__set_data_page_offset(file_bytes.size());
        chunk.__set_file_offset(file_bytes.size());
        chunk.__set_meta_data(metadata);
        file_bytes += page_bytes;

        tparquet::FileMetaData t_meta;
        t_meta.__set_schema(schema);
        ASSERT_OK(file_meta.init(t_meta, true));
    }
};

tparquet::SchemaElement root_element(int num_children) {
    tparquet::SchemaElement root;
    root.__set_name("root");
    root.__set_num_children(num_children);
    return root;
}

tparquet::SchemaElement int_element(const std::string& name, tparquet::FieldRepetitionType::type repetition) {
    tparquet::SchemaElement element;
    element.__set_name(name);
    element.__set_type(tparquet::Type::INT32);
    element.__set_repetition_type(repetition);
    return element;
}

// Row i of the flat columns: null if (nullable && i % 3 == 0), else i * 10.
std::optional<int32_t> flat_value(size_t i, bool nullable) {
    if (nullable && i % 3 == 0) return std::nullopt;
    return static_cast<int32_t>(i * 10);
}

// Row i of the list column: null list if i % 4 == 0, else (i % 3) elements; element j is null if j == 1,
// else i * 10 + j.
std::vector<std::optional<int32_t>> list_row(size_t i) {
    std::vector<std::optional<int32_t>> row;
    if (i % 4 == 0) return row;
    for (size_t j = 0; j < i % 3; ++j) {
        row.emplace_back(j == 1 ? std::nullopt : std::optional<int32_t>(i * 10 + j));
    }
    return row;
}

// Rep / def levels of row i of the list column (schema of RepeatedUnselectedSegmentInsideLoadedPage).
void append_list_row_levels(size_t i, std::vector<level_t>* rep_levels, std::vector<level_t>* def_levels) {
    auto row = list_row(i);
    if (row.empty()) {
        rep_levels->push_back(0);
        def_levels->push_back(i % 4 == 0 ? 0 : 1);
        return;
    }
    for (size_t j = 0; j < row.size(); ++j) {
        rep_levels->push_back(j == 0 ? 0 : 1);
        def_levels->push_back(row[j].has_value() ? 3 : 2);
    }
}

Values to_values(const Column& column) {
    Values out;
    for (size_t i = 0; i < column.size(); ++i) {
        Datum d = column.get(i);
        if (d.is_null()) {
            out.emplace_back(std::nullopt);
        } else {
            out.emplace_back(d.get_int32());
        }
    }
    return out;
}

class StoredColumnReaderTest : public ::testing::Test {
protected:
    void SetUp() override {
        CurrentThread::set_mem_tracker_source([] { return true; }, []() -> MemTracker* { return nullptr; });
    }
    void TearDown() override { CurrentThread::set_mem_tracker_source(nullptr, nullptr); }

    std::unique_ptr<StoredColumnReader> create_reader(SinglePageColumn* column, const ParquetField* field) {
        _file = std::make_unique<RandomAccessFile>(std::make_shared<io::StringInputStream>(column->file_bytes),
                                                   "stored_column_reader_test.parquet");
        _opts.stats = &_stats;
        _opts.file = _file.get();
        _opts.chunk_size = 4096;
        _opts.file_meta_data = &column->file_meta;
        std::unique_ptr<StoredColumnReader> reader;
        auto st = StoredColumnReader::create(_opts, field, &column->chunk, &reader);
        EXPECT_TRUE(st.ok()) << st.to_string();
        return reader;
    }

    MemTracker _tracker{-1, "stored_column_reader_test"};
    FormatScannerStats _stats;
    ColumnReaderOptions _opts{};
    std::unique_ptr<RandomAccessFile> _file;
};

// Read pattern shared by all cases (20 rows in one page):
//   [0, 5)   selected, loads the page
//   [5, 10)  all-zero filter: passes over part of the loaded page
//   [10, 15) selected in the same page: must start at row 10
//   [17, 20) selected after a skip
// and the variant where the page is first entered by an unselected read.
struct ReadStep {
    size_t begin;
    size_t end;
    bool selected;
};
const std::vector<ReadStep> kStepsLoadedFirst = {{0, 5, true}, {5, 10, false}, {10, 15, true}, {17, 20, true}};
const std::vector<ReadStep> kStepsUnselectedFirst = {{0, 5, false}, {5, 10, true}, {10, 12, false}, {12, 20, true}};
// Several selected / unselected alternations inside the same page, ending with an unselected tail.
const std::vector<ReadStep> kStepsAlternating = {{0, 3, true},   {3, 6, false},   {6, 7, true},   {7, 11, false},
                                                 {11, 14, true}, {14, 15, false}, {15, 17, true}, {17, 20, false}};

void build_flat_column(SinglePageColumn* column, bool nullable) {
    auto repetition = nullable ? tparquet::FieldRepetitionType::OPTIONAL : tparquet::FieldRepetitionType::REQUIRED;
    column->schema = {root_element(1), int_element("c", repetition)};
    column->num_levels = 20;
    for (size_t i = 0; i < 20; ++i) {
        auto v = flat_value(i, nullable);
        if (nullable) column->def_levels.push_back(v.has_value() ? 1 : 0);
        if (v.has_value()) column->values.push_back(*v);
    }
    column->build(nullable ? 1 : 0, 0);
}

TEST_F(StoredColumnReaderTest, RequiredUnselectedSegmentInsideLoadedPage) {
    SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(&_tracker);
    for (const auto* steps : {&kStepsLoadedFirst, &kStepsUnselectedFirst, &kStepsAlternating}) {
        SinglePageColumn column;
        build_flat_column(&column, false);
        const auto* field = column.file_meta.schema().get_stored_column_by_field_idx(0);
        auto reader = create_reader(&column, field);
        ASSERT_NE(nullptr, reader);
        for (const auto& step : *steps) {
            auto dst = Int32Column::create();
            Filter filter(step.end - step.begin, step.selected ? 1 : 0);
            ASSERT_OK(reader->read_range(Range<uint64_t>(step.begin, step.end), &filter, ColumnContentType::VALUE,
                                         dst.get()));
            Values expected;
            for (size_t i = step.begin; i < step.end; ++i) {
                expected.emplace_back(step.selected ? flat_value(i, false) : std::optional<int32_t>(0));
            }
            EXPECT_EQ(expected, to_values(*dst)) << "range [" << step.begin << ", " << step.end << ")";
        }
    }
}

TEST_F(StoredColumnReaderTest, OptionalUnselectedSegmentInsideLoadedPage) {
    SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(&_tracker);
    for (bool need_levels : {false, true}) {
        for (const auto* steps : {&kStepsLoadedFirst, &kStepsUnselectedFirst, &kStepsAlternating}) {
            SinglePageColumn column;
            build_flat_column(&column, true);
            const auto* field = column.file_meta.schema().get_stored_column_by_field_idx(0);
            auto reader = create_reader(&column, field);
            ASSERT_NE(nullptr, reader);
            reader->set_need_parse_levels(need_levels);
            for (const auto& step : *steps) {
                auto dst = ColumnHelper::create_column(TYPE_INT_DESC, true);
                Filter filter(step.end - step.begin, step.selected ? 1 : 0);
                ASSERT_OK(reader->read_range(Range<uint64_t>(step.begin, step.end), &filter, ColumnContentType::VALUE,
                                             dst.get()));
                Values expected;
                std::vector<level_t> expected_levels;
                for (size_t i = step.begin; i < step.end; ++i) {
                    auto v = step.selected ? flat_value(i, true) : std::nullopt;
                    expected.emplace_back(v);
                    expected_levels.push_back(v.has_value() ? 1 : 0);
                }
                EXPECT_EQ(expected, to_values(*dst))
                        << "need_levels=" << need_levels << " range [" << step.begin << ", " << step.end << ")";
                if (need_levels) {
                    level_t* def_levels = nullptr;
                    level_t* rep_levels = nullptr;
                    size_t num_levels = 0;
                    reader->get_levels(&def_levels, &rep_levels, &num_levels);
                    ASSERT_EQ(expected_levels.size(), num_levels);
                    EXPECT_EQ(expected_levels, std::vector<level_t>(def_levels, def_levels + num_levels));
                }
            }
        }
    }
}

// optional group l (LIST) { repeated group list { optional int32 element; } }: max_def 3, max_rep 1.
// Unselected segments of a repeated column only decode levels (to delimit rows); the values they pass over are
// skipped lazily by the next selected read in the page. Values and levels of every read must stay aligned.
TEST_F(StoredColumnReaderTest, RepeatedUnselectedSegmentInsideLoadedPage) {
    SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(&_tracker);
    for (const auto* steps : {&kStepsLoadedFirst, &kStepsUnselectedFirst, &kStepsAlternating}) {
        SinglePageColumn column;
        tparquet::SchemaElement list;
        list.__set_name("l");
        list.__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);
        list.__set_num_children(1);
        list.__set_converted_type(tparquet::ConvertedType::LIST);
        tparquet::SchemaElement repeated;
        repeated.__set_name("list");
        repeated.__set_repetition_type(tparquet::FieldRepetitionType::REPEATED);
        repeated.__set_num_children(1);
        column.schema = {root_element(1), list, repeated,
                         int_element("element", tparquet::FieldRepetitionType::OPTIONAL)};
        for (size_t i = 0; i < 20; ++i) {
            append_list_row_levels(i, &column.rep_levels, &column.def_levels);
            for (const auto& v : list_row(i)) {
                if (v.has_value()) column.values.push_back(*v);
            }
        }
        column.num_levels = column.def_levels.size();
        column.build(3, 1);

        const auto* list_field = column.file_meta.schema().get_stored_column_by_field_idx(0);
        ASSERT_EQ(1, list_field->children.size());
        const auto* field = &list_field->children[0];
        ASSERT_EQ(3, field->max_def_level());
        ASSERT_EQ(1, field->max_rep_level());
        auto reader = create_reader(&column, field);
        ASSERT_NE(nullptr, reader);
        reader->set_need_parse_levels(true);
        for (const auto& step : *steps) {
            auto dst = ColumnHelper::create_column(TYPE_INT_DESC, true);
            Filter filter(step.end - step.begin, step.selected ? 1 : 0);
            ASSERT_OK(reader->read_range(Range<uint64_t>(step.begin, step.end), &filter, ColumnContentType::VALUE,
                                         dst.get()));
            Values expected;
            std::vector<level_t> expected_rep_levels;
            std::vector<level_t> expected_def_levels;
            for (size_t i = step.begin; i < step.end; ++i) {
                for (const auto& v : list_row(i)) {
                    expected.emplace_back(step.selected ? v : std::nullopt);
                }
                append_list_row_levels(i, &expected_rep_levels, &expected_def_levels);
            }
            EXPECT_EQ(expected, to_values(*dst)) << "range [" << step.begin << ", " << step.end << ")";

            // The levels of the rows read are exposed for selected and unselected reads alike (the list reader
            // builds offsets from them).
            level_t* def_levels = nullptr;
            level_t* rep_levels = nullptr;
            size_t num_levels = 0;
            reader->get_levels(&def_levels, &rep_levels, &num_levels);
            ASSERT_EQ(expected_def_levels.size(), num_levels) << "range [" << step.begin << ", " << step.end << ")";
            EXPECT_EQ(expected_rep_levels, std::vector<level_t>(rep_levels, rep_levels + num_levels));
            EXPECT_EQ(expected_def_levels, std::vector<level_t>(def_levels, def_levels + num_levels));
        }
    }
}

TEST(ParquetSimdParityTest, BlockSplitBloomFilterBitExactParity) {
    static constexpr uint32_t kSalt[8] = {0x47b6137bU, 0x44974d91U, 0x8824ad5bU, 0xa2b7289dU,
                                          0x705495c7U, 0x2df1424bU, 0x9efc4947U, 0x5c6bfb31U};
    ParquetBlockSplitBloomFilter bf;
    ASSERT_OK(bf.init(1024, 0.01, Hasher::HashStrategy::XXHASH64, 0));
    const uint32_t num_bytes = bf.num_bytes();
    const uint32_t num_blocks = num_bytes / 32;
    std::vector<uint32_t> expected_words(num_bytes / sizeof(uint32_t), 0);

    for (uint64_t i = 0; i < 200; ++i) {
        uint64_t h = (i * 0x9E3779B97F4A7C15ULL) ^ (i << 17);
        bf.add_hash(h);

        uint32_t block_index = static_cast<uint32_t>(((h >> 32) * num_blocks) >> 32);
        uint32_t key = static_cast<uint32_t>(h);
        for (int lane = 0; lane < 8; ++lane) {
            uint32_t bit = (key * kSalt[lane]) >> 27;
            expected_words[block_index * 8 + lane] |= (1u << bit);
        }
    }

    ASSERT_EQ(0, std::memcmp(bf.data(), expected_words.data(), num_bytes));

    for (uint64_t i = 0; i < 200; ++i) {
        uint64_t h = (i * 0x9E3779B97F4A7C15ULL) ^ (i << 17);
        EXPECT_TRUE(bf.test_hash(h)) << "inserted i=" << i;
    }

    for (uint64_t i = 200; i < 700; ++i) {
        uint64_t h = (i * 0xBF58476D1CE4E5B9ULL) ^ (i << 23);
        uint32_t block_index = static_cast<uint32_t>(((h >> 32) * num_blocks) >> 32);
        uint32_t key = static_cast<uint32_t>(h);
        bool ref_present = true;
        for (int lane = 0; lane < 8; ++lane) {
            uint32_t mask = 1u << ((key * kSalt[lane]) >> 27);
            if ((expected_words[block_index * 8 + lane] & mask) == 0) {
                ref_present = false;
                break;
            }
        }
        EXPECT_EQ(ref_present, bf.test_hash(h)) << "probe i=" << i;
    }

    // Verify single-lane missing bit in each of the 8 lanes returns false.
    for (int missing_lane = 0; missing_lane < 8; ++missing_lane) {
        ParquetBlockSplitBloomFilter single_bf;
        ASSERT_OK(single_bf.init(32, 0.01, Hasher::HashStrategy::XXHASH64, 0));
        uint64_t h = 0x123456789ABCDEF0ULL;
        single_bf.add_hash(h);
        EXPECT_TRUE(single_bf.test_hash(h));

        uint32_t single_num_blocks = single_bf.num_bytes() / 32;
        uint32_t block_index = static_cast<uint32_t>(((h >> 32) * single_num_blocks) >> 32);
        uint32_t key = static_cast<uint32_t>(h);
        uint32_t mask = 1u << ((key * kSalt[missing_lane]) >> 27);
        auto* block_words = reinterpret_cast<uint32_t*>(single_bf.data() + 32 * block_index);
        block_words[missing_lane] &= ~mask;
        EXPECT_FALSE(single_bf.test_hash(h)) << "missing_lane=" << missing_lane;
    }
}

TEST(ParquetSimdParityTest, AssignDataWithNullsSparseAndTrailingNulls) {
    DictDecoder<int32_t> decoder;
    for (size_t count :
         {size_t{0}, size_t{15}, size_t{16}, size_t{17}, size_t{31}, size_t{32}, size_t{33}, size_t{64}, size_t{128}}) {
        // 1. All nulls (num_non_nulls = 0)
        {
            std::vector<uint8_t> nulls(count, 1);
            std::vector<int32_t> src;
            std::vector<int32_t> dst(count, -1);
            decoder.assign_data_with_nulls<int32_t>(count, 0, nulls.data(), src.data(), dst.data());
        }
        // 2. Single non-null at every position pos in [0, count)
        const bool is_dense = (1 >= count / 10);
        for (size_t pos = 0; pos < count; ++pos) {
            std::vector<uint8_t> nulls(count, 1);
            nulls[pos] = 0;
            // Dense expansion (base/simd/expand.h:31-38) requires src to remain readable at index num_valid.
            // Sparse scatter path is kept unpadded to verify its guarded tail never over-reads.
            std::vector<int32_t> src = is_dense ? std::vector<int32_t>{4242, 0} : std::vector<int32_t>{4242};
            std::vector<int32_t> dst(count, -1);
            decoder.assign_data_with_nulls<int32_t>(count, 1, nulls.data(), src.data(), dst.data());
            EXPECT_EQ(4242, dst[pos]) << "count=" << count << " pos=" << pos;

            std::vector<int64_t> src64 =
                    is_dense ? std::vector<int64_t>{9876543210LL, 0} : std::vector<int64_t>{9876543210LL};
            std::vector<int64_t> dst64(count, -1);
            decoder.assign_data_with_nulls<int64_t>(count, 1, nulls.data(), src64.data(), dst64.data());
            EXPECT_EQ(9876543210LL, dst64[pos]) << "count=" << count << " pos=" << pos;
        }
        // 3. Multiple sparse non-nulls (< 10%) across 16B chunks and scalar tail
        if (count >= 32) {
            std::vector<uint8_t> nulls(count, 1);
            std::vector<size_t> positions = {3, 19, count - 1};
            std::vector<int32_t> src;
            for (size_t idx = 0; idx < positions.size(); ++idx) {
                nulls[positions[idx]] = 0;
                src.push_back(static_cast<int32_t>(1000 + idx));
            }
            std::vector<int32_t> dst(count, -1);
            decoder.assign_data_with_nulls<int32_t>(count, src.size(), nulls.data(), src.data(), dst.data());
            for (size_t idx = 0; idx < positions.size(); ++idx) {
                EXPECT_EQ(static_cast<int32_t>(1000 + idx), dst[positions[idx]]) << "count=" << count;
            }
        }
    }
}

TEST(ParquetSimdParityTest, FLBAPlainDecoderNextBatchSlicesAndUnalignedDst) {
    for (size_t type_len : {size_t{7}, size_t{12}}) {
        const size_t total_values = 32;
        std::string backing(total_values * type_len, '\0');
        for (size_t i = 0; i < backing.size(); ++i) {
            backing[i] = static_cast<char>('a' + (i % 26));
        }

        for (size_t count :
             {size_t{0}, size_t{1}, size_t{3}, size_t{4}, size_t{5}, size_t{8}, size_t{16}, size_t{19}}) {
            FLBAPlainDecoder decoder;
            decoder.set_type_length(static_cast<int32_t>(type_len));
            ASSERT_OK(decoder.set_data(Slice(backing.data(), backing.size())));

            std::vector<Slice> aligned_dst(count + 1);
            ASSERT_OK(decoder.next_batch(count, reinterpret_cast<uint8_t*>(aligned_dst.data())));

            for (size_t i = 0; i < count; ++i) {
                EXPECT_EQ(backing.data() + i * type_len, aligned_dst[i].data)
                        << "type_len=" << type_len << " count=" << count << " i=" << i;
                EXPECT_EQ(type_len, aligned_dst[i].size) << "type_len=" << type_len << " count=" << count << " i=" << i;
            }

            // Reading beyond remaining values must return an error.
            std::vector<Slice> extra_buf(total_values + 1);
            EXPECT_FALSE(
                    decoder.next_batch(total_values - count + 1, reinterpret_cast<uint8_t*>(extra_buf.data())).ok());
        }

#if defined(__ARM_NEON) && defined(__aarch64__)
        // Verify 1-byte-aligned vst1q_u8 stores in the NEON 4-Slice loop on an odd byte address.
        for (size_t count : {size_t{0}, size_t{4}, size_t{8}, size_t{16}}) {
            FLBAPlainDecoder decoder;
            decoder.set_type_length(static_cast<int32_t>(type_len));
            ASSERT_OK(decoder.set_data(Slice(backing.data(), backing.size())));

            std::vector<uint8_t> raw_buf((count + 1) * sizeof(Slice) + 8, 0);
            uint8_t* unaligned_dst = raw_buf.data() + 1;
            ASSERT_OK(decoder.next_batch(count, unaligned_dst));

            for (size_t i = 0; i < count; ++i) {
                Slice s;
                std::memcpy(&s, unaligned_dst + i * sizeof(Slice), sizeof(Slice));
                EXPECT_EQ(backing.data() + i * type_len, s.data)
                        << "type_len=" << type_len << " count=" << count << " i=" << i;
                EXPECT_EQ(type_len, s.size) << "type_len=" << type_len << " count=" << count << " i=" << i;
            }
        }
#endif
    }
}

} // namespace
} // namespace starrocks::parquet
