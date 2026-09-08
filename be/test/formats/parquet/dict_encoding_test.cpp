#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include "column/binary_column.h"
#include "column/column_helper.h"
#include "column/nullable_column.h"
#include "column/type_traits.h"
#include "formats/parquet/encoding.h"
#include "formats/parquet/encoding_dict.h"
#include "formats/parquet/utils.h"
#include "runtime/mem_pool.h"
#include "runtime/types.h"
#include "testutil/assert.h"
#include "types/logical_type.h"
#include "util/slice.h"

namespace starrocks::parquet {
template <LogicalType LT>
class FakeDictDecoder final : public Decoder {
public:
    Status set_data(const Slice& data) override { throw std::runtime_error("not supported function set_data"); }
    Status skip(size_t values_to_skip) override { throw std::runtime_error("not supported skip"); }
    Status next_batch(size_t count, ColumnContentType content_type, Column* dst,
                      const FilterData* filter = nullptr) override {
        throw std::runtime_error("not supported skip");
    }
    Status next_batch(size_t count, uint8_t* dst) override {
        using RT = RunTimeCppType<LT>;
        auto* spec_dst = reinterpret_cast<RT*>(dst);
        for (size_t i = 0; i < count; ++i) {
            spec_dst[i] = i;
        }
        return Status::OK();
    }
};

template <>
class FakeDictDecoder<TYPE_VARCHAR> final : public Decoder {
public:
    Status set_data(const Slice& data) override { throw std::runtime_error("not supported function set_data"); }
    Status skip(size_t values_to_skip) override { throw std::runtime_error("not supported skip"); }
    Status next_batch(size_t count, ColumnContentType content_type, Column* dst,
                      const FilterData* filter = nullptr) override {
        throw std::runtime_error("not supported skip");
    }
    Status next_batch(size_t count, uint8_t* dst) override {
        auto* spec_dst = reinterpret_cast<Slice*>(dst);
        for (size_t i = 0; i < count; ++i) {
            auto data = std::to_string(i);
            Slice slice = Slice(_pool.allocate(data.size()), data.size());
            memcpy(slice.data, data.data(), data.size());
            spec_dst[i] = slice;
        }
        return Status::OK();
    }

private:
    MemPool _pool;
};

static Slice unquote(Slice slice) {
    if ((slice.starts_with("\"") && slice.ends_with("\"")) || (slice.starts_with("'") && slice.ends_with("'"))) {
        slice.remove_prefix(1);
        slice.remove_suffix(1);
    }
    return slice;
}

#define EXPECTED_UNQUOTE(lhs, rhs) EXPECT_EQ(unquote(lhs), rhs)

TEST(DictEncodingReadTest, BasicTest) {
    constexpr LogicalType PT = LogicalType::TYPE_INT;
    using RT = RunTimeCppType<PT>;
    faststring fs;
    RleEncoder<RT> encoder(&fs, 32);
    {
        for (size_t i = 0; i < 4096; ++i) {
            encoder.Put(i % 9 + 1, 10);
        }
    }

    DictDecoder<RT> decoder;
    FakeDictDecoder<PT> inner_decoder;
    faststring fs2;
    fs2.resize(fs.length() + 1);
    fs2.data()[0] = 32;
    memcpy(fs2.data() + 1, fs.data(), fs.length());
    ASSERT_OK(decoder.set_data(Slice(fs2.data(), fs2.length())));
    ASSERT_OK(decoder.set_dict(10, 10, &inner_decoder));

    // read dict code
    size_t chunk_size = 4095;
    NullInfos infos;
    infos.reset_with_capacity(chunk_size);
    {
        // interleave
        infos.num_nulls = 0;
        for (size_t i = 0; i < chunk_size; ++i) {
            infos.nulls_data()[i] = i % 2;
            infos.num_nulls += infos.nulls_data()[i];
        }
        infos.num_ranges = chunk_size / 2;
    }
    {
        TypeDescriptor type_desc = TypeDescriptor(TYPE_INT);
        auto dst = ColumnHelper::create_column(type_desc, true);
        ASSERT_OK(decoder.next_batch_with_nulls(chunk_size, infos, ColumnContentType::DICT_CODE, dst.get(), nullptr));
        EXPECT_EQ(dst->debug_item(0), "1");
        EXPECT_EQ(dst->debug_item(1), "NULL");
        EXPECT_EQ(dst->debug_item(2), "1");
        EXPECT_EQ(dst->size(), chunk_size);
    }
    {
        TypeDescriptor type_desc = TypeDescriptor(TYPE_INT);
        auto dst = ColumnHelper::create_column(type_desc, true);
        auto filter = std::make_unique<uint8_t[]>(chunk_size);
        memset(filter.get(), 0x01, chunk_size);
        filter[0] = 0;
        ASSERT_OK(decoder.next_batch_with_nulls(chunk_size, infos, ColumnContentType::DICT_CODE, dst.get(),
                                                filter.get()));
        EXPECT_EQ(dst->debug_item(0), "7");
        EXPECT_EQ(dst->debug_item(1), "NULL");
        EXPECT_EQ(dst->debug_item(2), "7");
        EXPECT_EQ(dst->size(), chunk_size);
    }

    {
        // sparse
        for (size_t i = 0; i < chunk_size; ++i) {
            infos.nulls_data()[i] = 1;
        }
        infos.nulls_data()[0] = 0;
        infos.nulls_data()[1000] = 0;
        infos.nulls_data()[2000] = 0;
        infos.nulls_data()[3000] = 0;
        infos.nulls_data()[4000] = 0;

        infos.num_nulls = chunk_size - 5;
        infos.num_ranges = chunk_size / 2;
    }
    {
        TypeDescriptor type_desc = TypeDescriptor(TYPE_INT);
        auto dst = ColumnHelper::create_column(type_desc, true);
        ASSERT_OK(decoder.next_batch_with_nulls(chunk_size, infos, ColumnContentType::DICT_CODE, dst.get(), nullptr));
        EXPECT_EQ(dst->debug_item(0), "5");
        EXPECT_EQ(dst->debug_item(1), "NULL");
        EXPECT_EQ(dst->debug_item(2), "NULL");
        EXPECT_EQ(dst->size(), chunk_size);
    }
    {
        TypeDescriptor type_desc = TypeDescriptor(TYPE_INT);
        auto dst = ColumnHelper::create_column(type_desc, true);
        auto filter = std::make_unique<uint8_t[]>(chunk_size);
        memset(filter.get(), 0x01, chunk_size);
        filter[0] = 0;
        ASSERT_OK(decoder.next_batch_with_nulls(chunk_size, infos, ColumnContentType::VALUE, dst.get(), filter.get()));
        EXPECT_EQ(dst->debug_item(0), "6");
        EXPECT_EQ(dst->debug_item(1), "NULL");
        EXPECT_EQ(dst->debug_item(2), "NULL");
        EXPECT_EQ(dst->size(), chunk_size);
    }
    {
        // all null
        for (size_t i = 0; i < chunk_size; ++i) {
            infos.nulls_data()[i] = 1;
        }
        infos.num_nulls = chunk_size;
        TypeDescriptor type_desc = TypeDescriptor(TYPE_INT);
        auto dst = ColumnHelper::create_column(type_desc, true);
        auto filter = std::make_unique<uint8_t[]>(chunk_size);
        ASSERT_OK(decoder.next_batch_with_nulls(chunk_size, infos, ColumnContentType::VALUE, dst.get(), filter.get()));
        EXPECT_EQ(dst->debug_item(0), "NULL");
        EXPECT_EQ(dst->debug_item(1), "NULL");
        EXPECT_EQ(dst->debug_item(2), "NULL");
        EXPECT_EQ(dst->size(), chunk_size);
    }
}

// Build a ready-to-read DictDecoder<Slice> backed by an int-keyed dictionary, mirroring the setup
// in BasicTest.
static void setup_slice_dict_decoder(DictDecoder<Slice>* decoder, FakeDictDecoder<TYPE_VARCHAR>* inner_decoder,
                                     faststring* backing) {
    faststring fs;
    RleEncoder<int32_t> encoder(&fs, 32);
    for (size_t i = 0; i < 4096; ++i) {
        encoder.Put(i % 9 + 1, 10);
    }
    backing->resize(fs.length() + 1);
    backing->data()[0] = 32;
    memcpy(backing->data() + 1, fs.data(), fs.length());
    ASSERT_OK(decoder->set_data(Slice(backing->data(), backing->length())));
    ASSERT_OK(decoder->set_dict(10, 10, inner_decoder));
}

// A row that a pushed-down filter excludes is never written, so it contributes no bytes to the
// BinaryColumn. It must therefore come back NULL, keeping the layout invariant that a non-NULL row
// occupies exactly the value width and a NULL row occupies nothing. Leaving such a row NOT NULL
// creates a third state that consumers walking FIXED_LEN_BYTE_ARRAY bytes with a fixed stride --
// BinaryToDecimalConverter does -- cannot represent: they consume the value width for it anyway,
// decode every following row (including the rows that survive the filter) from a misaligned offset,
// and read past the end of the buffer.
TEST(DictEncodingReadTest, FilterExcludedRowsAreMarkedNull) {
    constexpr size_t count = 100;
    // FakeDictDecoder<TYPE_VARCHAR> builds the dictionary from std::to_string(i), i in [0, 10),
    // so every dictionary value is exactly one byte wide.
    constexpr size_t kValueLen = 1;

    // 1. next_batch() with a filter and no NULLs: only filter-excluded rows.
    {
        DictDecoder<Slice> decoder;
        FakeDictDecoder<TYPE_VARCHAR> inner_decoder;
        faststring backing;
        setup_slice_dict_decoder(&decoder, &inner_decoder, &backing);

        auto dst = ColumnHelper::create_column(TypeDescriptor(TYPE_VARCHAR), true);
        auto filter = std::make_unique<uint8_t[]>(count);
        memset(filter.get(), 0x01, count);
        const std::vector<size_t> excluded = {3, 50, 99};
        for (size_t i : excluded) {
            filter[i] = 0;
        }
        ASSERT_OK(decoder.next_batch(count, ColumnContentType::VALUE, dst.get(), filter.get()));

        auto* nullable = down_cast<NullableColumn*>(dst.get());
        ASSERT_EQ(count, nullable->size());
        EXPECT_TRUE(nullable->has_null());
        for (size_t i = 0; i < count; ++i) {
            const bool is_excluded = std::find(excluded.begin(), excluded.end(), i) != excluded.end();
            EXPECT_EQ(is_excluded, nullable->is_null(i)) << "row " << i;
        }
        // The rows that survive still decode: rows [0, 10) all map to dictionary index 1.
        EXPECTED_UNQUOTE(dst->debug_item(0), "1");
        EXPECTED_UNQUOTE(dst->debug_item(4), "1");

        // The invariant itself: only the surviving rows occupy bytes.
        auto* binary = down_cast<BinaryColumn*>(nullable->mutable_data_column());
        EXPECT_EQ((count - excluded.size()) * kValueLen, binary->get_bytes().size());
        EXPECT_EQ(binary->get_bytes().size(), binary->get_offset().back());
    }

    // 2. next_batch_with_nulls(): real NULLs and filter-excluded rows together. It walks runs of
    // equal NULL-ness and hands each non-NULL run to next_batch(), so the filtered path above runs
    // once per run and its NULL bookkeeping has to be relative to the rows already appended.
    {
        DictDecoder<Slice> decoder;
        FakeDictDecoder<TYPE_VARCHAR> inner_decoder;
        faststring backing;
        setup_slice_dict_decoder(&decoder, &inner_decoder, &backing);
        // next_batch_with_nulls() drops the filter unless the dictionary outgrows the threshold,
        // and it has no BE_TEST bypass, so force the filtered path for this small dictionary.
        decoder._dict_size_threshold = 0;

        NullInfos infos;
        infos.reset_with_capacity(count);
        infos.num_nulls = 0;
        for (size_t i = 0; i < count; ++i) {
            infos.nulls_data()[i] = (i % 10 == 0);
            infos.num_nulls += infos.nulls_data()[i];
        }
        infos.num_ranges = count / 2;

        auto filter = std::make_unique<uint8_t[]>(count);
        memset(filter.get(), 0x01, count);
        // Two non-NULL rows, plus one row that is already NULL: the latter must not be counted twice.
        const std::vector<size_t> excluded = {3, 57};
        for (size_t i : excluded) {
            filter[i] = 0;
        }
        filter[10] = 0;

        auto dst = ColumnHelper::create_column(TypeDescriptor(TYPE_VARCHAR), true);
        ASSERT_OK(decoder.next_batch_with_nulls(count, infos, ColumnContentType::VALUE, dst.get(), filter.get()));

        auto* nullable = down_cast<NullableColumn*>(dst.get());
        ASSERT_EQ(count, nullable->size());
        EXPECT_TRUE(nullable->has_null());
        for (size_t i = 0; i < count; ++i) {
            const bool is_excluded = std::find(excluded.begin(), excluded.end(), i) != excluded.end();
            EXPECT_EQ(infos.nulls_data()[i] != 0 || is_excluded, nullable->is_null(i)) << "row " << i;
        }

        auto* binary = down_cast<BinaryColumn*>(nullable->mutable_data_column());
        const size_t surviving = count - infos.num_nulls - excluded.size();
        EXPECT_EQ(surviving * kValueLen, binary->get_bytes().size());
        EXPECT_EQ(binary->get_bytes().size(), binary->get_offset().back());
    }
}
} // namespace starrocks::parquet