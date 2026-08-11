#include <gtest/gtest.h>

#include <memory>
#include <stdexcept>
#include <string>

#include "column/column_helper.h"
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

template <LogicalType DICT_TYPE, LogicalType TARGET_TYPE>
void dict_encoding_test() {
    using DICT_CXX_TYPE = RunTimeCppType<DICT_TYPE>;
    using TARGET_CXX_TYPE = RunTimeCppType<TARGET_TYPE>;
    faststring fs;
    RleEncoder<DICT_CXX_TYPE> encoder(&fs, 32);
    {
        for (size_t i = 0; i < 4096; ++i) {
            encoder.Put(i % 9 + 1, 10);
        }
    }

    DictDecoder<TARGET_CXX_TYPE> decoder;
    FakeDictDecoder<TARGET_TYPE> inner_decoder;
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
        TypeDescriptor type_desc = TypeDescriptor(DICT_TYPE);
        auto dst = ColumnHelper::create_column(type_desc, true);
        ASSERT_OK(decoder.next_batch_with_nulls(chunk_size, infos, ColumnContentType::DICT_CODE, dst.get(), nullptr));
        EXPECTED_UNQUOTE(dst->debug_item(0), "1");
        EXPECTED_UNQUOTE(dst->debug_item(1), "NULL");
        EXPECTED_UNQUOTE(dst->debug_item(2), "1");
        EXPECT_EQ(dst->size(), chunk_size);
    }
    {
        TypeDescriptor type_desc = TypeDescriptor(DICT_TYPE);
        auto dst = ColumnHelper::create_column(type_desc, true);
        auto filter = std::make_unique<uint8_t[]>(chunk_size);
        memset(filter.get(), 0x01, chunk_size);
        filter[0] = 0;
        ASSERT_OK(decoder.next_batch_with_nulls(chunk_size, infos, ColumnContentType::DICT_CODE, dst.get(),
                                                filter.get()));
        EXPECTED_UNQUOTE(dst->debug_item(0), "7");
        EXPECTED_UNQUOTE(dst->debug_item(1), "NULL");
        EXPECTED_UNQUOTE(dst->debug_item(2), "7");
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
        TypeDescriptor type_desc = TypeDescriptor(DICT_TYPE);
        auto dst = ColumnHelper::create_column(type_desc, true);
        ASSERT_OK(decoder.next_batch_with_nulls(chunk_size, infos, ColumnContentType::DICT_CODE, dst.get(), nullptr));
        EXPECTED_UNQUOTE(dst->debug_item(0), "5");
        EXPECTED_UNQUOTE(dst->debug_item(1), "NULL");
        EXPECTED_UNQUOTE(dst->debug_item(2), "NULL");
        EXPECT_EQ(dst->size(), chunk_size);
    }
    {
        TypeDescriptor type_desc = TypeDescriptor(TARGET_TYPE);
        auto dst = ColumnHelper::create_column(type_desc, true);
        auto filter = std::make_unique<uint8_t[]>(chunk_size);
        memset(filter.get(), 0x01, chunk_size);
        filter[0] = 0;
        ASSERT_OK(decoder.next_batch_with_nulls(chunk_size, infos, ColumnContentType::VALUE, dst.get(), filter.get()));
        EXPECTED_UNQUOTE(dst->debug_item(0), "6");
        EXPECTED_UNQUOTE(dst->debug_item(1), "NULL");
        EXPECTED_UNQUOTE(dst->debug_item(2), "NULL");
        EXPECT_EQ(dst->size(), chunk_size);
    }
    {
        decoder._dict_size_threshold = 0;
        TypeDescriptor type_desc = TypeDescriptor(TARGET_TYPE);
        auto dst = ColumnHelper::create_column(type_desc, true);
        auto filter = std::make_unique<uint8_t[]>(chunk_size);
        memset(filter.get(), 0x01, chunk_size);
        filter[1] = 0;
        filter[1000] = 0;
        ASSERT_OK(decoder.next_batch_with_nulls(chunk_size, infos, ColumnContentType::VALUE, dst.get(), filter.get()));
        EXPECTED_UNQUOTE(dst->debug_item(0), "6");
        EXPECTED_UNQUOTE(dst->debug_item(2), "NULL");
        EXPECTED_UNQUOTE(dst->debug_item(2000), "6");
        EXPECT_EQ(dst->size(), chunk_size);
    }
    {
        // all filtered
        decoder._dict_size_threshold = 0;
        TypeDescriptor type_desc = TypeDescriptor(TARGET_TYPE);
        auto dst = ColumnHelper::create_column(type_desc, true);
        auto filter = std::make_unique<uint8_t[]>(chunk_size);
        memset(filter.get(), 0x00, chunk_size);
        ASSERT_OK(decoder.next_batch_with_nulls(chunk_size, infos, ColumnContentType::VALUE, dst.get(), filter.get()));
        EXPECT_EQ(dst->size(), chunk_size);
    }
    {
        // all null
        for (size_t i = 0; i < chunk_size; ++i) {
            infos.nulls_data()[i] = 1;
        }
        infos.num_nulls = chunk_size;
        TypeDescriptor type_desc = TypeDescriptor(TARGET_TYPE);
        auto dst = ColumnHelper::create_column(type_desc, true);
        auto filter = std::make_unique<uint8_t[]>(chunk_size);
        ASSERT_OK(decoder.next_batch_with_nulls(chunk_size, infos, ColumnContentType::VALUE, dst.get(), filter.get()));
        EXPECTED_UNQUOTE(dst->debug_item(0), "NULL");
        EXPECTED_UNQUOTE(dst->debug_item(1), "NULL");
        EXPECTED_UNQUOTE(dst->debug_item(2), "NULL");
        EXPECT_EQ(dst->size(), chunk_size);
    }
}

TEST(DictEncodingReadTest, BasicTest) {
    constexpr LogicalType TARGET_TYPE = LogicalType::TYPE_INT;
    constexpr LogicalType DICT_TYPE = LogicalType::TYPE_INT;
    dict_encoding_test<DICT_TYPE, TARGET_TYPE>();
}

TEST(DictEncodingReadTest, BinaryPageTest) {
    constexpr LogicalType TARGET_TYPE = LogicalType::TYPE_VARCHAR;
    constexpr LogicalType DICT_TYPE = LogicalType::TYPE_INT;
    dict_encoding_test<DICT_TYPE, TARGET_TYPE>();
}

// Build a ready-to-read DictDecoder<Slice> backed by an int-keyed dictionary, mirroring the setup
// in dict_encoding_test().
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

// DictDecoder<Slice> must reject a non-binary destination column instead of blindly down-casting it
// to BinaryColumn. This guards the path that could otherwise let a temporary Int32 dict-code column
// reach a string slot. Exercises both the filtered _next_batch_value() path and the
// next_value_batch_with_nulls() path.
TEST(DictEncodingReadTest, BinaryDestinationTypeGuard) {
    constexpr size_t count = 100;

    // 1. _next_batch_value() with a filter: a binary destination succeeds.
    {
        DictDecoder<Slice> decoder;
        FakeDictDecoder<TYPE_VARCHAR> inner_decoder;
        faststring backing;
        setup_slice_dict_decoder(&decoder, &inner_decoder, &backing);

        auto dst = ColumnHelper::create_column(TypeDescriptor(TYPE_VARCHAR), true);
        auto filter = std::make_unique<uint8_t[]>(count);
        memset(filter.get(), 0x01, count);
        filter[0] = 0;
        ASSERT_OK(decoder.next_batch(count, ColumnContentType::VALUE, dst.get(), filter.get()));
        EXPECT_EQ(dst.get()->size(), count);
    }

    // 2. _next_batch_value() with a filter: a non-binary destination is rejected.
    {
        DictDecoder<Slice> decoder;
        FakeDictDecoder<TYPE_VARCHAR> inner_decoder;
        faststring backing;
        setup_slice_dict_decoder(&decoder, &inner_decoder, &backing);

        auto dst = ColumnHelper::create_column(TypeDescriptor(TYPE_INT), true);
        auto filter = std::make_unique<uint8_t[]>(count);
        memset(filter.get(), 0x01, count);
        auto st = decoder.next_batch(count, ColumnContentType::VALUE, dst.get(), filter.get());
        ASSERT_FALSE(st.ok());
    }

    // 3. next_value_batch_with_nulls(): a non-binary destination is rejected.
    {
        DictDecoder<Slice> decoder;
        FakeDictDecoder<TYPE_VARCHAR> inner_decoder;
        faststring backing;
        setup_slice_dict_decoder(&decoder, &inner_decoder, &backing);

        NullInfos infos;
        infos.reset_with_capacity(count);
        infos.num_nulls = 0;
        for (size_t i = 0; i < count; ++i) {
            infos.nulls_data()[i] = i % 2;
            infos.num_nulls += infos.nulls_data()[i];
        }
        // num_ranges > 2 routes to next_value_batch_with_nulls() rather than the row-by-row fallback.
        infos.num_ranges = count / 2;

        auto dst = ColumnHelper::create_column(TypeDescriptor(TYPE_INT), true);
        auto filter = std::make_unique<uint8_t[]>(count);
        memset(filter.get(), 0x01, count);
        auto st = decoder.next_batch_with_nulls(count, infos, ColumnContentType::VALUE, dst.get(), filter.get());
        ASSERT_FALSE(st.ok());
    }
}

// A dictionary-encoded data page: one bit-width byte, then RLE/bit-packed runs. A single
// repeated run covers every value, so the page is a handful of bytes however many values it
// claims -- which is what keeps a multi-million-value test cheap. (Building the same page with
// RleEncoder::Put would not: it costs seconds per 100k values.)
static void build_repeated_run_page(faststring* page, uint32_t code, size_t num_values, int bit_width = 32) {
    auto put_byte = [page](uint32_t b) {
        auto byte = static_cast<uint8_t>(b);
        page->append(&byte, 1);
    };

    put_byte(bit_width);
    // Run indicator, ULEB128 of (num_values << 1); lsb 0 marks a repeated run.
    uint64_t indicator = static_cast<uint64_t>(num_values) << 1;
    for (; indicator >= 0x80; indicator >>= 7) {
        put_byte((indicator & 0x7f) | 0x80);
    }
    put_byte(indicator);
    // The repeated value, ceil(bit_width / 8) bytes, little endian.
    for (int i = 0; i < (bit_width + 7) / 8; ++i) {
        put_byte(code >> (8 * i));
    }
}

// The batch size handed to a decoder is not the chunk size: StoredColumnReaderImpl::_read() passes
// everything left in the current data page in one call, so a page holding millions of values
// produces a batch that big. Scratch space sized by that batch must live on the heap; the VLAs
// these paths used to declare killed the BE with a SIGSEGV raised inside the decoder.
template <LogicalType DICT_TYPE, LogicalType TARGET_TYPE>
static void large_batch_with_nulls_test() {
    using TARGET_CXX_TYPE = RunTimeCppType<TARGET_TYPE>;
    // 2^21 values: the removed VLAs wanted 8MB for the dict codes and the fixed-width values,
    // and 24MB for the slices, against the 8MB stack a scan thread has.
    constexpr size_t kCount = 1 << 21;
    constexpr size_t kNullStride = 8;
    constexpr uint32_t kCode = 3;

    faststring page;
    build_repeated_run_page(&page, kCode, kCount);

    NullInfos infos;
    infos.reset_with_capacity(kCount);
    infos.num_nulls = 0;
    for (size_t i = 0; i < kCount; ++i) {
        infos.nulls_data()[i] = (i % kNullStride) == 0;
        infos.num_nulls += infos.nulls_data()[i];
    }
    // num_ranges > 2 routes to the batched paths rather than the row-by-row fallback.
    infos.num_ranges = kCount / 2;

    // The dictionary holds "0".."9", every code in the page is kCode, and FakeDictDecoder makes
    // dictionary entry i render as "i" for both the int and the string dictionary.
    auto check = [&](const auto& dst) {
        EXPECT_EQ(dst->size(), kCount);
        EXPECTED_UNQUOTE(dst->debug_item(0), "NULL");
        EXPECTED_UNQUOTE(dst->debug_item(kNullStride), "NULL");
        EXPECTED_UNQUOTE(dst->debug_item(1), "3");
        EXPECTED_UNQUOTE(dst->debug_item(kCount - 1), "3");
    };

    {
        // dict codes
        DictDecoder<TARGET_CXX_TYPE> decoder;
        FakeDictDecoder<TARGET_TYPE> inner_decoder;
        ASSERT_OK(decoder.set_data(Slice(page.data(), page.length())));
        ASSERT_OK(decoder.set_dict(10, 10, &inner_decoder));

        auto dst = ColumnHelper::create_column(TypeDescriptor(DICT_TYPE), true);
        ASSERT_OK(decoder.next_batch_with_nulls(kCount, infos, ColumnContentType::DICT_CODE, dst.get(), nullptr));
        check(dst);
    }
    {
        // values
        DictDecoder<TARGET_CXX_TYPE> decoder;
        FakeDictDecoder<TARGET_TYPE> inner_decoder;
        ASSERT_OK(decoder.set_data(Slice(page.data(), page.length())));
        ASSERT_OK(decoder.set_dict(10, 10, &inner_decoder));

        auto dst = ColumnHelper::create_column(TypeDescriptor(TARGET_TYPE), true);
        ASSERT_OK(decoder.next_batch_with_nulls(kCount, infos, ColumnContentType::VALUE, dst.get(), nullptr));
        check(dst);
    }
}

TEST(DictEncodingReadTest, LargeBatchWithNulls) {
    large_batch_with_nulls_test<LogicalType::TYPE_INT, LogicalType::TYPE_INT>();
    large_batch_with_nulls_test<LogicalType::TYPE_INT, LogicalType::TYPE_VARCHAR>();
}
} // namespace starrocks::parquet