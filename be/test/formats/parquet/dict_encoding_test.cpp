#include <gtest/gtest.h>

#include <memory>
#include <stdexcept>
#include <string>

#include "base/string/slice.h"
#include "base/testutil/assert.h"
#include "column/column_helper.h"
#include "column/type_traits.h"
#include "formats/parquet/encoding.h"
#include "formats/parquet/encoding_dict.h"
#include "formats/parquet/utils.h"
#include "runtime/mem_pool.h"
#include "runtime/types.h"
#include "types/logical_type.h"

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
} // namespace starrocks::parquet