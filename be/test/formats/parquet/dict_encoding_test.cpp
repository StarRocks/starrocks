#include <gtest/gtest.h>

#include <memory>
#include <stdexcept>

#include "column/column_helper.h"
#include "column/type_traits.h"
#include "formats/parquet/encoding.h"
#include "formats/parquet/encoding_dict.h"
#include "formats/parquet/utils.h"
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
} // namespace starrocks::parquet