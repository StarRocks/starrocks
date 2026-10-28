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

#include "formats/parquet/column_converter.h"

#include <arrow/io/memory.h>
#include <gtest/gtest.h>
#include <parquet/api/reader.h>
#include <parquet/api/writer.h>

#include <algorithm>
#include <filesystem>
#include <limits>
#include <random>

#include "base/testutil/assert.h"
#include "base/time/timezone_utils.h"
#include "base/types/int96.h"
#include "base/utility/defer_op.h"
#include "column/binary_column.h"
#include "column/column_helper.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
#include "column/struct_column.h"
#include "common/config_exec_fwd.h"
#include "common/config_scan_io_fwd.h"
#include "compute_env/global_dict/fragment_dict_state.h"
#include "formats/parquet/column_materializer.h"
#include "formats/parquet/encoding_dict.h"
#include "formats/parquet/encoding_plain.h"
#include "formats/parquet/file_reader.h"
#include "formats/parquet/metadata.h"
#include "formats/parquet/statistics_helper.h"
#include "fs/fs.h"
#include "io/string_input_stream.h"
#include "parquet_test_util/util.h"
#include "parquet_ut_base.h"
#include "runtime/descriptor_helper.h"
#include "runtime/runtime_state.h"
#include "types/time_types.h"
#include "types/timestamp_value.h"

namespace starrocks::parquet {

static FormatScannerStats g_hdfs_stats{};

class ColumnConverterTest : public testing::Test {
public:
    ColumnConverterTest() = default;

    ~ColumnConverterTest() override = default;

protected:
    std::unique_ptr<RandomAccessFile> _create_file(const std::string& file_path) {
        return *FileSystem::Default()->new_random_access_file(file_path);
    }

    HdfsScannerContext* _create_scan_context() {
        auto* ctx = _pool.add(new HdfsScannerContext());
        auto* lazy_column_coalesce_counter = _pool.add(new std::atomic<int32_t>(0));

        ctx->format_scan_context.lazy_column_coalesce_counter = lazy_column_coalesce_counter;
        ctx->format_scan_context.timezone = "Asia/Shanghai";
        ctx->format_scan_context.stats = &g_hdfs_stats;
        ctx->format_scan_context.predicate_tree = &ctx->predicates.predicate_tree;
        return ctx;
    }

    THdfsScanRange* _create_scan_range(const std::string& file_path, size_t scan_length = 0) {
        auto* scan_range = _pool.add(new THdfsScanRange());
        scan_range->relative_path = file_path;
        scan_range->file_length = std::filesystem::file_size(file_path);
        scan_range->offset = 4;
        scan_range->length = scan_length > 0 ? scan_length : scan_range->file_length;
        return scan_range;
    }

    static void check_chunk_values(ChunkPtr& chunk, const std::string& expected_value) {
        chunk->check_or_die();
        size_t mid = chunk->num_rows() / 2;
        for (size_t i = 0; i < chunk->num_rows(); i++) {
            if (i == mid) {
                EXPECT_EQ("[NULL]", chunk->debug_row(i));
            } else {
                EXPECT_EQ(expected_value, chunk->debug_row(i));
            }
        }
    }

    void check(const std::string& filepath, const TypeDescriptor& col_type, const std::string& col_name,
               const std::string& expected_value, const size_t expected_rows, bool is_failed = false) {
        auto file = _create_file(filepath);
        auto file_reader = std::make_shared<FileReader>(config::vector_chunk_size, file.get(),
                                                        std::filesystem::file_size(filepath));

        // --------------init context---------------
        auto ctx = _create_scan_context();

        Utils::SlotDesc slot_descs[] = {{col_name, col_type}, {""}};

        TupleDescriptor* tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
        Utils::make_column_info_vector(tuple_desc, &ctx->format_scan_context.materialized_columns);
        ctx->slot_descs = tuple_desc->slots();
        ctx->scan_range = (_create_scan_range(filepath));
        ctx->format_scan_context.scan_range_offset = ctx->scan_range->offset;
        ctx->format_scan_context.scan_range_length = ctx->scan_range->length;
        // --------------finish init context---------------

        Status status = file_reader->init(&ctx->format_scan_context);
        if (is_failed) {
            EXPECT_TRUE(!status.ok());
            return;
        }
        if (!status.ok()) {
            std::cout << status.message() << std::endl;
        }
        ASSERT_TRUE(status.ok());

        EXPECT_EQ(file_reader->_row_group_readers.size(), 1);

        auto chunk = std::make_shared<Chunk>();
        chunk->append_column(ColumnHelper::create_column(col_type, true), chunk->num_columns());

        status = file_reader->get_next(&chunk);
        ASSERT_TRUE(status.ok());

        size_t total_rows = chunk->num_rows();

        check_chunk_values(chunk, expected_value);

        while (!status.is_end_of_file()) {
            chunk->reset();
            status = file_reader->get_next(&chunk);
            if (!status.ok() && !status.is_end_of_file()) {
                std::cout << status.message() << std::endl;
                break;
            }
            check_chunk_values(chunk, expected_value);
            total_rows += chunk->num_rows();
        }

        EXPECT_EQ(expected_rows, total_rows);
    }

    RuntimeState* _runtime_state = nullptr;
    ObjectPool _pool;
    HdfsScannerContext _scanner_ctx;
};

TEST_F(ColumnConverterTest, InspectByteArrayDecimalFixture) {
    auto reader = ::parquet::ParquetFileReader::OpenFile(
            "./be/test/formats/parquet/test_data/column_converter/byte_array.parquet");
    auto metadata = reader->metadata();
    int index = metadata->schema()->ColumnIndex("decimal_binary");
    ASSERT_GE(index, 0);
    const auto* descriptor = metadata->schema()->Column(index);
    ASSERT_EQ(::parquet::Type::BYTE_ARRAY, descriptor->physical_type());
    ASSERT_EQ(1, metadata->num_row_groups());
    ASSERT_EQ(5, metadata->num_rows());
    EXPECT_EQ(5, descriptor->type_precision());
    EXPECT_EQ(1, descriptor->type_scale());
    ASSERT_NE(nullptr, descriptor->logical_type());
    EXPECT_EQ("Decimal(precision=5, scale=1)", descriptor->logical_type()->ToString());
    for (int group = 0; group < metadata->num_row_groups(); ++group) {
        auto row_group = reader->RowGroup(group);
        auto column = row_group->Column(index);
        auto* binary = static_cast<::parquet::ByteArrayReader*>(column.get());
        int16_t defs[16];
        ::parquet::ByteArray values[16];
        int64_t count = 0;
        int64_t levels = binary->ReadBatch(16, defs, nullptr, values, &count);
        ASSERT_EQ(metadata->num_rows(), levels);
        ASSERT_EQ(4, count);
        const int16_t expected_defs[] = {1, 1, 0, 1, 1};
        for (int64_t i = 0; i < levels; ++i) EXPECT_EQ(expected_defs[i], defs[i]);
        for (int64_t i = 0; i < count; ++i) {
            ASSERT_EQ(1, values[i].len);
            EXPECT_EQ(0x43, values[i].ptr[0]);
        }
    }
    auto file = _create_file("./be/test/formats/parquet/test_data/column_converter/byte_array.parquet");
    auto sr_reader = std::make_shared<FileReader>(2, file.get(), std::filesystem::file_size(file->filename()));
    auto* ctx = _create_scan_context();
    Utils::SlotDesc slots[] = {{"string", TypeDescriptor::from_logical_type(TYPE_VARCHAR), 0}, {""}};
    auto* tuple = Utils::create_tuple_descriptor(_runtime_state, &_pool, slots);
    Utils::make_column_info_vector(tuple, &ctx->format_scan_context.materialized_columns);
    ctx->slot_descs = tuple->slots();
    ctx->format_scan_context.scan_range_offset = 0;
    ctx->format_scan_context.scan_range_length = std::filesystem::file_size(file->filename());
    ASSERT_OK(sr_reader->init(&ctx->format_scan_context));
    bool found = false;
    for (const auto& element : sr_reader->get_file_metadata()->t_metadata().schema) {
        if (element.name != "decimal_binary") continue;
        EXPECT_TRUE(element.__isset.precision);
        EXPECT_EQ(5, element.precision);
        EXPECT_TRUE(element.__isset.scale);
        EXPECT_EQ(1, element.scale);
        EXPECT_TRUE(element.__isset.logicalType && element.logicalType.__isset.DECIMAL);
        found = true;
    }
    ASSERT_TRUE(found);
}

TEST_F(ColumnConverterTest, TestByteArrayTest) {
    const std::string file_path = "./be/test/formats/parquet/test_data/column_converter/byte_array.parquet";
    const size_t expected_rows = 5;

    {
        const std::string col_name = "string";
        // string to char
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_CHAR);
            check(file_path, col_type, col_name, "['string_smith']", expected_rows);
        }
        // string to varchar
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR);
            check(file_path, col_type, col_name, "['string_smith']", expected_rows);
        }
    }

    {
        const std::string col_name = "enum";
        // enum to char
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_CHAR);
            check(file_path, col_type, col_name, "['enum_smith']", expected_rows);
        }
        // enum to varchar
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR);
            check(file_path, col_type, col_name, "['enum_smith']", expected_rows);
        }
    }

    {
        const std::string col_name = "decimal_binary";
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_DECIMAL);
            check(file_path, col_type, col_name, "[6.7]", expected_rows, true);
        }
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_DECIMAL32, -1, 5, 1);
            check(file_path, col_type, col_name, "[6.7]", expected_rows);
        }
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_DECIMAL64, -1, 5, 1);
            check(file_path, col_type, col_name, "[6.7]", expected_rows);
        }
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_DECIMAL128, -1, 5, 1);
            check(file_path, col_type, col_name, "[6.7]", expected_rows);
        }
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_DECIMALV2, -1, 27, 9);
            check(file_path, col_type, col_name, "[6.7]", expected_rows);
        }
    }

    {
        const std::string col_name = "json";
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR);
            check(file_path, col_type, col_name, R"(['{"name": "smith"}'])", expected_rows);
        }
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_CHAR);
            check(file_path, col_type, col_name, R"(['{"name": "smith"}'])", expected_rows);
        }
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_JSON);
            check(file_path, col_type, col_name, R"(['{"name": "smith"}'])", expected_rows, true);
        }
    }

    {
        const std::string col_name = "bson";
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR);
            check(file_path, col_type, col_name, R"(['{"name": "smith"}'])", expected_rows);
        }
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_CHAR);
            check(file_path, col_type, col_name, R"(['{"name": "smith"}'])", expected_rows);
        }
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_JSON);
            check(file_path, col_type, col_name, R"(['{"name": "smith"}'])", expected_rows, true);
        }
    }
}

TEST_F(ColumnConverterTest, Int32Test) {
    const std::string file_path = "./be/test/formats/parquet/test_data/column_converter/int32.parquet";
    const size_t expected_rows = 5;

    {
        const std::string col_name = "int8";
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_TINYINT);
            check(file_path, col_type, col_name, "[-5]", expected_rows);
        }
    }
    {
        const std::string col_name = "int16";
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_SMALLINT);
            check(file_path, col_type, col_name, "[-998]", expected_rows);
        }
    }
    {
        const std::string col_name = "int32";
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_INT);
            check(file_path, col_type, col_name, "[-99998]", expected_rows);
        }
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT);
            check(file_path, col_type, col_name, "[-99998]", expected_rows);
        }
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_DOUBLE);
            check(file_path, col_type, col_name, "[-99998]", expected_rows);
        }
    }
    {
        const std::string col_name = "time_millis";
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_TIME);
            check(file_path, col_type, col_name, "[3600]", expected_rows);
        }
    }
    {
        const std::string col_name = "uint8";
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_TINYINT);
            check(file_path, col_type, col_name, "[5]", expected_rows);
        }
    }
    {
        const std::string col_name = "uint16";
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_SMALLINT);
            check(file_path, col_type, col_name, "[6767]", expected_rows);
        }
    }
    {
        const std::string col_name = "uint32";
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_INT);
            check(file_path, col_type, col_name, "[67676767]", expected_rows);
        }
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT);
            check(file_path, col_type, col_name, "[67676767]", expected_rows);
        }
    }
    {
        const std::string col_name = "date";
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_DATE);
            check(file_path, col_type, col_name, "[2023-04-25]", expected_rows);
        }
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_DATETIME);
            check(file_path, col_type, col_name, "[2023-04-25 00:00:00]", expected_rows);
        }
    }
    {
        const std::string col_name = "decimal32";
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_DECIMAL, -1, 9, 2);
            check(file_path, col_type, col_name, "[77.58]", expected_rows, true);
        }
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_DECIMALV2, -1, 9, 2);
            check(file_path, col_type, col_name, "[77.58]", expected_rows);
        }
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_DECIMAL32, -1, 9, 2);
            check(file_path, col_type, col_name, "[77.58]", expected_rows);
        }
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_DECIMAL64, -1, 9, 2);
            check(file_path, col_type, col_name, "[77.58]", expected_rows);
        }
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_DECIMAL128, -1, 9, 1);
            check(file_path, col_type, col_name, "[77.5]", expected_rows);
        }
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_DECIMAL128, -1, 9, 2);
            check(file_path, col_type, col_name, "[77.58]", expected_rows);
        }
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_DECIMAL128, -1, 9, 3);
            check(file_path, col_type, col_name, "[77.580]", expected_rows);
        }
    }
}

TEST_F(ColumnConverterTest, Int64Test) {
    const std::string file_path = "./be/test/formats/parquet/test_data/column_converter/int64.parquet";
    const size_t expected_rows = 5;

    {
        const std::string col_name = "int64";
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_TINYINT);
            check(file_path, col_type, col_name, "[78]", expected_rows);
        }
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_SMALLINT);
            check(file_path, col_type, col_name, "[-25010]", expected_rows);
        }
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_INT);
            check(file_path, col_type, col_name, "[-7758258]", expected_rows);
        }
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT);
            check(file_path, col_type, col_name, "[-7758258]", expected_rows);
        }
    }
    {
        const std::string col_name = "uint64";
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT);
            check(file_path, col_type, col_name, "[7758258]", expected_rows);
        }
    }
    {
        const std::string col_name = "decimal64";
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_DECIMALV2, -1);
            check(file_path, col_type, col_name, "[77.58258]", expected_rows);
        }
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_DECIMAL32, -1, 9, 5);
            check(file_path, col_type, col_name, "[77.58258]", expected_rows);
        }
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_DECIMAL64, -1, 18, 5);
            check(file_path, col_type, col_name, "[77.58258]", expected_rows);
        }
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_DECIMAL128, -1, 18, 5);
            check(file_path, col_type, col_name, "[77.58258]", expected_rows);
        }
    }
    {
        const std::string col_name = "time_micros";
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_TIME);
            check(file_path, col_type, col_name, "[3600]", expected_rows);
        }
    }
    {
        const std::string col_name = "time_nanos";
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_TIME);
            check(file_path, col_type, col_name, "[3.6e+06]", expected_rows);
        }
    }
    {
        const std::string col_name = "timestamp_millis";
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_DATETIME);
            check(file_path, col_type, col_name, "[2023-04-25 20:20:10]", expected_rows);
        }
    }
    {
        const std::string col_name = "timestamp_micros";
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_DATETIME);
            check(file_path, col_type, col_name, "[2023-04-25 20:20:10]", expected_rows);
        }
    }
    {
        const std::string col_name = "timestamp_nanos";
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_DATETIME);
            check(file_path, col_type, col_name, "[2023-04-25 20:20:10]", expected_rows);
        }
    }
}

TEST_F(ColumnConverterTest, FLBATest) {
    const std::string file_path = "./be/test/formats/parquet/test_data/column_converter/fixed_len_byte_array.parquet";
    const size_t expected_rows = 5;

    {
        const std::string col_name = "uuid";
        {
            // FIXED_LEN_BYTE_ARRAY (UUID logical type) -> VARCHAR: bytes formatted as xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR);
            check(file_path, col_type, col_name, "['61626344-6546-4768-696a-6b4c6d6e4f70']", expected_rows);
        }
        {
            // FIXED_LEN_BYTE_ARRAY (UUID) -> VARBINARY: raw 16 bytes pass through without conversion
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_VARBINARY);
            check(file_path, col_type, col_name, "['abcDeFGhijkLmnOp']", expected_rows);
        }
    }
    {
        const std::string col_name = "decimal_flba";
        // flba's decimal result maybe wrong
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_DECIMAL);
            check(file_path, col_type, col_name, "[680.96]", expected_rows, true);
        }
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_DECIMALV2, -1, 5, 2);
            check(file_path, col_type, col_name, "[682.56]", expected_rows);
        }
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_DECIMAL32, -1, 5, 2);
            check(file_path, col_type, col_name, "[682.56]", expected_rows);
        }
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_DECIMAL64, -1, 5, 2);
            check(file_path, col_type, col_name, "[682.56]", expected_rows);
        }
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_DECIMAL128, -1, 5, 1);
            check(file_path, col_type, col_name, "[682.5]", expected_rows);
        }
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_DECIMAL128, -1, 5, 2);
            check(file_path, col_type, col_name, "[682.56]", expected_rows);
        }
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_DECIMAL128, -1, 5, 3);
            check(file_path, col_type, col_name, "[682.560]", expected_rows);
        }
    }
    {
        const std::string col_name = "interval";
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_TIME);
            check(file_path, col_type, col_name, "[6809.6]", expected_rows, true);
        }
    }
}

// Tests FIXED_LEN_BYTE_ARRAY with UUID logical type annotation.
// The parquet column carries raw 16-byte UUIDs; the converter must format them
// as "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx" strings.
TEST_F(ColumnConverterTest, FLBAUUIDTest) {
    const std::string file_path =
            "./be/test/formats/parquet/test_data/column_converter/fixed_len_byte_array_uuid.parquet";
    const size_t expected_rows = 5;

    {
        const std::string col_name = "uuid";
        // UUID logical type -> VARCHAR: bytes formatted as xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR);
            check(file_path, col_type, col_name, "['b4f20d71-755e-572f-95c1-518871b9ca71']", expected_rows);
        }
    }
}

TEST_F(ColumnConverterTest, Int96Test) {
    const std::string file_path = "./be/test/formats/parquet/test_data/column_converter/int96.parquet";
    const size_t expected_rows = 5;

    {
        const std::string col_name = "timestamp_int96";
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_DATETIME);
            check(file_path, col_type, col_name, "[2023-04-29 00:40:22.618760]", expected_rows);
        }
    }
}

TEST_F(ColumnConverterTest, Int96TimeZoneTest) {
    const std::string file_path =
            "./be/test/formats/parquet/test_data/column_converter/int96_timestamp_timezone.parquet";
    const size_t expected_rows = 2;

    {
        const std::string col_name = "time_with_new_york";
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_DATETIME);
            check(file_path, col_type, col_name, "[2019-04-17 04:00:00]", expected_rows);
        }
    }
    {
        const std::string col_name = "time_with_shanghai";
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_DATETIME);
            check(file_path, col_type, col_name, "[2019-04-17 04:00:00]", expected_rows);
        }
    }
    {
        const std::string col_name = "time_without_timezone";
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_DATETIME);
            check(file_path, col_type, col_name, "[2019-04-17 04:00:00]", expected_rows);
        }
    }
}

TEST_F(ColumnConverterTest, FloatTest) {
    const std::string file_path = "./be/test/formats/parquet/test_data/column_converter/float.parquet";
    const size_t expected_rows = 5;

    {
        const std::string col_name = "float";
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_FLOAT);
            check(file_path, col_type, col_name, "[-5.55]", expected_rows);
        }
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_DOUBLE);
            check(file_path, col_type, col_name, "[-5.55]", expected_rows);
        }
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_INT);
            check(file_path, col_type, col_name, "[-5.55]", expected_rows, true);
        }
    }
}

TEST_F(ColumnConverterTest, DoubleTest) {
    const std::string file_path = "./be/test/formats/parquet/test_data/column_converter/double.parquet";
    const size_t expected_rows = 5;

    {
        const std::string col_name = "double";
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_DOUBLE);
            check(file_path, col_type, col_name, "[-5.55556]", expected_rows);
        }
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_INT);
            check(file_path, col_type, col_name, "[-5.55556]", expected_rows, true);
        }
    }
}

TEST_F(ColumnConverterTest, BooleanTest) {
    const std::string file_path = "./be/test/formats/parquet/test_data/column_converter/boolean.parquet";
    const size_t expected_rows = 5;

    {
        const std::string col_name = "boolean";
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_BOOLEAN);
            check(file_path, col_type, col_name, "[1]", expected_rows);
        }
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_INT);
            check(file_path, col_type, col_name, "[1]", expected_rows, true);
        }
    }
}

// Regression test for unsigned 32-bit integers physically stored as parquet INT32.
// A UINT_32 column (logical INTEGER bitWidth=32 isSigned=false, or the legacy
// UINT_32 converted type) must be loaded into a wider StarRocks column by
// ZERO-extending the raw 32 bits, not sign-extending. Before the fix the high-bit
// value 3000000000 (0xB2D05E00) was sign-extended to -1294967296 -> silent
// data corruption.
TEST_F(ColumnConverterTest, UnsignedInt32ZeroExtend) {
    // 3000000000 has bit 31 set; its int32 reinterpretation is negative.
    const int32_t kHighBits = static_cast<int32_t>(3000000000u); // == -1294967296
    const int64_t kExpectedUnsigned = 3000000000LL;

    auto make_src = [](int32_t bits) {
        auto data = FixedLengthColumn<int32_t>::create();
        data->get_data().push_back(bits);
        auto nulls = NullColumn::create();
        nulls->get_data().push_back(0);
        return NullableColumn::create(std::move(data), std::move(nulls));
    };

    // use_logical_type selects logicalType.INTEGER vs the legacy converted_type encoding.
    auto make_field = [](bool use_logical_type, bool is_signed) {
        ParquetField field;
        field.scale = 0;
        field.precision = 0;
        field.physical_type = tparquet::Type::INT32;
        if (use_logical_type) {
            field.schema_element.__isset.logicalType = true;
            field.schema_element.logicalType.__isset.INTEGER = true;
            field.schema_element.logicalType.INTEGER.bitWidth = 32;
            field.schema_element.logicalType.INTEGER.isSigned = is_signed;
        } else {
            field.schema_element.__isset.converted_type = true;
            field.schema_element.converted_type =
                    is_signed ? tparquet::ConvertedType::INT_32 : tparquet::ConvertedType::UINT_32;
        }
        return field;
    };

    auto run_bigint = [&](const ParquetField& field) -> int64_t {
        const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT);
        std::unique_ptr<ColumnConverter> converter;
        Status st = ColumnConverterFactory::create_converter(field, col_type, "UTC", &converter);
        EXPECT_TRUE(st.ok()) << st.message();
        auto src = make_src(kHighBits);
        auto dst = ColumnHelper::create_column(col_type, true);
        st = converter->convert(src.get(), dst.get());
        EXPECT_TRUE(st.ok()) << st.message();
        auto* dst_nullable = down_cast<NullableColumn*>(dst.get());
        auto* dst_data = down_cast<FixedLengthColumn<int64_t>*>(dst_nullable->data_column_raw_ptr());
        return dst_data->get_data()[0];
    };

    // UINT_32 via modern logical type -> BIGINT must zero-extend.
    EXPECT_EQ(kExpectedUnsigned, run_bigint(make_field(/*use_logical_type=*/true, /*is_signed=*/false)));
    // UINT_32 via legacy converted type -> BIGINT must zero-extend.
    EXPECT_EQ(kExpectedUnsigned, run_bigint(make_field(/*use_logical_type=*/false, /*is_signed=*/false)));
    // Signed INT_32 with the same bits must still sign-extend (no regression).
    EXPECT_EQ(static_cast<int64_t>(kHighBits), run_bigint(make_field(/*use_logical_type=*/true, /*is_signed=*/true)));

    // A present-but-non-INTEGER logical type takes precedence over a contradictory
    // legacy UINT_32 converted type: the column must NOT be treated as unsigned.
    {
        ParquetField field;
        field.scale = 0;
        field.precision = 0;
        field.physical_type = tparquet::Type::INT32;
        field.schema_element.__isset.logicalType = true; // present, but INTEGER not set
        field.schema_element.__isset.converted_type = true;
        field.schema_element.converted_type = tparquet::ConvertedType::UINT_32;
        EXPECT_EQ(static_cast<int64_t>(kHighBits), run_bigint(field));
    }

    // UINT_32 -> DOUBLE must also widen via the unsigned value.
    {
        const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_DOUBLE);
        ParquetField field = make_field(/*use_logical_type=*/false, /*is_signed=*/false);
        std::unique_ptr<ColumnConverter> converter;
        Status st = ColumnConverterFactory::create_converter(field, col_type, "UTC", &converter);
        EXPECT_TRUE(st.ok()) << st.message();
        auto src = make_src(kHighBits);
        auto dst = ColumnHelper::create_column(col_type, true);
        st = converter->convert(src.get(), dst.get());
        EXPECT_TRUE(st.ok()) << st.message();
        auto* dst_nullable = down_cast<NullableColumn*>(dst.get());
        auto* dst_data = down_cast<FixedLengthColumn<double>*>(dst_nullable->data_column_raw_ptr());
        EXPECT_DOUBLE_EQ(static_cast<double>(kExpectedUnsigned), dst_data->get_data()[0]);
    }

    // UINT_32 -> DECIMAL128(38,0) must zero-extend the unscaled value.
    {
        const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_DECIMAL128, -1, 38, 0);
        ParquetField field = make_field(/*use_logical_type=*/false, /*is_signed=*/false);
        field.precision = 38;
        std::unique_ptr<ColumnConverter> converter;
        Status st = ColumnConverterFactory::create_converter(field, col_type, "UTC", &converter);
        EXPECT_TRUE(st.ok()) << st.message();
        auto src = make_src(kHighBits);
        auto dst = ColumnHelper::create_column(col_type, true);
        st = converter->convert(src.get(), dst.get());
        EXPECT_TRUE(st.ok()) << st.message();
        auto* dst_nullable = down_cast<NullableColumn*>(dst.get());
        auto* dst_data = down_cast<Decimal128Column*>(dst_nullable->data_column_raw_ptr());
        EXPECT_EQ(kExpectedUnsigned, static_cast<int64_t>(dst_data->get_data()[0]));
    }
}

// Footer-statistics counterpart of the zero-extension fix. The converter is shared
// with statistics decoding, so an unsigned INT32 column's min/max must be validated
// with UNSIGNED Parquet sort order. Pre-1.10 parquet-mr writers stored the deprecated
// min/max with SIGNED ordering, which would invert once decoded as unsigned, so those
// legacy stats must be rejected for an unsigned column instead of trusted.
TEST_F(ColumnConverterTest, UnsignedInt32StatsRejectsLegacySignedOrder) {
    // Old parquet-mr writer (< 1.10.0): deprecated min/max are signed-ordered.
    tparquet::FileMetaData t_meta;
    t_meta.__set_version(1);
    t_meta.__set_num_rows(2);
    t_meta.__set_created_by("parquet-mr version 1.0.0 (build test)");
    tparquet::SchemaElement root_sch;
    root_sch.__set_name("schema");
    root_sch.__set_num_children(1);
    tparquet::SchemaElement leaf_sch;
    leaf_sch.__set_name("u");
    leaf_sch.__set_type(tparquet::Type::INT32);
    t_meta.__set_schema({root_sch, leaf_sch});
    FileMetaData file_meta;
    ASSERT_TRUE(file_meta.init(t_meta, false).ok());

    // Deprecated min/max present and differing (raw little-endian int32 bytes 1 and 2).
    tparquet::ColumnMetaData column_meta;
    column_meta.__set_type(tparquet::Type::INT32);
    tparquet::Statistics stats;
    stats.__set_min(std::string("\x01\x00\x00\x00", 4));
    stats.__set_max(std::string("\x02\x00\x00\x00", 4));
    column_meta.__set_statistics(stats);

    const TypeDescriptor bigint = TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT);

    auto eval = [&](bool is_signed) {
        ParquetField field;
        field.physical_type = tparquet::Type::INT32;
        field.schema_element.__isset.logicalType = true;
        field.schema_element.logicalType.__isset.INTEGER = true;
        field.schema_element.logicalType.INTEGER.bitWidth = 32;
        field.schema_element.logicalType.INTEGER.isSigned = is_signed;
        std::vector<std::string> mins;
        std::vector<std::string> maxs;
        return StatisticsHelper::get_min_max_value(&file_meta, bigint, &column_meta, &field, mins, maxs);
    };

    // Unsigned column -> UNSIGNED sort order -> legacy signed-ordered stats are rejected.
    EXPECT_FALSE(eval(/*is_signed=*/false).ok());
    // Signed column -> SIGNED sort order -> legacy stats are accepted (no regression).
    EXPECT_TRUE(eval(/*is_signed=*/true).ok());
}

TEST_F(ColumnConverterTest, Int64_2_Timestamp) {
    const std::string file_path = "./be/test/formats/parquet/test_data/column_converter/int64_2_timestamp.parquet";
    const size_t expected_rows = 5;

    {
        const std::string col_name = "timestamp_millis";
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_DATETIME);
            check(file_path, col_type, col_name, "[2023-04-25 20:20:10]", expected_rows);
        }
    }
    {
        const std::string col_name = "timestamp_micros";
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_DATETIME);
            check(file_path, col_type, col_name, "[2023-04-25 20:20:10]", expected_rows);
        }
    }
}

// A pre-1970 (negative epoch tick) timestamp with a nonzero sub-second component must decode to the
// correct wall clock. C++ truncating division splits a negative tick into a too-high second and a
// negative sub-second; without a floor-borrow that negative sub-second corrupts the packed DATETIME.
TEST_F(ColumnConverterTest, Int64PreEpochTimestampSubSecond) {
    const std::string file_path =
            "./be/test/formats/parquet/test_data/column_converter/int64_timestamp_pre_epoch.parquet";
    const size_t expected_rows = 5;
    const std::string expected_value = "[1969-12-31 23:59:59.500000]";

    {
        const std::string col_name = "timestamp_millis";
        const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_DATETIME);
        check(file_path, col_type, col_name, expected_value, expected_rows);
    }
    {
        const std::string col_name = "timestamp_micros";
        const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_DATETIME);
        check(file_path, col_type, col_name, expected_value, expected_rows);
    }
}

// Paimon TIMESTAMP (NTZ) columns are written as INT96 for precision 7-9. Unlike Hive/Spark INT96
// (which stores a UTC instant that must be shifted into the session/local timezone), a Paimon NTZ
// value is a naive wall clock that must be kept as-is. The TypeDescriptor.datetime_is_ntz flag,
// plumbed from FE via TScalarType, selects the behavior in Int96ToDateTimeConverter.
TEST_F(ColumnConverterTest, Int96ReadAsTimestampNtz) {
    // Encode wall clock 2024-01-15 12:30:45 into an INT96 the way Int96ToDateTimeConverter decodes it:
    //   timestamp = (hi << TIMESTAMP_BITS) | (lo / 1000)
    const Timestamp packed = timestamp::from_datetime(2024, 1, 15, 12, 30, 45, 0);
    int96_t raw;
    raw.hi = static_cast<uint32_t>(packed >> TIMESTAMP_BITS);
    raw.lo = (packed & ((static_cast<uint64_t>(1) << TIMESTAMP_BITS) - 1)) * 1000;

    ParquetField field;
    field.physical_type = tparquet::Type::INT96;
    const std::string timezone = "Asia/Shanghai";

    auto read_back = [&](bool datetime_is_ntz) -> Timestamp {
        auto src_data = FixedLengthColumn<int96_t>::create();
        src_data->append(raw);
        auto src_null = NullColumn::create();
        src_null->append(0);
        ColumnPtr src = NullableColumn::create(std::move(src_data), std::move(src_null));

        TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_DATETIME);
        col_type.datetime_is_ntz = datetime_is_ntz;

        std::unique_ptr<ColumnConverter> converter;
        CHECK(ColumnConverterFactory::create_converter(field, col_type, timezone, &converter).ok());

        auto dst = ColumnHelper::create_column(col_type, true);
        CHECK(converter->convert(src.get(), dst.get()).ok());

        auto* dst_nullable = down_cast<NullableColumn*>(dst.get());
        const auto* ts_col = down_cast<const TimestampColumn*>(dst_nullable->data_column().get());
        return ts_col->get_data()[0].timestamp();
    };

    // NTZ (Paimon TIMESTAMP): wall clock kept as-is, no session/local timezone shift.
    EXPECT_EQ(packed, read_back(true));
    // Default (Hive/Spark INT96): shifted into the local timezone (+8h for Asia/Shanghai).
    EXPECT_EQ(timestamp::add<TimeUnit::SECOND>(packed, 8 * 3600), read_back(false));
}

namespace {

// Nullable source column; null rows hold extreme values, since the converters may compute them too.
template <typename T>
ColumnPtr make_nullable_source(const std::vector<T>& values, const std::vector<uint8_t>& nulls) {
    auto data = FixedLengthColumn<T>::create();
    auto null_column = NullColumn::create();
    for (size_t i = 0; i < values.size(); i++) {
        data->append(nulls[i] ? std::numeric_limits<T>::max() : values[i]);
        null_column->append(nulls[i]);
    }
    return NullableColumn::create(std::move(data), std::move(null_column));
}

ColumnPtr make_byte_array_decimal_source(const std::vector<std::string>& values,
                                         const std::vector<uint8_t>& null_bits) {
    auto data = BinaryColumn::create();
    for (const auto& value : values) {
        data->append_string(value);
    }
    auto nulls = NullColumn::create();
    nulls->get_data().assign(null_bits.begin(), null_bits.end());
    return NullableColumn::create(std::move(data), std::move(nulls));
}

// The INT64 timestamp conversion the converter implements, with a runtime tick unit.
Timestamp reference_int64_timestamp(int64_t ticks, int64_t second_mask, const cctz::time_zone* ctz) {
    int64_t seconds = ticks / second_mask;
    int64_t nanoseconds = (ticks % second_mask) * (NANOSECS_PER_SEC / second_mask);
    if (nanoseconds < 0) {
        seconds -= 1;
        nanoseconds += NANOSECS_PER_SEC;
    }
    if (ctz != nullptr) {
        seconds += timestamp::get_timezone_offset_by_epoch_seconds(seconds, *ctz);
    }
    return timestamp::of_epoch_second(seconds, nanoseconds);
}

} // namespace

// Every non-null row of INT64 TIMESTAMP(MILLIS/MICROS/NANOS) matches the reference conversion, for a UTC
// session (one offset per batch), zones with offset changes (per-row offsets) and a non-adjusted (NTZ) column,
// including pre-epoch ticks with a sub-second part.
TEST_F(ColumnConverterTest, Int64TimestampUnitsMatchReference) {
    std::mt19937_64 rng(29);
    std::vector<int64_t> seconds_values;
    std::vector<uint8_t> nulls;
    for (int i = 0; i < 4099; i++) {
        // roughly 1900 .. 2100
        seconds_values.push_back(static_cast<int64_t>(rng() % 6311390400ULL) - 2208988800LL);
        nulls.push_back(i % 7 == 3);
    }
    struct Unit {
        int64_t mask;
        tparquet::TimeUnit unit;
    };
    std::vector<Unit> units(3);
    units[0].mask = 1000;
    units[0].unit.__set_MILLIS(tparquet::MilliSeconds());
    units[1].mask = 1000000;
    units[1].unit.__set_MICROS(tparquet::MicroSeconds());
    units[2].mask = 1000000000;
    units[2].unit.__set_NANOS(tparquet::NanoSeconds());

    for (const auto& unit : units) {
        std::vector<int64_t> ticks;
        for (size_t i = 0; i < seconds_values.size(); i++) {
            int64_t sub = static_cast<int64_t>(rng() % static_cast<uint64_t>(unit.mask));
            ticks.push_back(seconds_values[i] * unit.mask + (i % 2 ? sub : -sub));
        }
        for (const auto& [timezone, adjusted] : std::vector<std::pair<std::string, bool>>{
                     {"UTC", true}, {"Asia/Shanghai", true}, {"Europe/London", true}, {"UTC", false}}) {
            SCOPED_TRACE(fmt::format("mask={} tz={} adjusted={}", unit.mask, timezone, adjusted));
            ParquetField field;
            field.physical_type = tparquet::Type::INT64;
            field.schema_element.type = tparquet::Type::INT64;
            tparquet::TimestampType ts;
            ts.isAdjustedToUTC = adjusted;
            ts.unit = unit.unit;
            tparquet::LogicalType logical_type;
            logical_type.__set_TIMESTAMP(ts);
            field.schema_element.__set_logicalType(logical_type);

            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_DATETIME);
            std::unique_ptr<ColumnConverter> converter;
            ASSERT_OK(ColumnConverterFactory::create_converter(field, col_type, timezone, &converter));
            auto src = make_nullable_source(ticks, nulls);
            auto dst = ColumnHelper::create_column(col_type, true);
            ASSERT_OK(converter->convert(src.get(), dst.get()));

            cctz::time_zone ctz;
            ASSERT_TRUE(TimezoneUtils::find_cctz_time_zone(timezone, ctz));
            auto* dst_nullable = down_cast<NullableColumn*>(dst.get());
            const auto& dst_data = down_cast<TimestampColumn*>(dst_nullable->data_column_raw_ptr())->get_data();
            for (size_t i = 0; i < ticks.size(); i++) {
                ASSERT_EQ(nulls[i], dst_nullable->is_null(i)) << "row " << i;
                if (!nulls[i]) {
                    ASSERT_EQ(reference_int64_timestamp(ticks[i], unit.mask, adjusted ? &ctz : nullptr),
                              dst_data[i].timestamp())
                            << "row " << i << " ticks " << ticks[i];
                }
            }
        }
    }
}

// A DST zone shifts each batch by the offset at its values, whether the batch lies in one offset window (one
// offset for all rows) or spans a transition, independent of the offset at query time.
TEST_F(ColumnConverterTest, Int64TimestampDstBatches) {
    ParquetField field;
    field.physical_type = tparquet::Type::INT64;
    field.schema_element.type = tparquet::Type::INT64;
    tparquet::TimestampType ts;
    ts.isAdjustedToUTC = true;
    ts.unit.__set_MICROS(tparquet::MicroSeconds());
    tparquet::LogicalType logical_type;
    logical_type.__set_TIMESTAMP(ts);
    field.schema_element.__set_logicalType(logical_type);
    const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_DATETIME);
    std::unique_ptr<ColumnConverter> converter;
    ASSERT_OK(ColumnConverterFactory::create_converter(field, col_type, "Europe/London", &converter));

    const int64_t summer = 1719835200;  // 2024-07-01 12:00:00 UTC
    const int64_t winter = 1704110400;  // 2024-01-01 12:00:00 UTC
    const int64_t bst_end = 1729990800; // 2024-10-27 01:00:00 UTC
    std::vector<std::pair<std::vector<int64_t>, std::vector<std::string>>> batches = {
            {{summer, summer + 60}, {"2024-07-01 13:00:00", "2024-07-01 13:01:00"}},
            {{winter, winter + 60}, {"2024-01-01 12:00:00", "2024-01-01 12:01:00"}},
            {{bst_end - 1800, bst_end + 600, summer, winter},
             {"2024-10-27 01:30:00", "2024-10-27 01:10:00", "2024-07-01 13:00:00", "2024-01-01 12:00:00"}}};
    for (const auto& [seconds, want] : batches) {
        std::vector<int64_t> micros;
        for (int64_t s : seconds) {
            micros.push_back(s * 1000000);
        }
        auto src = make_nullable_source(micros, std::vector<uint8_t>(micros.size(), 0));
        auto dst = ColumnHelper::create_column(col_type, true);
        ASSERT_OK(converter->convert(src.get(), dst.get()));
        for (size_t i = 0; i < want.size(); i++) {
            EXPECT_EQ(want[i], dst->debug_item(i)) << seconds[i];
        }
    }
}

// Paired min/max statistics of a zone-adjusted INT64 timestamp convert to bounds that hold every value between them,
// though local times are not monotonic across a backward shift (2024-10-27 00:30 UTC loads as 01:30 BST, 01:10 UTC
// as 01:10 GMT).
TEST_F(ColumnConverterTest, Int64TimestampMinMaxHoldValuesAcrossDst) {
    ParquetField field;
    field.physical_type = tparquet::Type::INT64;
    field.schema_element.type = tparquet::Type::INT64;
    tparquet::TimestampType ts;
    ts.isAdjustedToUTC = true;
    ts.unit.__set_MICROS(tparquet::MicroSeconds());
    tparquet::LogicalType logical_type;
    logical_type.__set_TIMESTAMP(ts);
    field.schema_element.__set_logicalType(logical_type);
    const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_DATETIME);

    std::mt19937_64 rng(43);
    for (const std::string zone : {"Europe/London", "Antarctica/Casey", "UTC"}) {
        SCOPED_TRACE(zone);
        std::unique_ptr<ColumnConverter> converter;
        ASSERT_OK(ColumnConverterFactory::create_converter(field, col_type, zone, &converter));
        std::vector<int64_t> lo = {1729989000LL * 1000000, 1520692200LL * 1000000};
        std::vector<int64_t> hi = {1729991400LL * 1000000, 1520701200LL * 1000000};
        for (int i = 0; i < 300; i++) {
            int64_t a = (static_cast<int64_t>(rng() % 4000000000ULL) - 1000000000LL) * 1000000;
            lo.push_back(a);
            hi.push_back(a + static_cast<int64_t>(rng() % (86400ULL * 400 * 1000000)));
        }
        std::vector<uint8_t> nulls(lo.size(), 0);
        nulls[5] = 1;
        auto min_src = make_nullable_source(lo, nulls);
        auto max_src = make_nullable_source(hi, nulls);
        auto min_dst = ColumnHelper::create_column(col_type, true);
        auto max_dst = ColumnHelper::create_column(col_type, true);
        ASSERT_OK(converter->convert_min_max(min_src.get(), max_src.get(), min_dst.get(), max_dst.get()));
        ASSERT_TRUE(min_dst->is_null(5));
        for (size_t i = 0; i < lo.size(); i++) {
            if (nulls[i]) {
                continue;
            }
            std::vector<int64_t> values;
            for (int64_t v = lo[i]; v <= hi[i]; v += std::max<int64_t>(1, (hi[i] - lo[i]) / 499)) {
                values.push_back(v);
            }
            values.push_back(hi[i]);
            auto src = make_nullable_source(values, std::vector<uint8_t>(values.size(), 0));
            auto dst = ColumnHelper::create_column(col_type, true);
            ASSERT_OK(converter->convert(src.get(), dst.get()));
            const TimestampValue min_bound = min_dst->get(i).get_timestamp();
            const TimestampValue max_bound = max_dst->get(i).get_timestamp();
            for (size_t j = 0; j < values.size(); j++) {
                const TimestampValue v = dst->get(j).get_timestamp();
                ASSERT_LE(min_bound, v) << "pair " << i << " value " << values[j];
                ASSERT_GE(max_bound, v) << "pair " << i << " value " << values[j];
            }
        }
    }
}

// INT96 values are shifted by the offset in force at 00:00 UTC of their day, one offset per batch when all days
// lie in one offset window, else one per row.
TEST_F(ColumnConverterTest, Int96TimestampDstBatches) {
    ParquetField field;
    field.physical_type = tparquet::Type::INT96;
    const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_DATETIME);
    cctz::time_zone london;
    ASSERT_TRUE(TimezoneUtils::find_cctz_time_zone("Europe/London", london));
    std::unique_ptr<ColumnConverter> converter;
    ASSERT_OK(ColumnConverterFactory::create_converter(field, col_type, "Europe/London", &converter));

    std::mt19937_64 rng(31);
    for (int64_t span_days : {1, 2000}) {
        std::vector<int96_t> values;
        std::vector<Timestamp> packed;
        std::vector<uint8_t> nulls;
        for (int i = 0; i < 1000; i++) {
            Timestamp t = timestamp::from_datetime(2020, 6, 1, 0, 0, 0, 0);
            t = timestamp::add<TimeUnit::SECOND>(t, static_cast<int64_t>(rng() % (span_days * 86400)));
            packed.push_back(t);
            int96_t raw;
            raw.hi = static_cast<uint32_t>(t >> TIMESTAMP_BITS);
            raw.lo = (t & ((static_cast<uint64_t>(1) << TIMESTAMP_BITS) - 1)) * 1000;
            values.push_back(raw);
            nulls.push_back(i % 9 == 4);
        }
        auto src = make_nullable_source(values, nulls);
        auto dst = ColumnHelper::create_column(col_type, true);
        ASSERT_OK(converter->convert(src.get(), dst.get()));
        auto* dst_nullable = down_cast<NullableColumn*>(dst.get());
        const auto& dst_data = down_cast<TimestampColumn*>(dst_nullable->data_column_raw_ptr())->get_data();
        for (size_t i = 0; i < packed.size(); i++) {
            ASSERT_EQ(nulls[i], dst_nullable->is_null(i)) << "row " << i;
            if (!nulls[i]) {
                int offset = timestamp::get_timezone_offset_by_timestamp(packed[i], london);
                ASSERT_EQ(timestamp::add<TimeUnit::SECOND>(packed[i], offset), dst_data[i].timestamp()) << "row " << i;
            }
        }
    }
}

// INT32 days read as DATETIME is midnight of that day, as TimestampValue::from_unix_second computes it,
// including days before 1970.
TEST_F(ColumnConverterTest, Int32DaysToDateTimeMatchesReference) {
    std::mt19937 rng(31);
    std::vector<int32_t> days;
    std::vector<uint8_t> nulls;
    for (int i = 0; i < 4099; i++) {
        days.push_back(static_cast<int32_t>(rng() % 146000) - 73000);
        nulls.push_back(i % 5 == 1);
    }
    ParquetField field;
    field.physical_type = tparquet::Type::INT32;
    field.schema_element.type = tparquet::Type::INT32;
    const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_DATETIME);
    std::unique_ptr<ColumnConverter> converter;
    ASSERT_OK(ColumnConverterFactory::create_converter(field, col_type, "UTC", &converter));
    auto src = make_nullable_source(days, nulls);
    auto dst = ColumnHelper::create_column(col_type, true);
    ASSERT_OK(converter->convert(src.get(), dst.get()));

    auto* dst_nullable = down_cast<NullableColumn*>(dst.get());
    const auto& dst_data = down_cast<TimestampColumn*>(dst_nullable->data_column_raw_ptr())->get_data();
    for (size_t i = 0; i < days.size(); i++) {
        ASSERT_EQ(nulls[i], dst_nullable->is_null(i)) << "row " << i;
        if (!nulls[i]) {
            TimestampValue expected;
            expected.from_unix_second(static_cast<int64_t>(days[i]) * 24 * 60 * 60, 0);
            ASSERT_EQ(expected.timestamp(), dst_data[i].timestamp()) << "row " << i << " day " << days[i];
        }
    }
}

TEST_F(ColumnConverterTest, ByteArrayDecimalValues) {
    ParquetField field;
    field.physical_column_index = 0;
    field.physical_type = tparquet::Type::BYTE_ARRAY;
    field.precision = 18;
    field.scale = 8;
    const TypeDescriptor type = TypeDescriptor::from_logical_type(TYPE_DECIMAL64, -1, 18, 8);
    std::unique_ptr<ColumnConverter> converter;
    ASSERT_OK(ColumnConverterFactory::create_converter(field, type, "UTC", &converter));

    auto src = make_byte_array_decimal_source(
            {std::string("\x07\x5b\xcd\x15", 4), std::string("\xff\x7f", 2), std::string("\x00", 1),
             std::string("\x00\x80", 2), std::string("\x55", 1), ""},
            {0, 0, 0, 0, 1, 0});
    auto dst = ColumnHelper::create_column(type, true);
    ASSERT_OK(converter->convert(src.get(), dst.get()));
    ASSERT_EQ(6, dst->size());
    EXPECT_TRUE(dst->has_null());
    EXPECT_EQ(123456789, dst->get(0).get_int64());
    EXPECT_EQ(-129, dst->get(1).get_int64());
    EXPECT_EQ(0, dst->get(2).get_int64());
    EXPECT_EQ(128, dst->get(3).get_int64());
    EXPECT_TRUE(dst->is_null(4));
    EXPECT_TRUE(dst->is_null(5));

    auto all_null = make_byte_array_decimal_source({std::string("\x42", 1)}, {1});
    ASSERT_OK(converter->convert(all_null.get(), dst.get()));
    ASSERT_EQ(1, dst->size());
    EXPECT_TRUE(dst->is_null(0));
    auto no_null = make_byte_array_decimal_source({std::string("\x2a", 1)}, {0});
    ASSERT_OK(converter->convert(no_null.get(), dst.get()));
    ASSERT_EQ(1, dst->size());
    EXPECT_FALSE(dst->has_null());
    EXPECT_EQ(42, dst->get(0).get_int64());

    const TypeDescriptor wide_type = TypeDescriptor::from_logical_type(TYPE_DECIMAL128, -1, 38, 8);
    ASSERT_OK(ColumnConverterFactory::create_converter(field, wide_type, "UTC", &converter));
    auto wide_src = make_byte_array_decimal_source(
            {std::string("\x00\x80\x00\x00\x00\x00\x00\x00\x00", 9), std::string("\x7f", 1) + std::string(15, '\xff')},
            {0, 0});
    auto wide_dst = ColumnHelper::create_column(wide_type, true);
    ASSERT_OK(converter->convert(wide_src.get(), wide_dst.get()));
    EXPECT_EQ(static_cast<int128_t>(1) << 63, wide_dst->get(0).get_int128());
    EXPECT_EQ(std::numeric_limits<int128_t>::max(), wide_dst->get(1).get_int128());
}

TEST_F(ColumnConverterTest, ByteArrayDecimalScaleAndTargets) {
    ParquetField field;
    field.physical_column_index = 0;
    field.physical_type = tparquet::Type::BYTE_ARRAY;
    field.precision = 18;
    field.scale = 2;
    auto src = make_byte_array_decimal_source({std::string("\xfb\x2e", 2)}, {0});

    for (LogicalType target : {TYPE_DECIMALV2, TYPE_DECIMAL32, TYPE_DECIMAL64, TYPE_DECIMAL128}) {
        const int precision =
                target == TYPE_DECIMAL32 ? 9 : target == TYPE_DECIMAL64 ? 18 : target == TYPE_DECIMALV2 ? 27 : 38;
        const int scale = target == TYPE_DECIMALV2 ? 9 : 2;
        const TypeDescriptor type = TypeDescriptor::from_logical_type(target, -1, precision, scale);
        std::unique_ptr<ColumnConverter> converter;
        ASSERT_OK(ColumnConverterFactory::create_converter(field, type, "UTC", &converter));
        auto dst = ColumnHelper::create_column(type, true);
        ASSERT_OK(converter->convert(src.get(), dst.get()));
        ASSERT_EQ(1, dst->size());
        EXPECT_FALSE(dst->has_null());
        switch (target) {
        case TYPE_DECIMALV2:
            EXPECT_EQ(-12340000000LL, dst->get(0).get_decimal().value());
            break;
        case TYPE_DECIMAL32:
            EXPECT_EQ(-1234, dst->get(0).get_int32());
            break;
        case TYPE_DECIMAL64:
            EXPECT_EQ(-1234, dst->get(0).get_int64());
            break;
        case TYPE_DECIMAL128:
            EXPECT_EQ(-1234, dst->get(0).get_int128());
            break;
        default:
            FAIL();
        }
    }

    for (int dst_scale : {1, 3}) {
        const TypeDescriptor type = TypeDescriptor::from_logical_type(TYPE_DECIMAL64, -1, 18, dst_scale);
        std::unique_ptr<ColumnConverter> converter;
        ASSERT_OK(ColumnConverterFactory::create_converter(field, type, "UTC", &converter));
        auto dst = ColumnHelper::create_column(type, true);
        ASSERT_OK(converter->convert(src.get(), dst.get()));
        EXPECT_EQ(dst_scale == 1 ? -123 : -12340, dst->get(0).get_int64());
    }
}

TEST_F(ColumnConverterTest, ByteArrayDecimalBounds) {
    ParquetField field;
    field.physical_column_index = 0;
    field.physical_type = tparquet::Type::BYTE_ARRAY;
    field.precision = 38;
    field.scale = 0;
    const TypeDescriptor wide_type = TypeDescriptor::from_logical_type(TYPE_DECIMAL128, -1, 38, 0);
    std::unique_ptr<ColumnConverter> converter;
    ASSERT_OK(ColumnConverterFactory::create_converter(field, wide_type, "UTC", &converter));

    auto src = make_byte_array_decimal_source(
            {std::string(16, '\x00') + std::string("\x01", 1), std::string(16, '\xff') + std::string("\xfe", 1),
             std::string("\x7f", 1) + std::string(15, '\xff'), std::string("\x80", 1) + std::string(15, '\x00')},
            {0, 0, 0, 0});
    auto dst = ColumnHelper::create_column(wide_type, true);
    ASSERT_OK(converter->convert(src.get(), dst.get()));
    EXPECT_EQ(1, dst->get(0).get_int128());
    EXPECT_EQ(-2, dst->get(1).get_int128());
    EXPECT_EQ(std::numeric_limits<int128_t>::max(), dst->get(2).get_int128());
    EXPECT_EQ(std::numeric_limits<int128_t>::min(), dst->get(3).get_int128());

    src = make_byte_array_decimal_source(
            {std::string("\x01", 1), std::string("\x01", 1) + std::string(16, '\x00'), std::string("\x02", 1)},
            {0, 0, 0});
    Status st = converter->convert(src.get(), dst.get());
    EXPECT_TRUE(st.is_invalid_argument()) << st.to_string();
    EXPECT_NE(std::string::npos, st.message().find("row 1"));

    for (auto [target, payload] : {std::pair{TYPE_DECIMAL32, std::string("\x00\x80\x00\x00\x00", 5)},
                                   std::pair{TYPE_DECIMAL64, std::string("\x00\x80\x00\x00\x00\x00\x00\x00\x00", 9)}}) {
        const int precision = target == TYPE_DECIMAL32 ? 9 : 18;
        const TypeDescriptor type = TypeDescriptor::from_logical_type(target, -1, precision, 0);
        ASSERT_OK(ColumnConverterFactory::create_converter(field, type, "UTC", &converter));
        src = make_byte_array_decimal_source({payload}, {0});
        dst = ColumnHelper::create_column(type, true);
        EXPECT_TRUE(converter->convert(src.get(), dst.get()).is_invalid_argument());
    }

    const TypeDescriptor scale_up_type = TypeDescriptor::from_logical_type(TYPE_DECIMAL128, -1, 38, 38);
    ASSERT_OK(ColumnConverterFactory::create_converter(field, scale_up_type, "UTC", &converter));
    src = make_byte_array_decimal_source({std::string("\x02", 1)}, {0});
    dst = ColumnHelper::create_column(scale_up_type, true);
    EXPECT_TRUE(converter->convert(src.get(), dst.get()).is_invalid_argument());

    for (int bad_scale : {-1, 39}) {
        field.scale = bad_scale;
        EXPECT_TRUE(
                ColumnConverterFactory::create_converter(field, wide_type, "UTC", &converter).is_invalid_argument());
        field.scale = 0;
        TypeDescriptor invalid_target = wide_type;
        invalid_target.scale = bad_scale;
        EXPECT_TRUE(ColumnConverterFactory::create_converter(field, invalid_target, "UTC", &converter)
                            .is_invalid_argument());
    }
}

TEST_F(ColumnConverterTest, DecimalScaleAndStorageBounds) {
    struct Case {
        int precision;
        int source_scale;
        LogicalType target;
        int target_scale;
        int128_t value;
        int128_t expected;
        bool overflow;
    };
    const int128_t ten37 = get_scale_factor<int128_t>(37);
    const int128_t max38 = get_scale_factor<int128_t>(38) - 1;
    const std::vector<Case> cases = {
            {9, 0, TYPE_DECIMAL32, 1, 999999999, 0, true},
            {9, 0, TYPE_DECIMAL32, 1, 99999999, 999999990, false},
            {18, 0, TYPE_DECIMAL32, 0, 3000000000LL, 0, true},
            {18, 0, TYPE_DECIMAL32, 0, 2147483647LL, 2147483647LL, false},
            {18, 0, TYPE_DECIMAL32, 0, 2147483649LL, 0, true},
            {18, 0, TYPE_DECIMAL64, 1, 999999999999999999LL, 0, true},
            {18, 0, TYPE_DECIMAL64, 1, 922337203685477580LL, 9223372036854775800LL, false},
            {18, 0, TYPE_DECIMAL64, 1, 922337203685477581LL, 0, true},
            {18, 1, TYPE_DECIMAL64, 18, 1, 100000000000000000LL, false},
            {9, 2, TYPE_DECIMAL32, 1, 1234, 123, false},
            {9, 2, TYPE_DECIMALV2, 9, 1234, 12340000000LL, false},
            {9, 1, TYPE_DECIMAL128, 38, 1, ten37, false},
            {9, 0, TYPE_DECIMAL128, 38, 2, 0, true},
            {38, 38, TYPE_DECIMAL64, 18, max38, 999999999999999999LL, false},
            {38, 38, TYPE_DECIMAL32, 0, max38, 0, false},
    };
    for (auto physical : {tparquet::Type::INT32, tparquet::Type::INT64, tparquet::Type::FIXED_LEN_BYTE_ARRAY,
                          tparquet::Type::BYTE_ARRAY}) {
        for (const auto& test : cases) {
            if (physical == tparquet::Type::INT32 && test.precision > 9) continue;
            if (physical == tparquet::Type::INT64 && test.precision > 18) continue;
            for (int sign : {1, -1}) {
                SCOPED_TRACE(
                        fmt::format("physical={}, precision={}, source_scale={}, target={}, target_scale={}, "
                                    "sign={}, overflow={}",
                                    static_cast<int>(physical), test.precision, test.source_scale,
                                    static_cast<int>(test.target), test.target_scale, sign, test.overflow));
                const int128_t value = sign * test.value;
                const int128_t expected = sign * test.expected;
                const int width = test.precision <= 9 ? 4 : test.precision <= 18 ? 8 : 16;
                const int target_precision =
                        test.target == TYPE_DECIMAL32
                                ? 9
                                : test.target == TYPE_DECIMAL64 ? 18 : test.target == TYPE_DECIMALV2 ? 27 : 38;
                const auto type =
                        TypeDescriptor::from_logical_type(test.target, -1, target_precision, test.target_scale);
                ParquetField field;
                field.physical_column_index = 0;
                field.physical_type = physical;
                field.type_length = width;
                field.precision = test.precision;
                field.scale = test.source_scale;
                std::unique_ptr<ColumnConverter> converter;
                ASSERT_OK(ColumnConverterFactory::create_converter(field, type, "UTC", &converter));
                auto src = converter->create_src_column();
                ASSERT_TRUE(src->append_nulls(1));
                auto* src_nullable = down_cast<NullableColumn*>(src.get());
                const int32_t value32 = static_cast<int32_t>(value);
                const int64_t value64 = static_cast<int64_t>(value);
                std::string bytes(width, '\0');
                uint128_t bits = static_cast<uint128_t>(value);
                for (int i = width - 1; i >= 0; --i) {
                    bytes[i] = static_cast<char>(bits & 0xff);
                    bits >>= 8;
                }
                if (physical == tparquet::Type::BYTE_ARRAY) {
                    while (bytes.size() > 1 &&
                           ((static_cast<uint8_t>(bytes[0]) == 0 && static_cast<uint8_t>(bytes[1]) < 0x80) ||
                            (static_cast<uint8_t>(bytes[0]) == 0xff && static_cast<uint8_t>(bytes[1]) >= 0x80))) {
                        bytes.erase(0, 1);
                    }
                }
                if (physical == tparquet::Type::INT32) {
                    auto* data = down_cast<FixedLengthColumn<int32_t>*>(src_nullable->data_column_raw_ptr());
                    data->get_data()[0] = std::numeric_limits<int32_t>::max();
                    ASSERT_EQ(1, src->append_numbers(&value32, sizeof(value32)));
                } else if (physical == tparquet::Type::INT64) {
                    auto* data = down_cast<FixedLengthColumn<int64_t>*>(src_nullable->data_column_raw_ptr());
                    data->get_data()[0] = std::numeric_limits<int64_t>::max();
                    ASSERT_EQ(1, src->append_numbers(&value64, sizeof(value64)));
                } else {
                    ASSERT_TRUE(src->append_strings(std::vector<Slice>{Slice(bytes)}));
                }

                auto check_values = [&](const Column* column) {
                    ASSERT_EQ(2, column->size());
                    ASSERT_TRUE(column->is_null(0));
                    ASSERT_FALSE(column->is_null(1));
                    switch (test.target) {
                    case TYPE_DECIMAL32:
                        EXPECT_EQ(expected, column->get(1).get_int32());
                        break;
                    case TYPE_DECIMAL64:
                        EXPECT_EQ(expected, column->get(1).get_int64());
                        break;
                    case TYPE_DECIMAL128:
                        EXPECT_EQ(expected, column->get(1).get_int128());
                        break;
                    case TYPE_DECIMALV2:
                        EXPECT_EQ(expected, column->get(1).get_decimal().value());
                        break;
                    default:
                        FAIL();
                    }
                };
                auto dst = ColumnHelper::create_column(type, true);
                Status status = converter->convert(src.get(), dst.get());
                if (test.overflow) {
                    ASSERT_TRUE(status.is_invalid_argument()) << status.to_string();
                } else {
                    ASSERT_OK(status);
                    check_values(dst.get());
                }
                src->resize(1);
                ASSERT_OK(converter->convert(src.get(), dst.get()));
                ASSERT_EQ(1, dst->size());
                EXPECT_TRUE(dst->has_null());
                EXPECT_TRUE(dst->is_null(0));

                auto node = ::parquet::schema::PrimitiveNode::Make(
                        "value", ::parquet::Repetition::OPTIONAL,
                        ::parquet::LogicalType::Decimal(test.precision, test.source_scale),
                        static_cast<::parquet::Type::type>(physical), width);
                auto schema = std::static_pointer_cast<::parquet::schema::GroupNode>(
                        ::parquet::schema::GroupNode::Make("root", ::parquet::Repetition::REQUIRED, {node}));
                auto sink_result = arrow::io::BufferOutputStream::Create();
                ASSERT_TRUE(sink_result.ok());
                auto sink = sink_result.ValueOrDie();
                ::parquet::WriterProperties::Builder properties;
                auto writer =
                        ::parquet::ParquetFileWriter::Open(sink, schema, properties.disable_dictionary()->build());
                auto* writer_column = writer->AppendRowGroup()->NextColumn();
                const int16_t levels[] = {0, 1};
                if (physical == tparquet::Type::INT32) {
                    static_cast<::parquet::Int32Writer*>(writer_column)->WriteBatch(2, levels, nullptr, &value32);
                } else if (physical == tparquet::Type::INT64) {
                    static_cast<::parquet::Int64Writer*>(writer_column)->WriteBatch(2, levels, nullptr, &value64);
                } else if (physical == tparquet::Type::BYTE_ARRAY) {
                    const ::parquet::ByteArray encoded(bytes.size(), reinterpret_cast<const uint8_t*>(bytes.data()));
                    static_cast<::parquet::ByteArrayWriter*>(writer_column)->WriteBatch(2, levels, nullptr, &encoded);
                } else {
                    const ::parquet::FixedLenByteArray encoded(reinterpret_cast<const uint8_t*>(bytes.data()));
                    static_cast<::parquet::FixedLenByteArrayWriter*>(writer_column)
                            ->WriteBatch(2, levels, nullptr, &encoded);
                }
                writer->Close();
                auto buffer_result = sink->Finish();
                ASSERT_TRUE(buffer_result.ok());
                auto buffer = buffer_result.ValueOrDie();
                auto file = std::make_shared<RandomAccessFile>(
                        std::make_shared<io::StringInputStream>(buffer->ToString()), "decimal-overflow.parquet");
                FileReader reader(8, file.get(), buffer->size());
                auto* ctx = _create_scan_context();
                Utils::SlotDesc slots[] = {{"value", type, 0}, {""}};
                auto* tuple = Utils::create_tuple_descriptor(nullptr, &_pool, slots);
                Utils::make_column_info_vector(tuple, &ctx->format_scan_context.materialized_columns);
                ctx->slot_descs = tuple->slots();
                ctx->format_scan_context.scan_range_offset = 0;
                ctx->format_scan_context.scan_range_length = buffer->size();
                ASSERT_OK(reader.init(&ctx->format_scan_context));
                auto chunk = std::make_shared<Chunk>();
                chunk->append_column(ColumnHelper::create_column(type, true), 0);
                status = reader.get_next(&chunk);
                if (test.overflow) {
                    ASSERT_TRUE(status.is_invalid_argument()) << status.to_string();
                } else {
                    ASSERT_OK(status);
                    check_values(chunk->get_column_by_index(0).get());
                }
            }
        }
    }
}

TEST_F(ColumnConverterTest, DecimalInvalidScales) {
    const auto type = TypeDescriptor::from_logical_type(TYPE_DECIMAL128, -1, 38, 0);
    for (auto physical : {tparquet::Type::INT32, tparquet::Type::INT64, tparquet::Type::FIXED_LEN_BYTE_ARRAY,
                          tparquet::Type::BYTE_ARRAY}) {
        ParquetField field;
        field.physical_type = physical;
        field.type_length = 8;
        field.precision = physical == tparquet::Type::INT32 ? 9 : 18;
        std::unique_ptr<ColumnConverter> converter;
        for (int scale : {-1, 39, std::numeric_limits<int32_t>::min(), std::numeric_limits<int32_t>::max()}) {
            field.scale = scale;
            EXPECT_TRUE(ColumnConverterFactory::create_converter(field, type, "UTC", &converter).is_invalid_argument());
            field.scale = 0;
            auto invalid_type = type;
            invalid_type.scale = scale;
            EXPECT_TRUE(ColumnConverterFactory::create_converter(field, invalid_type, "UTC", &converter)
                                .is_invalid_argument());
        }
    }
}

TEST_F(ColumnConverterTest, WideFixedByteArrayDecimalValues) {
    struct Target {
        LogicalType type;
        int precision;
        int scale;
        int64_t negative;
        int64_t positive;
    };
    const std::vector<Target> targets = {
            {TYPE_DECIMALV2, 27, 9, -12340000000LL, 1280000000LL},
            {TYPE_DECIMAL32, 9, 1, -123, 12},
            {TYPE_DECIMAL32, 9, 2, -1234, 128},
            {TYPE_DECIMAL32, 9, 3, -12340, 1280},
            {TYPE_DECIMAL64, 18, 1, -123, 12},
            {TYPE_DECIMAL64, 18, 2, -1234, 128},
            {TYPE_DECIMAL64, 18, 3, -12340, 1280},
            {TYPE_DECIMAL128, 38, 1, -123, 12},
            {TYPE_DECIMAL128, 38, 2, -1234, 128},
            {TYPE_DECIMAL128, 38, 3, -12340, 1280},
    };
    for (int width : {17, 20}) {
        SCOPED_TRACE(width);
        ParquetField field;
        field.physical_column_index = 0;
        field.physical_type = tparquet::Type::FIXED_LEN_BYTE_ARRAY;
        field.type_length = width;
        field.precision = 18;
        field.scale = 2;
        const std::string negative = std::string(width - 2, '\xff') + std::string("\xfb\x2e", 2);
        const std::string positive = std::string(width - 2, '\0') + std::string("\x00\x80", 2);
        // Include a null row and an empty non-null placeholder left by filtering.
        auto src =
                make_byte_array_decimal_source({negative, positive, std::string(width, '\0'), "", ""}, {0, 0, 0, 1, 0});
        for (const auto& target : targets) {
            SCOPED_TRACE(target.type);
            SCOPED_TRACE(target.scale);
            const auto type = TypeDescriptor::from_logical_type(target.type, -1, target.precision, target.scale);
            std::unique_ptr<ColumnConverter> converter;
            ASSERT_OK(ColumnConverterFactory::create_converter(field, type, "UTC", &converter));
            auto dst = ColumnHelper::create_column(type, true);
            ASSERT_OK(converter->convert(src.get(), dst.get()));
            ASSERT_EQ(5, dst->size());
            EXPECT_TRUE(dst->is_null(3));
            EXPECT_TRUE(dst->is_null(4));
            for (size_t row = 0; row < 3; ++row) {
                ASSERT_FALSE(dst->is_null(row));
                const int64_t expected = row == 0 ? target.negative : row == 1 ? target.positive : 0;
                switch (target.type) {
                case TYPE_DECIMALV2:
                    EXPECT_EQ(expected, dst->get(row).get_decimal().value());
                    break;
                case TYPE_DECIMAL32:
                    EXPECT_EQ(expected, dst->get(row).get_int32());
                    break;
                case TYPE_DECIMAL64:
                    EXPECT_EQ(expected, dst->get(row).get_int64());
                    break;
                case TYPE_DECIMAL128:
                    EXPECT_EQ(expected, dst->get(row).get_int128());
                    break;
                default:
                    FAIL();
                }
            }
            auto no_null = make_byte_array_decimal_source({positive}, {0});
            ASSERT_OK(converter->convert(no_null.get(), dst.get()));
            ASSERT_EQ(1, dst->size());
            EXPECT_FALSE(dst->has_null());
        }

        const auto type = TypeDescriptor::from_logical_type(TYPE_DECIMAL64, -1, 18, 2);
        auto min_column = ColumnHelper::create_column(type, true);
        auto max_column = ColumnHelper::create_column(type, true);
        ASSERT_OK(StatisticsHelper::decode_min_max_into_columns(min_column, max_column, {negative, ""}, {positive, ""},
                                                                {false, true}, type, &field, "UTC"));
        ASSERT_EQ(2, min_column->size());
        ASSERT_EQ(2, max_column->size());
        ASSERT_FALSE(min_column->is_null(0));
        ASSERT_FALSE(max_column->is_null(0));
        EXPECT_EQ(-1234, min_column->get(0).get_int64());
        EXPECT_EQ(128, max_column->get(0).get_int64());
        EXPECT_TRUE(min_column->is_null(1));
        EXPECT_TRUE(max_column->is_null(1));
    }
}

TEST_F(ColumnConverterTest, WideFixedByteArrayDecimalBounds) {
    for (int width : {17, 20}) {
        SCOPED_TRACE(width);
        ParquetField field;
        field.physical_column_index = 0;
        field.physical_type = tparquet::Type::FIXED_LEN_BYTE_ARRAY;
        field.type_length = width;
        field.precision = 38;
        field.scale = 0;
        const auto type = TypeDescriptor::from_logical_type(TYPE_DECIMAL128, -1, 38, 0);
        std::unique_ptr<ColumnConverter> converter;
        ASSERT_OK(ColumnConverterFactory::create_converter(field, type, "UTC", &converter));
        auto dst = ColumnHelper::create_column(type, true);
        for (const auto& payload :
             {std::string(width - 16, '\0') + std::string("\x80", 1) + std::string(15, '\0'),
              std::string(width - 16, '\xff') + std::string("\x7f", 1) + std::string(15, '\xff')}) {
            auto src = make_byte_array_decimal_source({payload}, {0});
            EXPECT_TRUE(converter->convert(src.get(), dst.get()).is_invalid_argument());
        }

        for (LogicalType target : {TYPE_DECIMAL32, TYPE_DECIMAL64}) {
            const int precision = target == TYPE_DECIMAL32 ? 9 : 18;
            const int value_bytes = target == TYPE_DECIMAL32 ? 4 : 8;
            const auto narrow_type = TypeDescriptor::from_logical_type(target, -1, precision, 0);
            ASSERT_OK(ColumnConverterFactory::create_converter(field, narrow_type, "UTC", &converter));
            dst = ColumnHelper::create_column(narrow_type, true);
            const std::string payload = std::string(width - value_bytes, '\0') + std::string("\x80", 1) +
                                        std::string(value_bytes - 1, '\0');
            auto src = make_byte_array_decimal_source({payload}, {0});
            EXPECT_TRUE(converter->convert(src.get(), dst.get()).is_invalid_argument());
        }

        const auto scale_up_type = TypeDescriptor::from_logical_type(TYPE_DECIMAL128, -1, 38, 38);
        ASSERT_OK(ColumnConverterFactory::create_converter(field, scale_up_type, "UTC", &converter));
        auto src = make_byte_array_decimal_source({std::string(width - 1, '\0') + std::string("\x02", 1)}, {0});
        dst = ColumnHelper::create_column(scale_up_type, true);
        EXPECT_TRUE(converter->convert(src.get(), dst.get()).is_invalid_argument());

        for (int scale : {-1, 39}) {
            field.scale = scale;
            EXPECT_TRUE(ColumnConverterFactory::create_converter(field, type, "UTC", &converter).is_invalid_argument());
            field.scale = 0;
            auto invalid_type = type;
            invalid_type.scale = scale;
            EXPECT_TRUE(ColumnConverterFactory::create_converter(field, invalid_type, "UTC", &converter)
                                .is_invalid_argument());
        }
    }
}

TEST_F(ColumnConverterTest, ByteArrayDecimalFilteredPlaceholders) {
    std::string page;
    for (const std::string& value : {std::string("\x01", 1), std::string("\x7f", 1), std::string("\xff", 1)}) {
        const uint32_t length = value.size();
        for (int i = 0; i < 4; ++i) {
            page.push_back(static_cast<char>(length >> (8 * i)));
        }
        page += value;
    }
    PlainDecoder<Slice> decoder;
    ASSERT_OK(decoder.set_data(Slice(page.data(), page.size())));
    auto src = NullableColumn::create(BinaryColumn::create(), NullColumn::create());
    Filter filter{1, 0, 1};
    ASSERT_OK(decoder.next_batch(filter.size(), ColumnContentType::VALUE, src.get(), filter.data()));
    ASSERT_EQ(3, src->size());
    EXPECT_FALSE(src->is_null(1));
    EXPECT_TRUE(down_cast<BinaryColumn*>(src->data_column_raw_ptr())->get_slice(1).empty());

    ParquetField field;
    field.physical_column_index = 0;
    field.physical_type = tparquet::Type::BYTE_ARRAY;
    field.precision = 18;
    field.scale = 8;
    const TypeDescriptor type = TypeDescriptor::from_logical_type(TYPE_DECIMAL64, -1, 18, 8);
    std::unique_ptr<ColumnConverter> converter;
    ASSERT_OK(ColumnConverterFactory::create_converter(field, type, "UTC", &converter));
    auto dst = ColumnHelper::create_column(type, true);
    ASSERT_OK(converter->convert(src.get(), dst.get()));
    EXPECT_EQ(1, dst->get(0).get_int64());
    EXPECT_TRUE(dst->is_null(1));
    EXPECT_EQ(-1, dst->get(2).get_int64());
    dst->filter(filter);
    ASSERT_EQ(2, dst->size());
    EXPECT_FALSE(dst->has_null());
    EXPECT_EQ(1, dst->get(0).get_int64());
    EXPECT_EQ(-1, dst->get(1).get_int64());
}

TEST_F(ColumnConverterTest, ByteArrayDecimalNested) {
    const bool old_late = config::parquet_late_materialization_enable;
    config::parquet_late_materialization_enable = true;
    DeferOp restore([&] { config::parquet_late_materialization_enable = old_late; });
    auto* runtime_state = _pool.add(new RuntimeState(TQueryGlobals()));
    auto fragment_dict_state = std::make_unique<FragmentDictState>();
    runtime_state->set_fragment_dict_state(fragment_dict_state.get());
    const auto decimal_type = TypeDescriptor::from_logical_type(TYPE_DECIMAL64, -1, 18, 8);
    const auto struct_type = TypeDescriptor::create_struct_type({"price"}, {decimal_type});
    const auto int_type = TypeDescriptor::from_logical_type(TYPE_INT);

    for (bool dictionary : {false, true}) {
        auto id = ::parquet::schema::PrimitiveNode::Make("id", ::parquet::Repetition::REQUIRED, ::parquet::Type::INT32);
        auto price = ::parquet::schema::PrimitiveNode::Make("price", ::parquet::Repetition::OPTIONAL,
                                                            ::parquet::LogicalType::Decimal(18, 8),
                                                            ::parquet::Type::BYTE_ARRAY);
        auto payload = ::parquet::schema::GroupNode::Make("payload", ::parquet::Repetition::OPTIONAL, {price});
        auto schema = std::static_pointer_cast<::parquet::schema::GroupNode>(
                ::parquet::schema::GroupNode::Make("root", ::parquet::Repetition::REQUIRED, {id, payload}));
        auto sink_result = arrow::io::BufferOutputStream::Create();
        ASSERT_TRUE(sink_result.ok());
        auto sink = sink_result.ValueOrDie();
        ::parquet::WriterProperties::Builder properties;
        if (dictionary)
            properties.enable_dictionary();
        else
            properties.disable_dictionary();
        auto writer = ::parquet::ParquetFileWriter::Open(sink, schema, properties.build());
        auto* group = writer->AppendRowGroup();
        const int32_t ids[] = {1, 2, 3, 4, 5};
        static_cast<::parquet::Int32Writer*>(group->NextColumn())->WriteBatch(5, nullptr, nullptr, ids);
        const int16_t levels[] = {2, 2, 2, 0, 1};
        const uint8_t raw[] = {1, 255, 0};
        const ::parquet::ByteArray values[] = {::parquet::ByteArray(1, &raw[0]), ::parquet::ByteArray(1, &raw[1]),
                                               ::parquet::ByteArray(1, &raw[2])};
        static_cast<::parquet::ByteArrayWriter*>(group->NextColumn())->WriteBatch(5, levels, nullptr, values);
        writer->Close();
        auto result = sink->Finish();
        ASSERT_TRUE(result.ok());
        auto buffer = result.ValueOrDie();

        auto apache_reader = ::parquet::ParquetFileReader::Open(std::make_shared<arrow::io::BufferReader>(buffer));
        auto row_metadata = apache_reader->metadata()->RowGroup(0);
        auto meta = row_metadata->ColumnChunk(1);
        ASSERT_EQ(::parquet::Type::BYTE_ARRAY, meta->type());
        bool data_encoding_seen = false;
        for (const auto& stat : meta->encoding_stats()) {
            if (stat.page_type == ::parquet::PageType::DATA_PAGE ||
                stat.page_type == ::parquet::PageType::DATA_PAGE_V2) {
                EXPECT_EQ(dictionary ? ::parquet::Encoding::RLE_DICTIONARY : ::parquet::Encoding::PLAIN, stat.encoding);
                data_encoding_seen = true;
            }
        }
        ASSERT_TRUE(data_encoding_seen);
        auto row_group = apache_reader->RowGroup(0);
        auto column = row_group->Column(1);
        auto* byte_reader = static_cast<::parquet::ByteArrayReader*>(column.get());
        int16_t actual_levels[5];
        ::parquet::ByteArray actual_values[5];
        int64_t values_read = 0;
        ASSERT_EQ(5, byte_reader->ReadBatch(5, actual_levels, nullptr, actual_values, &values_read));
        ASSERT_EQ(3, values_read);
        for (int i = 0; i < 5; ++i) EXPECT_EQ(levels[i], actual_levels[i]);
        for (int i = 0; i < 3; ++i) {
            ASSERT_EQ(1, actual_values[i].len);
            EXPECT_EQ(raw[i], actual_values[i].ptr[0]);
        }

        for (int projection = 0; projection < 3; ++projection) {
            auto file = std::make_shared<RandomAccessFile>(std::make_shared<io::StringInputStream>(buffer->ToString()),
                                                           "nested-decimal.parquet");
            auto reader = std::make_shared<FileReader>(2, file.get(), buffer->size());
            auto* ctx = _create_scan_context();
            Utils::SlotDesc id_only[] = {{"id", int_type, 0}, {""}};
            Utils::SlotDesc both[] = {{"id", int_type, 0}, {"payload", struct_type, 1}, {""}};
            auto* tuple = Utils::create_tuple_descriptor(runtime_state, &_pool, projection == 0 ? id_only : both);
            Utils::make_column_info_vector(tuple, &ctx->format_scan_context.materialized_columns);
            ctx->slot_descs = tuple->slots();
            ctx->format_scan_context.scan_range_offset = 0;
            ctx->format_scan_context.scan_range_length = buffer->size();
            if (projection == 2) {
                std::vector<TExpr> conjuncts;
                ParquetUTBase::append_int_conjunct(TExprOpcode::GT, 0, 1, &conjuncts);
                auto& exprs = ctx->format_scan_context.conjunct_ctxs_by_slot[0];
                ParquetUTBase::create_conjunct_ctxs(&_pool, runtime_state, &conjuncts, &exprs);
                ParquetUTBase::setup_conjuncts_manager(exprs, nullptr, tuple, runtime_state, ctx);
            }
            ASSERT_OK(reader->init(&ctx->format_scan_context));
            const auto& footer = reader->get_file_metadata()->t_metadata();
            ASSERT_EQ(4, footer.schema.size());
            EXPECT_EQ(tparquet::Type::BYTE_ARRAY, footer.schema[3].type);
            EXPECT_TRUE(footer.schema[3].__isset.precision);
            EXPECT_TRUE(footer.schema[3].__isset.scale);
            EXPECT_EQ(18, footer.schema[3].precision);
            EXPECT_EQ(8, footer.schema[3].scale);
            EXPECT_TRUE(footer.schema[3].__isset.logicalType);
            EXPECT_TRUE(footer.schema[3].logicalType.__isset.DECIMAL);
            if (projection == 2) {
                const auto& lazy = reader->group_readers()[0]->_column_materializer->lazy_slot_ids();
                EXPECT_NE(lazy.end(), std::find(lazy.begin(), lazy.end(), 1));
            }
            auto chunk = std::make_shared<Chunk>();
            chunk->append_column(ColumnHelper::create_column(int_type, true), 0);
            if (projection != 0) chunk->append_column(ColumnHelper::create_column(struct_type, true), 1);
            std::vector<int32_t> observed;
            for (;;) {
                chunk->reset();
                Status status = reader->get_next(&chunk);
                if (status.is_end_of_file()) break;
                ASSERT_OK(status);
                for (size_t row = 0; row < chunk->num_rows(); ++row) {
                    const int32_t current_id = chunk->get_column_by_index(0)->get(row).get_int32();
                    observed.push_back(current_id);
                    if (projection == 0) continue;
                    const auto& parent = chunk->get_column_by_index(1);
                    EXPECT_EQ(current_id == 4, parent->is_null(row));
                    if (current_id == 4) continue;
                    auto* structure = down_cast<const StructColumn*>(ColumnHelper::get_data_column(parent.get()));
                    auto field = structure->field_column("price");
                    ASSERT_TRUE(field.ok());
                    EXPECT_EQ(current_id == 5, field.value()->is_null(row));
                    if (current_id <= 3) {
                        EXPECT_EQ(current_id == 1 ? 1 : current_id == 2 ? -1 : 0, field.value()->get(row).get_int64());
                    }
                }
            }
            EXPECT_EQ(projection == 2 ? std::vector<int32_t>({2, 3, 4, 5}) : std::vector<int32_t>({1, 2, 3, 4, 5}),
                      observed);
        }
    }
}

TEST_F(ColumnConverterTest, ByteArrayDecimalNullability) {
    auto* runtime_state = _pool.add(new RuntimeState(TQueryGlobals()));
    auto fragment_dict_state = std::make_unique<FragmentDictState>();
    runtime_state->set_fragment_dict_state(fragment_dict_state.get());
    const auto int_type = TypeDescriptor::from_logical_type(TYPE_INT);
    const int16_t nested_levels[] = {0, 1, 0, 1, 0, 1, 2, 0, 1, 2, 1, 2};
    const int16_t scalar_levels[] = {0, 0, 0, 0, 0, 0, 1, 0, 1, 0, 1, 0};
    const int32_t ids[] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11};
    const uint8_t raw[] = {0, 255, 1};
    const ::parquet::ByteArray values[] = {::parquet::ByteArray(1, &raw[0]), ::parquet::ByteArray(1, &raw[1]),
                                           ::parquet::ByteArray(1, &raw[2])};

    for (bool dictionary : {false, true}) {
        for (int groups : {1, 2}) {
            SCOPED_TRACE(::testing::Message() << "dictionary=" << dictionary << " groups=" << groups);
            auto id = ::parquet::schema::PrimitiveNode::Make("id", ::parquet::Repetition::REQUIRED,
                                                             ::parquet::Type::INT32);
            auto scalar = ::parquet::schema::PrimitiveNode::Make("scalar", ::parquet::Repetition::OPTIONAL,
                                                                 ::parquet::LogicalType::Decimal(18, 8),
                                                                 ::parquet::Type::BYTE_ARRAY);
            auto price = ::parquet::schema::PrimitiveNode::Make("price", ::parquet::Repetition::OPTIONAL,
                                                                ::parquet::LogicalType::Decimal(18, 8),
                                                                ::parquet::Type::BYTE_ARRAY);
            auto payload = ::parquet::schema::GroupNode::Make("payload", ::parquet::Repetition::OPTIONAL, {price});
            auto schema = std::static_pointer_cast<::parquet::schema::GroupNode>(
                    ::parquet::schema::GroupNode::Make("root", ::parquet::Repetition::REQUIRED, {id, scalar, payload}));
            auto sink_result = arrow::io::BufferOutputStream::Create();
            ASSERT_TRUE(sink_result.ok());
            auto sink = sink_result.ValueOrDie();
            ::parquet::WriterProperties::Builder properties;
            if (dictionary)
                properties.enable_dictionary();
            else
                properties.disable_dictionary();
            auto writer = ::parquet::ParquetFileWriter::Open(sink, schema, properties.build());
            for (int group_index = 0; group_index < groups; ++group_index) {
                auto* group = writer->AppendRowGroup();
                static_cast<::parquet::Int32Writer*>(group->NextColumn())
                        ->WriteBatch(6, nullptr, nullptr, ids + 6 * group_index);
                static_cast<::parquet::ByteArrayWriter*>(group->NextColumn())
                        ->WriteBatch(6, scalar_levels + 6 * group_index, nullptr, values);
                static_cast<::parquet::ByteArrayWriter*>(group->NextColumn())
                        ->WriteBatch(6, nested_levels + 6 * group_index, nullptr, values);
            }
            writer->Close();
            auto result = sink->Finish();
            ASSERT_TRUE(result.ok());
            auto buffer = result.ValueOrDie();

            auto apache_reader = ::parquet::ParquetFileReader::Open(std::make_shared<arrow::io::BufferReader>(buffer));
            ASSERT_EQ(groups, apache_reader->metadata()->num_row_groups());
            for (int group_index = 0; group_index < groups; ++group_index) {
                for (int column_index : {1, 2}) {
                    if (group_index == 1) {
                        bool data_encoding_seen = false;
                        auto row_metadata = apache_reader->metadata()->RowGroup(group_index);
                        auto column_metadata = row_metadata->ColumnChunk(column_index);
                        for (const auto& stat : column_metadata->encoding_stats()) {
                            if (stat.page_type == ::parquet::PageType::DATA_PAGE ||
                                stat.page_type == ::parquet::PageType::DATA_PAGE_V2) {
                                EXPECT_EQ(dictionary ? ::parquet::Encoding::RLE_DICTIONARY : ::parquet::Encoding::PLAIN,
                                          stat.encoding);
                                data_encoding_seen = true;
                            }
                        }
                        ASSERT_TRUE(data_encoding_seen);
                    }
                    auto group = apache_reader->RowGroup(group_index);
                    auto column = group->Column(column_index);
                    auto* byte_reader = static_cast<::parquet::ByteArrayReader*>(column.get());
                    int16_t actual_levels[6];
                    ::parquet::ByteArray actual_values[6];
                    int64_t values_read = 0;
                    ASSERT_EQ(6, byte_reader->ReadBatch(6, actual_levels, nullptr, actual_values, &values_read));
                    ASSERT_EQ(group_index == 0 ? 0 : 3, values_read);
                    const int16_t* expected = column_index == 1 ? scalar_levels : nested_levels;
                    for (int i = 0; i < 6; ++i) EXPECT_EQ(expected[6 * group_index + i], actual_levels[i]);
                    for (int i = 0; i < values_read; ++i) {
                        ASSERT_EQ(1, actual_values[i].len);
                        EXPECT_EQ(raw[i], actual_values[i].ptr[0]);
                    }
                }
            }

            for (LogicalType target : {TYPE_DECIMALV2, TYPE_DECIMAL32, TYPE_DECIMAL64, TYPE_DECIMAL128}) {
                SCOPED_TRACE(target);
                int precision = target == TYPE_DECIMAL32
                                        ? 9
                                        : target == TYPE_DECIMAL64 ? 18 : target == TYPE_DECIMALV2 ? 27 : 38;
                int scale = target == TYPE_DECIMALV2 ? 9 : 8;
                auto decimal_type = TypeDescriptor::from_logical_type(target, -1, precision, scale);
                auto struct_type = TypeDescriptor::create_struct_type({"price"}, {decimal_type});
                auto file = std::make_shared<RandomAccessFile>(
                        std::make_shared<io::StringInputStream>(buffer->ToString()), "nullable-decimal.parquet");
                auto reader = std::make_shared<FileReader>(2, file.get(), buffer->size());
                auto* ctx = _create_scan_context();
                Utils::SlotDesc slots[] = {
                        {"id", int_type, 0}, {"scalar", decimal_type, 1}, {"payload", struct_type, 2}, {""}};
                auto* tuple = Utils::create_tuple_descriptor(runtime_state, &_pool, slots);
                Utils::make_column_info_vector(tuple, &ctx->format_scan_context.materialized_columns);
                ctx->slot_descs = tuple->slots();
                ctx->format_scan_context.scan_range_offset = 0;
                ctx->format_scan_context.scan_range_length = buffer->size();
                ASSERT_OK(reader->init(&ctx->format_scan_context));
                auto chunk = std::make_shared<Chunk>();
                chunk->append_column(ColumnHelper::create_column(int_type, true), 0);
                chunk->append_column(ColumnHelper::create_column(decimal_type, true), 1);
                chunk->append_column(ColumnHelper::create_column(struct_type, true), 2);
                int observed = 0;
                for (;;) {
                    chunk->reset();
                    Status status = reader->get_next(&chunk);
                    if (status.is_end_of_file()) break;
                    ASSERT_OK(status);
                    for (size_t row = 0; row < chunk->num_rows(); ++row) {
                        ASSERT_LT(observed, groups * 6);
                        EXPECT_EQ(observed, chunk->get_column_by_index(0)->get(row).get_int32());
                        auto scalar_column = chunk->get_column_by_index(1);
                        auto parent = chunk->get_column_by_index(2);
                        EXPECT_EQ(scalar_levels[observed] == 0, scalar_column->is_null(row));
                        EXPECT_EQ(nested_levels[observed] == 0, parent->is_null(row));
                        auto* structure = down_cast<const StructColumn*>(ColumnHelper::get_data_column(parent.get()));
                        auto field = structure->field_column("price");
                        ASSERT_TRUE(field.ok());
                        if (nested_levels[observed] != 0) {
                            EXPECT_EQ(nested_levels[observed] == 1, field.value()->is_null(row));
                        }
                        auto check_value = [&](const ColumnPtr& column, int64_t expected) {
                            ASSERT_FALSE(column->is_null(row));
                            switch (target) {
                            case TYPE_DECIMALV2:
                                EXPECT_EQ(expected * 10, column->get(row).get_decimal().value());
                                break;
                            case TYPE_DECIMAL32:
                                EXPECT_EQ(expected, column->get(row).get_int32());
                                break;
                            case TYPE_DECIMAL64:
                                EXPECT_EQ(expected, column->get(row).get_int64());
                                break;
                            case TYPE_DECIMAL128:
                                EXPECT_EQ(expected, column->get(row).get_int128());
                                break;
                            default:
                                FAIL();
                            }
                        };
                        if (scalar_levels[observed] == 1)
                            check_value(scalar_column, observed == 6 ? 0 : observed == 8 ? -1 : 1);
                        if (nested_levels[observed] == 2)
                            check_value(field.value(), observed == 6 ? 0 : observed == 9 ? -1 : 1);
                        ++observed;
                    }
                }
                EXPECT_EQ(groups * 6, observed);
            }
        }
    }
}

} // namespace starrocks::parquet
