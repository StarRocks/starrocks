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

#include <gtest/gtest.h>

#include <filesystem>

#include "column/binary_column.h"
#include "column/column_helper.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
#include "common/config_exec_fwd.h"
#include "formats/parquet/encoding_dict.h"
#include "formats/parquet/encoding_plain.h"
#include "formats/parquet/file_reader.h"
#include "formats/parquet/metadata.h"
#include "formats/parquet/statistics_helper.h"
#include "fs/fs.h"
#include "parquet_test_util/util.h"
#include "runtime/descriptor_helper.h"

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

        ctx->lazy_column_coalesce_counter = lazy_column_coalesce_counter;
        ctx->timezone = "Asia/Shanghai";
        ctx->format_scan_context.stats = &g_hdfs_stats;
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
        Utils::make_column_info_vector(tuple_desc, &ctx->materialized_columns);
        ctx->slot_descs = tuple_desc->slots();
        ctx->scan_range = (_create_scan_range(filepath));
        // --------------finish init context---------------

        Status status = file_reader->init(ctx);
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
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_DECIMAL32);
            check(file_path, col_type, col_name, "[6.7]", expected_rows, true);
        }
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_DECIMAL64);
            check(file_path, col_type, col_name, "[6.7]", expected_rows, true);
        }
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_DECIMAL128);
            check(file_path, col_type, col_name, "[6.7]", expected_rows, true);
        }
        {
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_DECIMALV2);
            check(file_path, col_type, col_name, "[6.7]", expected_rows, true);
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
} // namespace starrocks::parquet
