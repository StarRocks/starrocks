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
#include <limits>
#include <random>

#include "base/testutil/assert.h"
#include "base/time/timezone_utils.h"
#include "base/types/int96.h"
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

} // namespace starrocks::parquet
