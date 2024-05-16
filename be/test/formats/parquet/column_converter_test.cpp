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
#include "column/fixed_length_column.h"
#include "formats/parquet/encoding_dict.h"
#include "formats/parquet/encoding_plain.h"
#include "formats/parquet/file_reader.h"
#include "fs/fs.h"
#include "parquet_test_util/util.h"
#include "runtime/descriptor_helper.h"

namespace starrocks::parquet {

static HdfsScanStats g_hdfs_scan_stats{};

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
        ctx->stats = &g_hdfs_scan_stats;
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

    static void check_chunk_values(std::shared_ptr<Chunk>& chunk, const std::string& expected_value) {
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
                                                        std::filesystem::file_size(filepath), 0);

        // --------------init context---------------
        auto ctx = _create_scan_context();

        Utils::SlotDesc slot_descs[] = {{col_name, col_type}, {""}};

        ctx->tuple_desc = Utils::create_tuple_descriptor(_runtime_state, &_pool, slot_descs);
        Utils::make_column_info_vector(ctx->tuple_desc, &ctx->materialized_columns);
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
            const TypeDescriptor col_type = TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR);
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
} // namespace starrocks::parquet
