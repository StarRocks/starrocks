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

#include "formats/parquet/parquet_cli_reader.h"

#include <gtest/gtest.h>

#include <filesystem>

#include "base/testutil/parallel_test.h"

namespace starrocks::parquet {
class ParquetCLIReaderTest : public testing::Test {
public:
    ParquetCLIReaderTest() = default;
    ~ParquetCLIReaderTest() override = default;

protected:
    void traverse_directory_add_parquet(std::vector<std::string>& paths, const std::filesystem::path& directory,
                                        const std::string& suffix = "") {
        for (const auto& entry : std::filesystem::directory_iterator(directory)) {
            if (entry.is_directory()) {
                traverse_directory_add_parquet(paths, entry.path(), suffix);
            } else if (entry.is_regular_file()) {
                const std::string& path = entry.path().string();
                if (!suffix.empty() && _end_with(path, suffix)) {
                    paths.emplace_back(path);
                } else if (suffix.empty()) {
                    paths.emplace_back(path);
                }
            }
        }
    }

    void print_res(const std::string& filepath, const Status& status) {
        std::string res;
        if (status.ok()) {
            res = fmt::format("File path: {}, OK", filepath);
        } else {
            res = fmt::format("File path: {}, FAILED: {}", filepath, status.message());
        }
        std::cout << res << std::endl;
    }

private:
    bool _end_with(const std::string& str, const std::string& suffix) {
        if (str.length() < suffix.length()) {
            return false;
        }
        return str.compare(str.length() - suffix.length(), suffix.length(), suffix) == 0;
    }
};

GROUP_SLOW_TEST_F(ParquetCLIReaderTest, ReadAllParquetFiles) {
    std::vector<std::string> paths;
    // We don't support below files in init phase, but we need to make sure that will not make BE crashed.
    std::set<std::string> unsupported_paths_init;
    {
        // Invalid argument: MAP-annotated group must have a single child.
        // error format built by hudi
        unsupported_paths_init.emplace("./be/test/exec/test_data/parquet_scanner/hudi_array_map.parquet");
        // empty parquet file
        unsupported_paths_init.emplace("./be/test/formats/parquet/test_data/empty.parquet");
        // parquet column reader: not supported convert from parquet `INT64` to `TIME`
        unsupported_paths_init.emplace("./be/test/formats/parquet/test_data/column_converter/int32.parquet");
        // parquet column reader: not supported convert from parquet `INT64` to `TIME`
        unsupported_paths_init.emplace("./be/test/formats/parquet/test_data/column_converter/int64.parquet");
        // parquet column reader: not supported convert from parquet `FIXED_LEN_BYTE_ARRAY` to `TIME`
        unsupported_paths_init.emplace(
                "./be/test/formats/parquet/test_data/column_converter/fixed_len_byte_array.parquet");
        // parquet column reader: not supported convert from parquet `BYTE_ARRAY` to `DECIMAL128`
        unsupported_paths_init.emplace("./be/test/formats/parquet/test_data/column_converter/byte_array.parquet");
        // Invalid argument: Duplicate field name: col1
        unsupported_paths_init.emplace("./be/test/exec/test_data/parquet_data/schema4.parquet");
        //  Invalid argument: Map keys must be primitive type.
        unsupported_paths_init.emplace("./be/test/formats/parquet/test_data/map_key_is_struct.parquet");
        // Not supported: parquet column reader: not supported convert from parquet `BYTE_ARRAY` to `DECIMAL128`
        unsupported_paths_init.emplace(
                "./be/test/formats/parquet/test_data/data_with_page_index_and_bloom_filter.parquet");
    }
    // We don't support below files in get_next phase, it's illegal parquet files
    std::set<std::string> unsupported_paths_get_next;
    {
        unsupported_paths_get_next.emplace(
                "./be/test/exec/test_data/parquet_scanner/type_mismatch_decode_min_max.parquet");
        // corrupted file
        unsupported_paths_get_next.emplace("./be/test/exec/test_data/parquet_data/decimal.parquet");
    }
    traverse_directory_add_parquet(paths, "./be/test/exec/test_data/parquet_data", ".parquet");
    traverse_directory_add_parquet(paths, "./be/test/exec/test_data/parquet_scanner", ".parquet");
    traverse_directory_add_parquet(paths, "./be/test/formats/parquet/test_data", ".parquet");

    std::vector<std::pair<int, Status>> results;
    for (const std::string& path : paths) {
        LOG(INFO) << "Testing file: " << path;
        ParquetCLIReader reader{path};
        int stage = 0;
        auto st = reader.init();
        if (!st.ok()) {
            results.emplace_back(stage, st);
            continue;
        }
        stage++;
        auto res = reader.debug(1);
        st = res.status();
        if (!st.ok()) {
            results.emplace_back(stage, st);
            continue;
        }
        stage++;
        results.emplace_back(stage, st);
    }

    ASSERT_EQ(results.size(), paths.size());
    for (int i = 0; i < results.size(); i++) {
        const auto& [stage, status] = results[i];
        const auto& path = paths[i];
        if (stage == 0) {
            EXPECT_TRUE(!status.ok() && unsupported_paths_init.contains(path))
                    << "File path: " << path << ", stage: " << stage << ", status: " << status.to_string();
        } else if (stage == 1) {
            EXPECT_TRUE(!status.ok() && unsupported_paths_get_next.contains(path))
                    << "File path: " << path << ", stage: " << stage << ", status: " << status.to_string();
        } else {
            EXPECT_TRUE(status.ok()) << "File path: " << path << ", stage: " << stage
                                     << ", status: " << status.to_string();
        }
    }
}

GROUP_SLOW_TEST_F(ParquetCLIReaderTest, ReadArrowFuzzingParquetFiles) {
    std::vector<std::string> read_paths;
    {
        traverse_directory_add_parquet(read_paths, "./be/test/formats/parquet/arrow_fuzzing_data/fuzzing/");
        traverse_directory_add_parquet(read_paths,
                                       "./be/test/formats/parquet/arrow_fuzzing_data/generated_simple_numerics/");
        read_paths.emplace_back("./be/test/formats/parquet/arrow_fuzzing_data/ARROW-17100.parquet");
    }

    for (const std::string& path : read_paths) {
        LOG(INFO) << "Testing file: " << path;
        ParquetCLIReader reader{path};
        auto st = reader.init();
        if (st.ok()) {
            auto res = reader.debug(1);
            st = res.status();
        }
    }
}

TEST_F(ParquetCLIReaderTest, ReadDataPageV2ParquetFiles) {
    std::vector<std::string> files = {
            // generated by duckdb: https://duckdb.org/2025/01/22/parquet-encodings.html
            "data_page_v2_float10k.parquet", "data_page_v2_int1m.parquet", "data_page_v2_string4k.parquet",
            // other sources.
            "data_page_v2_test.parquet", "data_page_v2_test2.parquet"};
    for (const auto& file : files) {
        std::string path = "./be/test/formats/parquet/test_data/" + file;
        LOG(INFO) << "Testing file: " << path;
        ParquetCLIReader reader{path};
        auto st = reader.init();
        ASSERT_TRUE(st.ok()) << st.to_string();
        auto res = reader.debug(10);
        st = res.status();
        ASSERT_TRUE(st.ok()) << st.to_string();
        LOG(INFO) << "data rows = " << res.value();
    }
}

} // namespace starrocks::parquet