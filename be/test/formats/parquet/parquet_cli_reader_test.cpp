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
            res = fmt::format("File path: {}, FAILED: {}", filepath, status.get_error_msg());
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

TEST_F(ParquetCLIReaderTest, ReadAllParquetFiles) {
    GTEST_SKIP();
    std::vector<std::string> paths;
    // We don't support below files in init phase, but we need to make sure that will not make BE crashed.
    std::set<std::string> unsupported_paths_init;
    {
        // error format built by hudi
        unsupported_paths_init.emplace("./be/test/exec/test_data/parquet_scanner/hudi_array_map.parquet");
        // error format built by hudi
        unsupported_paths_init.emplace("./be/test/exec/test_data/parquet_data/hudi_mor_two_level_nested_array.parquet");
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
    for (const std::string& path : paths) {
        ParquetCLIReader reader{path};
        auto st = reader.init();
        if (unsupported_paths_init.find(path) != unsupported_paths_init.end()) {
            ASSERT_FALSE(st.ok());
        } else {
            ASSERT_TRUE(st.ok());
            auto res = reader.debug(1);
            if (unsupported_paths_get_next.find(path) != unsupported_paths_get_next.end()) {
                ASSERT_FALSE(res.ok());
            } else {
                ASSERT_TRUE(res.ok()) << res.status().get_error_msg();
            }
            st = res.status();
        }
        // print_res(path, st);
    }
}

TEST_F(ParquetCLIReaderTest, ReadArrowFuzzingParquetFiles) {
    std::vector<std::string> read_paths;
    {
        traverse_directory_add_parquet(read_paths, "./be/test/formats/parquet/arrow_fuzzing_data/fuzzing/");
        traverse_directory_add_parquet(read_paths,
                                       "./be/test/formats/parquet/arrow_fuzzing_data/generated_simple_numerics/");
        read_paths.emplace_back("./be/test/formats/parquet/arrow_fuzzing_data/ARROW-17100.parquet");
    }
    std::set<std::string> ignore_dcheck_paths;
#ifndef NDEBUG
    {
        // ignore below files in DEBUG mode, because below code will consume lots of memory and face failed with DCHECK,
        // so we only run below two files in Release mode(It will not occur be crashed).
        ignore_dcheck_paths.emplace(
                "./be/test/formats/parquet/arrow_fuzzing_data/fuzzing/"
                "clusterfuzz-testcase-minimized-parquet-arrow-fuzz-4819270771146752");
        ignore_dcheck_paths.emplace(
                "./be/test/formats/parquet/arrow_fuzzing_data/fuzzing/"
                "clusterfuzz-testcase-minimized-parquet-arrow-fuzz-5667493425446912");
    }
#endif
    for (const std::string& path : read_paths) {
        if (ignore_dcheck_paths.find(path) != ignore_dcheck_paths.end()) {
            continue;
        }
        ParquetCLIReader reader{path};
        auto st = reader.init();
        if (st.ok()) {
            auto res = reader.debug(1);
            st = res.status();
        }
        // print_res(path, st);
    }
}
} // namespace starrocks::parquet