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

#include <random>
#include <string>
#include <vector>

#include "column/column_helper.h"

namespace starrocks {

class BenchUtil {
public:
    template <typename T>
    static std::vector<T> create_series_values(int num_rows, T init_value, T delta_value) {
        std::vector<T> elements(num_rows);
        elements[0] = init_value;
        for (int i = 1; i < num_rows; i++) {
            elements[i] = elements[i - 1] + delta_value;
        }
        return elements;
    }

    template <typename T>
    static std::vector<T> create_random_values(int num_rows, T min_value, T max_value) {
        static_assert(std::is_integral_v<T> || std::is_floating_point_v<T>,
                      "Unsupported type. Must be integral or floating point.");
        std::random_device dev;
        std::mt19937 rng(dev());
        std::vector<T> elements(num_rows);

        if constexpr (std::is_integral_v<T>) {
            std::uniform_int_distribution<T> dist(min_value, max_value);
            for (int i = 0; i < num_rows; i++) {
                elements[i] = dist(rng);
            }
        } else if constexpr (std::is_floating_point_v<T>) {
            std::uniform_real_distribution<T> dist(min_value, max_value);
            for (int i = 0; i < num_rows; i++) {
                elements[i] = dist(rng);
            }
        } else {
        }
        return elements;
    }

    static std::vector<string> create_lowcard_string(int num_rows, int cardinality, int min_len, int max_len) {
        static std::string alphanum =
                "0123456789"
                "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                "abcdefghijklmnopqrstuvwxyz";

        std::uniform_int_distribution<int> dist_len(min_len, max_len);
        std::uniform_int_distribution<int> dist_char(0, alphanum.size() - 1);
        std::mt19937 rng(std::random_device{}());
        std::vector<std::string> dictionary;

        // generate dictionary values.
        for (size_t i = 0; i < cardinality; ++i) {
            int len = dist_len(rng);
            std::string str;
            for (int j = 0; j < len; ++j) {
                str += alphanum[dist_char(rng)];
            }
            dictionary.push_back(str);
        }

        std::vector<string> elements(num_rows);
        std::uniform_int_distribution<size_t> dist_index(0, cardinality - 1);
        for (int i = 0; i < num_rows; ++i) {
            elements[i] = dictionary[dist_index(rng)];
        }
        return elements;
    }

    static std::vector<string> create_random_string(int num_rows, int min_length, int max_len) {
        std::vector<string> elements(num_rows);
        static std::string alphanum =
                "0123456789"
                "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                "abcdefghijklmnopqrstuvwxyz";

        std::random_device dev;
        std::mt19937 rng(dev());
        std::uniform_int_distribution<int> dist_len(min_length, max_len);
        std::uniform_int_distribution<int> dist_char(0, alphanum.size() - 1);

        for (int i = 0; i < num_rows; i++) {
            int len = dist_len(rng);
            std::string str;
            for (int j = 0; j < len; ++j) {
                str += alphanum[dist_char(rng)];
            }
            elements[i] = str;
        }
        return elements;
    }

    static ColumnPtr create_series_int_column(int num_rows) {
        TypeDescriptor type_desc = TypeDescriptor(TYPE_INT);
        auto column = ColumnHelper::create_column(type_desc, false)->as_mutable_ptr();
        std::vector<int32_t> elements = create_series_values<int32_t>(num_rows, 0, 1);
        for (auto& x : elements) {
            column->append_datum(Datum(x));
        }
        return column;
    }

    static ColumnPtr create_random_string_column(int num_rows, int min_length) {
        std::vector<string> elements = create_random_string(num_rows, min_length, 60);
        TypeDescriptor type_desc = TypeDescriptor(TYPE_VARCHAR);
        auto column = ColumnHelper::create_column(type_desc, false)->as_mutable_ptr();
        for (auto& x : elements) {
            column->append_datum(Datum(Slice(x)));
        }
        return column;
    }
};

} // namespace starrocks
