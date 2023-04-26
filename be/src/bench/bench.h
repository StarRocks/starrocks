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

#include <benchmark/benchmark.h>
#include <gtest/gtest.h>
#include <testutil/assert.h>

#include <memory>
#include <numeric>
#include <random>

#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/datum_tuple.h"
#include "common/config.h"

namespace starrocks {

inline int kTestChunkSize = 4096;

class Bench {
public:
    static ColumnPtr create_series_column(const TypeDescriptor& type_desc, int num_rows, bool nullable = true) {
        // TODO: support more types.
        DCHECK_EQ(TYPE_INT, type_desc.type);

        ColumnPtr column = ColumnHelper::create_column(type_desc, nullable);
        std::vector<int32_t> elements(num_rows);
        std::iota(elements.begin(), elements.end(), 0);
        for (auto& x : elements) {
            column->append_datum(Datum((int32_t)x));
        }
        return column;
    }

    static ColumnPtr create_random_column(const TypeDescriptor& type_desc, int num_rows, bool low_card, bool nullable) {
        using UniformInt = std::uniform_int_distribution<std::mt19937::result_type>;
        using PoissonInt = std::poisson_distribution<std::mt19937::result_type>;
        ColumnPtr column = ColumnHelper::create_column(type_desc, nullable);

        std::random_device dev;
        std::mt19937 rng(dev());
        UniformInt uniform_int;
        if (low_card) {
            uniform_int.param(UniformInt::param_type(1, 100 * std::pow(2, num_rows)));
        } else {
            uniform_int.param(UniformInt::param_type(1, 100'000 * std::pow(2, num_rows)));
        }
        PoissonInt poisson_int(100'000);
        static std::string alphanum =
                "0123456789"
                "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                "abcdefghijklmnopqrstuvwxyz";

        auto gen_rand_str = [&]() {
            int str_len = uniform_int(rng) % 20;
            int str_start = std::min(poisson_int(rng) % alphanum.size(), alphanum.size() - str_len);
            Slice rand_str(alphanum.c_str() + str_start, str_len);
            return rand_str;
        };

        for (int i = 0; i < num_rows; i++) {
            if (nullable) {
                int32_t x = uniform_int(rng);
                if (x % 1000 == 0) {
                    column->append_nulls(1);
                    continue;
                }
            }
            if (type_desc.type == TYPE_INT) {
                int32_t x = uniform_int(rng);
                column->append_datum(Datum(x));
            } else if (type_desc.type == TYPE_VARCHAR) {
                column->append_datum(Datum(gen_rand_str()));
            } else {
                std::cerr << "not supported" << std::endl;
            }
        }
        return column;
    }
};

} // namespace starrocks
