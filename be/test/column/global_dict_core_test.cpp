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

#include <gtest/gtest.h>

#include <algorithm>
#include <sstream>
#include <string>
#include <vector>

#include "base/testutil/assert.h"
#include "column/binary_column.h"
#include "column/fixed_length_column.h"
#include "column/global_dict/decoder.h"
#include "column/global_dict/miscs.h"
#include "column/global_dict/types.h"
#include "column/nullable_column.h"

namespace starrocks {
namespace {

std::string slice_to_string(const Slice& slice) {
    return std::string(slice.data, slice.size);
}

} // namespace

TEST(GlobalDictCoreTest, MapTypesAndExtraction) {
    std::string alpha = "alpha";
    std::string beta = "beta";

    GlobalDictMap dict;
    dict.emplace(Slice(alpha), 1);
    dict.emplace(Slice(beta), 2);

    ColumnIdToGlobalDictMap dict_by_column;
    dict_by_column.emplace(7, &dict);

    EXPECT_TRUE(EMPTY_GLOBAL_DICTMAPS.empty());
    ASSERT_EQ(1, dict_by_column.size());
    EXPECT_EQ(2, dict_by_column[7]->size());

    std::ostringstream stream;
    stream << dict;
    EXPECT_NE(std::string::npos, stream.str().find("alpha"));
    EXPECT_NE(std::string::npos, stream.str().find("beta"));

    auto [column, codes] = extract_column_with_codes(dict);
    ASSERT_EQ(3, column->size());
    EXPECT_TRUE(column->is_null(0));

    std::sort(codes.begin(), codes.end());
    EXPECT_EQ((std::vector<int32_t>{0, 1, 2}), codes);
}

TEST(GlobalDictCoreTest, DecodeStringColumn) {
    std::string alpha = "alpha";
    std::string beta = "beta";

    RGlobalDictMap reverse_dict;
    reverse_dict.emplace(1, Slice(alpha));
    reverse_dict.emplace(2, Slice(beta));

    auto decoder = create_global_dict_decoder(reverse_dict);
    auto input = Int32Column::create();
    input->append(1);
    input->append(2);

    auto output = BinaryColumn::create();
    ASSERT_OK(decoder->decode_string(input.get(), output.get()));

    ASSERT_EQ(2, output->size());
    EXPECT_EQ("alpha", slice_to_string(output->get_slice(0)));
    EXPECT_EQ("beta", slice_to_string(output->get_slice(1)));
}

} // namespace starrocks
