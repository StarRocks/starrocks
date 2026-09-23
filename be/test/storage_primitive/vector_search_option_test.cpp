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

#include "storage_primitive/vector_search_option.h"

#include <gtest/gtest.h>

#include <cmath>
#include <cstring>
#include <limits>
#include <memory>
#include <string>
#include <vector>

#include "gen_cpp/PlanNodes_types.h"

namespace starrocks {

static std::string little_endian_blob(const std::vector<float>& values) {
    std::string blob(values.size() * sizeof(float), '\0');
    std::memcpy(blob.data(), values.data(), blob.size());
    return blob;
}

TEST(VectorQueryVectorTest, DecodesBinaryForm) {
    TVectorSearchOptions options;
    options.__set_query_vector_f32(little_endian_blob({0.5f, -0.25f, 1024.0f}));

    std::shared_ptr<const std::vector<float>> out;
    ASSERT_TRUE(decode_vector_query_vector(options, &out).ok());
    ASSERT_NE(nullptr, out);
    ASSERT_EQ(std::vector<float>({0.5f, -0.25f, 1024.0f}), *out);
}

TEST(VectorQueryVectorTest, RejectsBinaryOfWrongWidth) {
    TVectorSearchOptions options;
    options.__set_query_vector_f32(std::string(7, '\0'));

    std::shared_ptr<const std::vector<float>> out;
    ASSERT_TRUE(decode_vector_query_vector(options, &out).is_invalid_argument());
}

TEST(VectorQueryVectorTest, RejectsNonFiniteInBinaryForm) {
    // The FE never emits these, but the vector reaches tenann as a raw pointer, so do not trust it.
    for (float bad : {std::numeric_limits<float>::infinity(), -std::numeric_limits<float>::infinity(),
                      std::numeric_limits<float>::quiet_NaN()}) {
        TVectorSearchOptions options;
        options.__set_query_vector_f32(little_endian_blob({1.0f, bad}));
        std::shared_ptr<const std::vector<float>> out;
        ASSERT_TRUE(decode_vector_query_vector(options, &out).is_invalid_argument());
    }
}

TEST(VectorQueryVectorTest, DecodesLegacyTextFormFromAnOlderFe) {
    // The supported upgrade order is BE/CN first, so a new BE keeps serving an old FE that still
    // sends one decimal string per dimension.
    TVectorSearchOptions options;
    options.__set_query_vector(std::vector<std::string>{"0.5", "-0.25", "1024"});

    std::shared_ptr<const std::vector<float>> out;
    ASSERT_TRUE(decode_vector_query_vector(options, &out).ok());
    ASSERT_EQ(std::vector<float>({0.5f, -0.25f, 1024.0f}), *out);
}

TEST(VectorQueryVectorTest, RejectsMalformedLegacyElement) {
    TVectorSearchOptions options;
    options.__set_query_vector(std::vector<std::string>{"1.0", "not-a-float"});

    std::shared_ptr<const std::vector<float>> out;
    ASSERT_TRUE(decode_vector_query_vector(options, &out).is_invalid_argument());
}

TEST(VectorQueryVectorTest, RejectsLegacyElementThatOverflowsFloat) {
    // 3.5e38 is a finite double but overflows float32, and string_to_float<float> reports success
    // while returning +inf. Both wire forms go through the same finiteness check, so it is rejected
    // here instead of reaching tenann.
    TVectorSearchOptions options;
    options.__set_query_vector(std::vector<std::string>{"1.0", "3.5e38"});

    std::shared_ptr<const std::vector<float>> out;
    ASSERT_TRUE(decode_vector_query_vector(options, &out).is_invalid_argument());
}

TEST(VectorQueryVectorTest, PrefersBinaryWhenBothFormsArePresent) {
    TVectorSearchOptions options;
    options.__set_query_vector_f32(little_endian_blob({7.0f}));
    options.__set_query_vector(std::vector<std::string>{"1.0", "2.0"});

    std::shared_ptr<const std::vector<float>> out;
    ASSERT_TRUE(decode_vector_query_vector(options, &out).ok());
    ASSERT_EQ(std::vector<float>({7.0f}), *out);
}

TEST(VectorQueryVectorTest, FailsLoudlyWhenNeitherFormIsPresent) {
    // Only reachable by upgrading the FE ahead of the BE/CN. Returning an empty vector here would
    // hand tenann a zero-length query and produce quietly wrong rows.
    TVectorSearchOptions options;
    std::shared_ptr<const std::vector<float>> out;
    Status st = decode_vector_query_vector(options, &out);
    ASSERT_FALSE(st.ok());
    ASSERT_TRUE(st.is_internal_error()) << st;
}

} // namespace starrocks
