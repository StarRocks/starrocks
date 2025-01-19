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

#include "connector/deletion_vector/deletion_vector.h"

#include <gtest/gtest.h>

#include "util/base85.h"

namespace starrocks {

class DeletionVectorTest : public testing::Test {
public:
    void SetUp() override { dv = std::make_shared<DeletionVector>(params); }

    HdfsScannerParams params;
    std::shared_ptr<DeletionVector> dv;
};

TEST_F(DeletionVectorTest, inlineDeletionVectorTest) {
    std::string encoded_inline_dv = "^Bg9^0rr910000000000iXQKl0rr91000f55c8Xg0@@D72lkbi5=-{L";
    SkipRowsContextPtr skipRowsContext = std::make_shared<SkipRowsContext>();
    Status status = dv->deserialized_inline_dv(encoded_inline_dv, skipRowsContext);
    ASSERT_TRUE(status.ok());

    uint64_t cardinality = skipRowsContext->deletion_bitmap->get_cardinality();
    vector<uint64_t> bitmap_vector(cardinality);
    skipRowsContext->deletion_bitmap->to_array(bitmap_vector);
    ASSERT_EQ(6, bitmap_vector.size());
    std::stringstream ss;
    for (int64_t rowid : bitmap_vector) {
        ss << rowid << " ";
    }
    ASSERT_EQ("3 4 7 11 18 29 ", ss.str());
}

TEST_F(DeletionVectorTest, absolutePathTest) {
    // test relative path
    params.deletion_vector_descriptor = std::make_shared<TDeletionVectorDescriptor>();
    params.deletion_vector_descriptor->__set_storageType("u");
    params.deletion_vector_descriptor->__set_pathOrInlineDv("ab^-aqEH.-t@S}K{vb[*k^");
    dv = std::make_shared<DeletionVector>(params);

    std::string table_location = "s3://mytable";
    StatusOr<std::string> absolute_path = dv->get_absolute_path(table_location);
    ASSERT_TRUE(absolute_path.ok());
    ASSERT_EQ("s3://mytable/ab/deletion_vector_d2c639aa-8816-431a-aaf6-d3fe2512ff61.bin", absolute_path.value());
    // test absolute path
    params.deletion_vector_descriptor = std::make_shared<TDeletionVectorDescriptor>();
    params.deletion_vector_descriptor->__set_storageType("p");
    params.deletion_vector_descriptor->__set_pathOrInlineDv(
            "s3://mytable/deletion_vector_d2c639aa-8816-431a-aaf6-d3fe2512ff61.bin");
    dv = std::make_shared<DeletionVector>(params);
    absolute_path = dv->get_absolute_path(table_location);
    ASSERT_TRUE(absolute_path.ok());
    ASSERT_EQ("s3://mytable/deletion_vector_d2c639aa-8816-431a-aaf6-d3fe2512ff61.bin", absolute_path.value());
}
} // namespace starrocks