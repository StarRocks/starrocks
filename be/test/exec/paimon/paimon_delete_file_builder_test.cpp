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

#include "exec/paimon/paimon_delete_file_builder.h"

#include <gtest/gtest.h>

#include "fs/fs.h"
#include "gen_cpp/PlanNodes_types.h"
#include "testutil/assert.h"

namespace starrocks {

class PaimonDeleteFileBuilderTest : public testing::Test {
public:
    PaimonDeleteFileBuilderTest() = default;
    ~PaimonDeleteFileBuilderTest() override = default;

protected:
    std::string _path = "./be/test/exec/test_data/paimon_data/index-41b983cd-b835-450a-ad38-6ffee8ddbebd-0";
    int64_t _offset = 1;
    int64_t _length = 22;

    SkipRowsContextPtr _skip_rows_ctx = std::make_shared<SkipRowsContext>();
};

TEST_F(PaimonDeleteFileBuilderTest, TestParquetBuilder) {
    std::unique_ptr<PaimonDeleteFileBuilder> builder(
            new PaimonDeleteFileBuilder(FileSystem::Default(), _skip_rows_ctx));
    TPaimonDeletionFile paimonDeletionFile;
    paimonDeletionFile.__set_path(_path);
    paimonDeletionFile.__set_offset(_offset);
    paimonDeletionFile.__set_length(_length);
    std::shared_ptr<TPaimonDeletionFile> paimon_deletion_file =
            std::make_shared<TPaimonDeletionFile>(paimonDeletionFile);
    ASSERT_OK(builder->build(paimon_deletion_file.get()));
    ASSERT_EQ(1, _skip_rows_ctx->deletion_bitmap->get_cardinality());
}

} // namespace starrocks
