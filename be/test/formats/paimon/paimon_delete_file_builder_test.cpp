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

#include "formats/paimon/paimon_delete_file_builder.h"

#include <gtest/gtest.h>

#include "base/testutil/assert.h"
#include "fs/fs.h"
#include "gen_cpp/PlanNodes_types.h"

namespace starrocks::formats {

class PaimonDeleteFileBuilderTest : public testing::Test {
public:
    PaimonDeleteFileBuilderTest() = default;
    ~PaimonDeleteFileBuilderTest() override = default;

protected:
    std::string _path = "./be/test/formats/paimon/test_data/index-41b983cd-b835-450a-ad38-6ffee8ddbebd-0";
    int64_t _offset = 1;
    int64_t _length = 22;
};

TEST_F(PaimonDeleteFileBuilderTest, TestParquetBuilder) {
    PaimonDeleteFileBuilder builder(FileSystem::Default());
    TPaimonDeletionFile paimonDeletionFile;
    paimonDeletionFile.__set_path(_path);
    paimonDeletionFile.__set_offset(_offset);
    paimonDeletionFile.__set_length(_length);
    ASSIGN_OR_ABORT(auto deletion_bitmap, builder.build(paimonDeletionFile));
    ASSERT_EQ(1, deletion_bitmap->get_cardinality());
}

} // namespace starrocks::formats
