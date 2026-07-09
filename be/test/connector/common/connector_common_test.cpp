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

#include "connector/common/connector_sink_commit.h"
#include "formats/utils.h"

namespace starrocks::connector {

TEST(ConnectorCommonTest, CommitResultWrapsFileResultWithConnectorMetadata) {
    CommitResult result{
            .file_result =
                    {
                            .io_status = Status::OK(),
                            .format = formats::PARQUET,
                            .file_statistics =
                                    {
                                            .record_count = 10,
                                            .file_size = 128,
                                    },
                            .location = "s3://bucket/table/data.parquet",
                    },
    };

    auto& fingerprint_ref = result.set_partition_null_fingerprint("01");
    auto& referenced_file_ref = result.set_referenced_data_file("s3://bucket/table/original.parquet");

    ASSERT_EQ(&result, &fingerprint_ref);
    ASSERT_EQ(&result, &referenced_file_ref);
    ASSERT_TRUE(result.file_result.io_status.ok());
    ASSERT_EQ(formats::PARQUET, result.file_result.format);
    ASSERT_EQ(10, result.file_result.file_statistics.record_count);
    ASSERT_EQ(128, result.file_result.file_statistics.file_size);
    ASSERT_EQ("01", result.partition_null_fingerprint);
    ASSERT_EQ("s3://bucket/table/original.parquet", result.referenced_data_file);
}

} // namespace starrocks::connector
