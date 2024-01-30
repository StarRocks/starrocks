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

package com.starrocks.connector.share.iceberg;

import com.google.common.collect.ImmutableList;

public class IcebergSchemaDesc {
    public static final ImmutableList<String> SCHEMAS =
            new ImmutableList.Builder<String>()
                    .add("content")
                    .add("file_path")
                    .add("file_format")
                    .add("spec_id")
                    .add("partition_data")
                    .add("record_count")
                    .add("file_size_bytes")
                    .add("split_offsets")
                    .add("sort_id")
                    .add("equality_ids")
                    .add("file_sequence_number")
                    .add("data_sequence_number")
                    .build();

}
