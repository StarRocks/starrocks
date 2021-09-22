// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/test/util/arrow/arrow_row_batch_test.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <gtest/gtest.h>

#include <sstream>
#include <string>

#include "common/logging.h"
#include "util/arrow/row_batch.h"

#define ARROW_UTIL_LOGGING_H
#include <arrow/buffer.h>
#include <arrow/json/api.h>
#include <arrow/result.h>

#include "common/compiler_util.h"
DIAGNOSTIC_PUSH
DIAGNOSTIC_IGNORE("-Wclass-memaccess")
#include <arrow/json/test_common.h>
DIAGNOSTIC_POP

#include <arrow/pretty_print.h>

#include "common/object_pool.h"
#include "runtime/mem_tracker.h"
#include "runtime/row_batch.h"
#include "util/debug_util.h"

namespace starrocks {

class ArrowRowBatchTest : public testing::Test {};

static std::string test_str() {
    return R"(
    { "c1": 1, "c2": 1.1 }
    { "c1": 2, "c2": 2.2 }
    { "c1": 3, "c2": 3.3 }
        )";
}

static void MakeBuffer(const std::string& data, std::shared_ptr<arrow::Buffer>* out) {
    auto buffer_res = arrow::AllocateBuffer(data.size(), arrow::default_memory_pool());
    *out = std::move(buffer_res.ValueOrDie());
    std::copy(std::begin(data), std::end(data), (*out)->mutable_data());
}

TEST_F(ArrowRowBatchTest, PrettyPrint) {
    auto json = test_str();
    std::shared_ptr<arrow::Buffer> buffer;
    MakeBuffer(test_str(), &buffer);
    arrow::json::ParseOptions parse_opts = arrow::json::ParseOptions::Defaults();
    parse_opts.explicit_schema = arrow::schema({
            arrow::field("c1", arrow::int64()),
    });

    auto arrow_st = arrow::json::ParseOne(parse_opts, buffer);
    ASSERT_TRUE(arrow_st.ok());
    std::shared_ptr<arrow::RecordBatch> record_batch = arrow_st.ValueOrDie();

    ObjectPool obj_pool;
    RowDescriptor* row_desc;
    auto starrocks_st = convert_to_row_desc(&obj_pool, *record_batch->schema(), &row_desc);
    ASSERT_TRUE(starrocks_st.ok());
    MemTracker tracker;
    std::shared_ptr<RowBatch> row_batch;
    starrocks_st = convert_to_row_batch(*record_batch, *row_desc, &tracker, &row_batch);
    ASSERT_TRUE(starrocks_st.ok());

    {
        std::shared_ptr<arrow::Schema> check_schema;
        starrocks_st = convert_to_arrow_schema(*row_desc, &check_schema);
        ASSERT_TRUE(starrocks_st.ok());

        arrow::MemoryPool* pool = arrow::default_memory_pool();
        std::shared_ptr<arrow::RecordBatch> check_batch;
        starrocks_st = convert_to_arrow_batch(*row_batch, check_schema, pool, &check_batch);
        ASSERT_TRUE(starrocks_st.ok());
        ASSERT_EQ(3, check_batch->num_rows());
        ASSERT_TRUE(record_batch->Equals(*check_batch));
    }
}

} // namespace starrocks
