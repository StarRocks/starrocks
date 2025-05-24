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

#include "runtime/arrow_result_writer.h"

#include <arrow/api.h>
#include <gtest/gtest.h>
#include <pthread.h>

#include <string>

#include "runtime/buffer_control_block.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/result_buffer_mgr.h"
#include "runtime/runtime_state.h"
#include "util/runtime_profile.h"

namespace starrocks {

TEST(ArrowResultWriterTest, close) {
    BufferControlBlock* sinker = new BufferControlBlock(TUniqueId(), 1024);
    ASSERT_TRUE(sinker->init().ok());

    std::vector<ExprContext*> output_expr_ctxs;

    RowDescriptor row_desc;

    RuntimeProfile* profile = new RuntimeProfile("ArrowResultWriterTest");

    ArrowResultWriter writer(sinker, output_expr_ctxs, profile, row_desc);

    Status st = writer.close();
    ASSERT_TRUE(st.ok());

    delete profile;
    delete sinker;
}

} // namespace starrocks