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

#include "storage/non_retryable_load_errors.h"

#include <gtest/gtest.h>

namespace starrocks {

TEST(NonRetryableLoadErrorsTest, testPrimaryKeySizeExceed) {
    ASSERT_TRUE(is_non_retryable_load_error(kPrimaryKeySizeExceedError));
    ASSERT_TRUE(is_non_retryable_load_error("primary key size exceed the limit."));
    ASSERT_TRUE(is_non_retryable_load_error("some prefix: primary key size exceed the limit."));
}

TEST(NonRetryableLoadErrorsTest, testRetryableErrors) {
    ASSERT_FALSE(is_non_retryable_load_error(""));
    ASSERT_FALSE(is_non_retryable_load_error("timeout"));
    ASSERT_FALSE(is_non_retryable_load_error("too many filtered rows"));
    ASSERT_FALSE(is_non_retryable_load_error("Offset out of range"));
    ASSERT_FALSE(is_non_retryable_load_error("some random error"));
}

} // namespace starrocks
