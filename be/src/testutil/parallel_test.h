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

#pragma once

// In order to run tests more faster, we are using the gtest-parallel script(https://github.com/google/gtest-parallel)
// to execute test binaries. Since many of our test cases use globally-shared resources and cannot be run in parallel,
// gtest-parallel must be executed with the option `--serialize_test_cases`, which will run tests within the same test
// case sequentially.
// The PARALLEL_TEST is just a simple wrapper on TEST to give each test a unique case name, make them be able to run
// in parallel.
#define TOKENPASTE(x, y) x##y
#define TOKENPASTE2(x, y) TOKENPASTE(x, y)
#define PARALLEL_TEST(test_case_name, test_name) TEST(TOKENPASTE2(test_case_name, __LINE__), test_name)
