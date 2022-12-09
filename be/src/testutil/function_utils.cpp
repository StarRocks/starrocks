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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/testutil/function_utils.cpp

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

#include "testutil/function_utils.h"

#include <vector>

#include "exprs/function_context.h"
#include "runtime/mem_pool.h"

namespace starrocks {

FunctionUtils::FunctionUtils() {
    FunctionContext::TypeDesc return_type;
    std::vector<FunctionContext::TypeDesc> arg_types;
    _memory_pool = new MemPool();
    _fn_ctx = FunctionContext::create_context(_state, _memory_pool, return_type, arg_types);
}
FunctionUtils::FunctionUtils(RuntimeState* state) {
    _state = state;
    FunctionContext::TypeDesc return_type;
    std::vector<FunctionContext::TypeDesc> arg_types;
    _memory_pool = new MemPool();
    _fn_ctx = FunctionContext::create_context(_state, _memory_pool, return_type, arg_types);
}

FunctionUtils::~FunctionUtils() {
    delete _fn_ctx;
    delete _memory_pool;
}

} // namespace starrocks
