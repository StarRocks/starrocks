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

#include <memory>
#include <vector>

#include "runtime/env/java/java_env.h"
#include "udf/java/java_call_stub.h"
#include "udf/java/java_function_loader.h"

namespace starrocks {

struct JavaUDFContext {
    JavaUDFContext() = default;
    ~JavaUDFContext();

    std::unique_ptr<ClassLoader> udf_classloader;
    std::unique_ptr<ClassAnalyzer> analyzer;
    std::unique_ptr<BatchEvaluateStub> call_stub;

    JVMClass udf_class = nullptr;
    JavaGlobalRef udf_handle = nullptr;

    // Java Method
    std::unique_ptr<JavaMethodDescriptor> prepare;
    std::unique_ptr<JavaMethodDescriptor> evaluate;
    std::unique_ptr<JavaMethodDescriptor> close;

    // Per-argument and return-type UdfTypeDesc Java objects mirroring the SQL
    // TypeDescriptor of each slot, with the formal Java record class captured at
    // every STRUCT slot. Built once at UDF context construction (walking
    // Method.getGenericParameterTypes() / getGenericReturnType() in lockstep with
    // the SQL type tree). Passed verbatim to the unified UDFHelper.writeResult and
    // input-boxing entry points so the recursion lives entirely on the Java side.
    std::vector<JavaGlobalRef> evaluate_arg_type_descs;
    JavaGlobalRef evaluate_return_type_desc = nullptr;
};

} // namespace starrocks
