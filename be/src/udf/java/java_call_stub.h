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

#include <utility>

#include "common/statusor.h"
#include "jni.h"
#include "runtime/env/java/java_env.h"

namespace starrocks {

class FunctionContext;

class AggBatchCallStub {
public:
    static inline const char* stub_clazz_name = "com.starrocks.udf.gen.CallStub";
    static inline const char* batch_update_method_name = "batchCallV";

    AggBatchCallStub(FunctionContext* ctx, jobject caller, JVMClass&& clazz, JavaGlobalRef&& method)
            : _ctx(ctx), _caller(caller), _stub_clazz(std::move(clazz)), _stub_method(std::move(method)) {}

    FunctionContext* ctx() { return _ctx; }

    void batch_update_single(int num_rows, jobject state, jobject* input, int cols);

private:
    FunctionContext* _ctx;
    // UDAF object handle, owned by JavaUDAFUniqueContext
    jobject _caller;
    JVMClass _stub_clazz;
    JavaGlobalRef _stub_method;
};

class BatchEvaluateStub {
public:
    static inline const char* stub_clazz_name = "com.starrocks.udf.gen.CallStub";
    static inline const char* batch_evaluate_method_name = "batchCallV";

    BatchEvaluateStub(jobject caller, JVMClass&& clazz, JavaGlobalRef&& method)
            : _caller(caller), _stub_clazz(std::move(clazz)), _stub_method(std::move(method)) {}

    StatusOr<jobject> batch_evaluate(int num_rows, jobject* input, int cols);

private:
    jobject _caller;
    JVMClass _stub_clazz;
    JavaGlobalRef _stub_method;
};

} // namespace starrocks
