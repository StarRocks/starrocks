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

#include "udf/java/java_call_stub.h"

#include "exprs/function_context.h"
#include "udf/java/jni_util.h"
#include "udf/java/jvm_function_helper.h"

namespace starrocks {

void AggBatchCallStub::batch_update_single(int num_rows, jobject state, jobject* input, int cols) {
    jvalue jni_inputs[3 + cols];
    jni_inputs[0].i = num_rows;
    jni_inputs[1].l = _caller;
    jni_inputs[2].l = state;
    for (int i = 0; i < cols; ++i) {
        jni_inputs[3 + i].l = input[i];
    }
    auto* env = JVMFunctionHelper::getInstance().getEnv();
    env->CallStaticVoidMethodA(_stub_clazz.clazz(), env->FromReflectedMethod(_stub_method.handle()), jni_inputs);
    CHECK_UDF_CALL_EXCEPTION(env, _ctx);
}

StatusOr<jobject> BatchEvaluateStub::batch_evaluate(int num_rows, jobject* input, int cols) {
    jvalue jni_inputs[2 + cols];
    jni_inputs[0].i = num_rows;
    jni_inputs[1].l = _caller;
    for (int i = 0; i < cols; ++i) {
        jni_inputs[2 + i].l = input[i];
    }
    auto* env = JVMFunctionHelper::getInstance().getEnv();
    auto res = env->CallStaticObjectMethodA(_stub_clazz.clazz(), env->FromReflectedMethod(_stub_method.handle()),
                                            jni_inputs);
    RETURN_ERROR_IF_JNI_EXCEPTION(env);
    return res;
}

} // namespace starrocks
