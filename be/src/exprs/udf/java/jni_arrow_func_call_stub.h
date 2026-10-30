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
#include <string>

#include "exprs/function_context.h"
#include "exprs/udf/udf_call_stub.h"

namespace starrocks {
class RuntimeState;

// Build a UDFCallStub for a vectorized ("input"="arrow") scalar Java UDF. Input columns are
// converted to an Arrow RecordBatch by AbstractArrowFuncCallStub and handed to the Java side
// zero-copy over the Arrow C Data Interface (com.starrocks.udf.ArrowUDFHelper.evaluateArrow),
// which invokes the user's evaluate(FieldVector...) and returns a FieldVector.
//
// On any JNI/classloading failure the returned stub is an error stub whose evaluate() surfaces
// the failure Status (mirrors the Python path).
std::unique_ptr<UDFCallStub> create_jni_arrow_call_stub(FunctionContext* ctx, RuntimeState* state,
                                                        const std::string& libpath, const std::string& symbol);

} // namespace starrocks
