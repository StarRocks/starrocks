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

#include "exprs/agg/aggregate_factory.h"
#include "exprs/udf/java/java_function_fwd.h"

namespace starrocks {

namespace {

const AggregateFunction* exprs_non_builtin_aggregate_function_provider(TFunctionBinaryType::type binary_type,
                                                                       bool is_window_function,
                                                                       bool is_input_nullable) {
    if (binary_type != TFunctionBinaryType::SRJAR) {
        return nullptr;
    }
    if (is_window_function) {
        return getJavaWindowFunction();
    }
    return getJavaUDAFFunction(is_input_nullable);
}

struct AggregateFactoryExprsExtensionRegistrar {
    AggregateFactoryExprsExtensionRegistrar() {
        set_non_builtin_aggregate_function_provider(exprs_non_builtin_aggregate_function_provider);
    }
};

AggregateFactoryExprsExtensionRegistrar k_aggregate_factory_exprs_extension_registrar;

} // namespace

} // namespace starrocks
