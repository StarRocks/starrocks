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

#include "exprs/paimon_functions.h"

#include <cstdlib>
#include <mutex>

#include "base/testutil/sync_point.h"
#include "base/utility/dynamic_util.h"
#include "column/column_helper.h"
#include "runtime/current_thread.h"
#include "runtime/runtime_state.h"

namespace starrocks {
namespace {
struct BucketConfig {
    std::string json;
    TypeDescriptor keys;
    RuntimeProfile::Counter* timer = nullptr;
};
struct BucketThreadState final : FunctionThreadState {
    Status status;
    std::unique_ptr<PaimonBucketCalculator> calculator;
};

StatusOr<CreatePaimonBucketCalculator> load_calculator() {
    CreatePaimonBucketCalculator linked_factory = nullptr;
    TEST_SYNC_POINT_CALLBACK("PaimonFunctions::load_calculator", &linked_factory);
    if (linked_factory != nullptr) return linked_factory;
    static std::mutex mutex;
    static void* handle = nullptr;
    static CreatePaimonBucketCalculator create = nullptr;
    std::lock_guard<std::mutex> lock(mutex);
    if (create != nullptr) return create;
    if (handle == nullptr) {
        const char* home = std::getenv("STARROCKS_HOME");
        std::string path =
                home ? std::string(home) + "/lib/paimon-cpp-lib/libstarrocks_paimon.so" : "libstarrocks_paimon.so";
        RETURN_IF_ERROR(dynamic_open(path.c_str(), &handle));
    }
    void* symbol = nullptr;
    auto status = dynamic_lookup(handle, STARROCKS_PAIMON_BUCKET_CALCULATOR_SYMBOL, &symbol);
    if (!status.ok()) return Status::NotSupported("Install the matching Paimon bucket function shim");
    create = reinterpret_cast<CreatePaimonBucketCalculator>(symbol);
    return create; // Never dlclose: calculators retain the shim's vtable until query destruction.
}
} // namespace

Status PaimonFunctions::prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (context->get_num_args() != 2 || !context->is_notnull_constant_column(0) ||
        context->get_arg_type(1)->type != TYPE_STRUCT) {
        return Status::InvalidArgument("Paimon bucket requires constant configuration and a key STRUCT");
    }
    auto json = ColumnHelper::get_const_value<TYPE_VARCHAR>(context->get_constant_column(0)).to_string();
    if (json.empty() || json.size() > 1024 * 1024) {
        return Status::InvalidArgument("Invalid Paimon bucket configuration size");
    }
    RuntimeProfile::Counter* timer = nullptr;
    if (context->state() != nullptr && context->state()->runtime_profile() != nullptr) {
        timer = ADD_TIMER(context->state()->runtime_profile(), "NativeBucketComputeTime");
    }
    context->set_function_state(scope, new BucketConfig{std::move(json), *context->get_arg_type(1), timer});
    return Status::OK();
}

Status PaimonFunctions::close(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    delete static_cast<BucketConfig*>(context->get_function_state(scope));
    context->set_function_state(scope, nullptr);
    return Status::OK();
}

StatusOr<ColumnPtr> PaimonFunctions::bucket(FunctionContext* context, const Columns& columns) {
    auto* config = static_cast<const BucketConfig*>(context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
    if (config == nullptr || columns.size() != 2) return Status::InvalidArgument("Paimon bucket is not prepared");
    SCOPED_TIMER(config->timer);
    auto* state = context->get_or_create_thread_state<BucketThreadState>([&]() {
        auto state = std::make_unique<BucketThreadState>();
        auto create = load_calculator();
        if (!create.ok()) {
            state->status = create.status();
        } else {
            auto result = create.value()(config->json, config->keys, CurrentThread::mem_tracker());
            if (!result.ok())
                state->status = result.status();
            else
                state->calculator = std::move(result).value();
        }
        return state;
    });
    RETURN_IF_ERROR(state->status);
    return state->calculator->calculate(columns[1]);
}
} // namespace starrocks

#include "gen_cpp/opcode/PaimonFunctions.inc"
