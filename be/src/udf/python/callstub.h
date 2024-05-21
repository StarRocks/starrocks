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

#include "exprs/function_context.h"
#include "udf/udf_call_stub.h"

namespace starrocks {
class PyWorkerHandle;

class ArrowFlightFuncCallStub final : public AbstractArrowFuncCallStub {
public:
    ArrowFlightFuncCallStub(FunctionContext* func_ctx, std::shared_ptr<PyWorkerHandle> handle)
            : AbstractArrowFuncCallStub(func_ctx), _py_worker_handle(std::move(handle)) {}
    ~ArrowFlightFuncCallStub() override = default;

    StatusOr<std::shared_ptr<RecordBatch>> do_evaluate(RecordBatch&& batch) final;

private:
    std::shared_ptr<PyWorkerHandle> _py_worker_handle;
};

struct PyEnvDescriptor {
    std::string python_path;
    std::string python_home;
    bool isolated;
};

struct PyFunctionDescriptor {
    std::string symbol;
    std::string location;
    std::string content;
    std::string input_type;
    TypeDescriptor return_type;
    std::vector<TypeDescriptor> input_types;
    StatusOr<std::string> to_json_string() const;
};

std::unique_ptr<UDFCallStub> build_py_call_stub(FunctionContext* context, const PyFunctionDescriptor& func_desc);

} // namespace starrocks