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

#include <arrow/record_batch.h>

#include <memory>
#include <mutex>
#include <vector>

#include "column/vectorized_fwd.h"
#include "common/status.h"
#include "common/statusor.h"
#include "exprs/function_context.h"

namespace starrocks {
class UDFCallStub {
public:
    UDFCallStub() = default;
    virtual ~UDFCallStub() = default;
    virtual StatusOr<ColumnPtr> evaluate(const Columns& columns, size_t row_num) = 0;
};

class AbstractArrowFuncCallStub : public UDFCallStub {
public:
    using RecordBatch = arrow::RecordBatch;
    using arrowArray = arrow::Array;
    using arrowSchema = arrow::Schema;
    using arrowField = arrow::Field;

    // The stub is cached per driver and outlives the FunctionContext it is built from: a scan's IO
    // tasks evaluate conjuncts through ExprContext clones that are freed with their data source,
    // while the stub stays in the expr's cache. Copy the types instead of keeping the context.
    explicit AbstractArrowFuncCallStub(FunctionContext* func_ctx)
            : _arg_types(func_ctx->get_arg_types()), _return_type(func_ctx->get_return_type()) {}
    ~AbstractArrowFuncCallStub() override = default;
    StatusOr<ColumnPtr> evaluate(const Columns& columns, size_t row_num) final;

protected:
    virtual StatusOr<std::shared_ptr<RecordBatch>> do_evaluate(RecordBatch&& batch) = 0;

private:
    StatusOr<ColumnPtr> _convert_arrow_to_native(const arrowArray& arrow_batch, const arrowField& field_type);

    const std::vector<TypeDescriptor> _arg_types;
    const TypeDescriptor _return_type;
    // A driver's scan IO tasks run on different threads but share the driver's stub, and
    // do_evaluate is a write-then-read exchange on one stream, so calls must not overlap.
    std::mutex _evaluate_lock;
};

std::unique_ptr<UDFCallStub> create_error_call_stub(Status error_status);

} // namespace starrocks