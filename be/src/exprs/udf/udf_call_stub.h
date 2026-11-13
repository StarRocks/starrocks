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

#include <functional>
#include <memory>
#include <mutex>
#include <vector>

#include "column/vectorized_fwd.h"
#include "common/status.h"
#include "common/statusor.h"
#include "exprs/function_context.h"

namespace starrocks {
// A stub is not thread-safe: do_evaluate is a write-then-read exchange on one stream, so callers
// must not overlap. UDFCallStubPool hands each stub to one caller at a time.
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

    // The stub is pooled and outlives the FunctionContext it is built from: a scan's IO tasks
    // evaluate conjuncts through ExprContext clones that are freed with their data source, while
    // the stub stays in the expr's pool. Copy the types instead of keeping the context.
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
};

std::unique_ptr<UDFCallStub> create_error_call_stub(Status error_status);

// The stubs of one UDF call site. A stub owns one exchange stream with the UDF worker, so a call
// checks out an idle stub (building one when none is idle) and returns it when done; concurrent
// calls, such as a scan's IO tasks evaluating the same conjunct, then run on separate streams
// instead of queueing on one.
class UDFCallStubPool {
public:
    using StubBuilder = std::function<std::unique_ptr<UDFCallStub>()>;

    StatusOr<ColumnPtr> evaluate(const StubBuilder& build, const Columns& columns, size_t row_num);

    // Stubs built so far, idle or in use.
    size_t num_built() const;
    size_t num_idle() const;

private:
    mutable std::mutex _lock;
    std::vector<std::unique_ptr<UDFCallStub>> _idle;
    size_t _num_built = 0;
};

} // namespace starrocks