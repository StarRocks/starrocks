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

#include "butil/time.h"
#include "column/column_builder.h"
#include "column/column_helper.h"
#include "column/const_column.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
#include "column/type_traits.h"
#include "common/object_pool.h"
#include "exprs/expr.h"
#include "glog/logging.h"

namespace starrocks {
class MockExpr : public starrocks::Expr {
public:
    explicit MockExpr(const TExprNode& dummy, ColumnPtr result) : Expr(dummy), _column(std::move(result)) {}

    StatusOr<ColumnPtr> evaluate_checked(ExprContext*, Chunk*) override { return _column; }

    Expr* clone(ObjectPool* pool) const override { return pool->add(new MockExpr(*this)); }

private:
    ColumnPtr _column;
};

class MockCostExpr : public Expr {
public:
    explicit MockCostExpr(const TExprNode& t) : Expr(t) {}

    StatusOr<ColumnPtr> evaluate_checked(ExprContext*, Chunk*) override {
        DCHECK(false);
        __builtin_unreachable();
    }
    Expr* clone(ObjectPool* pool) const override { return pool->add(new MockCostExpr(*this)); }

    int64_t cost_ns() { return _ns; }
    int64_t cost_us() { return _us; }
    int64_t cost_ms() { return _ms; }

    void reset_cost() {
        _ns = 0;
        _us = 0;
        _ms = 0;
    }

protected:
    void start() { _timer.start(); }

    void stop() {
        _timer.stop();
        _ns += _timer.n_elapsed();
        _us += _timer.u_elapsed();
        _ms += _timer.m_elapsed();
    }

protected:
    butil::Timer _timer;
    int64_t _ns = 0;
    int64_t _us = 0;
    int64_t _ms = 0;
};

template <LogicalType Type>
class MockVectorizedExpr : public MockCostExpr {
public:
    MockVectorizedExpr(const TExprNode& t, size_t size, RunTimeCppType<Type> value)
            : MockCostExpr(t), size(size), value(value) {}

    StatusOr<ColumnPtr> evaluate_checked(ExprContext* context, Chunk* ptr) override {
        start();
        ColumnPtr col;
        if constexpr (lt_is_decimal<Type>) {
            col = RunTimeColumnType<Type>::create(this->type().precision, this->type().scale);
        } else {
            col = RunTimeColumnType<Type>::create();
        }
        auto* concrete_col = ColumnHelper::cast_to_raw<Type>(col);
        concrete_col->reserve(size);
        for (int j = 0; j < size; ++j) {
            concrete_col->append(value);
        }
        stop();
        return col;
    }

private:
    size_t size;
    RunTimeCppType<Type> value;
};

template <LogicalType Type>
class MockMultiVectorizedExpr : public MockCostExpr {
public:
    MockMultiVectorizedExpr(const TExprNode& t, size_t size, RunTimeCppType<Type> num1, RunTimeCppType<Type> num2)
            : MockCostExpr(t), size(size), num1(num1), num2(num2) {}

    StatusOr<ColumnPtr> evaluate_checked(ExprContext* context, Chunk* ptr) override {
        start();
        auto col = RunTimeColumnType<Type>::create();
        col->reserve(size);
        for (int j = 0; j < size; ++j) {
            if (j % 2 == 0) {
                col->append(num1);
            } else {
                col->append(num2);
            }
        }
        stop();
        return col;
    }

private:
    size_t size;
    RunTimeCppType<Type> num1;
    RunTimeCppType<Type> num2;
};

template <LogicalType Type>
class MockNullVectorizedExpr : public MockCostExpr {
public:
    MockNullVectorizedExpr(const TExprNode& t, size_t size, RunTimeCppType<Type> value)
            : MockNullVectorizedExpr(t, size, value, false) {}

    MockNullVectorizedExpr(const TExprNode& t, size_t size, RunTimeCppType<Type> value, bool only_null)
            : MockCostExpr(t), only_null(only_null), flag(0), size(size), value(value) {}

    StatusOr<ColumnPtr> evaluate_checked(ExprContext* context, Chunk* ptr) override {
        start();
        if (only_null) {
            return ColumnHelper::create_const_null_column(1);
        }
        ColumnPtr col = nullptr;
        if constexpr (lt_is_decimal<Type>) {
            col = RunTimeColumnType<Type>::create(this->type().precision, this->type().scale);
        } else {
            col = RunTimeColumnType<Type>::create();
        }
        auto* concrete_col = ColumnHelper::cast_to_raw<Type>(col);
        for (int j = 0; j < size; ++j) {
            concrete_col->append(value);
        }
        auto nul = NullColumn::create();
        if (all_null) {
            for (int i = 0; i < size; ++i) {
                nul->append(1);
            }
        } else {
            for (int i = 0; i < size; ++i) {
                nul->append((flag + i) % 2);
            }
        }
        auto re = NullableColumn::create(col, nul);
        re->update_has_null();
        stop();
        return re;
    }

public:
    bool all_null = false;
    bool only_null = false;
    int flag;
    size_t size;
    RunTimeCppType<Type> value;
};

template <LogicalType Type>
class MockConstVectorizedExpr : public MockCostExpr {
public:
    MockConstVectorizedExpr(const TExprNode& t, RunTimeCppType<Type> value) : MockCostExpr(t), value(value) {
        col = ColumnHelper::create_const_column<Type>(value, 1);
    }

    StatusOr<ColumnPtr> evaluate_checked(ExprContext* context, Chunk* ptr) override {
        start();
        stop();
        return col;
    }

public:
    RunTimeCppType<Type> value;
    ColumnPtr col;
};

} // namespace starrocks
