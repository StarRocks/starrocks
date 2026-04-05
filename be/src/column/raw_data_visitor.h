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

#include "base/status_fmt.hpp"
#include "column/adaptive_nullable_column.h"
#include "column/array_column.h"
#include "column/binary_column.h"
#include "column/column_visitor_adapter.h"
#include "column/const_column.h"
#include "column/json_column.h"
#include "column/map_column.h"
#include "column/nullable_column.h"
#include "column/object_column.h"
#include "column/struct_column.h"
#include "column/variant_column.h"

namespace starrocks {

// MutableRawDataVisitor calls mutable_raw_data() on the visited column and stores
// the resulting pointer for the caller to retrieve via result().
// Supported column types:
//   - FixedLengthColumn<T>, DecimalV3Column<T>: returns raw data pointer directly
//   - NullableColumn, ConstColumn: recurses into the inner data column
//   - ArrayColumn: recurses into the elements column
//   - AdaptiveNullableColumn: materializes first, then recurses into the data column
// All other column types return NotSupported.
class MutableRawDataVisitor final : public ColumnVisitorMutableAdapter<MutableRawDataVisitor> {
public:
    MutableRawDataVisitor() : ColumnVisitorMutableAdapter(this) {}

    template <typename T>
    Status do_visit(FixedLengthColumn<T>* column) {
        _result = column->mutable_raw_data();
        return Status::OK();
    }

    template <typename T>
    Status do_visit(DecimalV3Column<T>* column) {
        _result = column->mutable_raw_data();
        return Status::OK();
    }

    Status do_visit(ArrayColumn* column) { return column->elements_column_raw_ptr()->accept_mutable(this); }

    Status do_visit(ConstColumn* column) { return column->data_column_raw_ptr()->accept_mutable(this); }

    Status do_visit(NullableColumn* column) { return column->data_column_raw_ptr()->accept_mutable(this); }

    Status do_visit(AdaptiveNullableColumn* column) {
        return column->materialized_raw_data_column()->as_mutable_raw_ptr()->accept_mutable(this);
    }

    // Fallback for all unsupported column types. Uses const T& instead of T* so that
    // it does not compete with the T* overloads above in partial ordering.
    // T is always a Column-derived pointer type, so -> dereferences to call get_name().
    template <typename T>
    static Status do_visit(const T& column) {
        return Status::NotSupported("MutableRawDataVisitor: unsupported column type {}", column->get_name());
    }

    uint8_t* result() const { return _result; }

private:
    uint8_t* _result = nullptr;
};

// RawBytesVisitor calls raw_bytes() on the visited column and stores
// the resulting pointer for the caller to retrieve via result().
// Supported column types:
//   - FixedLengthColumn<T>, DecimalV3Column<T>: returns the raw element buffer
//   - BinaryColumn, LargeBinaryColumn: returns the flat bytes buffer (not the Slice cache)
//   - NullableColumn, ConstColumn: recurses into the inner data column
//   - ArrayColumn: recurses into the elements column
//   - AdaptiveNullableColumn: materializes first, then recurses into the data column
// All other column types return NotSupported.
class RawBytesVisitor final : public ColumnVisitorAdapter<RawBytesVisitor> {
public:
    RawBytesVisitor() : ColumnVisitorAdapter(this) {}

    template <typename T>
    Status do_visit(const FixedLengthColumn<T>& column) {
        _result = column.raw_bytes();
        return Status::OK();
    }

    template <typename T>
    Status do_visit(const DecimalV3Column<T>& column) {
        _result = column.raw_bytes();
        return Status::OK();
    }

    Status do_visit(const BinaryColumn& column) {
        _result = column.raw_bytes();
        return Status::OK();
    }

    Status do_visit(const LargeBinaryColumn& column) {
        _result = column.raw_bytes();
        return Status::OK();
    }

    Status do_visit(const ArrayColumn& column) { return column.elements_column_raw_ptr()->accept(this); }

    Status do_visit(const ConstColumn& column) { return column.data_column_raw_ptr()->accept(this); }

    Status do_visit(const NullableColumn& column) { return column.data_column_raw_ptr()->accept(this); }

    Status do_visit(const AdaptiveNullableColumn& column) {
        return column.materialized_raw_data_column()->accept(this);
    }

    // Fallback for all unsupported column types.
    template <typename T>
    static Status do_visit(const T& column) {
        return Status::NotSupported("RawBytesVisitor: unsupported column type {}", column.get_name());
    }

    const uint8_t* result() const { return _result; }

private:
    const uint8_t* _result = nullptr;
};

} // namespace starrocks