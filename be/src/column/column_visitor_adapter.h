// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "column/column_visitor.h"
#include "column/column_visitor_mutable.h"

namespace starrocks {

// ColumnVisitorAdapter is a helper class to implement a new ColumnVisitor without writing many tedious
// codes for each specific column type.
// By inheriting the ColumnVisitorAdapter, the derived classes can be implemented with template functions,
// so there is no need to implement a `visit` method for each of the specific column type.
// You can take the class `serde::ColumnSerializingVisitor` as a reference.
template <typename Impl>
class ColumnVisitorAdapter : public ColumnVisitor {
public:
    explicit ColumnVisitorAdapter(Impl* impl) : _impl(impl) {}

    ~ColumnVisitorAdapter() override = default;

    Status visit(const vectorized::JsonColumn& column) override { return _impl->do_visit(column); }

    Status visit(const vectorized::HyperLogLogColumn& column) override { return _impl->do_visit(column); }

    Status visit(const vectorized::BitmapColumn& column) override { return _impl->do_visit(column); }

    Status visit(const vectorized::PercentileColumn& column) override { return _impl->do_visit(column); }

    Status visit(const vectorized::Int8Column& column) override { return _impl->_impl->do_visit(column); }

    Status visit(const vectorized::UInt8Column& column) override { return _impl->do_visit(column); }

    Status visit(const vectorized::Int16Column& column) override { return _impl->do_visit(column); }

    Status visit(const vectorized::UInt16Column& column) override { return _impl->do_visit(column); }

    Status visit(const vectorized::Int32Column& column) override { return _impl->do_visit(column); }

    Status visit(const vectorized::UInt32Column& column) override { return _impl->do_visit(column); }

    Status visit(const vectorized::Int64Column& column) override { return _impl->do_visit(column); }

    Status visit(const vectorized::UInt64Column& column) override { return _impl->do_visit(column); }

    Status visit(const vectorized::Int128Column& column) override { return _impl->do_visit(column); }

    Status visit(const vectorized::DoubleColumn& column) override { return _impl->do_visit(column); }

    Status visit(const vectorized::FloatColumn& column) override { return _impl->do_visit(column); }

    Status visit(const vectorized::DateColumn& column) override { return _impl->do_visit(column); }

    Status visit(const vectorized::TimestampColumn& column) override { return _impl->do_visit(column); }

    Status visit(const vectorized::DecimalColumn& column) override { return _impl->do_visit(column); }

    Status visit(const vectorized::Decimal32Column& column) override { return _impl->do_visit(column); }

    Status visit(const vectorized::Decimal64Column& column) override { return _impl->do_visit(column); }

    Status visit(const vectorized::Decimal128Column& column) override { return _impl->do_visit(column); }

    Status visit(const vectorized::FixedLengthColumn<int96_t>& column) override { return _impl->do_visit(column); }

    Status visit(const vectorized::FixedLengthColumn<uint24_t>& column) override { return _impl->do_visit(column); }

    Status visit(const vectorized::FixedLengthColumn<decimal12_t>& column) override { return _impl->do_visit(column); }

    Status visit(const vectorized::NullableColumn& column) override { return _impl->do_visit(column); }

    Status visit(const vectorized::ConstColumn& column) override { return _impl->do_visit(column); }

    Status visit(const vectorized::ArrayColumn& column) override { return _impl->do_visit(column); }

    Status visit(const vectorized::MapColumn& column) override { return _impl->do_visit(column); }

    Status visit(const vectorized::BinaryColumn& column) override { return _impl->do_visit(column); }

    Status visit(const vectorized::LargeBinaryColumn& column) override { return _impl->do_visit(column); }

private:
    Impl* _impl;
};

template <typename Impl>
class ColumnVisitorMutableAdapter : public ColumnVisitorMutable {
public:
    explicit ColumnVisitorMutableAdapter(Impl* impl) : _impl(impl) {}

    ~ColumnVisitorMutableAdapter() override = default;

    Status visit(vectorized::JsonColumn* column) override { return _impl->do_visit(column); }

    Status visit(vectorized::HyperLogLogColumn* column) override { return _impl->do_visit(column); }

    Status visit(vectorized::BitmapColumn* column) override { return _impl->do_visit(column); }

    Status visit(vectorized::PercentileColumn* column) override { return _impl->do_visit(column); }

    Status visit(vectorized::Int8Column* column) override { return _impl->_impl->do_visit(column); }

    Status visit(vectorized::UInt8Column* column) override { return _impl->do_visit(column); }

    Status visit(vectorized::Int16Column* column) override { return _impl->do_visit(column); }

    Status visit(vectorized::UInt16Column* column) override { return _impl->do_visit(column); }

    Status visit(vectorized::Int32Column* column) override { return _impl->do_visit(column); }

    Status visit(vectorized::UInt32Column* column) override { return _impl->do_visit(column); }

    Status visit(vectorized::Int64Column* column) override { return _impl->do_visit(column); }

    Status visit(vectorized::UInt64Column* column) override { return _impl->do_visit(column); }

    Status visit(vectorized::Int128Column* column) override { return _impl->do_visit(column); }

    Status visit(vectorized::DoubleColumn* column) override { return _impl->do_visit(column); }

    Status visit(vectorized::FloatColumn* column) override { return _impl->do_visit(column); }

    Status visit(vectorized::DateColumn* column) override { return _impl->do_visit(column); }

    Status visit(vectorized::TimestampColumn* column) override { return _impl->do_visit(column); }

    Status visit(vectorized::DecimalColumn* column) override { return _impl->do_visit(column); }

    Status visit(vectorized::Decimal32Column* column) override { return _impl->do_visit(column); }

    Status visit(vectorized::Decimal64Column* column) override { return _impl->do_visit(column); }

    Status visit(vectorized::Decimal128Column* column) override { return _impl->do_visit(column); }

    Status visit(vectorized::FixedLengthColumn<int96_t>* column) override { return _impl->do_visit(column); }

    Status visit(vectorized::FixedLengthColumn<uint24_t>* column) override { return _impl->do_visit(column); }

    Status visit(vectorized::FixedLengthColumn<decimal12_t>* column) override { return _impl->do_visit(column); }

    Status visit(vectorized::NullableColumn* column) override { return _impl->do_visit(column); }

    Status visit(vectorized::ConstColumn* column) override { return _impl->do_visit(column); }

    Status visit(vectorized::ArrayColumn* column) override { return _impl->do_visit(column); }

    Status visit(vectorized::MapColumn* column) override { return _impl->do_visit(column); }

    Status visit(vectorized::BinaryColumn* column) override { return _impl->do_visit(column); }

    Status visit(vectorized::LargeBinaryColumn* column) override { return _impl->do_visit(column); }

private:
    Impl* _impl;
};

} // namespace starrocks