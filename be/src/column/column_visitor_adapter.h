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

    Status visit(const JsonColumn& column) override { return _impl->do_visit(column); }

    Status visit(const HyperLogLogColumn& column) override { return _impl->do_visit(column); }

    Status visit(const BitmapColumn& column) override { return _impl->do_visit(column); }

    Status visit(const PercentileColumn& column) override { return _impl->do_visit(column); }

    Status visit(const Int8Column& column) override { return _impl->_impl->do_visit(column); }

    Status visit(const UInt8Column& column) override { return _impl->do_visit(column); }

    Status visit(const Int16Column& column) override { return _impl->do_visit(column); }

    Status visit(const UInt16Column& column) override { return _impl->do_visit(column); }

    Status visit(const Int32Column& column) override { return _impl->do_visit(column); }

    Status visit(const UInt32Column& column) override { return _impl->do_visit(column); }

    Status visit(const Int64Column& column) override { return _impl->do_visit(column); }

    Status visit(const UInt64Column& column) override { return _impl->do_visit(column); }

    Status visit(const Int128Column& column) override { return _impl->do_visit(column); }

    Status visit(const DoubleColumn& column) override { return _impl->do_visit(column); }

    Status visit(const FloatColumn& column) override { return _impl->do_visit(column); }

    Status visit(const DateColumn& column) override { return _impl->do_visit(column); }

    Status visit(const TimestampColumn& column) override { return _impl->do_visit(column); }

    Status visit(const DecimalColumn& column) override { return _impl->do_visit(column); }

    Status visit(const Decimal32Column& column) override { return _impl->do_visit(column); }

    Status visit(const Decimal64Column& column) override { return _impl->do_visit(column); }

    Status visit(const Decimal128Column& column) override { return _impl->do_visit(column); }

    Status visit(const FixedLengthColumn<int96_t>& column) override { return _impl->do_visit(column); }

    Status visit(const FixedLengthColumn<uint24_t>& column) override { return _impl->do_visit(column); }

    Status visit(const FixedLengthColumn<decimal12_t>& column) override { return _impl->do_visit(column); }

    Status visit(const NullableColumn& column) override { return _impl->do_visit(column); }

    Status visit(const ConstColumn& column) override { return _impl->do_visit(column); }

    Status visit(const ArrayColumn& column) override { return _impl->do_visit(column); }

    Status visit(const MapColumn& column) override { return _impl->do_visit(column); }

    Status visit(const StructColumn& column) override { return _impl->do_visit(column); }

    Status visit(const BinaryColumn& column) override { return _impl->do_visit(column); }

    Status visit(const LargeBinaryColumn& column) override { return _impl->do_visit(column); }

private:
    Impl* _impl;
};

template <typename Impl>
class ColumnVisitorMutableAdapter : public ColumnVisitorMutable {
public:
    explicit ColumnVisitorMutableAdapter(Impl* impl) : _impl(impl) {}

    ~ColumnVisitorMutableAdapter() override = default;

    Status visit(JsonColumn* column) override { return _impl->do_visit(column); }

    Status visit(HyperLogLogColumn* column) override { return _impl->do_visit(column); }

    Status visit(BitmapColumn* column) override { return _impl->do_visit(column); }

    Status visit(PercentileColumn* column) override { return _impl->do_visit(column); }

    Status visit(Int8Column* column) override { return _impl->_impl->do_visit(column); }

    Status visit(UInt8Column* column) override { return _impl->do_visit(column); }

    Status visit(Int16Column* column) override { return _impl->do_visit(column); }

    Status visit(UInt16Column* column) override { return _impl->do_visit(column); }

    Status visit(Int32Column* column) override { return _impl->do_visit(column); }

    Status visit(UInt32Column* column) override { return _impl->do_visit(column); }

    Status visit(Int64Column* column) override { return _impl->do_visit(column); }

    Status visit(UInt64Column* column) override { return _impl->do_visit(column); }

    Status visit(Int128Column* column) override { return _impl->do_visit(column); }

    Status visit(DoubleColumn* column) override { return _impl->do_visit(column); }

    Status visit(FloatColumn* column) override { return _impl->do_visit(column); }

    Status visit(DateColumn* column) override { return _impl->do_visit(column); }

    Status visit(TimestampColumn* column) override { return _impl->do_visit(column); }

    Status visit(DecimalColumn* column) override { return _impl->do_visit(column); }

    Status visit(Decimal32Column* column) override { return _impl->do_visit(column); }

    Status visit(Decimal64Column* column) override { return _impl->do_visit(column); }

    Status visit(Decimal128Column* column) override { return _impl->do_visit(column); }

    Status visit(FixedLengthColumn<int96_t>* column) override { return _impl->do_visit(column); }

    Status visit(FixedLengthColumn<uint24_t>* column) override { return _impl->do_visit(column); }

    Status visit(FixedLengthColumn<decimal12_t>* column) override { return _impl->do_visit(column); }

    Status visit(NullableColumn* column) override { return _impl->do_visit(column); }

    Status visit(ConstColumn* column) override { return _impl->do_visit(column); }

    Status visit(ArrayColumn* column) override { return _impl->do_visit(column); }

    Status visit(MapColumn* column) override { return _impl->do_visit(column); }

    Status visit(StructColumn* column) override { return _impl->do_visit(column); }

    Status visit(BinaryColumn* column) override { return _impl->do_visit(column); }

    Status visit(LargeBinaryColumn* column) override { return _impl->do_visit(column); }

private:
    Impl* _impl;
};

} // namespace starrocks
