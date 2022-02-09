#pragma once

namespace starrocks::vectorized {
class AggregateFunction;
class TableFunction;
const AggregateFunction* getJavaUDAFFunction(bool input_nullable);
const AggregateFunction* getJavaWindowFunction();
const TableFunction* getJavaUDTFFunction();
} // namespace starrocks::vectorized