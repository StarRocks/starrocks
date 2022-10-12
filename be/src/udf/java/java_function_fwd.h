// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

namespace starrocks::vectorized {
class AggregateFunction;
class TableFunction;
const AggregateFunction* getJavaUDAFFunction(bool input_nullable);
const AggregateFunction* getJavaWindowFunction();
const TableFunction* getJavaUDTFFunction();
} // namespace starrocks::vectorized