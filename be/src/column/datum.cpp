// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "column/datum.h"

#include <variant>

namespace starrocks::vectorized {

Datum convert2Datum(const DatumKey& key) {
    return std::visit([](auto&& arg) -> Datum { return arg; }, key);
}

} // namespace starrocks::vectorized
