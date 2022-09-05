// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "column/datum_tuple.h"

#include "column/schema.h"

namespace starrocks::vectorized {

int DatumTuple::compare(const Schema& schema, const DatumTuple& rhs) const {
    CHECK_EQ(_datums.size(), schema.num_fields());
    CHECK_EQ(_datums.size(), rhs._datums.size());
    for (size_t i = 0; i < _datums.size(); i++) {
        int r = schema.field(i)->type()->cmp(_datums[i], rhs[i]);
        if (r != 0) {
            return r;
        }
    }
    return 0;
}

} // namespace starrocks::vectorized
