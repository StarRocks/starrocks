// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "column/vectorized_fwd.h"
#include "column/object_column.h"
#include "column/vectorized_fwd.h"
#include "util/json.h"

namespace starrocks::vectorized {
    

// TODO(mofei) implement separated JsonColumn class
using JsonColumn = ObjectColumn<JsonValue>;

} // namespace starrocks::vectorized
