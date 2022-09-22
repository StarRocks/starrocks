// This file is licensed under the Elastic License 2.0. Copyright 2022-present, StarRocks Inc.

#pragma once

namespace orc {

enum class LoadType {
    NO_LAZY_LOAD = 0,
    LAZY_LOAD,
    // Only struct type needs this value, means it's subfields contain both lazy load field and no lazy field.
    COMPOUND_LOAD
};

} // namespace orc
