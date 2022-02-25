// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#ifdef STARROCKS_WITH_AWS

#pragma once

#include <memory>

#include "common/statusor.h"
#include "object_store/object_store.h"

namespace starrocks {

// [thread-safe]
void s3_global_init();

StatusOr<std::unique_ptr<ObjectStore>> new_s3_object_store();

} // namespace starrocks

#endif
