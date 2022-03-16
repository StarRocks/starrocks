// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "env/env.h"

namespace starrocks {

std::unique_ptr<Env> new_env_s3();

} // namespace starrocks
