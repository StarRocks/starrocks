// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "util/logging.h"

namespace starrocks {
extern bool k_starrocks_exit;
} // namespace starrocks

void start_cn();
void start_be();
