// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "storage/metastore.h"

namespace starrocks {

std::unique_ptr<Metastore> new_object_metastore(const std::string& meta_dir);

} // namespace starrocks