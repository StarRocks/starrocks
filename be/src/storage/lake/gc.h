// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <string_view>

#include "common/status.h"

namespace starrocks::lake {

class TabletManager;

Status metadata_gc(std::string_view root_location, TabletManager* tablet_mgr);

Status segment_gc(std::string_view root_location, TabletManager* tablet_mgr);

} // namespace starrocks::lake