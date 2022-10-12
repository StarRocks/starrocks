// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <memory>

#include "gen_cpp/lake_types.pb.h"

namespace starrocks::lake {

using TabletMetadata = TabletMetadataPB;
using TabletMetadataPtr = std::shared_ptr<const TabletMetadata>;
using MutableTabletMetadataPtr = std::shared_ptr<TabletMetadata>;

} // namespace starrocks::lake
