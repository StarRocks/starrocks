// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <iostream>
#include <string>

#include "runtime/global_dict/types_fwd_decl.h"
// For esay of use these types, include phmap.h here
#include "util/phmap/phmap.h"

namespace starrocks {
namespace vectorized {

extern ColumnIdToGlobalDictMap EMPTY_GLOBAL_DICTMAPS;

std::ostream& operator<<(std::ostream& stream, const RGlobalDictMap& map);
std::ostream& operator<<(std::ostream& stream, const GlobalDictMap& map);

} // namespace vectorized
} // namespace starrocks
