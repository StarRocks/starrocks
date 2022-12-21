// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <iostream>
#include <string>

#include "runtime/global_dict/types_fwd_decl.h"
// For esay of use these types, include phmap.h here
#include "util/phmap/phmap.h"

namespace starrocks {

extern ColumnIdToGlobalDictMap EMPTY_GLOBAL_DICTMAPS;

std::ostream& operator<<(std::ostream& stream, const RGlobalDictMap& map);
std::ostream& operator<<(std::ostream& stream, const GlobalDictMap& map);

} // namespace starrocks
