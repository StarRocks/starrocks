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

#include <string>

namespace starrocks::csv {

inline const std::string DEFAULT_FIELD_DELIM = "\001";
inline const std::string DEFAULT_COLLECTION_DELIM = "\002";
inline const std::string DEFAULT_MAPKEY_DELIM = "\003";

// LF = Line Feed = '\n'
inline const std::string LINE_DELIM_LF = "\n";
// Most Hive TextFile files use LF as line delimiter.
inline const std::string DEFAULT_LINE_DELIM = LINE_DELIM_LF;
// CR = Carriage Return = '\r'
inline const std::string LINE_DELIM_CR = "\r";
// TODO(SmithCruise): support CR + LF when CSV row delimiter handling accepts multi-character delimiters.
inline const std::string LINE_DELIM_CR_LF = "\r\n";

} // namespace starrocks::csv
