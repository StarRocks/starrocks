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

#include <avrocpp/Generic.hh>
#include <vector>

#include "common/statusor.h"
#include "exprs/json_functions.h" // SimpleJsonPath

namespace starrocks {

// Rewrites a jsonpaths string for the avrocpp navigator: ".*." -> "." and a trailing ".*" -> "",
// so paths written for the old union-tag style (e.g. "$.*.id") resolve as "$.id". Must run before
// JsonFunctions::parse_json_paths.
std::string avro_preprocess_jsonpaths(std::string jsonpaths);

// Navigates a decoded Avro datum along a parsed jsonpath (the same SimpleJsonPath vector produced by
// JsonFunctions::parse_json_paths) and returns a pointer to the selected sub-datum. The pointer is
// valid only as long as `root` is alive and unmodified.
//
// Returns NotFound when a record/map key along the path is absent, so the caller can append a null.
// Returns InternalError on a structural mismatch (e.g. indexing a non-array). Wildcard `[*]`
// (idx == -2) is intentionally unsupported and reported as an error.
StatusOr<const avro::GenericDatum*> avro_extract_field(const avro::GenericDatum& root,
                                                       const std::vector<SimpleJsonPath>& path);

} // namespace starrocks
