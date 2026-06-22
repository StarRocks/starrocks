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

#include <cstdint>
#include <string_view>

#include "common/status.h"
#include "storage/tablet_schema.h"

namespace starrocks {
struct SegmentWriterOptions;
class FileSystem;

namespace lake {
class TabletManager;
class LocationProvider;

// async/sync is effectively a table-level setting: every vector index on a given schema shares the
// same index_build_mode, so "any vector index is async" is equivalent to "the table builds vector
// indexes asynchronously" for scheduling purposes.
bool has_async_vector_index(const TabletSchemaCSPtr& schema);

// Returns the configured deferred-build row-count threshold for the schema's vector index, or the
// config default (config_vector_index_default_build_threshold) when unset or unparsable.
uint32_t get_vector_index_build_threshold(const TabletSchemaCSPtr& schema);

// For each column with a vector index, resolve the full segment-level path for the upcoming .vi
// file (keyed on |segment_name| via the location provider, or the tablet manager fallback) and
// stash it in |opts.vector_index_file_paths|. The SegmentWriter picks these up to direct tenann's
// writer at object storage. Leaving the map empty would make SegmentWriter fall back to the
// IndexDescriptor-based path, which in shared-data mode is not reachable via the location provider.
Status fill_vector_index_file_paths(const TabletSchemaCSPtr& schema, int64_t tablet_id, std::string_view segment_name,
                                    TabletManager* tablet_mgr, LocationProvider* location_provider, FileSystem* fs,
                                    SegmentWriterOptions& opts);

} // namespace lake
} // namespace starrocks
