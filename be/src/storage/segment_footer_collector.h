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
#include <utility>
#include <vector>

#include "gen_cpp/segment.pb.h"
#include "storage/olap_common.h"

namespace starrocks {

class Tablet;

// Read the footer of every segment currently visible in `tablet`, so callers that
// only need segment metadata (column encoding, compression, page pointers) do not
// have to open files themselves. Opening files is a storage-layer concern; keeping
// it here lets metadata consumers such as the information_schema scanners stay off
// the filesystem layer.
//
// Best effort by design: a segment that cannot be opened or parsed (encrypted,
// bundled, or already vacuumed) is skipped rather than failing the whole call, so
// a metadata query degrades to fewer rows instead of an error. Returns the
// (segment id, footer) pairs actually read, in rowset then segment order.
std::vector<std::pair<int64_t, SegmentFooterPB>> collect_visible_segment_footers(const std::shared_ptr<Tablet>& tablet);

} // namespace starrocks
