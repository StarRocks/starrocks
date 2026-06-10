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

#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "common/statusor.h"
#include "storage/index/compound_index_common.h"

namespace starrocks {
class FileSystem;

// Parses the header of a compound `.idx` file produced by
// CompoundIndexFileWriter. Subfile data bytes are NOT loaded — callers
// receive (offset, length) pairs and read the data via BlockCache or any
// other mechanism appropriate to their storage backend.
class CompoundIndexFileReader {
public:
    // Open and parse the header of `bin_path`. Returns a reader if the file
    // exists, has a valid magic + version, and the header table parses
    // cleanly. Header parsing reads at most a few KB; the data region is
    // never touched.
    static StatusOr<std::unique_ptr<CompoundIndexFileReader>> open(const std::string& bin_path);
    static StatusOr<std::unique_ptr<CompoundIndexFileReader>> open(const std::string& bin_path, FileSystem* fs);

    // Look up an index entry by (kind, index_id). Returns NotFound if no
    // matching entry exists in the header.
    StatusOr<CompoundIndexLayout> find_index(CompoundIndexKind kind, int64_t index_id) const;

    // Inspector — full list of indices in the file (for diagnostics or
    // exhaustive iteration).
    const std::vector<CompoundIndexLayout>& layouts() const { return _layouts; }

    const std::string& bin_path() const { return _bin_path; }

private:
    CompoundIndexFileReader(std::string bin_path, std::vector<CompoundIndexLayout> layouts)
            : _bin_path(std::move(bin_path)), _layouts(std::move(layouts)) {}

    std::string _bin_path;
    std::vector<CompoundIndexLayout> _layouts;
};

} // namespace starrocks
