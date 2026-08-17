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

#include "storage/index/gist/gist_index_writer.h"

#include <fstream>

#include "column/binary_column.h"
#include "column/column_helper.h"
#include "column/nullable_column.h"
#include "common/logging.h"
#include "fs/fs.h"
#include "geo/geo_types.h"
#include "storage/index/gist/rtree.h"

namespace starrocks {

static constexpr char kNodeCapacityKey[] = "node_capacity";

void GiSTIndexWriter::create(const std::shared_ptr<TabletIndex>& tablet_index,
                             const std::string& index_file_path,
                             std::unique_ptr<GiSTIndexWriter>* res) {
    *res = std::make_unique<GiSTIndexWriter>(tablet_index, index_file_path);
}

GiSTIndexWriter::GiSTIndexWriter(const std::shared_ptr<TabletIndex>& tablet_index,
                                 std::string index_file_path)
        : _tablet_index(tablet_index), _index_file_path(std::move(index_file_path)) {
    // Parse node_capacity from index properties (default 50)
    const auto& props = tablet_index->index_properties();
    auto it = props.find(kNodeCapacityKey);
    if (it != props.end()) {
        try {
            int cap = std::stoi(it->second);
            if (cap >= 4 && cap <= 1024) _node_capacity = cap;
        } catch (...) {
            // keep default
        }
    }
}

Status GiSTIndexWriter::init() {
    _entries.clear();
    _next_row_id = 0;
    return Status::OK();
}

Status GiSTIndexWriter::append(const Column& src) {
    const BinaryColumn* binary_col = nullptr;

    if (src.is_nullable()) {
        const auto& nullable = static_cast<const NullableColumn&>(src);
        binary_col = ColumnHelper::as_raw_column<BinaryColumn>(nullable.data_column());
    } else {
        binary_col = ColumnHelper::as_raw_column<BinaryColumn>(&src);
    }
    if (binary_col == nullptr) {
        return Status::InternalError("GiSTIndexWriter: column is not a BinaryColumn");
    }

    const size_t n = src.size();
    _entries.reserve(_entries.size() + n);

    for (size_t i = 0; i < n; ++i) {
        uint32_t row_id = _next_row_id++;
        if (src.is_null(i)) {
            continue; // NULL geometries are excluded from the index
        }

        Slice slice = binary_col->get_slice(i);
        std::unique_ptr<GeoShape> shape(GeoShape::from_encoded(slice.data, slice.size));
        if (shape == nullptr) continue;

        double min_x, min_y, max_x, max_y;
        if (!geo_bounding_box(shape.get(), &min_x, &min_y, &max_x, &max_y)) continue;

        _entries.push_back({row_id, {min_x, min_y, max_x, max_y}});
    }
    return Status::OK();
}

Status GiSTIndexWriter::finish(uint64_t* index_size) {
    std::string tree_data = rtree_build_str(_entries, _node_capacity);

    // Use StarRocks' file-system abstraction so the write goes through the
    // same path as other standalone index files (vector, inverted).
    // The parent directory is expected to exist (created by segment infrastructure).
    auto fs = FileSystem::Default();
    ASSIGN_OR_RETURN(auto wf, fs->new_writable_file(_index_file_path));
    RETURN_IF_ERROR(wf->append(Slice(tree_data)));
    RETURN_IF_ERROR(wf->close());

    if (index_size) *index_size = tree_data.size();
    return Status::OK();
}

uint64_t GiSTIndexWriter::estimate_buffer_size() const {
    return _entries.size() * 40 + RTREE_HEADER_SIZE;
}

} // namespace starrocks
