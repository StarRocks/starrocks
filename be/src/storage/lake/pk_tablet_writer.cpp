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

#include "storage/lake/pk_tablet_writer.h"

#include <fmt/format.h>

#include "column/chunk.h"
#include "fs/fs_util.h"
#include "serde/column_array_serde.h"
#include "storage/lake/filenames.h"
#include "storage/rowset/segment_writer.h"

namespace starrocks::lake {

<<<<<<< HEAD
HorizontalPkTabletWriter::HorizontalPkTabletWriter(Tablet tablet, std::shared_ptr<const TabletSchema> schema,
                                                   int64_t txn_id)
        : HorizontalGeneralTabletWriter(tablet, std::move(schema), txn_id),
=======
HorizontalPkTabletWriter::HorizontalPkTabletWriter(TabletManager* tablet_mgr, int64_t tablet_id,
                                                   std::shared_ptr<const TabletSchema> schema, int64_t txn_id,
                                                   ThreadPool* flush_pool)
        : HorizontalGeneralTabletWriter(tablet_mgr, tablet_id, std::move(schema), txn_id, flush_pool),
>>>>>>> bd9d3cbd0a ([Enhancement] Support async segment writer for lake compaction (#36630))
          _rowset_txn_meta(std::make_unique<RowsetTxnMetaPB>()) {}

HorizontalPkTabletWriter::~HorizontalPkTabletWriter() = default;

Status HorizontalPkTabletWriter::flush_del_file(const Column& deletes) {
    auto name = gen_del_filename(_txn_id);
    ASSIGN_OR_RETURN(auto of, fs::new_writable_file(_tablet.del_location(name)));
    size_t sz = serde::ColumnArraySerde::max_serialized_size(deletes);
    std::vector<uint8_t> content(sz);
    if (serde::ColumnArraySerde::serialize(deletes, content.data()) == nullptr) {
        return Status::InternalError("deletes column serialize failed");
    }
    RETURN_IF_ERROR(of->append(Slice(content.data(), content.size())));
    RETURN_IF_ERROR(of->close());
    _files.emplace_back(FileInfo{std::move(name), content.size()});
    return Status::OK();
}

Status HorizontalPkTabletWriter::flush_segment_writer() {
    if (_seg_writer != nullptr) {
        uint64_t segment_size = 0;
        uint64_t index_size = 0;
        uint64_t footer_position = 0;
        RETURN_IF_ERROR(_seg_writer->finalize(&segment_size, &index_size, &footer_position));
        // partial update
        auto* partial_rowset_footer = _rowset_txn_meta->add_partial_rowset_footers();
        partial_rowset_footer->set_position(footer_position);
        partial_rowset_footer->set_size(segment_size - footer_position);
        const std::string& segment_path = _seg_writer->segment_path();
        std::string segment_name = std::string(basename(segment_path));
        _files.emplace_back(FileInfo{segment_name, segment_size});
        _data_size += segment_size;
        _seg_writer.reset();
    }
    return Status::OK();
}

<<<<<<< HEAD
VerticalPkTabletWriter::VerticalPkTabletWriter(Tablet tablet, std::shared_ptr<const TabletSchema> schema,
                                               int64_t txn_id, uint32_t max_rows_per_segment)
        : VerticalGeneralTabletWriter(tablet, std::move(schema), txn_id, max_rows_per_segment) {}
=======
VerticalPkTabletWriter::VerticalPkTabletWriter(TabletManager* tablet_mgr, int64_t tablet_id,
                                               std::shared_ptr<const TabletSchema> schema, int64_t txn_id,
                                               uint32_t max_rows_per_segment, ThreadPool* flush_pool)
        : VerticalGeneralTabletWriter(tablet_mgr, tablet_id, std::move(schema), txn_id, max_rows_per_segment,
                                      flush_pool) {}
>>>>>>> bd9d3cbd0a ([Enhancement] Support async segment writer for lake compaction (#36630))

VerticalPkTabletWriter::~VerticalPkTabletWriter() = default;

} // namespace starrocks::lake
