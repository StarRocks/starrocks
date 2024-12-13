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

<<<<<<< HEAD
=======
#include <memory>
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
#include <string>
#include <vector>

#include "gutil/macros.h"
#include "storage/lake/general_tablet_writer.h"
<<<<<<< HEAD
#include "storage/lake/tablet_metadata.h"
=======
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

namespace starrocks {
class SegmentWriter;
class RowsMapperBuilder;
} // namespace starrocks

namespace starrocks::lake {

class HorizontalPkTabletWriter : public HorizontalGeneralTabletWriter {
public:
<<<<<<< HEAD
    explicit HorizontalPkTabletWriter(Tablet tablet, std::shared_ptr<const TabletSchema> schema, int64_t txn_id,
                                      bool is_compaction);
=======
    explicit HorizontalPkTabletWriter(TabletManager* tablet_mgr, int64_t tablet_id,
                                      std::shared_ptr<const TabletSchema> schema, int64_t txn_id,
                                      ThreadPool* flush_pool, bool is_compaction);
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

    ~HorizontalPkTabletWriter() override;

    DISALLOW_COPY(HorizontalPkTabletWriter);

<<<<<<< HEAD
    Status write(const Chunk& data, const std::vector<uint64_t>& rssid_rowids) override;
=======
    Status write(const Chunk& data, const std::vector<uint64_t>& rssid_rowids, SegmentPB* segment = nullptr) override;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

    Status flush_del_file(const Column& deletes) override;

    Status flush_columns() override {
        return Status::NotSupported("HorizontalPkTabletWriter flush_columns not support");
    }

<<<<<<< HEAD
    Status finish() override;
=======
    Status finish(SegmentPB* segment = nullptr) override;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

    RowsetTxnMetaPB* rowset_txn_meta() override { return _rowset_txn_meta.get(); }

protected:
<<<<<<< HEAD
    Status flush_segment_writer() override;
=======
    Status flush_segment_writer(SegmentPB* segment = nullptr) override;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

private:
    std::unique_ptr<RowsetTxnMetaPB> _rowset_txn_meta;
    std::unique_ptr<RowsMapperBuilder> _rows_mapper_builder;
};

class VerticalPkTabletWriter : public VerticalGeneralTabletWriter {
public:
<<<<<<< HEAD
    explicit VerticalPkTabletWriter(Tablet tablet, std::shared_ptr<const TabletSchema> schema, int64_t txn_id,
                                    uint32_t max_rows_per_segment, bool is_compaction);
=======
    explicit VerticalPkTabletWriter(TabletManager* tablet_mgr, int64_t tablet_id,
                                    std::shared_ptr<const TabletSchema> schema, int64_t txn_id,
                                    uint32_t max_rows_per_segment, ThreadPool* flush_pool, bool is_compaction);
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

    ~VerticalPkTabletWriter() override;

    DISALLOW_COPY(VerticalPkTabletWriter);

<<<<<<< HEAD
    Status write(const starrocks::Chunk& data) override {
=======
    Status write(const starrocks::Chunk& data, SegmentPB* segment = nullptr) override {
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        return Status::NotSupported("VerticalPkTabletWriter write not support");
    }

    Status flush_del_file(const Column& deletes) override {
        return Status::NotSupported("VerticalPkTabletWriter flush_del_file not support");
    }

    Status write_columns(const Chunk& data, const std::vector<uint32_t>& column_indexes, bool is_key,
                         const std::vector<uint64_t>& rssid_rowids) override;

    // Finalize all segments footer.
<<<<<<< HEAD
    Status finish() override;
=======
    Status finish(SegmentPB* segment = nullptr) override;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

private:
    std::unique_ptr<RowsMapperBuilder> _rows_mapper_builder;
};

} // namespace starrocks::lake
