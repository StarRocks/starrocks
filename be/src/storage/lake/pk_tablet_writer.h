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
#include <vector>

#include "gutil/macros.h"
#include "runtime/global_dict/types_fwd_decl.h"
#include "storage/lake/general_tablet_writer.h"

namespace starrocks {
class SegmentWriter;
class RowsMapperBuilder;
class BundleWritableFileContext;
} // namespace starrocks

namespace starrocks::lake {

class DefaultSSTWriter;

class HorizontalPkTabletWriter : public HorizontalGeneralTabletWriter {
public:
    explicit HorizontalPkTabletWriter(TabletManager* tablet_mgr, int64_t tablet_id,
                                      std::shared_ptr<const TabletSchema> schema, int64_t txn_id,
                                      ThreadPool* flush_pool, bool is_compaction,
                                      BundleWritableFileContext* bundle_file_context = nullptr,
                                      GlobalDictByNameMaps* _global_dicts = nullptr);

    ~HorizontalPkTabletWriter() override;

    DISALLOW_COPY(HorizontalPkTabletWriter);

    Status write(const Chunk& data, const std::vector<uint64_t>& rssid_rowids, SegmentPB* segment = nullptr) override;

    Status write(const Chunk& data, SegmentPB* segment = nullptr, bool eos = false) override;

    Status flush_del_file(const Column& deletes) override;

    Status flush_columns() override {
        return Status::NotSupported("HorizontalPkTabletWriter flush_columns not support");
    }

    Status finish(SegmentPB* segment = nullptr) override;

    StatusOr<std::unique_ptr<TabletWriter>> clone() const override;

    RowsetTxnMetaPB* rowset_txn_meta() override { return _rowset_txn_meta.get(); }

protected:
    Status reset_segment_writer(bool eos) override;
    Status flush_segment_writer(SegmentPB* segment = nullptr) override;

private:
    void _init_rows_mapper_builder();

    std::unique_ptr<RowsetTxnMetaPB> _rowset_txn_meta;
    std::unique_ptr<RowsMapperBuilder> _rows_mapper_builder;
    std::unique_ptr<DefaultSSTWriter> _pk_sst_writer;
    bool _rows_mapper_init_attempted = false;
};

class VerticalPkTabletWriter : public VerticalGeneralTabletWriter {
public:
    explicit VerticalPkTabletWriter(TabletManager* tablet_mgr, int64_t tablet_id,
                                    std::shared_ptr<const TabletSchema> schema, int64_t txn_id,
                                    uint32_t max_rows_per_segment, ThreadPool* flush_pool, bool is_compaction);

    ~VerticalPkTabletWriter() override;

    DISALLOW_COPY(VerticalPkTabletWriter);

    Status write(const starrocks::Chunk& data, SegmentPB* segment = nullptr, bool eos = false) override {
        return Status::NotSupported("VerticalPkTabletWriter write not support");
    }

    Status flush_del_file(const Column& deletes) override {
        return Status::NotSupported("VerticalPkTabletWriter flush_del_file not support");
    }

    Status write_columns(const Chunk& data, const std::vector<uint32_t>& column_indexes, bool is_key,
                         const std::vector<uint64_t>& rssid_rowids) override;

    // Finalize all segments footer.
    Status finish(SegmentPB* segment = nullptr) override;

private:
    void _init_rows_mapper_builder();

    std::unique_ptr<RowsMapperBuilder> _rows_mapper_builder;
    std::vector<std::unique_ptr<DefaultSSTWriter>> _pk_sst_writers;
    bool _rows_mapper_init_attempted = false;
};

} // namespace starrocks::lake
