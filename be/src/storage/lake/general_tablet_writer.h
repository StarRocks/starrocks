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
#include <future>
#include <memory>
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
#include <string>
#include <vector>

#include "gutil/macros.h"
<<<<<<< HEAD
#include "storage/lake/tablet_metadata.h"
#include "storage/lake/tablet_writer.h"

namespace starrocks {
class SegmentWriter;
}
=======
#include "storage/lake/tablet_writer.h"

namespace starrocks {
class ConcurrencyLimitedThreadPoolToken;
class SegmentWriter;
class ThreadPool;
} // namespace starrocks
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))

namespace starrocks::lake {

class HorizontalGeneralTabletWriter : public TabletWriter {
public:
<<<<<<< HEAD
    explicit HorizontalGeneralTabletWriter(Tablet tablet, std::shared_ptr<const TabletSchema> schema, int64_t txn_id);
=======
    explicit HorizontalGeneralTabletWriter(TabletManager* tablet_mgr, int64_t tablet_id,
                                           std::shared_ptr<const TabletSchema> schema, int64_t txn_id,
                                           bool is_compaction, ThreadPool* flush_pool = nullptr);
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))

    ~HorizontalGeneralTabletWriter() override;

    DISALLOW_COPY(HorizontalGeneralTabletWriter);

    Status open() override;

<<<<<<< HEAD
    Status write(const starrocks::Chunk& data) override;

    Status write(const Chunk& data, const std::vector<uint64_t>& rssid_rowids) {
=======
    Status write(const Chunk& data, SegmentPB* segment = nullptr) override;

    Status write(const Chunk& data, const std::vector<uint64_t>& rssid_rowids, SegmentPB* segment = nullptr) {
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        return Status::NotSupported("HorizontalGeneralTabletWriter write not support");
    }

    Status write_columns(const Chunk& data, const std::vector<uint32_t>& column_indexes, bool is_key) override {
        return Status::NotSupported("HorizontalGeneralTabletWriter write_columns not support");
    }

    Status write_columns(const Chunk& data, const std::vector<uint32_t>& column_indexes, bool is_key,
                         const std::vector<uint64_t>& rssid_rowids) override {
        return Status::NotSupported("HorizontalGeneralTabletWriter write_columns not support");
    }

    Status flush_del_file(const Column& deletes) override {
        return Status::NotSupported("HorizontalGeneralTabletWriter flush_del_file not support");
    }

<<<<<<< HEAD
    Status flush() override;
=======
    Status flush(SegmentPB* segment = nullptr) override;
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))

    Status flush_columns() override {
        return Status::NotSupported("HorizontalGeneralTabletWriter flush_columns not support");
    }

<<<<<<< HEAD
    Status finish() override;
=======
    Status finish(SegmentPB* segment = nullptr) override;
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))

    void close() override;

    RowsetTxnMetaPB* rowset_txn_meta() override { return nullptr; }

protected:
    Status reset_segment_writer();
<<<<<<< HEAD
    virtual Status flush_segment_writer();
=======
    virtual Status flush_segment_writer(SegmentPB* segment = nullptr);
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))

    std::unique_ptr<SegmentWriter> _seg_writer;
};

class VerticalGeneralTabletWriter : public TabletWriter {
public:
<<<<<<< HEAD
    explicit VerticalGeneralTabletWriter(Tablet tablet, std::shared_ptr<const TabletSchema> schema, int64_t txn_id,
                                         uint32_t max_rows_per_segment);
=======
    explicit VerticalGeneralTabletWriter(TabletManager* tablet_mgr, int64_t tablet_id,
                                         std::shared_ptr<const TabletSchema> schema, int64_t txn_id,
                                         uint32_t max_rows_per_segment, bool is_compaction,
                                         ThreadPool* flush_pool = nullptr);
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))

    ~VerticalGeneralTabletWriter() override;

    DISALLOW_COPY(VerticalGeneralTabletWriter);

    Status open() override;

<<<<<<< HEAD
    Status write(const starrocks::Chunk& data) override {
        return Status::NotSupported("VerticalGeneralTabletWriter write not support");
    }

    Status write(const Chunk& data, const std::vector<uint64_t>& rssid_rowids) override {
=======
    Status write(const Chunk& data, SegmentPB* segment = nullptr) override {
        return Status::NotSupported("VerticalGeneralTabletWriter write not support");
    }

    Status write(const Chunk& data, const std::vector<uint64_t>& rssid_rowids, SegmentPB* segment = nullptr) override {
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        return Status::NotSupported("HorizontalGeneralTabletWriter write not support");
    }

    Status write_columns(const Chunk& data, const std::vector<uint32_t>& column_indexes, bool is_key) override;

    Status write_columns(const Chunk& data, const std::vector<uint32_t>& column_indexes, bool is_key,
                         const std::vector<uint64_t>& rssid_rowids) override {
        return Status::NotSupported("VerticalGeneralTabletWriter write_columns not support");
    }

    Status flush_del_file(const Column& deletes) override {
        return Status::NotSupported("VerticalGeneralTabletWriter flush_del_file not support");
    }

<<<<<<< HEAD
    Status flush() override;
=======
    Status flush(SegmentPB* segment = nullptr) override;
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))

    Status flush_columns() override;

    // Finalize all segments footer.
<<<<<<< HEAD
    Status finish() override;
=======
    Status finish(SegmentPB* segment = nullptr) override;
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))

    void close() override;

    RowsetTxnMetaPB* rowset_txn_meta() override { return nullptr; }

<<<<<<< HEAD
private:
    StatusOr<std::unique_ptr<SegmentWriter>> create_segment_writer(const std::vector<uint32_t>& column_indexes,
                                                                   bool is_key);

    Status flush_columns(std::unique_ptr<SegmentWriter>* segment_writer);

    uint32_t _max_rows_per_segment = 0;
    std::vector<std::unique_ptr<SegmentWriter>> _segment_writers;
    size_t _current_writer_index = 0;
=======
protected:
    StatusOr<std::shared_ptr<SegmentWriter>> create_segment_writer(const std::vector<uint32_t>& column_indexes,
                                                                   bool is_key);

    Status flush_columns(const std::shared_ptr<SegmentWriter>& segment_writer);
    Status check_futures();
    Status wait_futures_finish();

    uint32_t _max_rows_per_segment = 0;
    std::vector<std::shared_ptr<SegmentWriter>> _segment_writers;
    size_t _current_writer_index = 0;

    static constexpr int64_t kDefaultTimeoutForAsyncWriteSegment = 1 * 60 * 1000L; // 1 minutes

    std::unique_ptr<ConcurrencyLimitedThreadPoolToken> _segment_writer_finalize_token;
    std::vector<std::future<Status>> _futures;
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
};

} // namespace starrocks::lake
