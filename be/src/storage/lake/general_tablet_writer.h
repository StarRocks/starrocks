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
#include <vector>

#include "gutil/macros.h"
#include "storage/lake/tablet_metadata.h"
#include "storage/lake/tablet_writer.h"

namespace starrocks {
class SegmentWriter;
}

namespace starrocks::lake {

class HorizontalGeneralTabletWriter : public TabletWriter {
public:
    explicit HorizontalGeneralTabletWriter(Tablet tablet, std::shared_ptr<const TabletSchema> schema, int64_t txn_id);

    ~HorizontalGeneralTabletWriter() override;

    DISALLOW_COPY(HorizontalGeneralTabletWriter);

    Status open() override;

    Status write(const starrocks::Chunk& data) override;

    Status write_columns(const Chunk& data, const std::vector<uint32_t>& column_indexes, bool is_key) override {
        return Status::NotSupported("HorizontalGeneralTabletWriter write_columns not support");
    }

    Status flush_del_file(const Column& deletes) override {
        return Status::NotSupported("HorizontalGeneralTabletWriter flush_del_file not support");
    }

    Status flush() override;

    Status flush_columns() override {
        return Status::NotSupported("HorizontalGeneralTabletWriter flush_columns not support");
    }

    Status finish() override;

    void close() override;

    RowsetTxnMetaPB* rowset_txn_meta() override { return nullptr; }

protected:
    Status reset_segment_writer();
    virtual Status flush_segment_writer();

    std::unique_ptr<SegmentWriter> _seg_writer;
};

class VerticalGeneralTabletWriter : public TabletWriter {
public:
    explicit VerticalGeneralTabletWriter(Tablet tablet, std::shared_ptr<const TabletSchema> schema, int64_t txn_id,
                                         uint32_t max_rows_per_segment);

    ~VerticalGeneralTabletWriter() override;

    DISALLOW_COPY(VerticalGeneralTabletWriter);

    Status open() override;

    Status write(const starrocks::Chunk& data) override {
        return Status::NotSupported("VerticalGeneralTabletWriter write not support");
    }

    Status write_columns(const Chunk& data, const std::vector<uint32_t>& column_indexes, bool is_key) override;

    Status flush_del_file(const Column& deletes) override {
        return Status::NotSupported("VerticalGeneralTabletWriter flush_del_file not support");
    }

    Status flush() override;

    Status flush_columns() override;

    // Finalize all segments footer.
    Status finish() override;

    void close() override;

    RowsetTxnMetaPB* rowset_txn_meta() override { return nullptr; }

private:
    StatusOr<std::unique_ptr<SegmentWriter>> create_segment_writer(const std::vector<uint32_t>& column_indexes,
                                                                   bool is_key);

    Status flush_columns(std::unique_ptr<SegmentWriter>* segment_writer);

    uint32_t _max_rows_per_segment = 0;
    std::vector<std::unique_ptr<SegmentWriter>> _segment_writers;
    size_t _current_writer_index = 0;
};

} // namespace starrocks::lake
