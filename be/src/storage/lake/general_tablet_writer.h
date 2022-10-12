// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <string>
#include <vector>

#include "gutil/macros.h"
#include "storage/lake/tablet.h"
#include "storage/lake/tablet_metadata.h"
#include "storage/lake/tablet_writer.h"

namespace starrocks {
class SegmentWriter;
}

namespace starrocks::lake {

class GeneralTabletWriter : public TabletWriter {
public:
    explicit GeneralTabletWriter(Tablet tablet);

    ~GeneralTabletWriter() override;

    DISALLOW_COPY(GeneralTabletWriter);

    int64_t tablet_id() const override { return _tablet.id(); }

    Status open() override;

    Status write(const starrocks::vectorized::Chunk& data) override;

    Status flush() override;

    Status finish() override;

    void close() override;

    std::vector<std::string> files() const override { return _files; }

    int64_t data_size() const override { return _data_size; }

    int64_t num_rows() const override { return _num_rows; }

private:
    Status reset_segment_writer();
    Status flush_segment_writer();

    Tablet _tablet;
    std::shared_ptr<const TabletSchema> _schema;
    std::unique_ptr<SegmentWriter> _seg_writer;
    std::vector<std::string> _files;
    int64_t _num_rows = 0;
    int64_t _data_size = 0;
    uint32_t _seg_id = 0;
    bool _finished = false;
};

} // namespace starrocks::lake
