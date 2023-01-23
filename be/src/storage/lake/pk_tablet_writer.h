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
#include "storage/lake/tablet.h"
#include "storage/lake/tablet_metadata.h"
#include "storage/lake/tablet_writer.h"

namespace starrocks {
class SegmentWriter;
}

namespace starrocks::lake {

class PkTabletWriter : public TabletWriter {
public:
    explicit PkTabletWriter(std::shared_ptr<const TabletSchema> tschema, Tablet tablet);

    ~PkTabletWriter() override;

    DISALLOW_COPY(PkTabletWriter);

    int64_t tablet_id() const override { return _tablet.id(); }

    Status open() override;

    Status write(const starrocks::Chunk& data) override;

    Status flush_del_file(const Column& deletes) override;

    Status flush() override;

    Status finish() override;

    void close() override;

    std::vector<std::string> files() const override { return _files; }

    int64_t data_size() const override { return _data_size; }

    int64_t num_rows() const override { return _num_rows; }

    RowsetTxnMetaPB* rowset_txn_meta() override { return _rowset_txn_meta.get(); }

private:
    Status reset_segment_writer();
    Status flush_segment_writer();

    Tablet _tablet;
    std::unique_ptr<SegmentWriter> _seg_writer;
    std::vector<std::string> _files;
    int64_t _num_rows = 0;
    int64_t _data_size = 0;
    uint32_t _seg_id = 0;
    bool _finished = false;
    std::unique_ptr<RowsetTxnMetaPB> _rowset_txn_meta;
};

} // namespace starrocks::lake
