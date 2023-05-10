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

class PkTabletWriter : public TabletWriter {
public:
    explicit PkTabletWriter(Tablet tablet, std::shared_ptr<const TabletSchema> tschema);

    ~PkTabletWriter() override;

    DISALLOW_COPY(PkTabletWriter);

    Status open() override;

    Status write(const starrocks::Chunk& data) override;

    Status write_columns(const Chunk& data, const std::vector<uint32_t>& column_indexes, bool is_key) override {
        return Status::NotSupported("PkTabletWriter write_columns not support");
    }

    Status flush_del_file(const Column& deletes) override;

    Status flush() override;

    Status flush_columns() override { return Status::NotSupported("PkTabletWriter flush_columns not support"); }

    Status finish() override;

    void close() override;

    RowsetTxnMetaPB* rowset_txn_meta() override { return _rowset_txn_meta.get(); }

private:
    Status reset_segment_writer();
    Status flush_segment_writer();

    std::unique_ptr<SegmentWriter> _seg_writer;
    std::unique_ptr<RowsetTxnMetaPB> _rowset_txn_meta;
};

} // namespace starrocks::lake
