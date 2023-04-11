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

#include "common/statusor.h"
#include "storage/rowset/rowset.h"
#include "storage/rowset/rowset_writer.h"

namespace starrocks {

class Chunk;
class SegmentPB;
class RowsetWriterContext;
class SegmentWriter;

// used for column mode partial update
class HorizontalUpdateRowsetWriter final : public RowsetWriter {
public:
    explicit HorizontalUpdateRowsetWriter(const RowsetWriterContext& context);
    ~HorizontalUpdateRowsetWriter() override;
    Status add_chunk(const Chunk& chunk) override;

    Status flush_chunk(const Chunk& chunk, SegmentPB* seg_info = nullptr) override;

    Status flush() override;

private:
    StatusOr<std::unique_ptr<SegmentWriter>> _create_update_file_writer();

    std::unique_ptr<SegmentWriter> _update_file_writer;
};

} // namespace starrocks