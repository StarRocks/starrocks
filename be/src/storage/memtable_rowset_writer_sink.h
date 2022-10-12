// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
#pragma once

#include "gen_cpp/data.pb.h"
#include "gutil/macros.h"
#include "storage/memtable_sink.h"
#include "storage/rowset/rowset_writer.h"

namespace starrocks::vectorized {

class MemTableRowsetWriterSink : public MemTableSink {
public:
    explicit MemTableRowsetWriterSink(RowsetWriter* w) : _rowset_writer(w) {}
    ~MemTableRowsetWriterSink() override = default;

    DISALLOW_COPY(MemTableRowsetWriterSink);

    Status flush_chunk(const Chunk& chunk, SegmentPB* seg_info = nullptr) override {
        return _rowset_writer->flush_chunk(chunk, seg_info);
    }

    Status flush_chunk_with_deletes(const Chunk& upserts, const Column& deletes) override {
        return _rowset_writer->flush_chunk_with_deletes(upserts, deletes);
    }

private:
    RowsetWriter* _rowset_writer;
};

} // namespace starrocks::vectorized
