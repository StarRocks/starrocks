// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <memory>

#include "storage/convert_helper.h"
#include "storage/rowset/rowset_writer.h"

namespace starrocks::vectorized {

// RowsetWriterAdapter used to convert data from |memory_format_version| to |storage_format_version|.
//
// When loading data into StarRocks, the in-memory data format may differ from the on-disk format if
// the |memory_format_version| of `RowsetWriterContext` is different from |storage_format_version|.
// In this case, we should convert the in-memory data format from |memory_format_version| to
// |storage_format_version| before call `RowsetWriter::add_row` or `RowsetWriter::add_chunk`.
class RowsetWriterAdapter : public RowsetWriter {
public:
    explicit RowsetWriterAdapter(const RowsetWriterContext& context);

    ~RowsetWriterAdapter() override = default;

    Status init() override;

    Status add_chunk(const vectorized::Chunk& chunk) override;

    Status flush_chunk(const vectorized::Chunk& chunk) override;

    Status flush_chunk_with_deletes(const vectorized::Chunk& upserts, const vectorized::Column& deletes) override;

    Status add_rowset(RowsetSharedPtr rowset) override { return _writer->add_rowset(rowset); }

    Status add_rowset_for_linked_schema_change(RowsetSharedPtr rowset, const SchemaMapping& schema_mapping) override {
        return _writer->add_rowset_for_linked_schema_change(std::move(rowset), schema_mapping);
    }

    Status flush() override { return _writer->flush(); }

    StatusOr<RowsetSharedPtr> build() override { return _writer->build(); }

    Version version() override { return _writer->version(); }

    int64_t num_rows() override { return _writer->num_rows(); }

    int64_t total_data_size() override { return _writer->total_data_size(); }

    RowsetId rowset_id() override { return _writer->rowset_id(); }

private:
    Status _init_chunk_converter();

    std::unique_ptr<TabletSchema> _in_schema;
    std::unique_ptr<TabletSchema> _out_schema;
    std::unique_ptr<RowsetWriter> _writer;
    std::unique_ptr<ChunkConverter> _chunk_converter;

    Status _status;
};

} // namespace starrocks::vectorized
