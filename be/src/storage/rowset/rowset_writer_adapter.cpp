// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "rowset_writer_adapter.h"

#include "column/chunk.h"
#include "column/schema.h"
#include "storage/chunk_helper.h"
#include "storage/convert_helper.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/type_utils.h"
#include "storage/uint24.h"

namespace starrocks::vectorized {

RowsetWriterAdapter::RowsetWriterAdapter(const RowsetWriterContext& context) {
    DCHECK_NE(context.memory_format_version, context.storage_format_version);
    // Actually, either |_in_schema| or |_out_schema| must be exactly the same
    // as |context._tablet_schema|, so one of the converting is unnecessary.
    _in_schema = context.tablet_schema->convert_to_format(context.memory_format_version);
    _out_schema = context.tablet_schema->convert_to_format(context.storage_format_version);
    if (_in_schema == nullptr || _out_schema == nullptr) {
        _status = Status::InvalidArgument("invalid schema");
    } else {
        RowsetWriterContext rowset_context = context;
        rowset_context.memory_format_version = rowset_context.storage_format_version;
        _status = RowsetFactory::create_rowset_writer(rowset_context, &_writer);
    }
}

Status RowsetWriterAdapter::init() {
    return _status;
}

Status RowsetWriterAdapter::add_chunk(const vectorized::Chunk& chunk) {
    if (_chunk_converter == nullptr) {
        RETURN_IF_ERROR(_init_chunk_converter());
    }
    return _writer->add_chunk(*_chunk_converter->copy_convert(chunk));
}

Status RowsetWriterAdapter::flush_chunk(const vectorized::Chunk& chunk) {
    if (_chunk_converter == nullptr) {
        RETURN_IF_ERROR(_init_chunk_converter());
    }
    return _writer->flush_chunk(*_chunk_converter->copy_convert(chunk));
}

Status RowsetWriterAdapter::_init_chunk_converter() {
    _chunk_converter = std::make_unique<ChunkConverter>();
    RETURN_IF_ERROR(_chunk_converter->init(ChunkHelper::convert_schema(*_in_schema),
                                           ChunkHelper::convert_schema(*_out_schema)));
    // |_in_schema| and |_out_schema| can be freed now.
    _in_schema.reset();
    _out_schema.reset();
    return Status::OK();
}

Status RowsetWriterAdapter::flush_chunk_with_deletes(const vectorized::Chunk& upserts,
                                                     const vectorized::Column& deletes) {
    LOG(WARNING) << "updatable tablet should not use RowsetWriterAdapter";
    if (_chunk_converter == nullptr) {
        RETURN_IF_ERROR(_init_chunk_converter());
    }
    return _writer->flush_chunk_with_deletes(*_chunk_converter->copy_convert(upserts), deletes);
}

} // namespace starrocks::vectorized
