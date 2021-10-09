// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "storage/rowset/vectorized/rowset_writer_adapter.h"

#include "storage/row.h"
#include "storage/rowset/beta_rowset_writer.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/uint24.h"
#include "storage/vectorized/chunk_helper.h"
#include "storage/vectorized/convert_helper.h"
#include "storage/vectorized/type_utils.h"

namespace starrocks::vectorized {

RowsetWriterAdapter::RowsetWriterAdapter(const RowsetWriterContext& context) {
    DCHECK_NE(context.memory_format_version, context.storage_format_version);
    // Actually, either |_in_schema| or |_out_schema| must be exactly the same
    // as |context._tablet_schema|, so one of the converting is unnecessary.
    _in_schema = context.tablet_schema->convert_to_format(context.memory_format_version);
    _out_schema = context.tablet_schema->convert_to_format(context.storage_format_version);
    if (_in_schema == nullptr || _out_schema == nullptr) {
        _status = OLAP_ERR_INVALID_SCHEMA;
    } else {
        RowsetWriterContext rowset_context = context;
        rowset_context.memory_format_version = rowset_context.storage_format_version;
        _status = RowsetFactory::create_rowset_writer(rowset_context, &_writer).ok() ? OLAP_SUCCESS
                                                                                     : OLAP_ERR_OTHER_ERROR;
    }
}

OLAPStatus RowsetWriterAdapter::init() {
    return _status;
}

OLAPStatus RowsetWriterAdapter::add_row(const RowCursor& row) {
    if (_row_converter == nullptr) {
        RETURN_NOT_OK(_init_row_converter());
    }
    _row_converter->convert(_row.get(), row);
    return _writer->add_row(*_row);
}

OLAPStatus RowsetWriterAdapter::add_row(const ContiguousRow& row) {
    if (_row_converter == nullptr) {
        RETURN_NOT_OK(_init_row_converter());
    }
    _row_converter->convert(_row.get(), row);
    return _writer->add_row(*_row);
}

OLAPStatus RowsetWriterAdapter::add_chunk(const vectorized::Chunk& chunk) {
    if (_chunk_converter == nullptr) {
        RETURN_NOT_OK(_init_chunk_converter());
    }
    return _writer->add_chunk(*_chunk_converter->copy_convert(chunk));
}

OLAPStatus RowsetWriterAdapter::flush_chunk(const vectorized::Chunk& chunk) {
    if (_chunk_converter == nullptr) {
        RETURN_NOT_OK(_init_chunk_converter());
    }
    return _writer->flush_chunk(*_chunk_converter->copy_convert(chunk));
}

OLAPStatus RowsetWriterAdapter::_init_row_converter() {
    _row_converter = std::make_unique<RowConverter>();
    auto st = _row_converter->init(*_in_schema, *_out_schema);
    if (!st.ok()) {
        _row_converter.reset();
        return OLAP_ERR_INVALID_SCHEMA;
    }
    _row = std::make_unique<RowCursor>();
    if (_row->init(*_out_schema) != OLAP_SUCCESS) {
        _row_converter.reset();
        _row.reset();
        return OLAP_ERR_INVALID_SCHEMA;
    }
    // |_in_schema| and |_out_schema| can be freed now.
    _in_schema.reset();
    _out_schema.reset();
    return OLAP_SUCCESS;
}

OLAPStatus RowsetWriterAdapter::_init_chunk_converter() {
    _chunk_converter = std::make_unique<ChunkConverter>();
    auto st =
            _chunk_converter->init(ChunkHelper::convert_schema(*_in_schema), ChunkHelper::convert_schema(*_out_schema));
    if (!st.ok()) {
        _chunk_converter.reset();
        return OLAP_ERR_INVALID_SCHEMA;
    }
    // |_in_schema| and |_out_schema| can be freed now.
    _in_schema.reset();
    _out_schema.reset();
    return OLAP_SUCCESS;
}

OLAPStatus RowsetWriterAdapter::flush_chunk_with_deletes(const vectorized::Chunk& upserts,
                                                         const vectorized::Column& deletes) {
    LOG(WARNING) << "updatable tablet should not use RowsetWriterAdapter";
    if (_chunk_converter == nullptr) {
        RETURN_NOT_OK(_init_chunk_converter());
    }
    return _writer->flush_chunk_with_deletes(*_chunk_converter->copy_convert(upserts), deletes);
}

} // namespace starrocks::vectorized
