// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "column/datum.h"
#include "column/datum_convert.h"
#include "column/vectorized_fwd.h"
#include "storage/chunk_helper.h"
#include "storage/datum_row_iterator.h"
#include "storage/projection_iterator.h"
#include "storage/table_read_view_impl.h"
#include "storage/tablet_reader.h"

namespace starrocks {

TableReadViewImpl::TableReadViewImpl(const TableReadViewParams& params, TabletSharedPtr tablet)
        : TableReadView(params),
          _tablet(tablet) {
    // TODO refer to olap_chunk_source.cpp to construct schema for TabletReader
    // * _init_olap_reader
    // * _init_unused_output_columns
    // * _init_scanner_columns
    // * _init_reader_params
    _schema = ChunkHelper::convert_schema_to_format_v2(_tablet->tablet_schema());
}

StatusOr<RowIteratorSharedPtr> TableReadViewImpl::get(const Row& key, const ReadOption& read_option) {
    StatusOr<ChunkIteratorPtr> status_or = get_chunk(key, read_option);
    if (!status_or.ok()) {
        return status_or.status();
    }
    return std::make_shared<DatumRowIterator>(status_or.value());
}

std::vector<StatusOr<RowIteratorSharedPtr>> TableReadViewImpl::batch_get(
        const std::vector<const Row*>& keys, const std::vector<const ReadOption*>& read_options) {
    DCHECK_EQ(keys.size(), read_options.size());
    std::vector<StatusOr<RowIteratorSharedPtr>> result;
    result.resize(keys.size());
    for (size_t i = 0; i < keys.size(); i++) {
        result.push_back(get(*keys[i], *read_options[i]));
    }
    return result;
}

StatusOr<ChunkIteratorPtr> TableReadViewImpl::get_chunk(const Row& key, const ReadOption& read_option) {
    // TODO support reverse order
    DCHECK(!read_option.reverse_order);
    DCHECK(key.size() <= _params.sort_key_schema.num_fields());
    std::shared_ptr<vectorized::TabletReader> reader = std::make_shared<vectorized::TabletReader>(
            _tablet, _params.version, _schema);
    Status status = reader->prepare();
    if (!status.ok()) {
        return status;
    }

    vectorized::TabletReaderParams tablet_reader_params;
    tablet_reader_params.predicates = build_column_predicates(key, read_option.output_predicates);
    status = reader->open(tablet_reader_params);
    if (!status.ok()) {
        return status;
    }

    return new_projection_iterator(_params.output_schema, reader);
}

std::vector<StatusOr<ChunkIteratorPtr>> TableReadViewImpl::batch_get_chunk(
        const std::vector<const Row*>& keys, const std::vector<const ReadOption*>& read_options) {
    DCHECK_EQ(keys.size(), read_options.size());
    std::vector<StatusOr<ChunkIteratorPtr>> result;
    result.resize(keys.size());
    for (size_t i = 0; i < keys.size(); i++) {
        result.push_back(get_chunk(*keys[i], *read_options[i]));
    }
    return result;
}

std::vector<const ColumnPredicate*> TableReadViewImpl::build_column_predicates(
        const Row& key, const std::vector<const vectorized::ColumnPredicate*>& output_predicates) {
    const vectorized::Schema& sort_key_schema = _params.sort_key_schema;
    std::vector<const ColumnPredicate*> column_predicates(output_predicates);
    // TODO currently use predicates to look up key
    for (size_t i = 0; i < key.size(); i++) {
        const vectorized::Datum& datum = key.get_datum(i);
        const vectorized::FieldPtr& field = sort_key_schema.field(i);
        ColumnPredicate* predicate = vectorized::new_column_eq_predicate(
                sort_key_schema.field(i)->type(), field->id(),
                vectorized::datum_to_string(sort_key_schema.field(i)->type().get(), datum));
        column_predicates.push_back(predicate);
        // TODO free predicates friendly
        _free_predicates.push_back(std::shared_ptr<ColumnPredicate>(predicate));
    }
    return column_predicates;
}

} // namespace starrocks
