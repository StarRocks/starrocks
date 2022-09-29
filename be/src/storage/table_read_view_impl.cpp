// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "storage/table_read_view_impl.h"

#include "column/datum.h"
#include "column/datum_convert.h"
#include "column/vectorized_fwd.h"
#include "storage/chunk_helper.h"
#include "storage/datum_row_iterator.h"
#include "storage/projection_iterator.h"
#include "storage/storage_engine.h"
#include "storage/table_read_view_impl.h"
#include "storage/tablet_manager.h"
#include "storage/tablet_reader.h"
#include "storage/union_iterator.h"

namespace starrocks {

TableReadViewImpl::TableReadViewImpl(const TableReadViewParams& params, const std::vector<int64_t>& tablet_ids)
        : TableReadView(params), _tablet_ids(tablet_ids) {
    // TODO support to visit remote tablets
    for (int64_t tablet_id : tablet_ids) {
        TabletSharedPtr tablet = StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id, true);
        if (tablet != nullptr) {
            _local_tablets.push_back(tablet);
        }
    }

    // TODO refer to olap_chunk_source.cpp to construct schema for TabletReader
    // * _init_olap_reader
    // * _init_unused_output_columns
    // * _init_scanner_columns
    // * _init_reader_params
    if (!_local_tablets.empty()) {
        _schema = ChunkHelper::convert_schema_to_format_v2(_local_tablets[0]->tablet_schema());
    }
}

StatusOr<ChunkIteratorPtr> TableReadViewImpl::get_chunk(const Row& key, const ReadOption& read_option) {
    // TODO support reverse order
    DCHECK(!read_option.reverse_order);
    DCHECK(!read_option.strict_order);
    DCHECK_EQ(read_option.row_count_limit, -1);
    DCHECK(key.size() <= _view_params.sort_key_schema.num_fields());
    vectorized::TabletReaderParams tablet_reader_params;
    tablet_reader_params.predicates = build_column_predicates(key, read_option.predicates);

    std::vector<ChunkIteratorPtr> tablet_readers;
    for (const TabletSharedPtr& tablet : _local_tablets) {
        std::shared_ptr<vectorized::TabletReader> reader =
                std::make_shared<vectorized::TabletReader>(tablet, _view_params.version, _schema);
        Status status = reader->prepare();
        if (!status.ok()) {
            return status;
        }

        status = reader->open(tablet_reader_params);
        if (!status.ok()) {
            return status;
        }
        tablet_readers.push_back(reader);
    }

    // TODO support sort merge
    ChunkIteratorPtr union_iterator = new_union_iterator(std::move(tablet_readers));
    return new_projection_iterator(_view_params.output_schema, union_iterator);
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
        const Row& key, const std::vector<const vectorized::ColumnPredicate*>& predicates) {
    const vectorized::Schema& sort_key_schema = _view_params.sort_key_schema;
    std::vector<const ColumnPredicate*> column_predicates(predicates);
    for (auto common_predicate : _view_params.common_predicates) {
        column_predicates.push_back(common_predicate);
    }

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
