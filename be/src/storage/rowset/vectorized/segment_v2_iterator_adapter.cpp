// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "storage/rowset/vectorized/segment_v2_iterator_adapter.h"

#include <memory>

#include "storage/row_block2.h"

namespace starrocks::vectorized {

SegmentV2IteratorAdapter::SegmentV2IteratorAdapter(const TabletSchema& tablet_schema,
                                                   const std::vector<FieldType>& new_types,
                                                   const ::starrocks::Schema& out_schema)
        : _tablet_schema(tablet_schema), _new_types(new_types), _out_schema(out_schema) {}

SegmentV2IteratorAdapter::~SegmentV2IteratorAdapter() = default;

Status SegmentV2IteratorAdapter::init(const ::starrocks::StorageReadOptions& options) {
    // rewrite schema
    std::unique_ptr<::starrocks::Schema> new_schema;
    bool is_converted = false;
    RETURN_IF_ERROR(_out_schema.convert_to(_new_types, &is_converted, &new_schema));
    if (is_converted) {
        _in_schema = _obj_pool.add(new_schema.release());
    } else {
        _in_schema = &_out_schema;
    }

    // rewrite options
    RETURN_IF_ERROR(options.convert_to(&_in_read_options, _tablet_schema, _new_types, &_obj_pool));

    _converter.init(*_in_schema, _out_schema);

    return Status::OK();
}

Status SegmentV2IteratorAdapter::next_batch(RowBlockV2* block) {
    if (_in_block == nullptr) {
        // init row_block
        _in_block = std::make_unique<::starrocks::RowBlockV2>(*_in_schema, block->capacity());
    }
    _in_block->clear();

    RETURN_IF_ERROR(_iter->next_batch(_in_block.get()));
    // convert to out block
    RETURN_IF_ERROR(_converter.convert(block, _in_block.get()));
    return Status::OK();
}

} // namespace starrocks::vectorized
