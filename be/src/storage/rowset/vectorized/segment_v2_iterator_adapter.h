// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <memory>
#include <vector>

#include "common/object_pool.h"
#include "common/status.h"
#include "storage/iterators.h"
#include "storage/schema.h"
#include "storage/tablet_schema.h"
#include "storage/vectorized/convert_helper.h"

namespace starrocks {
class RowBlockV2;
class StorageReadOptions;
} // namespace starrocks

namespace starrocks::vectorized {

class SegmentV2IteratorAdapter : public RowwiseIterator {
public:
    SegmentV2IteratorAdapter(const TabletSchema& tablet_schema, const std::vector<FieldType>& new_types,
                             const ::starrocks::Schema& out_schema);
    ~SegmentV2IteratorAdapter() override;

    Status init(const ::starrocks::StorageReadOptions& opts) override;

    Status next_batch(RowBlockV2* block) override;

    const ::starrocks::Schema& schema() const override { return _out_schema; }

    const ::starrocks::Schema& in_schema() const { return *_in_schema; }
    const ::starrocks::StorageReadOptions& in_read_options() const { return *_in_read_options; }
    void set_iterator(std::unique_ptr<::starrocks::RowwiseIterator> iter) { _iter = std::move(iter); }

private:
    const TabletSchema& _tablet_schema;
    const std::vector<FieldType>& _new_types;
    // NOTE: DSDB-2337
    // must copy _out_schema here, because this schema will be used in next_batch
    // however the input _output_schema is a stack variable
    ::starrocks::Schema _out_schema;

    // rewritten schema and options according to
    const ::starrocks::Schema* _in_schema = nullptr;
    const ::starrocks::StorageReadOptions* _in_read_options = nullptr;

    ObjectPool _obj_pool;

    std::unique_ptr<::starrocks::RowwiseIterator> _iter;
    std::unique_ptr<::starrocks::RowBlockV2> _in_block;

    BlockConverter _converter;
};

} // namespace starrocks::vectorized
