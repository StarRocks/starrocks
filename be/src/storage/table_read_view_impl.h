// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "storage/table_read_view.h"
#include "storage/tablet.h"

namespace starrocks {

class TableReadViewImpl : public TableReadView {
public:
    explicit TableReadViewImpl(const TableReadViewParams& params, const std::vector<int64_t>& tablet_ids);

    StatusOr<ChunkIteratorPtr> get_chunk(const Row& key, const ReadOption& read_option) override;

    std::vector<StatusOr<ChunkIteratorPtr>> batch_get_chunk(
            const std::vector<const Row*>& keys, const std::vector<const ReadOption*>& read_options) override;

    void close() override{};

private:
    std::vector<const ColumnPredicate*> build_column_predicates(
            const Row& key, const std::vector<const vectorized::ColumnPredicate*>& output_predicates);

    std::vector<int64_t> _tablet_ids;
    std::vector<TabletSharedPtr> _local_tablets;
    vectorized::Schema _schema;
    std::vector<std::shared_ptr<ColumnPredicate>> _free_predicates;
};

} // namespace starrocks