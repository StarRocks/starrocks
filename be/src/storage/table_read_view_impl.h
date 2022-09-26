// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "storage/table_read_view.h"
#include "storage/tablet.h"

namespace starrocks {

class TableReadViewImpl: public TableReadView {
public:
    explicit TableReadViewImpl(const TableReadViewParams& params, TabletSharedPtr tablet);

    bool native_columnar_storage() override { return true; };

    StatusOr<RowIteratorSharedPtr> get(const Row& key, const ReadOption& read_option) override;

    std::vector<StatusOr<RowIteratorSharedPtr>> batch_get(
            const std::vector<const Row*>& keys, const std::vector<const ReadOption*>& read_options) override;

    StatusOr<ChunkIteratorPtr> get_chunk(const Row& key, const ReadOption& read_option) override;

    std::vector<StatusOr<ChunkIteratorPtr>> batch_get_chunk(
            const std::vector<const Row*>& keys, const std::vector<const ReadOption*>& read_options) override;

    // TODO support async_get
    std::future<StatusOr<RowIteratorSharedPtr>> async_get(const Row& key, const ReadOption& read_option) override {
        return std::future<StatusOr<RowIteratorSharedPtr>>();
    }

    // TODO support async_batch_get
    std::vector<std::future<StatusOr<RowIteratorSharedPtr>>> async_batch_get(
            const std::vector<const Row*>& keys, const std::vector<const ReadOption*>& read_options) override {
        return std::vector<std::future<StatusOr<RowIteratorSharedPtr>>>();
    }

    // TODO support async_get_chunk
    std::future<StatusOr<ChunkIteratorPtr>> async_get_chunk(const Row& key, const ReadOption& read_option) override {
        return std::future<StatusOr<ChunkIteratorPtr>>();
    }

    // TODO support async_batch_get_chunk
    std::vector<std::future<StatusOr<ChunkIteratorPtr>>> async_batch_get_chunk(
            const std::vector<const Row*>& keys, const std::vector<const ReadOption*>& read_options) override {
        return std::vector<std::future<StatusOr<ChunkIteratorPtr>>>();
    }

    void close() override {};

private:

    std::vector<const ColumnPredicate*> build_column_predicates(
            const Row& key, const std::vector<const vectorized::ColumnPredicate*>& output_predicates);

    TabletSharedPtr _tablet;
    vectorized::Schema _schema;
    std::vector<std::shared_ptr<ColumnPredicate>> _free_predicates;
};

} // namespace starrocks