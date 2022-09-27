// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <future>
#include <utility>

#include "storage/chunk_iterator.h"
#include "storage/row_iterator.h"
#include "storage/vectorized_column_predicate.h"

namespace starrocks {

using ColumnPredicate = vectorized::ColumnPredicate;
using ChunkIteratorPtr = vectorized::ChunkIteratorPtr;

// Parameters used to create a TableReadView
struct TableReadViewParams {
    // Version of data to read
    Version version;

    // Schema for the sort key
    vectorized::Schema sort_key_schema;

    // Schema for the output
    vectorized::Schema output_schema;
};

// Options used to control the behaviour of a read
struct ReadOption {
    // Whether to return rows in reverse order by the sort key
    bool reverse_order = false;

    // Chunk size used to read data
    int chunk_size = 1024;

    // Predicates applied to the result
    std::vector<const vectorized::ColumnPredicate*> output_predicates;
};

// A view to read data from the underlying table. Given a key, it will return all
// rows containing the key in the table. The key contains one or multiple columns
// of the table, and it must either be a sort key whose schema is defined by
// TableReadViewParams#sort_key_schema, or a prefix of a sort key. Each returned
// row also contains one or multiple columns of the table, and it's schema is
// defined by TableReadViewParams#output_schema. Because the key is sorted, and
// there may be many rows under a key, we need to clarify the order to return rows.
// We only guarantee those rows containing a smaller sort key will be returned first,
// but there is no guarantee about the order of rows under the same sort key.
class TableReadView {
public:
    explicit TableReadView(const TableReadViewParams& params) : _params(params) {}

    // Whether the data read on this view is organized in columnar format in the underlying
    // storage. This flag can be decided according to what data to read, and how they are
    // indexed. The caller can use this flag to decide whether to iterate the result Row by
    // Row or Chunk by Chunk. If the storage is columnar, and the caller wants vectorized
    // processing, it's better to use the method get_chunk to iterate rows by Chunk. Otherwise,
    // using the method get to iterate rows by Row may be a better choice. Format conversion
    // between Row and Chunk has extra cost.
    virtual bool native_columnar_storage() = 0;

    // sync interface =======================

    // Get the rows under the given key, and iterate the result Row by Row. The key should
    // either be a sort key whose schema is defined by TableReadViewParams#sort_key_schema,
    // or the prefix of a sort key. For the returned StatusOr<RowIteratorSharedPtr>, you
    // should check StatusOr.status().ok() first, and the RowIteratorSharedPtr is valid when
    // status is ok. The schema of the returned Row is defined by TableReadViewParams#output_schema.
    // You can control the read behaviour via ReadOption.
    virtual StatusOr<RowIteratorSharedPtr> get(const Row& key, const ReadOption& read_option) = 0;

    // Get the rows for a batch of keys, and iterate the result of each key Row by Row.
    // This method needs a vector of keys and a corresponding vector of read options, and
    // each option controls the read behaviour of the key at the same position. A vector of
    // results with the same size as the key vector will be returned, and the order of the
    // results corresponds to the order of keys in the key vector.
    //
    // This batch method may be used to reduce network cost if the data is on the remote.
    // The potential implementation may pack multiple keys' read requests into one RPC
    // rather one RPC for each key, but it's not guaranteed.
    virtual std::vector<StatusOr<RowIteratorSharedPtr>> batch_get(
            const std::vector<const Row*>& keys, const std::vector<const ReadOption*>& read_options) = 0;

    // Get the rows under the given key, and iterate rows Chunk by Chunk. The key should
    // either be a sort key whose schema is defined by TableReadViewParams#sort_key_schema,
    // or the prefix of a sort key. For the returned StatusOr<ChunkIteratorPtr>, you should
    // check StatusOr.status().ok() first, and the ChunkIteratorPtr is valid when status is
    // ok. The schema of the returned Row is defined by TableReadViewParams#output_schema.
    // You can control the read behaviour via ReadOption.
    virtual StatusOr<ChunkIteratorPtr> get_chunk(const Row& key, const ReadOption& read_option) = 0;

    // Get the rows for a batch of keys, and iterate the result of each key Chunk by Chunk.
    // This method needs a vector of keys and a corresponding vector of read options, and
    // each option controls the read behaviour of the key at the same position. A vector of
    // results with the same size as the key vector will be returned, and the order of the
    // results corresponds to the order of keys in the key vector.
    //
    // This batch method may be used to reduce network cost if the data is on the remote.
    // The potential implementation may pack multiple keys' read requests into one RPC
    // rather one RPC for each key, but it's not guaranteed.
    virtual std::vector<StatusOr<ChunkIteratorPtr>> batch_get_chunk(
            const std::vector<const Row*>& keys, const std::vector<const ReadOption*>& read_options) = 0;

    // async interface =======================

    // Get the rows under the given key asynchronously. It's almost same as the method get
    // except that a future will be returned to get the result.
    virtual std::future<StatusOr<RowIteratorSharedPtr>> async_get(const Row& key, const ReadOption& read_option) = 0;

    // Get the rows for a batch of keys asynchronously. It's almost same as the method batch_get
    // except that a vector of future will be returned to get the results.
    virtual std::vector<std::future<StatusOr<RowIteratorSharedPtr>>> async_batch_get(
            const std::vector<const Row*>& keys, const std::vector<const ReadOption*>& read_options) = 0;

    // Get the rows under the given key asynchronously. It's almost same as the method get_chunk
    // except that a future will be returned to get the result.
    virtual std::future<StatusOr<ChunkIteratorPtr>> async_get_chunk(const Row& key, const ReadOption& read_option) = 0;

    // Get the rows for a batch of keys asynchronously. It's almost same as the method batch_get_chunk
    // except that a vector of future will be returned to get the results.
    virtual std::vector<std::future<StatusOr<ChunkIteratorPtr>>> async_batch_get_chunk(
            const std::vector<const Row*>& keys, const std::vector<const ReadOption*>& read_options) = 0;

    virtual void close() = 0;

protected:
    const TableReadViewParams& _params;
};

using TableReadViewSharedPtr = std::shared_ptr<TableReadView>;

} // namespace starrocks