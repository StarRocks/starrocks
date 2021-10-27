// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <memory>

#include "column/schema.h"
#include "storage/vectorized/tablet_reader_params.h"
#include "util/runtime_profile.h"
#ifndef NDEBUG
#include "column/chunk.h"
#endif

namespace starrocks {
class Status;
} // namespace starrocks

namespace starrocks::vectorized {

class Chunk;

class ChunkIterator {
public:
    // |schema| is the output fields.
    ChunkIterator(vectorized::Schema schema) : _schema(std::move(schema)) {}

    ChunkIterator(vectorized::Schema schema, int chunk_size) : _schema(std::move(schema)), _chunk_size(chunk_size) {}

    virtual ~ChunkIterator() = default;

    // Fetch records from |this| iterator into |chunk|.
    //
    // REQUIRES: |chunk| is not null and is empty. the type of each column in |chunk| must
    // correspond to each field in `schema()`, in the same order.
    //
    // if the returned status is `OK`, at least on record is appended to |chunk|,
    // i.e, size of |chunk| must be greater than zero.
    // if the returned status is `EndOfFile`, size of |chunk| must be zero;
    // otherwise, the size of |chunk| is undefined.
    Status get_next(Chunk* chunk) {
        Status st = do_get_next(chunk);
#ifndef NDEBUG
        DCHECK_CHUNK(chunk);
#endif
        return st;
    }

    // like get_next(Chunk* chunk), but also returns each row's rowid(ordinal id)
    Status get_next(Chunk* chunk, vector<uint32_t>* rowid) {
        Status st = do_get_next(chunk, rowid);
#ifndef NDEBUG
        DCHECK_CHUNK(chunk);
#endif
        return st;
    }

    // Release resources associated with this iterator, e.g, deallocate memory.
    // This routine can be called at most once.
    virtual void close() = 0;

    virtual std::size_t merged_rows() const { return 0; }

    const Schema& schema() const { return _schema; }

    // Returns the Schema of the result.
    // If a Field uses the global dictionary strategy, the field will be rewritten as INT
    const Schema& encoded_schema() const { return _encoded_schema; }

    virtual Status init_encoded_schema(ColumnIdToGlobalDictMap& dict_maps) {
        for (const auto& field : schema().fields()) {
            const auto cid = field->id();
            const auto& name = field->name();
            bool is_nullable = field->is_nullable();
            if (dict_maps.count(cid)) {
                _encoded_schema.append(std::make_shared<Field>(cid, name, OLAP_FIELD_TYPE_INT, -1, -1, is_nullable));
            } else {
                _encoded_schema.append(field);
            }
        }
        return Status::OK();
    }

    int chunk_size() const { return _chunk_size; }

protected:
    virtual Status do_get_next(Chunk* chunk) = 0;
    virtual Status do_get_next(Chunk* chunk, vector<uint32_t>* rowid) {
        return Status::NotSupported("Chunk* chunk, vector<uint32_t>* rowid) not supported");
    }

    vectorized::Schema _schema;
    vectorized::Schema _encoded_schema;

    int _chunk_size = DEFAULT_CHUNK_SIZE;
};

using ChunkIteratorPtr = std::shared_ptr<ChunkIterator>;

class TimedChunkIterator final : public ChunkIterator {
public:
    TimedChunkIterator(ChunkIteratorPtr iter, RuntimeProfile::Counter* counter)
            : ChunkIterator(iter->schema(), iter->chunk_size()), _iter(std::move(iter)), _cost(0), _counter(counter) {}

    ~TimedChunkIterator() override = default;

    void close() override {
        COUNTER_UPDATE(_counter, _cost);
        _iter->close();
        _iter.reset();
    }

    size_t merged_rows() const override { return _iter->merged_rows(); }

    virtual Status init_encoded_schema(ColumnIdToGlobalDictMap& dict_maps) override {
        ChunkIterator::init_encoded_schema(dict_maps);
        _iter->init_encoded_schema(dict_maps);
        return Status::OK();
    }

private:
    Status do_get_next(Chunk* chunk) override {
        SCOPED_RAW_TIMER(&_cost);
        return _iter->get_next(chunk);
    }

    Status do_get_next(Chunk* chunk, vector<uint32_t>* rowid) override {
        SCOPED_RAW_TIMER(&_cost);
        return _iter->get_next(chunk, rowid);
    }

    ChunkIteratorPtr _iter;
    int64_t _cost;
    RuntimeProfile::Counter* _counter;
};

inline ChunkIteratorPtr timed_chunk_iterator(const ChunkIteratorPtr& iter, RuntimeProfile::Counter* counter) {
    return std::make_shared<TimedChunkIterator>(iter, counter);
}

} // namespace starrocks::vectorized
