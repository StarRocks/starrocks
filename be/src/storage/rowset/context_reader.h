#pragma once

#include "column/schema.h"
#include "common/status.h"
#include "storage/rowset/column_iterator.h"
#include "storage/rowset/common.h"

namespace starrocks::vectorized {
class Chunk;
class SparseRange;

class ColumnCollector {
public:
    ColumnCollector(std::vector<ColumnIterator*> column_iterators) : _column_iterators(std::move(column_iterators)) {}
    Status seek_columns(ordinal_t pos);
    Status read_columns(Chunk* chunk, const SparseRange& range);

private:
    std::vector<ColumnIterator*> _column_iterators;
};

// some buffer
class ScanContext {
public:
private:
    // schema
    // pool
};

class ReadTransfer {
public:
    bool has_next_transfer() { return _next != nullptr; }
    virtual Status seek(ordinal_t pos);
    virtual Status process(ScanContext* context, Chunk* chunk) = 0;
    virtual void reset(ScanContext* context) = 0;

protected:
    ReadTransfer* _next = nullptr;
};

class DataReadTransfer : public ReadTransfer {
public:
    Status process(ScanContext* context, Chunk* chunk) override;
    void reset(ScanContext* context) override;

private:
};

class DictDecodeTransfer : public ReadTransfer {
public:
    Status process(ScanContext* context, Chunk* chunk) override;

private:
    ReadTransfer* _child;
};

class LazyMaterializeTransfer : public ReadTransfer {
public:
    Status process(ScanContext* context, Chunk* chunk) override;
};

class lateMaterializeReader {
public:
    // Read
    // Read Dict
    // LazyMaterized
    // Fetch by value
private:
};

class OverFlowReader {
public:
};

} // namespace starrocks::vectorized