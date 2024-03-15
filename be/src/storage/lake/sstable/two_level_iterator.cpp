// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.
// (https://developers.google.com/open-source/licenses/bsd)

#include "storage/lake/sstable/two_level_iterator.h"

#include "storage/lake/sstable/block.h"
#include "storage/lake/sstable/format.h"
#include "storage/lake/sstable/table.h"

namespace starrocks {
namespace lake {
namespace sstable {

// A internal wrapper class with an interface similar to Iterator that
// caches the valid() and key() results for an underlying iterator.
// This can help avoid virtual function calls and also gives better
// cache locality.
class IteratorWrapper {
public:
    IteratorWrapper() : iter_(nullptr), valid_(false) {}
    explicit IteratorWrapper(Iterator* iter) : iter_(nullptr) { Set(iter); }
    ~IteratorWrapper() { delete iter_; }
    Iterator* iter() const { return iter_; }

    // Takes ownership of "iter" and will delete it when destroyed, or
    // when Set() is invoked again.
    void Set(Iterator* iter) {
        delete iter_;
        iter_ = iter;
        if (iter_ == nullptr) {
            valid_ = false;
        } else {
            Update();
        }
    }

    // Iterator interface methods
    bool Valid() const { return valid_; }
    Slice key() const {
        assert(Valid());
        return key_;
    }
    Slice value() const {
        assert(Valid());
        return iter_->value();
    }
    // Methods below require iter() != nullptr
    Status status() const {
        assert(iter_);
        return iter_->status();
    }
    void Next() {
        assert(iter_);
        iter_->Next();
        Update();
    }
    void Prev() {
        assert(iter_);
        iter_->Prev();
        Update();
    }
    void Seek(const Slice& k) {
        assert(iter_);
        iter_->Seek(k);
        Update();
    }
    void SeekToFirst() {
        assert(iter_);
        iter_->SeekToFirst();
        Update();
    }
    void SeekToLast() {
        assert(iter_);
        iter_->SeekToLast();
        Update();
    }

private:
    void Update() {
        valid_ = iter_->Valid();
        if (valid_) {
            key_ = iter_->key();
        }
    }

    Iterator* iter_;
    bool valid_;
    Slice key_;
};

typedef Iterator* (*BlockFunction)(void*, const ReadOptions&, const Slice&);

class TwoLevelIterator : public Iterator {
public:
    TwoLevelIterator(Iterator* index_iter, BlockFunction block_function, void* arg, const ReadOptions& options);

    ~TwoLevelIterator() override;

    void Seek(const Slice& target) override;
    void SeekToFirst() override;
    void SeekToLast() override;
    void Next() override;
    void Prev() override;

    bool Valid() const override { return data_iter_.Valid(); }
    Slice key() const override {
        assert(Valid());
        return data_iter_.key();
    }
    Slice value() const override {
        assert(Valid());
        return data_iter_.value();
    }
    Status status() const override {
        // It'd be nice if status() returned a const Status& instead of a Status
        if (!index_iter_.status().ok()) {
            return index_iter_.status();
        } else if (data_iter_.iter() != nullptr && !data_iter_.status().ok()) {
            return data_iter_.status();
        } else {
            return status_;
        }
    }

private:
    void SaveError(const Status& s) {
        if (status_.ok() && !s.ok()) status_ = s;
    }
    void SkipEmptyDataBlocksForward();
    void SkipEmptyDataBlocksBackward();
    void SetDataIterator(Iterator* data_iter);
    void InitDataBlock();

    BlockFunction block_function_;
    void* arg_;
    const ReadOptions options_;
    Status status_;
    IteratorWrapper index_iter_;
    IteratorWrapper data_iter_; // May be nullptr
    // If data_iter_ is non-null, then "data_block_handle_" holds the
    // "index_value" passed to block_function_ to create the data_iter_.
    std::string data_block_handle_;
};

TwoLevelIterator::TwoLevelIterator(Iterator* index_iter, BlockFunction block_function, void* arg,
                                   const ReadOptions& options)
        : block_function_(block_function), arg_(arg), options_(options), index_iter_(index_iter), data_iter_(nullptr) {}

TwoLevelIterator::~TwoLevelIterator() = default;

void TwoLevelIterator::Seek(const Slice& target) {
    index_iter_.Seek(target);
    InitDataBlock();
    if (data_iter_.iter() != nullptr) data_iter_.Seek(target);
    SkipEmptyDataBlocksForward();
}

void TwoLevelIterator::SeekToFirst() {
    index_iter_.SeekToFirst();
    InitDataBlock();
    if (data_iter_.iter() != nullptr) data_iter_.SeekToFirst();
    SkipEmptyDataBlocksForward();
}

void TwoLevelIterator::SeekToLast() {
    index_iter_.SeekToLast();
    InitDataBlock();
    if (data_iter_.iter() != nullptr) data_iter_.SeekToLast();
    SkipEmptyDataBlocksBackward();
}

void TwoLevelIterator::Next() {
    assert(Valid());
    data_iter_.Next();
    SkipEmptyDataBlocksForward();
}

void TwoLevelIterator::Prev() {
    assert(Valid());
    data_iter_.Prev();
    SkipEmptyDataBlocksBackward();
}

void TwoLevelIterator::SkipEmptyDataBlocksForward() {
    while (data_iter_.iter() == nullptr || !data_iter_.Valid()) {
        // Move to next block
        if (!index_iter_.Valid()) {
            SetDataIterator(nullptr);
            return;
        }
        index_iter_.Next();
        InitDataBlock();
        if (data_iter_.iter() != nullptr) data_iter_.SeekToFirst();
    }
}

void TwoLevelIterator::SkipEmptyDataBlocksBackward() {
    while (data_iter_.iter() == nullptr || !data_iter_.Valid()) {
        // Move to next block
        if (!index_iter_.Valid()) {
            SetDataIterator(nullptr);
            return;
        }
        index_iter_.Prev();
        InitDataBlock();
        if (data_iter_.iter() != nullptr) data_iter_.SeekToLast();
    }
}

void TwoLevelIterator::SetDataIterator(Iterator* data_iter) {
    if (data_iter_.iter() != nullptr) SaveError(data_iter_.status());
    data_iter_.Set(data_iter);
}

void TwoLevelIterator::InitDataBlock() {
    if (!index_iter_.Valid()) {
        SetDataIterator(nullptr);
    } else {
        Slice handle = index_iter_.value();
        if (data_iter_.iter() != nullptr && handle.compare(data_block_handle_) == 0) {
            // data_iter_ is already constructed with this iterator, so
            // no need to change anything
        } else {
            Iterator* iter = (*block_function_)(arg_, options_, handle);
            data_block_handle_.assign(handle.get_data(), handle.get_size());
            SetDataIterator(iter);
        }
    }
}

Iterator* NewTwoLevelIterator(Iterator* index_iter, BlockFunction block_function, void* arg,
                              const ReadOptions& options) {
    return new TwoLevelIterator(index_iter, block_function, arg, options);
}

} // namespace sstable
} // namespace lake
} // namespace starrocks