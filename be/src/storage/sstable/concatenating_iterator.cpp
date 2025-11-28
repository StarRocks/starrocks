// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.
// (https://developers.google.com/open-source/licenses/bsd)

#include "storage/sstable/concatenating_iterator.h"

#include "storage/sstable/iterator.h"
#include "storage/sstable/iterator_wrapper.h"

namespace starrocks::sstable {

namespace {
class ConcatenatingIterator : public Iterator {
public:
    ConcatenatingIterator(Iterator** children, int n) : children_(new IteratorWrapper[n]), n_(n), current_index_(-1) {
        for (int i = 0; i < n; i++) {
            children_[i].Set(children[i]);
        }
    }

    ~ConcatenatingIterator() override { delete[] children_; }

    bool Valid() const override { return current_index_ >= 0 && current_index_ < n_ && children_[current_index_].Valid(); }

    void SeekToFirst() override {
        for (int i = 0; i < n_; i++) {
            children_[i].SeekToFirst();
            if (children_[i].Valid()) {
                current_index_ = i;
                return;
            }
        }
        current_index_ = -1;
    }

    void SeekToLast() override {
        for (int i = n_ - 1; i >= 0; i--) {
            children_[i].SeekToLast();
            if (children_[i].Valid()) {
                current_index_ = i;
                return;
            }
        }
        current_index_ = -1;
    }

    void Seek(const Slice& target) override {
        // Find the first child that might contain the target
        for (int i = 0; i < n_; i++) {
            children_[i].Seek(target);
            if (children_[i].Valid()) {
                current_index_ = i;
                return;
            }
        }
        current_index_ = -1;
    }

    void Next() override {
        assert(Valid());
        children_[current_index_].Next();

        // If current iterator becomes invalid, move to next iterator
        if (!children_[current_index_].Valid()) {
            for (int i = current_index_ + 1; i < n_; i++) {
                if (children_[i].Valid()) {
                    current_index_ = i;
                    return;
                }
                // Try to seek to first in case the iterator hasn't been initialized
                children_[i].SeekToFirst();
                if (children_[i].Valid()) {
                    current_index_ = i;
                    return;
                }
            }
            current_index_ = -1;
        }
    }

    void Prev() override {
        assert(Valid());
        children_[current_index_].Prev();

        // If current iterator becomes invalid, move to previous iterator's last element
        if (!children_[current_index_].Valid()) {
            for (int i = current_index_ - 1; i >= 0; i--) {
                children_[i].SeekToLast();
                if (children_[i].Valid()) {
                    current_index_ = i;
                    return;
                }
            }
            current_index_ = -1;
        }
    }

    Slice key() const override {
        assert(Valid());
        return children_[current_index_].key();
    }

    Slice value() const override {
        assert(Valid());
        return children_[current_index_].value();
    }

    Status status() const override {
        // Check all children for errors
        for (int i = 0; i < n_; i++) {
            Status s = children_[i].status();
            if (!s.ok()) {
                return s;
            }
        }
        return Status::OK();
    }

    uint64_t max_rss_rowid() const override {
        assert(Valid());
        return children_[current_index_].max_rss_rowid();
    }

    SstablePredicateSPtr predicate() const override {
        assert(Valid());
        return children_[current_index_].predicate();
    }

    uint32_t shared_rssid() const override {
        assert(Valid());
        return children_[current_index_].shared_rssid();
    }

    int64_t shared_version() const override {
        assert(Valid());
        return children_[current_index_].shared_version();
    }

    DelVectorPtr delvec() const override {
        assert(Valid());
        return children_[current_index_].delvec();
    }

private:
    IteratorWrapper* children_;
    int n_;
    int current_index_;
};
} // namespace

Iterator* NewConcatenatingIterator(Iterator** children, int n) {
    assert(n >= 0);
    if (n == 0) {
        return NewEmptyIterator();
    } else {
        return new ConcatenatingIterator(children, n);
    }
}

} // namespace starrocks::sstable
