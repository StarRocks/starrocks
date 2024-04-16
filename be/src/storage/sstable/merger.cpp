// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.
// (https://developers.google.com/open-source/licenses/bsd)

#include "storage/sstable/merger.h"

#include "storage/sstable/comparator.h"
#include "storage/sstable/iterator.h"
#include "storage/sstable/iterator_wrapper.h"

namespace starrocks::sstable {

namespace {
class MergingIterator : public Iterator {
public:
    MergingIterator(const Comparator* comparator, const std::vector<Iterator*>& childrens,
                    const std::vector<int64_t>& versions)
            : comparator_(comparator) {
        DCHECK(childrens.size() == versions.size());
        childrens_.resize(childrens.size());
        for (size_t i = 0; i < childrens_.size(); i++) {
            childrens_[i].Set(childrens[i], versions[i], i);
        }
    }

    ~MergingIterator() override {}

    bool Valid() const override { return (current_ != nullptr); }

    void SeekToFirst() override {
        for (int i = 0; i < childrens_.size(); i++) {
            childrens_[i].SeekToFirst();
        }
        FindSmallest();
        direction_ = kForward;
    }

    void SeekToLast() override {
        for (int i = 0; i < childrens_.size(); i++) {
            childrens_[i].SeekToLast();
        }
        FindLargest();
        direction_ = kReverse;
    }

    void Seek(const Slice& target) override {
        for (int i = 0; i < childrens_.size(); i++) {
            childrens_[i].Seek(target);
        }
        FindSmallest();
        direction_ = kForward;
    }

    void Next() override {
        assert(Valid());

        // Ensure that all children are positioned after key().
        // If we are moving in the forward direction, it is already
        // true for all of the non-current_ children since current_ is
        // the smallest child and key() == current_->key().  Otherwise,
        // we explicitly position the non-current_ children.
        if (direction_ != kForward) {
            for (int i = 0; i < childrens_.size(); i++) {
                IteratorWrapper* child = &childrens_[i];
                if (child != current_) {
                    child->Seek(key());
                    if (child->Valid() && comparator_->Compare(key(), child->key()) == 0) {
                        child->Next();
                    }
                }
            }
            direction_ = kForward;
        }

        current_->Next();
        FindSmallest();
    }

    void Prev() override {
        assert(Valid());

        // Ensure that all children are positioned before key().
        // If we are moving in the reverse direction, it is already
        // true for all of the non-current_ children since current_ is
        // the largest child and key() == current_->key().  Otherwise,
        // we explicitly position the non-current_ children.
        if (direction_ != kReverse) {
            for (int i = 0; i < childrens_.size(); i++) {
                IteratorWrapper* child = &childrens_[i];
                if (child != current_) {
                    child->Seek(key());
                    if (child->Valid()) {
                        // Child is at first entry >= key().  Step back one to be < key()
                        child->Prev();
                    } else {
                        // Child has no entries >= key().  Position at last entry.
                        child->SeekToLast();
                    }
                }
            }
            direction_ = kReverse;
        }

        current_->Prev();
        FindLargest();
    }

    Slice key() const override {
        assert(Valid());
        return current_->key();
    }

    Slice value() const override {
        assert(Valid());
        return current_->value();
    }

    EntryVersion entry_version() const override { return {current_->version(), current_->index()}; }

    Status status() const override {
        Status status;
        for (int i = 0; i < childrens_.size(); i++) {
            status = childrens_[i].status();
            if (!status.ok()) {
                break;
            }
        }
        return status;
    }

private:
    // Which direction is the iterator moving?
    enum Direction { kForward, kReverse };

    void FindSmallest();
    void FindLargest();

    // We might want to use a heap in case there are lots of children.
    // For now we use a simple array since we expect a very small number
    // of children in leveldb.
    const Comparator* comparator_;
    std::vector<IteratorWrapper> childrens_;
    IteratorWrapper* current_{nullptr};
    Direction direction_{kForward};
};

void MergingIterator::FindSmallest() {
    IteratorWrapper* smallest = nullptr;
    for (int i = 0; i < childrens_.size(); i++) {
        IteratorWrapper* child = &childrens_[i];
        if (child->Valid()) {
            if (smallest == nullptr) {
                smallest = child;
            } else if (comparator_->Compare(child->key(), smallest->key()) < 0) {
                smallest = child;
            }
        }
    }
    current_ = smallest;
}

void MergingIterator::FindLargest() {
    IteratorWrapper* largest = nullptr;
    for (int i = childrens_.size() - 1; i >= 0; i--) {
        IteratorWrapper* child = &childrens_[i];
        if (child->Valid()) {
            if (largest == nullptr) {
                largest = child;
            } else if (comparator_->Compare(child->key(), largest->key()) > 0) {
                largest = child;
            }
        }
    }
    current_ = largest;
}
} // namespace

Iterator* NewMergingIterator(const Comparator* comparator, const std::vector<Iterator*>& childrens,
                             const std::vector<int64_t>& versions) {
    if (childrens.empty()) {
        return NewEmptyIterator();
    } else {
        return new MergingIterator(comparator, childrens, versions);
    }
}

} // namespace starrocks::sstable
