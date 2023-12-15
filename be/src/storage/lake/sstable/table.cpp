// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.
// (https://developers.google.com/open-source/licenses/bsd)

#include "storage/lake/sstable/table.h"

#include "common/status.h"
#include "fs/fs.h"
#include "storage/lake/sstable/block.h"
#include "storage/lake/sstable/comparator.h"
#include "storage/lake/sstable/filter_block.h"
#include "storage/lake/sstable/filter_policy.h"
#include "storage/lake/sstable/format.h"
#include "storage/lake/sstable/options.h"
#include "storage/lake/sstable/two_level_iterator.h"

namespace starrocks {
namespace lake {
namespace sstable {

struct Table::Rep {
    ~Rep() {
        delete filter;
        delete[] filter_data;
        delete index_block;
    }

    Options options;
    Status status;
    RandomAccessFile* file;
    uint64_t cache_id;
    FilterBlockReader* filter;
    const char* filter_data;

    BlockHandle metaindex_handle; // Handle to metaindex_block: saved from footer
    Block* index_block;
};

Status Table::Open(const Options& options, RandomAccessFile* file, uint64_t size, Table** table) {
    *table = nullptr;
    if (size < Footer::kEncodedLength) {
        return Status::Corruption("file is too short to be an sstable");
    }

    char footer_space[Footer::kEncodedLength];
    //Status s = file->Read(size - Footer::kEncodedLength, Footer::kEncodedLength, &footer_input, footer_space);
    Status s = file->read_at_fully(size - Footer::kEncodedLength, footer_space, Footer::kEncodedLength);
    if (!s.ok()) return s;

    Slice footer_input(footer_space, Footer::kEncodedLength);
    Footer footer;
    s = footer.DecodeFrom(&footer_input);
    if (!s.ok()) return s;

    // Read the index block
    BlockContents index_block_contents;
    ReadOptions opt;
    if (options.paranoid_checks) {
        opt.verify_checksums = true;
    }
    s = ReadBlock(file, opt, footer.index_handle(), &index_block_contents);

    if (s.ok()) {
        // We've successfully read the footer and the index block: we're
        // ready to serve requests.
        Block* index_block = new Block(index_block_contents);
        Rep* rep = new Table::Rep;
        rep->options = options;
        rep->file = file;
        rep->metaindex_handle = footer.metaindex_handle();
        rep->index_block = index_block;
        rep->cache_id = 0;
        rep->filter_data = nullptr;
        rep->filter = nullptr;
        *table = new Table(rep);
        (*table)->ReadMeta(footer);
    }

    return s;
}

void Table::ReadMeta(const Footer& footer) {
    if (rep_->options.filter_policy == nullptr) {
        return; // Do not need any metadata
    }

    // TODO(sanjay): Skip this if footer.metaindex_handle() size indicates
    // it is an empty block.
    ReadOptions opt;
    if (rep_->options.paranoid_checks) {
        opt.verify_checksums = true;
    }
    BlockContents contents;
    if (!ReadBlock(rep_->file, opt, footer.metaindex_handle(), &contents).ok()) {
        // Do not propagate errors since meta info is not needed for operation
        return;
    }
    Block* meta = new Block(contents);

    Iterator* iter = meta->NewIterator(BytewiseComparator());
    std::string key = "filter.";
    key.append(rep_->options.filter_policy->Name());
    iter->Seek(key);
    if (iter->Valid() && iter->key() == Slice(key)) {
        ReadFilter(iter->value());
    }
    delete iter;
    delete meta;
}

void Table::ReadFilter(const Slice& filter_handle_value) {
    Slice v = filter_handle_value;
    BlockHandle filter_handle;
    if (!filter_handle.DecodeFrom(&v).ok()) {
        return;
    }

    // We might want to unify with ReadBlock() if we start
    // requiring checksum verification in Table::Open.
    ReadOptions opt;
    if (rep_->options.paranoid_checks) {
        opt.verify_checksums = true;
    }
    BlockContents block;
    if (!ReadBlock(rep_->file, opt, filter_handle, &block).ok()) {
        return;
    }
    if (block.heap_allocated) {
        rep_->filter_data = block.data.get_data(); // Will need to delete later
    }
    rep_->filter = new FilterBlockReader(rep_->options.filter_policy, block.data);
}

Table::~Table() {
    delete rep_;
}

static void DeleteBlock(void* arg, void* ignored) {
    delete reinterpret_cast<Block*>(arg);
}

/*
static void DeleteCachedBlock(const Slice& key, void* value) {
    Block* block = reinterpret_cast<Block*>(value);
    delete block;
}

static void ReleaseBlock(void* arg, void* h) {
    Cache* cache = reinterpret_cast<Cache*>(arg);
    Cache::Handle* handle = reinterpret_cast<Cache::Handle*>(h);
    cache->Release(handle);
}*/

// Convert an index iterator value (i.e., an encoded BlockHandle)
// into an iterator over the contents of the corresponding block.
Iterator* Table::BlockReader(void* arg, const ReadOptions& options, const Slice& index_value) {
    Table* table = reinterpret_cast<Table*>(arg);
    Block* block = nullptr;

    BlockHandle handle;
    Slice input = index_value;
    Status s = handle.DecodeFrom(&input);
    // We intentionally allow extra stuff in index_value so that we
    // can add more features in the future.

    if (s.ok()) {
        BlockContents contents;
        s = ReadBlock(table->rep_->file, options, handle, &contents);
        if (s.ok()) {
            block = new Block(contents);
        }
    }

    Iterator* iter;
    if (block != nullptr) {
        iter = block->NewIterator(table->rep_->options.comparator);
        iter->RegisterCleanup(&DeleteBlock, block, nullptr);
    } else {
        iter = NewErrorIterator(s);
    }
    return iter;
}

Iterator* Table::NewIterator(const ReadOptions& options) const {
    return NewTwoLevelIterator(rep_->index_block->NewIterator(rep_->options.comparator), &Table::BlockReader,
                               const_cast<Table*>(this), options);
}

Status Table::InternalGet(const ReadOptions& options, const Slice& k, void* arg,
                          void (*handle_result)(void*, const Slice&, const Slice&)) {
    Status s;
    Iterator* iiter = rep_->index_block->NewIterator(rep_->options.comparator);
    iiter->Seek(k);
    if (iiter->Valid()) {
        Slice handle_value = iiter->value();
        FilterBlockReader* filter = rep_->filter;
        BlockHandle handle;
        if (filter != nullptr && handle.DecodeFrom(&handle_value).ok() && !filter->KeyMayMatch(handle.offset(), k)) {
            // Not found
        } else {
            Iterator* block_iter = BlockReader(this, options, iiter->value());
            block_iter->Seek(k);
            if (block_iter->Valid()) {
                (*handle_result)(arg, block_iter->key(), block_iter->value());
            }
            s = block_iter->status();
            delete block_iter;
        }
    }
    if (s.ok()) {
        s = iiter->status();
    }
    delete iiter;
    return s;
}

uint64_t Table::ApproximateOffsetOf(const Slice& key) const {
    Iterator* index_iter = rep_->index_block->NewIterator(rep_->options.comparator);
    index_iter->Seek(key);
    uint64_t result;
    if (index_iter->Valid()) {
        BlockHandle handle;
        Slice input = index_iter->value();
        Status s = handle.DecodeFrom(&input);
        if (s.ok()) {
            result = handle.offset();
        } else {
            // Strange: we can't decode the block handle in the index block.
            // We'll just return the offset of the metaindex block, which is
            // close to the whole file size for this case.
            result = rep_->metaindex_handle.offset();
        }
    } else {
        // key is past the last key in the file.  Approximate the offset
        // by returning the offset of the metaindex block (which is
        // right near the end of the file).
        result = rep_->metaindex_handle.offset();
    }
    delete index_iter;
    return result;
}

} // namespace sstable
} // namespace lake
} // namespace starrocks