// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.
// (https://developers.google.com/open-source/licenses/bsd)

#include "storage/sstable/table.h"

#include <butil/time.h> // NOLINT

#include "common/status.h"
#include "fs/fs.h"
#include "runtime/exec_env.h"
#include "storage/lake/tablet_manager.h"
#include "storage/sstable/block.h"
#include "storage/sstable/comparator.h"
#include "storage/sstable/filter_block.h"
#include "storage/sstable/filter_policy.h"
#include "storage/sstable/format.h"
#include "storage/sstable/options.h"
#include "storage/sstable/two_level_iterator.h"
#include "util/coding.h"
#include "util/lru_cache.h"
#include "util/trace.h"

namespace starrocks::sstable {

struct Table::Rep {
    ~Rep() {
        delete filter;
        delete[] filter_data;
        filter_data_size = 0;
        delete index_block;
    }

    Options options;
    Status status;
    RandomAccessFile* file = nullptr;
    uint64_t cache_id = 0;
    FilterBlockReader* filter = nullptr;
    const char* filter_data = nullptr;
    size_t filter_data_size = 0;

    BlockHandle metaindex_handle; // Handle to metaindex_block: saved from footer
    Block* index_block = nullptr;
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
    ReadIOStat iostat;
    opt.stat = &iostat;
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
        rep->cache_id = (options.block_cache ? options.block_cache->new_id() : 0);
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
    ReadIOStat iostat;
    opt.stat = &iostat;
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
        rep_->filter_data = block.data.get_data();      // Will need to delete later
        rep_->filter_data_size = block.data.get_size(); // mem tracker will track this piece of memory.
    }
    rep_->filter = new FilterBlockReader(rep_->options.filter_policy, block.data);
}

Table::~Table() {
    delete rep_;
}

static void DeleteBlock(void* arg, void* ignored) {
    delete reinterpret_cast<Block*>(arg);
}

static void DeleteCachedBlock(const CacheKey& key, void* value) {
    Block* block = reinterpret_cast<Block*>(value);
    delete block;
}

static void ReleaseBlock(void* arg, void* h) {
    Cache* cache = reinterpret_cast<Cache*>(arg);
    Cache::Handle* handle = reinterpret_cast<Cache::Handle*>(h);
    cache->release(handle);
}

// Convert an index iterator value (i.e., an encoded BlockHandle)
// into an iterator over the contents of the corresponding block.
Iterator* Table::BlockReader(void* arg, const ReadOptions& options, const Slice& index_value) {
    Table* table = reinterpret_cast<Table*>(arg);
    Cache* block_cache = table->rep_->options.block_cache;
    Block* block = nullptr;
    Cache::Handle* cache_handle = nullptr;

    BlockHandle handle;
    Slice input = index_value;
    Status s = handle.DecodeFrom(&input);
    // We intentionally allow extra stuff in index_value so that we
    // can add more features in the future.

    if (s.ok()) {
        BlockContents contents;
        if (block_cache != nullptr) {
            char cache_key_buffer[16];
            encode_fixed64_le(reinterpret_cast<uint8_t*>(cache_key_buffer), table->rep_->cache_id);
            encode_fixed64_le(reinterpret_cast<uint8_t*>(cache_key_buffer + 8), handle.offset());
            CacheKey key(cache_key_buffer, sizeof(cache_key_buffer));
            cache_handle = block_cache->lookup(key);
            if (cache_handle != nullptr) {
                block = reinterpret_cast<Block*>(block_cache->value(cache_handle));
                if (options.stat != nullptr) {
                    options.stat->block_cnt_from_cache++;
                }
            } else {
                s = ReadBlock(table->rep_->file, options, handle, &contents);
                if (s.ok()) {
                    block = new Block(contents);
                    if (contents.cachable && options.fill_cache) {
                        size_t block_size = block->size();
                        cache_handle = block_cache->insert(key, block, block_size, block_size, &DeleteCachedBlock);
                    }
                }
                if (options.stat != nullptr) {
                    options.stat->block_cnt_from_file++;
                }
            }
        } else {
            BlockContents contents;
            s = ReadBlock(table->rep_->file, options, handle, &contents);
            if (s.ok()) {
                block = new Block(contents);
            }
            if (options.stat != nullptr) {
                options.stat->block_cnt_from_file++;
            }
        }
    }

    Iterator* iter;
    if (block != nullptr) {
        iter = block->NewIterator(table->rep_->options.comparator);
        if (cache_handle == nullptr) {
            iter->RegisterCleanup(&DeleteBlock, block, nullptr);
        } else {
            iter->RegisterCleanup(&ReleaseBlock, block_cache, cache_handle);
        }
    } else {
        iter = NewErrorIterator(s);
    }
    return iter;
}

Iterator* Table::NewIterator(const ReadOptions& options) const {
    return NewTwoLevelIterator(rep_->index_block->NewIterator(rep_->options.comparator), &Table::BlockReader,
                               const_cast<Table*>(this), options);
}

template <class ForwardIt>
Status Table::MultiGet(const ReadOptions& options, const Slice* keys, ForwardIt begin, ForwardIt end,
                       std::vector<std::string>* values) {
    Status s;
    Iterator* iiter = rep_->index_block->NewIterator(rep_->options.comparator);
    std::unique_ptr<Iterator> current_block_itr_ptr;

    // return true if find k
    auto search_in_block = [](const Slice& k, std::string* value, Iterator* current_block_itr) -> StatusOr<bool> {
        current_block_itr->Seek(k);
        if (current_block_itr->Valid() && k == current_block_itr->key()) {
            value->assign(current_block_itr->value().data, current_block_itr->value().size);
            return true;
        }
        if (!current_block_itr->status().ok()) {
            return current_block_itr->status();
        }
        return false;
    };

    int64_t continue_block_read_cnt = 0;
    int64_t sst_bloom_filter_rows = 0;
    int64_t multiget_t1_us = 0;
    int64_t multiget_t2_us = 0;
    int64_t multiget_t3_us = 0;
    size_t i = 0;
    bool founded = false;
    for (auto it = begin; it != end; ++it, ++i) {
        auto& k = keys[*it];
        int64_t t0 = butil::gettimeofday_us();
        if (current_block_itr_ptr != nullptr && current_block_itr_ptr->Valid()) {
            // keep searching current block
            ASSIGN_OR_RETURN(founded, search_in_block(k, &(*values)[i], current_block_itr_ptr.get()));
            if (founded) {
                continue_block_read_cnt++;
                continue;
            } else {
                current_block_itr_ptr.reset(nullptr);
            }
        }
        int64_t t1 = butil::gettimeofday_us();
        iiter->Seek(k);
        int64_t t2 = butil::gettimeofday_us();
        if (iiter->Valid()) {
            Slice handle_value = iiter->value();
            FilterBlockReader* filter = rep_->filter;
            BlockHandle handle;
            if (filter != nullptr && handle.DecodeFrom(&handle_value).ok() &&
                !filter->KeyMayMatch(handle.offset(), k)) {
                // Not found
                sst_bloom_filter_rows++;
            } else {
                current_block_itr_ptr.reset(BlockReader(this, options, iiter->value()));
                ASSIGN_OR_RETURN(founded, search_in_block(k, &(*values)[i], current_block_itr_ptr.get()));
            }
        }
        int64_t t3 = butil::gettimeofday_us();
        multiget_t1_us += t1 - t0;
        multiget_t2_us += t2 - t1;
        multiget_t3_us += t3 - t2;
    }
    if (s.ok()) {
        s = iiter->status();
    }
    delete iiter;
    TRACE_COUNTER_INCREMENT("continue_block_read_cnt", continue_block_read_cnt);
    TRACE_COUNTER_INCREMENT("sst_bloom_filter_rows", sst_bloom_filter_rows);
    TRACE_COUNTER_INCREMENT("multiget_t1_us", multiget_t1_us);
    TRACE_COUNTER_INCREMENT("multiget_t2_us", multiget_t2_us);
    TRACE_COUNTER_INCREMENT("multiget_t3_us", multiget_t3_us);
    return s;
}

size_t Table::memory_usage() const {
    const size_t index_block_sz = (rep_->index_block != nullptr) ? rep_->index_block->size() : 0;
    const size_t filter_data_sz = rep_->filter_data_size;
    return index_block_sz + filter_data_sz;
}

// If new container wants to be supported in MultiGet, the initialization can be added here.
template Status Table::MultiGet<std::set<size_t>::iterator>(const ReadOptions& options, const Slice* keys,
                                                            std::set<size_t>::iterator begin,
                                                            std::set<size_t>::iterator end,
                                                            std::vector<std::string>* values);

} // namespace starrocks::sstable
