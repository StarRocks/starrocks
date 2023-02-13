// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <shared_mutex>
#include <atomic>
#include <butil/iobuf.h>
#include <butil/memory/singleton.h>
#include <boost/dynamic_bitset.hpp>
#include "common/statusor.h"
#include "star_cache/utils.h"
#include "star_cache/block_file.h"

namespace starrocks::starcache {

class CacheDir {
public:
    static const std::string BLOCK_FILE_PREFIX;
    static const size_t BLOCK_COUNT_IN_SPACE;

    using BlockFilePtr = std::shared_ptr<BlockFile>;
    struct BlockSpace {
        uint64_t start_block_index;
        boost::dynamic_bitset<> free_bits;
        uint32_t free_count = 0; 
    };

    CacheDir(uint8_t index, size_t quota_bytes, const std::string& path)
        : _index(index)
        , _quota_bytes(quota_bytes)
        , _path(path)
        , _total_block_count(quota_bytes / config::FLAGS_block_size)
        , _used_block_count(0)
    {}

    Status init();

    int64_t alloc_block();
    void free_block(uint64_t block_index);

    BlockFilePtr get_block_file(uint32_t block_index) const {
        size_t file_index = block_index / file_block_count();
        return _block_files[file_index];
    }

    off_t get_block_file_offset(uint32_t block_index, off_t offset_in_block) const {
        uint32_t block_index_in_file = block_index % file_block_count();
        off_t offset = block_index_in_file * config::FLAGS_block_size + offset_in_block;
        return offset;
    }

    Status write_block(uint32_t block_index, off_t offset_in_block, const IOBuf& buf) const;
    Status read_block(uint32_t block_index, off_t offset_in_block, size_t size, IOBuf* buf) const;
    Status writev_block(uint32_t block_index, off_t offset_in_block, const std::vector<IOBuf*>& bufv) const;
    Status readv_block(uint32_t block_index, off_t offset_in_block, const std::vector<size_t> sizev,
                       std::vector<IOBuf*>* bufv) const;

    uint8_t index() const {
        return _index;
    }

    size_t quota_bytes() const {
        return _quota_bytes;
    }

    std::string path() const {
        return _path;
    }

private:
    Status init_free_space_list();
    Status init_block_files();

    uint8_t _index;
    size_t _quota_bytes;
    std::string _path;

    uint32_t _total_block_count;
    uint32_t _used_block_count;
    
    BlockSpace* _block_spaces;
    // store free space index
    std::list<uint32_t> _free_space_list;
    std::vector<BlockFilePtr> _block_files;

    std::mutex _space_mutex;
};

using CacheDirPtr = std::shared_ptr<CacheDir>;

class CacheDirRouter {
public:
    struct DirWeight {
        uint8_t index;
        uint64_t weight;
        uint64_t cur_weight;
    };

    void add_dir(CacheDirPtr dir);

    void remove_dir(uint8_t dir_index);

    int next_dir_index();

private:
    std::vector<DirWeight> _dir_weights;
    std::mutex _mutex;
};

class DiskSpaceManager {
public:
    static DiskSpaceManager* GetInstance() {
        return Singleton<DiskSpaceManager>::get();
    }


    Status add_cache_dir(const DirSpace& dir);

    Status alloc_block(BlockId* block_id);
    Status free_block(BlockId block_id);
    Status write_block(BlockId block_id, off_t offset_in_block, const IOBuf& buf) const;
    Status read_block(BlockId block_id, off_t offset_in_block, size_t size, IOBuf* buf) const;
    Status writev_block(BlockId block_id, off_t offset_in_block, const std::vector<IOBuf*>& bufv) const;
    Status readv_block(BlockId block_id, off_t offset_in_block, const std::vector<size_t> sizev, std::vector<IOBuf*>* bufv) const;

    size_t quota_bytes() const {
        return _quota_bytes;
    }

    size_t used_bytes() const {
        return _used_bytes;
    }

private:
    DiskSpaceManager() {}
    friend struct DefaultSingletonTraits<DiskSpaceManager>;

    DISALLOW_COPY_AND_ASSIGN(DiskSpaceManager);

    size_t _quota_bytes = 0;
    std::atomic<size_t> _used_bytes = 0;
    std::vector<CacheDirPtr> _cache_dirs;
    CacheDirRouter _cache_dir_router;
};

} // namespace starrocks::starcache
