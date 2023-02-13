// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "star_cache/disk_space_manager.h"

#include <fmt/format.h>
#include "common/logging.h"
#include "star_cache/types.h"

namespace starrocks::starcache {

const std::string CacheDir::BLOCK_FILE_PREFIX = "blockfile";
const size_t CacheDir::BLOCK_COUNT_IN_SPACE = 1024;

Status CacheDir::init() {
    RETURN_IF_ERROR(init_free_space_list());
    RETURN_IF_ERROR(init_block_files());
    return Status::OK();
}

int64_t CacheDir::alloc_block() {
    std::lock_guard<std::mutex> lg(_space_mutex);
    while (!_free_space_list.empty()) {
        auto& space = _block_spaces[_free_space_list.front()];
        int inner_index = space.free_bits.find_first();
        DCHECK_NE(inner_index, boost::dynamic_bitset<>::npos);
        space.free_bits.reset(inner_index);
        --space.free_count;
        if (space.free_count == 0) {
            _free_space_list.pop_front();
        }
        return space.start_block_index + inner_index;
    }
    return -1;
}

void CacheDir::free_block(uint64_t block_index) {
    int space_index = block_index / BLOCK_COUNT_IN_SPACE;

    std::lock_guard<std::mutex> lg(_space_mutex);
    auto& space = _block_spaces[space_index];
    int inner_index = block_index - space.start_block_index;
    DCHECK_EQ(space.free_bits.test(inner_index), false);
    space.free_bits.set(inner_index);
    ++space.free_count;
    if (space.free_count == 1) {
        _free_space_list.push_back(space_index);
    }
}

Status CacheDir::write_block(uint32_t block_index, off_t offset_in_block, const IOBuf& buf) const {
    BlockFilePtr file = get_block_file(block_index);
    off_t offset = get_block_file_offset(block_index, offset_in_block);
    return file->write(offset, buf);
}

Status CacheDir::read_block(uint32_t block_index, off_t offset_in_block, size_t size, IOBuf* buf) const {
    BlockFilePtr file = get_block_file(block_index);
    off_t offset = get_block_file_offset(block_index, offset_in_block);
    return file->read(offset, size, buf);
}

Status CacheDir::writev_block(uint32_t block_index, off_t offset_in_block,
                              const std::vector<IOBuf*>& bufv) const {
    BlockFilePtr file = get_block_file(block_index);
    off_t offset = get_block_file_offset(block_index, offset_in_block);
    return file->writev(offset, bufv);
}

Status CacheDir::readv_block(uint32_t block_index, off_t offset_in_block, const std::vector<size_t> sizev,
                             std::vector<IOBuf*>* bufv) const {
    BlockFilePtr file = get_block_file(block_index);
    off_t offset = get_block_file_offset(block_index, offset_in_block);
    return file->readv(offset, sizev, bufv);
}

Status CacheDir::init_free_space_list() {
    size_t space_count = _total_block_count / BLOCK_COUNT_IN_SPACE;
    _block_spaces = new BlockSpace[space_count + 1];

    for (size_t i = 0; i < space_count; ++i) {
        BlockSpace& space = _block_spaces[i];
        space.start_block_index = i * BLOCK_COUNT_IN_SPACE;
        space.free_bits.resize(BLOCK_COUNT_IN_SPACE, true);
        space.free_count = BLOCK_COUNT_IN_SPACE;
        _free_space_list.push_back(i);
    }

    // The last free space structure that is not full 
    size_t cur_block_count = space_count * BLOCK_COUNT_IN_SPACE;
    if (cur_block_count < _total_block_count) {
        size_t remain_blocks = _total_block_count - cur_block_count;
        BlockSpace& space = _block_spaces[space_count];
        space.start_block_index = cur_block_count;
        space.free_bits.resize(remain_blocks, true);
        space.free_count = remain_blocks;
        _free_space_list.push_back(space_count);
    }
    return Status::OK();
}

Status CacheDir::init_block_files() {
    size_t free_bytes = _quota_bytes;
    size_t file_count = free_bytes / config::FLAGS_block_file_size;
    for (size_t i = 0; i < file_count; ++i) {
        std::string file_path = fmt::format("{}/{}_{}", _path, BLOCK_FILE_PREFIX, i);
        size_t file_size = std::min(free_bytes, static_cast<size_t>(config::FLAGS_block_file_size));
        BlockFilePtr file(new BlockFile(file_path, file_size));
        RETURN_IF_ERROR(file->open(config::FLAGS_pre_allocate_block_file));
        _block_files.push_back(file);
        free_bytes -= file_size;
    }

    // The last block file
    if (free_bytes > 0) {
        std::string file_path = fmt::format("{}/{}_{}", _path, BLOCK_FILE_PREFIX, file_count);
        BlockFilePtr file(new BlockFile(file_path, free_bytes));
        RETURN_IF_ERROR(file->open(config::FLAGS_pre_allocate_block_file));
        _block_files.push_back(file);
    }
    return Status::OK();
}

void CacheDirRouter::add_dir(CacheDirPtr dir) {
    DirWeight dw = {
        .index = dir->index(),
        //.weight = dir->quota() / 1024 / 1024,
        .weight = dir->quota_bytes(),
        .cur_weight = 0
    };
    std::unique_lock<std::mutex> lck(_mutex);
    _dir_weights.emplace_back(dw);
}

void CacheDirRouter::remove_dir(uint8_t dir_index) {
    std::unique_lock<std::mutex> lck(_mutex);
    std::vector<DirWeight>::iterator iter = _dir_weights.begin();
    while (iter != _dir_weights.end()) {
        if (iter->index == dir_index) {
            break;
        }
        ++iter;
    }
    if (iter != _dir_weights.end()) {
        _dir_weights.erase(iter);
    }
}

// nginx smooth weighted round-robin balancing algorithm
int CacheDirRouter::next_dir_index() {
    std::unique_lock<std::mutex> lck(_mutex);
    if (_dir_weights.empty()) {
        return -1;
    }

    int index = -1;  
    int64_t total = 0;  
    for (int idx = 0; idx < _dir_weights.size(); ++idx)  {  
        _dir_weights[idx].cur_weight += _dir_weights[idx].weight;  
        total += _dir_weights[idx].weight;  
  
        if (index == -1 || _dir_weights[index].cur_weight < _dir_weights[idx].cur_weight)  {  
            index = idx;  
        }  
    }  
    _dir_weights[index].cur_weight -= total;  
    return index;  
}

Status DiskSpaceManager::add_cache_dir(const DirSpace& dir) {
    CacheDirPtr cache_dir(new CacheDir(_cache_dirs.size(), dir.quota_bytes, dir.path));
    RETURN_IF_ERROR(cache_dir->init());
    _cache_dirs.push_back(cache_dir);
    _cache_dir_router.add_dir(cache_dir);
    _quota_bytes += dir.quota_bytes;
    return Status::OK();
}

Status DiskSpaceManager::alloc_block(BlockId* block_id) {
    while (true) {
        int dir_index = _cache_dir_router.next_dir_index();
        // all disks are full
        if (dir_index == -1) {
            DCHECK(false);
            break;
        }

        int64_t block_index = _cache_dirs[dir_index]->alloc_block();
        if (block_index != -1) {
            block_id->dir_index = dir_index;
            block_id->block_index = block_index;
            _used_bytes += config::FLAGS_block_size;
            return Status::OK();
        }
        _cache_dir_router.remove_dir(dir_index);
    }

    return Status::InternalError("allocate block from disk failed");
}

Status DiskSpaceManager::free_block(BlockId block_id) {
    auto& dir = _cache_dirs[block_id.dir_index];
    dir->free_block(block_id.block_index);
    _used_bytes -= config::FLAGS_block_size;
    return Status::OK();
}

Status DiskSpaceManager::write_block(BlockId block_id, off_t offset_in_block, const IOBuf& buf) const {
    auto& dir = _cache_dirs[block_id.dir_index];
    return dir->write_block(block_id.block_index, offset_in_block, buf);
}

Status DiskSpaceManager::read_block(BlockId block_id, off_t offset_in_block, size_t size, IOBuf* buf) const {
    auto& dir = _cache_dirs[block_id.dir_index];
    return dir->read_block(block_id.block_index, offset_in_block, size, buf);
}

Status DiskSpaceManager::writev_block(BlockId block_id, off_t offset_in_block,
                                      const std::vector<IOBuf*>& bufv) const {
    auto& dir = _cache_dirs[block_id.dir_index];
    return dir->writev_block(block_id.block_index, offset_in_block, bufv);
}

Status DiskSpaceManager::readv_block(BlockId block_id, off_t offset_in_block, const std::vector<size_t> sizev,
                                     std::vector<IOBuf*>* bufv) const {
    auto& dir = _cache_dirs[block_id.dir_index];
    return dir->readv_block(block_id.block_index, offset_in_block, sizev, bufv);
}

} // namespace starrocks::starcache
