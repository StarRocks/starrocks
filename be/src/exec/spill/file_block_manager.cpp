// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "exec/spill/file_block_manager.h"

#include <utility>

#include "exec/spill/block_manager.h"
#include "exec/spill/common.h"
#include "fmt/format.h"
#include "gen_cpp/Types_types.h"
#include "gutil/casts.h"
#include "util/defer_op.h"
#include "util/uid_util.h"

namespace starrocks::spill {
class FileBlockContainer {
public:
    FileBlockContainer(DirPtr dir, const TUniqueId& query_id, const TUniqueId& fragment_instance_id,
                       int32_t plan_node_id, std::string plan_node_name, uint64_t id, size_t acquired_size,
                       bool skip_parent_path_deletion)
            : _dir(std::move(dir)),
              _query_id(query_id),
              _fragment_instance_id(fragment_instance_id),
              _plan_node_id(plan_node_id),
              _plan_node_name(std::move(plan_node_name)),
              _id(id),
              _acquired_data_size(acquired_size),
              _skip_parent_path_deletion(skip_parent_path_deletion) {}

    ~FileBlockContainer() {
        // @TODO we need add a gc thread to delete file
        TRACE_SPILL_LOG << "delete spill container file: " << path();
        WARN_IF_ERROR(_dir->fs()->delete_file(path()), fmt::format("cannot delete spill container file: {}", path()));
        _dir->dec_size(_acquired_data_size);
        if (!_skip_parent_path_deletion) {
            // try to delete related dir, only the last one can success, we ignore the error
            (void)(_dir->fs()->delete_dir(parent_path()));
        }
    }

    Status open();

    Status close();

    Dir* dir() const { return _dir.get(); }
    int32_t plan_node_id() const { return _plan_node_id; }
    std::string plan_node_name() const { return _plan_node_name; }

    size_t size() const {
        DCHECK(_writable_file != nullptr);
        return _writable_file->size();
    }
    std::string path() const {
        return fmt::format("{}/{}/{}-{}-{}-{}", _dir->dir(), print_id(_query_id), print_id(_fragment_instance_id),
                           _plan_node_name, _plan_node_id, _id);
    }
    std::string parent_path() const { return fmt::format("{}/{}", _dir->dir(), print_id(_query_id)); }
    uint64_t id() const { return _id; }

    Status append_data(const std::vector<Slice>& data, size_t total_size);

    Status flush();

    bool try_acquire_sizes(size_t allocate_size) {
        if (_data_size + allocate_size <= _acquired_data_size) {
            return true;
        }
        size_t extra_size = _data_size + allocate_size - _acquired_data_size;
        if (_dir->inc_size(extra_size)) {
            _acquired_data_size += extra_size;
            return true;
        }
        return false;
    }

    StatusOr<std::unique_ptr<io::InputStreamWrapper>> get_readable();

    static StatusOr<FileBlockContainerPtr> create(const DirPtr& dir, const TUniqueId& query_id,
                                                  const TUniqueId& fragment_instance_id, int32_t plan_node_id,
                                                  const std::string& plan_node_name, uint64_t id, size_t block_size,
                                                  bool skip_parent_path_deletion);

private:
    DirPtr _dir;
    TUniqueId _query_id;
    TUniqueId _fragment_instance_id;
    int32_t _plan_node_id;
    std::string _plan_node_name;
    uint64_t _id;
    std::unique_ptr<WritableFile> _writable_file;
    bool _has_open = false;
    // data size in this container
    size_t _data_size = 0;
    // acquired data size from Dir
    size_t _acquired_data_size = 0;
    // When true, skip deleting the parent directory in destructor.
    // The caller (e.g. LoadSpillBlockManager) is responsible for cleaning up the parent path.
    bool _skip_parent_path_deletion = false;
};

Status FileBlockContainer::open() {
    if (_has_open) {
        return Status::OK();
    }
    std::string file_path = path();
    WritableFileOptions opt;
    opt.mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE;
    ASSIGN_OR_RETURN(_writable_file, _dir->fs()->new_writable_file(opt, file_path));
    TRACE_SPILL_LOG << "create new container file: " << file_path;
    _has_open = true;
    return Status::OK();
}

Status FileBlockContainer::close() {
    _writable_file.reset();
    return Status::OK();
}

Status FileBlockContainer::append_data(const std::vector<Slice>& data, size_t total_size) {
    auto* dir = _dir.get();
    RETURN_IF(!try_acquire_sizes(total_size), DISK_ACQUIRE_ERROR(total_size, dir));
    RETURN_IF_ERROR(_writable_file->appendv(data.data(), data.size()));
    _data_size += total_size;
    return Status::OK();
}

Status FileBlockContainer::flush() {
    return _writable_file->flush(WritableFile::FLUSH_ASYNC);
}

StatusOr<std::unique_ptr<io::InputStreamWrapper>> FileBlockContainer::get_readable() {
    std::string file_path = path();
    ASSIGN_OR_RETURN(auto f, _dir->fs()->new_sequential_file(file_path));
    return f;
}

StatusOr<FileBlockContainerPtr> FileBlockContainer::create(const DirPtr& dir, const TUniqueId& query_id,
                                                           const TUniqueId& fragment_instance_id, int32_t plan_node_id,
                                                           const std::string& plan_node_name, uint64_t id,
                                                           size_t block_size, bool skip_parent_path_deletion) {
    auto container = std::make_shared<FileBlockContainer>(dir, query_id, fragment_instance_id, plan_node_id,
                                                          plan_node_name, id, block_size, skip_parent_path_deletion);
    RETURN_IF_ERROR(container->open());
    return container;
}

class FileBlockReader final : public BlockReader {
public:
    FileBlockReader(const Block* block, const BlockReaderOptions& options = {}) : BlockReader(block, options) {}

    ~FileBlockReader() override = default;

    std::string debug_string() override { return _block->debug_string(); }

    const Block* block() const override { return _block; }
};

class FileBlock : public Block {
public:
    FileBlock(FileBlockContainerPtr container) : _container(std::move(container)) {}

    ~FileBlock() override = default;

    FileBlockContainerPtr container() const { return _container; }

    Status append(const std::vector<Slice>& data) override {
        size_t total_size = 0;
        std::for_each(data.begin(), data.end(), [&](const Slice& slice) { total_size += slice.size; });
        RETURN_IF_ERROR(_container->append_data(data, total_size));
        _size += total_size;
        return Status::OK();
    }

    Status flush() override { return _container->flush(); }

    StatusOr<std::unique_ptr<io::InputStreamWrapper>> get_readable() const override {
        return _container->get_readable();
    }

    std::shared_ptr<BlockReader> get_reader(const BlockReaderOptions& options) override {
        return std::make_shared<FileBlockReader>(this, options);
    }

    std::string debug_string() const override {
#ifndef BE_TEST
        return fmt::format("FileBlock:{}[container={}, len={}]", (void*)this, _container->path(), _size);
#else
        return fmt::format("FileBlock[container={}]", _container->path());
#endif
    }

    bool try_acquire_sizes(size_t size) override { return _container->try_acquire_sizes(size); }

private:
    FileBlockContainerPtr _container;
};

FileBlockManager::FileBlockManager(const TUniqueId& query_id, DirManager* dir_mgr)
        : _query_id(query_id), _dir_mgr(dir_mgr) {}

Status FileBlockManager::open() {
    return Status::OK();
}

void FileBlockManager::close() {}

StatusOr<BlockPtr> FileBlockManager::acquire_block(const AcquireBlockOptions& opts) {
    AcquireDirOptions acquire_dir_opts;
    acquire_dir_opts.data_size = opts.block_size;
    ASSIGN_OR_RETURN(auto dir, _dir_mgr->acquire_writable_dir(acquire_dir_opts));
    ASSIGN_OR_RETURN(auto block_container,
                     get_or_create_container(dir, opts.fragment_instance_id, opts.plan_node_id, opts.name,
                                             opts.block_size, opts.skip_parent_path_deletion));
    auto res = std::make_shared<FileBlock>(block_container);
    res->set_is_remote(dir->is_remote());
    return res;
}

Status FileBlockManager::release_block(BlockPtr block) {
    auto file_block = down_cast<FileBlock*>(block.get());
    auto container = file_block->container();
    TRACE_SPILL_LOG << "release block: " << block->debug_string();
    RETURN_IF_ERROR(container->close());
    return Status::OK();
}

StatusOr<FileBlockContainerPtr> FileBlockManager::get_or_create_container(
        const DirPtr& dir, const TUniqueId& fragment_instance_id, int32_t plan_node_id,
        const std::string& plan_node_name, size_t block_size, bool skip_parent_path_deletion) {
    TRACE_SPILL_LOG << "get_or_create_container at dir: " << dir->dir() << ", plan node:" << plan_node_id << ", "
                    << plan_node_name << ", block size: " << block_size << " bytes"
                    << ", skip parent path deletion: " << skip_parent_path_deletion;
    uint64_t id = _next_container_id++;
    std::string container_dir = dir->dir() + "/" + print_id(_query_id);
    if (_last_created_container_dir != container_dir) {
        RETURN_IF_ERROR(dir->fs()->create_dir_if_missing(container_dir));
        _last_created_container_dir = container_dir;
    }
    ASSIGN_OR_RETURN(auto block_container,
                     FileBlockContainer::create(dir, _query_id, fragment_instance_id, plan_node_id, plan_node_name, id,
                                                block_size, skip_parent_path_deletion));
    RETURN_IF_ERROR(block_container->open());
    return block_container;
}
} // namespace starrocks::spill