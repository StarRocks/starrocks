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
                       int32_t plan_node_id, std::string plan_node_name, uint64_t id)
            : _dir(dir),
              _query_id(query_id),
              _fragment_instance_id(fragment_instance_id),
              _plan_node_id(plan_node_id),
              _plan_node_name(std::move(plan_node_name)),
              _id(id) {}

    ~FileBlockContainer() {
        // @TODO we need add a gc thread to delete file
        TRACE_SPILL_LOG << "delete spill container file: " << path();
        WARN_IF_ERROR(_dir->fs()->delete_file(path()), fmt::format("cannot delete spill container file: {}", path()));
        _dir->dec_size(_data_size);
        // try to delete related dir, only the last one can success, we ignore the error
        (void)(_dir->fs()->delete_dir(parent_path()));
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

    StatusOr<std::unique_ptr<io::InputStreamWrapper>> get_readable();

    static StatusOr<FileBlockContainerPtr> create(DirPtr dir, TUniqueId query_id, TUniqueId fragment_instance_id,
                                                  int32_t plan_node_id, const std::string& plan_node_name, uint64_t id);

private:
    DirPtr _dir;
    TUniqueId _query_id;
    TUniqueId _fragment_instance_id;
    int32_t _plan_node_id;
    std::string _plan_node_name;
    uint64_t _id;
    std::unique_ptr<WritableFile> _writable_file;
    bool _has_open = false;
    size_t _data_size = 0;
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

StatusOr<FileBlockContainerPtr> FileBlockContainer::create(DirPtr dir, TUniqueId query_id,
                                                           TUniqueId fragment_instance_id, int32_t plan_node_id,
                                                           const std::string& plan_node_name, uint64_t id) {
    auto container =
            std::make_shared<FileBlockContainer>(dir, query_id, fragment_instance_id, plan_node_id, plan_node_name, id);
    RETURN_IF_ERROR(container->open());
    return container;
}

class FileBlockReader final : public BlockReader {
public:
    FileBlockReader(const Block* block) : BlockReader(block), _length(block->size()) {}
    ~FileBlockReader() override = default;

    Status read_fully(void* data, int64_t count) override;

    std::string debug_string() override { return _block->debug_string(); }

    const Block* block() const override { return _block; }

private:
    std::unique_ptr<io::InputStreamWrapper> _readable;
    size_t _length = 0;
    size_t _offset = 0;
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

    StatusOr<std::unique_ptr<io::InputStreamWrapper>> get_readable() const { return _container->get_readable(); }

    std::shared_ptr<BlockReader> get_reader() override { return std::make_shared<FileBlockReader>(this); }

    std::string debug_string() const override {
#ifndef BE_TEST
        return fmt::format("FileBlock:{}[container={}, len={}]", (void*)this, _container->path(), _size);
#else
        return fmt::format("FileBlock[container={}]", _container->path());
#endif
    }

private:
    FileBlockContainerPtr _container;
};

Status FileBlockReader::read_fully(void* data, int64_t count) {
    if (_readable == nullptr) {
        auto file_block = down_cast<const FileBlock*>(_block);
        ASSIGN_OR_RETURN(_readable, file_block->get_readable());
    }

    if (_offset + count > _length) {
        return Status::EndOfFile("no more data in this block");
    }

    ASSIGN_OR_RETURN(auto read_len, _readable->read(data, count));
    RETURN_IF(read_len == 0, Status::EndOfFile("no more data in this block"));
    RETURN_IF(read_len != count, Status::InternalError(fmt::format(
                                         "block's length is mismatched, expected: {}, actual: {}", count, read_len)));
    _offset += count;
    return Status::OK();
}

FileBlockManager::FileBlockManager(const TUniqueId& query_id, DirManager* dir_mgr)
        : _query_id(query_id), _dir_mgr(dir_mgr) {}

FileBlockManager::~FileBlockManager() {
    for (auto& container : _containers) {
        container.reset();
    }
}

Status FileBlockManager::open() {
    return Status::OK();
}

void FileBlockManager::close() {}

StatusOr<BlockPtr> FileBlockManager::acquire_block(const AcquireBlockOptions& opts) {
    AcquireDirOptions acquire_dir_opts;
    acquire_dir_opts.data_size = opts.block_size;
    ASSIGN_OR_RETURN(auto dir, _dir_mgr->acquire_writable_dir(acquire_dir_opts));
    ASSIGN_OR_RETURN(auto block_container,
                     get_or_create_container(dir, opts.fragment_instance_id, opts.plan_node_id, opts.name));
    auto res = std::make_shared<FileBlock>(block_container);
    res->set_is_remote(dir->is_remote());
    return res;
}

Status FileBlockManager::release_block(const BlockPtr& block) {
    auto file_block = down_cast<FileBlock*>(block.get());
    auto container = file_block->container();
    TRACE_SPILL_LOG << "release block: " << block->debug_string();
    RETURN_IF_ERROR(container->close());
    _containers.emplace_back(std::move(container));
    return Status::OK();
}

StatusOr<FileBlockContainerPtr> FileBlockManager::get_or_create_container(DirPtr dir, TUniqueId fragment_instance_id,
                                                                          int32_t plan_node_id,
                                                                          const std::string& plan_node_name) {
    TRACE_SPILL_LOG << "get_or_create_container at dir: " << dir->dir() << ", plan node:" << plan_node_id << ", "
                    << plan_node_name;
    uint64_t id = _next_container_id++;
    std::string container_dir = dir->dir() + "/" + print_id(_query_id);
    RETURN_IF_ERROR(dir->fs()->create_dir_if_missing(container_dir));
    ASSIGN_OR_RETURN(auto block_container, FileBlockContainer::create(dir, _query_id, fragment_instance_id,
                                                                      plan_node_id, plan_node_name, id));
    RETURN_IF_ERROR(block_container->open());
    return block_container;
}
} // namespace starrocks::spill