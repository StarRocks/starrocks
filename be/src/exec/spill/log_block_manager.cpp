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

#include "exec/spill/log_block_manager.h"

#include <memory>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <utility>

#include "common/config.h"
#include "exec/spill/block_manager.h"
#include "exec/spill/common.h"
#include "fmt/format.h"
#include "fs/fs.h"
#include "gutil/casts.h"
#include "io/input_stream.h"
#include "runtime/exec_env.h"
#include "storage/options.h"
#include "util/defer_op.h"
#include "util/uid_util.h"

namespace starrocks::spill {
class LogBlockContainer {
public:
    LogBlockContainer(Dir* dir, const TUniqueId& query_id, int32_t plan_node_id, std::string plan_node_name,
                      uint64_t id, bool direct_io)
            : _dir(dir),
              _query_id(query_id),
              _plan_node_id(plan_node_id),
              _plan_node_name(std::move(plan_node_name)),
              _id(id),
              _direct_io(direct_io) {}

    ~LogBlockContainer() {
        TRACE_SPILL_LOG << "delete spill container file: " << path();
        WARN_IF_ERROR(_dir->fs()->delete_file(path()), fmt::format("cannot delete spill container file: {}", path()));
        _dir->dec_size(_data_size);
        // try to delete related dir, only the last one can success, we ignore the error
        (void)(_dir->fs()->delete_dir(parent_path()));
    }

    Status open();

    Status close();

    Dir* dir() const { return _dir; }
    int32_t plan_node_id() const { return _plan_node_id; }
    std::string plan_node_name() const { return _plan_node_name; }

    size_t size() const {
        DCHECK(_writable_file != nullptr);
        return _writable_file->size();
    }
    std::string path() const {
        return fmt::format("{}/{}/{}-{}-{}", _dir->dir(), print_id(_query_id), _plan_node_name, _plan_node_id, _id);
    }
    std::string parent_path() const { return fmt::format("{}/{}", _dir->dir(), print_id(_query_id)); }
    uint64_t id() const { return _id; }

    Status append_data(const std::vector<Slice>& data, size_t total_size);

    Status flush();

    StatusOr<std::unique_ptr<io::InputStreamWrapper>> get_readable(size_t offset, size_t length);

    static StatusOr<LogBlockContainerPtr> create(Dir* dir, TUniqueId query_id, int32_t plan_node_id,
                                                 const std::string& plan_node_name, uint64_t id, bool enable_direct_io);

private:
    Dir* _dir;
    TUniqueId _query_id;
    int32_t _plan_node_id;
    std::string _plan_node_name;
    uint64_t _id;
    std::unique_ptr<WritableFile> _writable_file;
    bool _has_open = false;
    bool _direct_io = false;
    size_t _data_size = 0;
};

Status LogBlockContainer::open() {
    if (_has_open) {
        return Status::OK();
    }
    std::string file_path = path();
    WritableFileOptions opt;
    opt.mode = FileSystem::CREATE_OR_OPEN;
    if (config::experimental_spill_skip_sync) {
        opt.sync_on_close = false;
    }
    opt.direct_write = _direct_io;
    ASSIGN_OR_RETURN(_writable_file, _dir->fs()->new_writable_file(opt, file_path));
    TRACE_SPILL_LOG << "create new container file: " << file_path;
    _has_open = true;
    return Status::OK();
}

Status LogBlockContainer::close() {
    _writable_file.reset();
    return Status::OK();
}

Status LogBlockContainer::append_data(const std::vector<Slice>& data, size_t total_size) {
    RETURN_IF_ERROR(_writable_file->pre_allocate(total_size));
    RETURN_IF_ERROR(_writable_file->appendv(data.data(), data.size()));
    _data_size += total_size;
    return Status::OK();
}

Status LogBlockContainer::flush() {
    if (config::experimental_spill_skip_sync) {
        return Status::OK();
    }
    return _writable_file->flush(WritableFile::FLUSH_ASYNC);
}

StatusOr<std::unique_ptr<io::InputStreamWrapper>> LogBlockContainer::get_readable(size_t offset, size_t length) {
    std::string file_path = path();
    ASSIGN_OR_RETURN(auto f, _dir->fs()->new_sequential_file(file_path));
    RETURN_IF_ERROR(f->skip(offset));
    return f;
}

StatusOr<LogBlockContainerPtr> LogBlockContainer::create(Dir* dir, TUniqueId query_id, int32_t plan_node_id,
                                                         const std::string& plan_node_name, uint64_t id,
                                                         bool direct_io) {
    auto container = std::make_shared<LogBlockContainer>(dir, query_id, plan_node_id, plan_node_name, id, direct_io);
    RETURN_IF_ERROR(container->open());
    return container;
}

class LogBlockReader final : public BlockReader {
public:
    LogBlockReader(const Block* block) : _block(block) {}
    ~LogBlockReader() override = default;

    Status read_fully(void* data, int64_t count) override;

    std::string debug_string() override { return _block->debug_string(); }

private:
    const Block* _block = nullptr;
    std::unique_ptr<io::InputStreamWrapper> _readable;
    size_t _offset = 0;
    size_t _length = 0;
};

class LogBlock : public Block {
public:
    LogBlock(LogBlockContainerPtr container, size_t offset) : _container(std::move(container)), _offset(offset) {}

    ~LogBlock() override = default;

    size_t offset() const { return _offset; }

    LogBlockContainerPtr container() const { return _container; }

    Status append(const std::vector<Slice>& data) override {
        size_t total_size = 0;
        std::for_each(data.begin(), data.end(), [&](const Slice& slice) { total_size += slice.size; });
        RETURN_IF_ERROR(_container->append_data(data, total_size));
        _size += total_size;
        return Status::OK();
    }

    Status flush() override { return _container->flush(); }

    StatusOr<std::unique_ptr<io::InputStreamWrapper>> get_readable() const {
        return _container->get_readable(_offset, _size);
    }

    std::shared_ptr<BlockReader> get_reader() override { return std::make_shared<LogBlockReader>(this); }

    std::string debug_string() const override {
#ifndef BE_TEST
        return fmt::format("LogBlock:{}[container={}, offset={}, len={}]", (void*)this, _container->path(), _offset,
                           _size);
#else
        return fmt::format("LogBlock[container={}]", _container->path(), _offset, _size);
#endif
    }

private:
    LogBlockContainerPtr _container;
    size_t _offset{};
};

Status LogBlockReader::read_fully(void* data, int64_t count) {
    if (_readable == nullptr) {
        auto log_block = down_cast<const LogBlock*>(_block);
        ASSIGN_OR_RETURN(_readable, log_block->get_readable());
        _length = log_block->size();
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

LogBlockManager::LogBlockManager(TUniqueId query_id) : _query_id(std::move(query_id)) {
    _max_container_bytes = config::spill_max_log_block_container_bytes > 0 ? config::spill_max_log_block_container_bytes
                                                                           : kDefaultMaxContainerBytes;
}

LogBlockManager::~LogBlockManager() {
    for (auto& container : _full_containers) {
        container.reset();
    }
    for (auto& [dir, container_map] : _available_containers) {
        for (auto& [_, containers] : *container_map) {
            containers.reset();
        }
    }
}

Status LogBlockManager::open() {
    return Status::OK();
}

void LogBlockManager::close() {}

StatusOr<BlockPtr> LogBlockManager::acquire_block(const AcquireBlockOptions& opts) {
    DCHECK(opts.block_size > 0) << "block size should be larger than 0";
    AcquireDirOptions acquire_dir_opts;
#ifdef BE_TEST
    ASSIGN_OR_RETURN(auto dir, _dir_mgr->acquire_writable_dir(acquire_dir_opts));
#else
    ASSIGN_OR_RETURN(auto dir, ExecEnv::GetInstance()->spill_dir_mgr()->acquire_writable_dir(acquire_dir_opts));
#endif

    ASSIGN_OR_RETURN(auto block_container, get_or_create_container(dir, opts.plan_node_id, opts.name, opts.direct_io));
    return std::make_shared<LogBlock>(block_container, block_container->size());
}

Status LogBlockManager::release_block(const BlockPtr& block) {
    auto log_block = down_cast<LogBlock*>(block.get());
    auto container = log_block->container();
    TRACE_SPILL_LOG << "release block: " << block->debug_string();
    bool is_full = container->size() >= _max_container_bytes;
    if (is_full) {
        RETURN_IF_ERROR(container->close());
    }

    std::lock_guard<std::mutex> l(_mutex);
    if (is_full) {
        TRACE_SPILL_LOG << "mark container as full: " << container->path();
        _full_containers.emplace_back(container);
    } else {
        TRACE_SPILL_LOG << "return container to the pool: " << container->path();
        auto dir = container->dir();
        int32_t plan_node_id = container->plan_node_id();

        auto iter = _available_containers.find(dir);
        CHECK(iter != _available_containers.end());
        auto sub_iter = iter->second->find(plan_node_id);
        sub_iter->second->push(container);
    }
    return Status::OK();
}

StatusOr<LogBlockContainerPtr> LogBlockManager::get_or_create_container(Dir* dir, int32_t plan_node_id,
                                                                        const std::string& plan_node_name,
                                                                        bool direct_io) {
    TRACE_SPILL_LOG << "get_or_create_container at dir: " << dir->dir() << ", plan node:" << plan_node_id << ", "
                    << plan_node_name;

    std::lock_guard<std::mutex> l(_mutex);
    auto iter = _available_containers.find(dir);
    if (iter == _available_containers.end()) {
        _available_containers.insert({dir, std::make_shared<PlanNodeContainerMap>()});
        iter = _available_containers.find(dir);
    }
    auto sub_iter = iter->second->find(plan_node_id);
    if (sub_iter == iter->second->end()) {
        iter->second->insert({plan_node_id, std::make_shared<ContainerQueue>()});
        sub_iter = iter->second->find(plan_node_id);
    }
    auto& q = sub_iter->second;
    if (!q->empty()) {
        auto container = q->front();
        TRACE_SPILL_LOG << "return an existed container: " << container->path();
        q->pop();
        return container;
    }
    uint64_t id = _next_container_id++;
    std::string container_dir = dir->dir() + "/" + print_id(_query_id);
    RETURN_IF_ERROR(dir->fs()->create_dir_if_missing(container_dir));
    ASSIGN_OR_RETURN(auto block_container,
                     LogBlockContainer::create(dir, _query_id, plan_node_id, plan_node_name, id, direct_io));
    RETURN_IF_ERROR(block_container->open());
    return block_container;
}

} // namespace starrocks::spill