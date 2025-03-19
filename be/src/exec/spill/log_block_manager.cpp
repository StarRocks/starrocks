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

#include <fmt/core.h>

#include <memory>
#include <mutex>
#include <optional>
#include <string_view>
#include <unordered_map>
#include <utility>

#include "block_manager.h"
#include "common/config.h"
#include "common/status.h"
#include "exec/spill/block_manager.h"
#include "exec/spill/common.h"
#include "fmt/format.h"
#include "fs/fs.h"
#include "gutil/casts.h"
#include "io/input_stream.h"
#include "io/io_profiler.h"
#include "runtime/exec_env.h"
#include "storage/options.h"
#include "util/defer_op.h"
#include "util/raw_container.h"
#include "util/stack_util.h"
#include "util/uid_util.h"

namespace starrocks::spill {
class LogBlockContainer {
public:
    LogBlockContainer(DirPtr dir, const TUniqueId& query_id, const TUniqueId& fragment_instance_id,
                      int32_t plan_node_id, std::string plan_node_name, uint64_t id, bool direct_io,
                      size_t acquired_size)
            : _dir(std::move(dir)),
              _query_id(query_id),
              _fragment_instance_id(fragment_instance_id),
              _plan_node_id(plan_node_id),
              _plan_node_name(std::move(plan_node_name)),
              _id(id),
              _direct_io(direct_io),
              _acquired_data_size(acquired_size) {}

    ~LogBlockContainer() {
        TRACE_SPILL_LOG << "delete spill container file: " << path();
        WARN_IF_ERROR(_dir->fs()->delete_file(path()), fmt::format("cannot delete spill container file: {}", path()));
        _dir->dec_size(_acquired_data_size);
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

    bool pre_allocate(size_t allocate_size) {
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

    Status append_data(const std::vector<Slice>& data, size_t total_size);

    Status flush();

    StatusOr<std::unique_ptr<io::InputStreamWrapper>> get_readable(size_t offset, size_t length);

    static StatusOr<LogBlockContainerPtr> create(const DirPtr& dir, const TUniqueId& query_id,
                                                 const TUniqueId& fragment_instance_id, int32_t plan_node_id,
                                                 const std::string& plan_node_name, uint64_t id, bool enable_direct_io,
                                                 size_t block_size);

private:
    DirPtr _dir;
    TUniqueId _query_id;
    TUniqueId _fragment_instance_id;
    int32_t _plan_node_id;
    std::string _plan_node_name;
    uint64_t _id;
    std::unique_ptr<WritableFile> _writable_file;
    bool _has_open = false;
    bool _direct_io = false;
    // real data size used in container
    size_t _data_size = 0;
    // acquired data size from Dir
    size_t _acquired_data_size = 0;
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
    auto scope = IOProfiler::scope(IOProfiler::TAG::TAG_SPILL, 0);
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

StatusOr<LogBlockContainerPtr> LogBlockContainer::create(const DirPtr& dir, const TUniqueId& query_id,
                                                         const TUniqueId& fragment_instance_id, int32_t plan_node_id,
                                                         const std::string& plan_node_name, uint64_t id, bool direct_io,
                                                         size_t block_size) {
    auto container = std::make_shared<LogBlockContainer>(dir, query_id, fragment_instance_id, plan_node_id,
                                                         plan_node_name, id, direct_io, block_size);
    RETURN_IF_ERROR(container->open());
    return container;
}

class LogBlockReader final : public BlockReader {
public:
    LogBlockReader(const Block* block, const BlockReaderOptions& options = {}) : BlockReader(block, options) {}

    ~LogBlockReader() override = default;

    Status read_fully(void* data, int64_t count) override {
        auto scope = IOProfiler::scope(IOProfiler::TAG_SPILL, 0);
        return BlockReader::read_fully(data, count);
    }

    std::string debug_string() override { return _block->debug_string(); }

    const Block* block() const override { return _block; }
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

    StatusOr<std::unique_ptr<io::InputStreamWrapper>> get_readable() const override {
        return _container->get_readable(_offset, _size);
    }

    std::shared_ptr<BlockReader> get_reader(const BlockReaderOptions& options) override {
        return std::make_shared<LogBlockReader>(this, options);
    }

    std::string debug_string() const override {
#ifndef BE_TEST
        return fmt::format("LogBlock:{}[container={}, offset={}, len={}, affinity_group={}]", (void*)this,
                           _container->path(), _offset, _size, _affinity_group);
#else
        return fmt::format("LogBlock[container={}]", _container->path());
#endif
    }

    bool preallocate(size_t write_size) override { return _container->pre_allocate(write_size); }

private:
    LogBlockContainerPtr _container;
    size_t _offset{};
};

LogBlockManager::LogBlockManager(const TUniqueId& query_id, DirManager* dir_mgr)
        : _query_id(std::move(query_id)), _dir_mgr(dir_mgr) {
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
    acquire_dir_opts.data_size = opts.block_size;
#ifdef BE_TEST
    ASSIGN_OR_RETURN(auto dir, _dir_mgr->acquire_writable_dir(acquire_dir_opts));
#else
    ASSIGN_OR_RETURN(auto dir, ExecEnv::GetInstance()->spill_dir_mgr()->acquire_writable_dir(acquire_dir_opts));
#endif

    ASSIGN_OR_RETURN(auto block_container,
                     get_or_create_container(dir, opts.fragment_instance_id, opts.plan_node_id, opts.name,
                                             opts.direct_io, opts.affinity_group, opts.block_size));
    auto res = std::make_shared<LogBlock>(block_container, block_container->size());
    res->set_is_remote(dir->is_remote());
    res->set_affinity_group(opts.affinity_group);
    return res;
}

Status LogBlockManager::release_block(BlockPtr block) {
    auto log_block = down_cast<LogBlock*>(block.get());
    auto container = log_block->container();
    auto affinity_group = block->affinity_group();
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
        auto dir = container->dir();
        int32_t plan_node_id = container->plan_node_id();
        auto iter = _available_containers.find(affinity_group);
        CHECK(iter != _available_containers.end());
        iter->second->find(dir)->second->find(plan_node_id)->second->push(container);
    }
    return Status::OK();
}

Status LogBlockManager::release_affinity_group(const BlockAffinityGroup affinity_group) {
    std::lock_guard<std::mutex> l(_mutex);
    size_t count = _available_containers.erase(affinity_group);
    DCHECK(count == 1) << "can't find affinity_group: " << affinity_group;
    return count == 1 ? Status::OK()
                      : Status::InternalError(fmt::format("can't find affinity_group {}", affinity_group));
}

StatusOr<LogBlockContainerPtr> LogBlockManager::get_or_create_container(
        const DirPtr& dir, const TUniqueId& fragment_instance_id, int32_t plan_node_id,
        const std::string& plan_node_name, bool direct_io, BlockAffinityGroup affinity_group, size_t block_size) {
    TRACE_SPILL_LOG << "get_or_create_container at dir: " << dir->dir()
                    << ". fragment instance: " << print_id(fragment_instance_id) << ", plan node:" << plan_node_id
                    << ", " << plan_node_name;

    std::lock_guard<std::mutex> l(_mutex);

    auto avaiable_containers =
            _available_containers.try_emplace(affinity_group, std::make_shared<DirContainerMap>()).first->second;
    auto dir_container_map =
            avaiable_containers->try_emplace(dir.get(), std::make_shared<PlanNodeContainerMap>()).first->second;
    auto q = dir_container_map->try_emplace(plan_node_id, std::make_shared<ContainerQueue>()).first->second;
    if (!q->empty()) {
        auto container = q->front();
        TRACE_SPILL_LOG << "return an existed container: " << container->path();
        q->pop();
        return container;
    }
    uint64_t id = _next_container_id++;
    std::string container_dir = dir->dir() + "/" + print_id(_query_id);
    RETURN_IF_ERROR(dir->fs()->create_dir_if_missing(container_dir));
    ASSIGN_OR_RETURN(auto block_container, LogBlockContainer::create(dir, _query_id, fragment_instance_id, plan_node_id,
                                                                     plan_node_name, id, direct_io, block_size));
    RETURN_IF_ERROR(block_container->open());
    return block_container;
}
} // namespace starrocks::spill