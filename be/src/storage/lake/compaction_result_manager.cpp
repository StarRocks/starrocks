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

#include "storage/lake/compaction_result_manager.h"

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <ctime>

#include "common/config.h"
#include "common/logging.h"
#include "fs/fs.h"
#include "fs/fs_factory.h"
#include "fs/fs_util.h"
#include "gutil/strings/numbers.h"
#include "gutil/strings/split.h"
#include "gutil/strings/substitute.h"
#include "storage/lake/join_path.h"

namespace starrocks::lake {

namespace {
constexpr const char* kFileSuffix = ".pb";
constexpr const char* kCorruptSuffix = ".corrupt";
} // namespace

CompactionResultManager::CompactionResultManager(std::vector<std::string> root_dirs)
        : _root_dirs(std::move(root_dirs)) {}

std::string CompactionResultManager::make_file_name(int64_t tablet_id, int64_t base_version, int64_t result_id) {
    return strings::Substitute("$0_$1_$2$3", tablet_id, base_version, result_id, kFileSuffix);
}

bool CompactionResultManager::parse_file_name(const std::string& name, int64_t* tablet_id, int64_t* base_version,
                                              int64_t* result_id) {
    // Expected pattern: <tablet_id>_<base_version>_<result_id>.pb
    if (name.size() <= std::strlen(kFileSuffix)) return false;
    if (name.compare(name.size() - std::strlen(kFileSuffix), std::strlen(kFileSuffix), kFileSuffix) != 0) {
        return false;
    }
    std::string stem = name.substr(0, name.size() - std::strlen(kFileSuffix));
    std::vector<std::string> parts = strings::Split(stem, "_");
    if (parts.size() != 3) return false;
    return safe_strto64(parts[0], tablet_id) && safe_strto64(parts[1], base_version) &&
           safe_strto64(parts[2], result_id);
}

std::string CompactionResultManager::pick_root_dir() const {
    // Round-robin by hashing pid+random would be overkill; we just pick the
    // first usable root and rely on most BEs only having a single store path.
    // If multi-disk balancing is needed later, add a per-tablet hash here.
    return _root_dirs.empty() ? std::string() : _root_dirs.front();
}

Status CompactionResultManager::scan_on_startup() {
    std::lock_guard<std::mutex> guard(_mu);
    _tablet_results.clear();
    _pending_inputs.clear();
    _next_result_id.clear();
    _total_bytes.store(0, std::memory_order_relaxed);

    for (const auto& root : _root_dirs) {
        std::string dir = join_path(root, kSubDir);
        ASSIGN_OR_RETURN(auto fs, FileSystemFactory::CreateSharedFromString(dir));
        if (!fs->path_exists(dir).ok()) {
            // Lazy-create on first use; nothing to scan.
            continue;
        }
        std::vector<std::string> entries;
        auto st = fs->iterate_dir(dir, [&](std::string_view name) -> bool {
            entries.emplace_back(name);
            return true;
        });
        if (!st.ok()) {
            LOG(WARNING) << "Fail to iterate compaction result dir " << dir << ": " << st;
            continue;
        }
        for (const auto& name : entries) {
            std::string full_path = join_path(dir, name);
            int64_t tid = 0, ver = 0, rid = 0;
            if (!parse_file_name(name, &tid, &ver, &rid)) {
                LOG(WARNING) << "Skip unrecognized file in compaction result dir: " << full_path;
                continue;
            }
            auto load_st = load_one_file(full_path);
            if (!load_st.ok()) {
                LOG(WARNING) << "Corrupt compaction result file " << full_path << ": " << load_st
                             << "; renaming with .corrupt suffix";
                (void)fs->rename_file(full_path, full_path + kCorruptSuffix);
            }
        }
    }
    LOG(INFO) << "CompactionResultManager: scan_on_startup loaded result_count=" << result_count()
              << " total_bytes=" << total_bytes();
    return Status::OK();
}

Status CompactionResultManager::load_one_file(const std::string& path) {
    // Caller already holds _mu.
    ASSIGN_OR_RETURN(auto rf, fs::new_random_access_file(path));
    ASSIGN_OR_RETURN(auto file_size, rf->get_size());
    std::string buf;
    buf.resize(file_size);
    RETURN_IF_ERROR(rf->read_at_fully(0, buf.data(), buf.size()));

    CompactionResultPB result;
    if (!result.ParseFromArray(buf.data(), static_cast<int>(buf.size()))) {
        return Status::Corruption("CompactionResultPB ParseFromArray failed");
    }
    if (!result.has_tablet_id() || !result.has_base_version() || !result.has_op_compaction()) {
        return Status::Corruption("CompactionResultPB missing required fields");
    }

    ResultRef ref;
    ref.base_version = result.base_version();
    ref.result_id = result.has_result_id() ? result.result_id() : 0;
    ref.file_path = path;
    for (uint32_t rid : result.op_compaction().input_rowsets()) {
        ref.input_rowsets.push_back(rid);
    }

    auto& vec = _tablet_results[result.tablet_id()];
    vec.push_back(ref);
    auto& pending = _pending_inputs[result.tablet_id()];
    for (uint32_t rid : ref.input_rowsets) {
        pending.insert(rid);
    }
    auto& next_rid = _next_result_id[result.tablet_id()];
    if (ref.result_id >= next_rid) next_rid = ref.result_id + 1;
    _total_bytes.fetch_add(static_cast<int64_t>(file_size), std::memory_order_relaxed);
    return Status::OK();
}

Status CompactionResultManager::append_result(const CompactionResultPB& result) {
    if (!result.has_tablet_id() || !result.has_base_version() || !result.has_op_compaction() ||
        !result.has_result_id()) {
        return Status::InvalidArgument("CompactionResultPB missing required fields");
    }
    if (_root_dirs.empty()) {
        return Status::InternalError("CompactionResultManager has no root dirs configured");
    }

    int64_t cap = config::lake_autonomous_compaction_local_result_dir_max_bytes;
    if (cap > 0 && _total_bytes.load(std::memory_order_relaxed) >= cap) {
        return Status::ResourceBusy("local compaction result dir capacity reached");
    }

    std::string root = pick_root_dir();
    std::string dir = join_path(root, kSubDir);
    RETURN_IF_ERROR(fs::create_directories(dir));

    std::string fname = make_file_name(result.tablet_id(), result.base_version(), result.result_id());
    std::string tmp_path = join_path(dir, fname + ".tmp");
    std::string final_path = join_path(dir, fname);

    std::string serialized;
    if (!result.SerializeToString(&serialized)) {
        return Status::InternalError("failed to serialize CompactionResultPB");
    }

    {
        WritableFileOptions opts{.sync_on_close = true, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
        ASSIGN_OR_RETURN(auto wf, fs::new_writable_file(opts, tmp_path));
        RETURN_IF_ERROR(wf->append(serialized));
        RETURN_IF_ERROR(wf->close());
    }
    ASSIGN_OR_RETURN(auto fs, FileSystemFactory::CreateSharedFromString(dir));
    RETURN_IF_ERROR(fs->rename_file(tmp_path, final_path));

    {
        std::lock_guard<std::mutex> guard(_mu);
        ResultRef ref;
        ref.base_version = result.base_version();
        ref.result_id = result.result_id();
        ref.file_path = final_path;
        for (uint32_t rid : result.op_compaction().input_rowsets()) {
            ref.input_rowsets.push_back(rid);
        }
        _tablet_results[result.tablet_id()].push_back(ref);
        auto& pending = _pending_inputs[result.tablet_id()];
        for (uint32_t rid : ref.input_rowsets) {
            pending.insert(rid);
        }
        auto& next_rid = _next_result_id[result.tablet_id()];
        if (ref.result_id >= next_rid) next_rid = ref.result_id + 1;
    }
    _total_bytes.fetch_add(static_cast<int64_t>(serialized.size()), std::memory_order_relaxed);

    LOG(INFO) << "CompactionResultManager appended result tablet_id=" << result.tablet_id()
              << " base_version=" << result.base_version() << " result_id=" << result.result_id()
              << " bytes=" << serialized.size();
    return Status::OK();
}

StatusOr<std::vector<CompactionResultPB>> CompactionResultManager::load_results(int64_t tablet_id,
                                                                                int64_t upper_bound_version) {
    std::vector<ResultRef> refs_to_load;
    {
        std::lock_guard<std::mutex> guard(_mu);
        auto it = _tablet_results.find(tablet_id);
        if (it == _tablet_results.end()) return std::vector<CompactionResultPB>{};
        for (const auto& ref : it->second) {
            if (ref.base_version <= upper_bound_version) {
                refs_to_load.push_back(ref);
            }
        }
    }

    std::sort(refs_to_load.begin(), refs_to_load.end(), [](const ResultRef& a, const ResultRef& b) {
        if (a.base_version != b.base_version) return a.base_version < b.base_version;
        return a.result_id < b.result_id;
    });

    std::vector<CompactionResultPB> out;
    out.reserve(refs_to_load.size());
    for (const auto& ref : refs_to_load) {
        ASSIGN_OR_RETURN(auto rf, fs::new_random_access_file(ref.file_path));
        ASSIGN_OR_RETURN(auto file_size, rf->get_size());
        std::string buf;
        buf.resize(file_size);
        RETURN_IF_ERROR(rf->read_at_fully(0, buf.data(), buf.size()));
        CompactionResultPB result;
        if (!result.ParseFromArray(buf.data(), static_cast<int>(buf.size()))) {
            return Status::Corruption("ParseFromArray failed for " + ref.file_path);
        }
        out.push_back(std::move(result));
    }
    return out;
}

Status CompactionResultManager::delete_results(int64_t tablet_id, const std::vector<int64_t>& result_ids) {
    if (result_ids.empty()) return Status::OK();
    std::vector<std::string> paths_to_delete;
    {
        std::lock_guard<std::mutex> guard(_mu);
        auto it = _tablet_results.find(tablet_id);
        if (it == _tablet_results.end()) return Status::OK();
        std::unordered_set<int64_t> id_set(result_ids.begin(), result_ids.end());
        auto& vec = it->second;
        auto& pending = _pending_inputs[tablet_id];
        std::vector<ResultRef> remaining;
        remaining.reserve(vec.size());
        for (auto& ref : vec) {
            if (id_set.count(ref.result_id) > 0) {
                paths_to_delete.push_back(ref.file_path);
                for (uint32_t rid : ref.input_rowsets) {
                    auto pit = pending.find(rid);
                    if (pit != pending.end()) pending.erase(pit);
                }
            } else {
                remaining.push_back(std::move(ref));
            }
        }
        if (remaining.empty()) {
            _tablet_results.erase(it);
        } else {
            it->second = std::move(remaining);
        }
        if (pending.empty()) {
            _pending_inputs.erase(tablet_id);
        }
    }
    for (const auto& p : paths_to_delete) {
        int64_t sz = 0;
        // Best-effort size accounting: read size before delete.
        auto fs_or = FileSystemFactory::CreateSharedFromString(p);
        if (fs_or.ok()) {
            auto sz_or = (*fs_or)->get_file_size(p);
            if (sz_or.ok()) sz = sz_or.value();
        }
        auto st = fs::delete_file(p);
        if (!st.ok()) {
            LOG(WARNING) << "Fail to delete compaction result file " << p << ": " << st;
        } else if (sz > 0) {
            _total_bytes.fetch_sub(sz, std::memory_order_relaxed);
        }
    }
    return Status::OK();
}

std::unordered_set<uint32_t> CompactionResultManager::pending_inputs(int64_t tablet_id) const {
    std::lock_guard<std::mutex> guard(_mu);
    auto it = _pending_inputs.find(tablet_id);
    if (it == _pending_inputs.end()) return {};
    return std::unordered_set<uint32_t>(it->second.begin(), it->second.end());
}

size_t CompactionResultManager::result_count() const {
    std::lock_guard<std::mutex> guard(_mu);
    size_t n = 0;
    for (const auto& kv : _tablet_results) n += kv.second.size();
    return n;
}

std::vector<CompactionResultManager::ResultRef> CompactionResultManager::list_results_for_tablet(
        int64_t tablet_id) const {
    std::lock_guard<std::mutex> guard(_mu);
    auto it = _tablet_results.find(tablet_id);
    if (it == _tablet_results.end()) return {};
    return it->second;
}

int64_t CompactionResultManager::next_result_id(int64_t tablet_id) {
    std::lock_guard<std::mutex> guard(_mu);
    auto& v = _next_result_id[tablet_id];
    int64_t out = v;
    ++v;
    return out;
}

std::shared_ptr<TxnLogPB> merge_results_to_txn_log(const std::vector<CompactionResultPB>& results, int64_t tablet_id,
                                                   int64_t txn_id) {
    auto log = std::make_shared<TxnLogPB>();
    log->set_tablet_id(tablet_id);
    log->set_txn_id(txn_id);
    auto* op_parallel = log->mutable_op_parallel_compaction();
    int32_t next_subtask_id = 0;
    for (const auto& r : results) {
        auto* sub = op_parallel->add_subtask_compactions();
        sub->CopyFrom(r.op_compaction());
        sub->set_subtask_id(next_subtask_id);
        op_parallel->add_success_subtask_ids(next_subtask_id);
        ++next_subtask_id;
    }
    return log;
}

Status persist_compaction_result_from_txn_log(CompactionResultManager* mgr, int64_t tablet_id, int64_t base_version,
                                              const TxnLogPB& txn_log) {
    if (mgr == nullptr) return Status::InternalError("CompactionResultManager not initialized");
    if (!txn_log.has_op_compaction()) return Status::InvalidArgument("TxnLog missing op_compaction");
    CompactionResultPB result;
    result.set_tablet_id(tablet_id);
    result.set_base_version(base_version);
    result.mutable_op_compaction()->CopyFrom(txn_log.op_compaction());
    result.set_finish_time_ms(static_cast<int64_t>(time(nullptr)) * 1000);
    result.set_result_id(mgr->next_result_id(tablet_id));
    return mgr->append_result(result);
}

} // namespace starrocks::lake
