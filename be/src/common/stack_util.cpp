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

#include "common/stack_util.h"

#include <dirent.h>
#include <fmt/core.h>
#include <fmt/format.h>
#include <fmt/ostream.h>
#include <signal.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <limits>
#include <memory>
#include <mutex>
#include <sstream>
#include <thread>
#include <tuple>
#include <unordered_map>

#ifdef __APPLE__
#ifndef SIGRTMIN
#define SIGRTMIN (SIGUSR1)
#endif
#ifndef SYS_rt_tgsigqueueinfo
#define SYS_rt_tgsigqueueinfo 0
#endif
#endif

#include "base/hash/hash.h"
#include "base/phmap/phmap.h"
#include "base/testutil/sync_point.h"
#include "base/time/time.h"
#include "base/utility/defer_op.h"
#include "common/logging.h"
#include "gutil/strings/substitute.h"

namespace google {
std::string GetStackTrace();
}

// import hidden stack trace functions from glog
namespace google::glog_internal_namespace_ {
enum class SymbolizeOptions { kNone = 0, kNoLineNumbers = 1 };
int GetStackTrace(void** result, int max_depth, int skip_count);
bool Symbolize(void* pc, char* out, unsigned long out_size, SymbolizeOptions options = SymbolizeOptions::kNone);
} // namespace google::glog_internal_namespace_

namespace starrocks {

std::string get_stack_trace() {
    return google::GetStackTrace();
}

struct StackTraceTask {
    std::thread::id id;
    static constexpr int kMaxStackDepth = 64;
    void* addrs[kMaxStackDepth];
    int depth{0};
    bool done = false;
    int64_t cost_us = 0;
    std::string to_string(const std::string& line_prefix = "") const {
        std::string ret;
        for (int i = 0; i < depth; ++i) {
            char line[2048];
            char buf[1024];
            bool success = false;
            // symbolize costs a lot of time, so mock it in test mode
#if !defined(BE_TEST) && !defined(__APPLE__)
            success = google::glog_internal_namespace_::Symbolize(addrs[i], buf, sizeof(buf));
#else
            std::tuple<void*, char*, size_t> tuple = {addrs[i], buf, sizeof(buf)};
            TEST_SYNC_POINT_CALLBACK("StackTraceTask::symbolize", &tuple);
            success = true;
#endif
            if (success) {
                snprintf(line, 2048, "%s  %16p  %s\n", line_prefix.c_str(), addrs[i], buf);
            } else {
                snprintf(line, 2048, "%s  %16p  (unknown)\n", line_prefix.c_str(), addrs[i]);
            }
            ret += line;
        }
        return ret;
    }
    bool operator==(const StackTraceTask& other) const {
        if (depth != other.depth) {
            return false;
        }
        for (int i = 0; i < depth; ++i) {
            if (addrs[i] != other.addrs[i]) {
                return false;
            }
        }
        return true;
    }
};

struct StackTraceTaskHash {
    size_t operator()(const StackTraceTask& task) const {
        size_t hash = 0;
        for (int i = 0; i < task.depth; ++i) {
            hash = hash * 31 + reinterpret_cast<size_t>(task.addrs[i]);
        }
        return hash;
    }
};

// allocate an id for each stack trace request
std::atomic_int g_stack_trace_id{0};
// TID -> StackTraceTask
using StackTraceTaskMap = std::unordered_map<int, StackTraceTask>;
using StackTraceTaskMapSharedPtr = std::shared_ptr<StackTraceTaskMap>;
/// stack trace id -> StackTraceTaskMap
using StackTraceMap = phmap::parallel_flat_hash_map<int32_t, StackTraceTaskMapSharedPtr, phmap::Hash<int32_t>,
                                                    phmap::EqualTo<int32_t>, phmap::Allocator<int32_t>, 5, std::mutex>;
StackTraceMap g_running_stack_trace;

void get_stack_trace_sighandler(int signum, siginfo_t* siginfo, void* ucontext) {
    int64_t start_us = MonotonicMicros();
    int tid = static_cast<int>(syscall(SYS_gettid));
    auto stack_trace_id = siginfo->si_value.sival_int;
    StackTraceTaskMapSharedPtr stack_trace_task_map;
    bool ret =
            g_running_stack_trace.if_contains(stack_trace_id, [&](const auto& value) { stack_trace_task_map = value; });
    if (!ret) {
        LOG(WARNING) << "stack trace id " << stack_trace_id << " not found, tid: " << tid;
        return;
    }
    auto it = stack_trace_task_map->find(tid);
    if (it == stack_trace_task_map->end()) {
        LOG(WARNING) << "tid " << tid << " not found, stack trace id " << stack_trace_id;
        return;
    }
    auto& task = it->second;
    task.depth = google::glog_internal_namespace_::GetStackTrace(task.addrs, StackTraceTask::kMaxStackDepth, 2);
    // get_stack_trace_for_thread first checks done flag then gets the cost.
    // To ensure the cost is valid, set cost before done flag
    task.cost_us = MonotonicMicros() - start_us;
    task.done = true;
    task.id = std::this_thread::get_id();
}

bool install_stack_trace_sighandler() {
    struct sigaction action;
    memset(&action, 0, sizeof(action));
    action.sa_sigaction = get_stack_trace_sighandler;
    action.sa_flags = SA_RESTART | SA_SIGINFO;
    return 0 == sigaction(SIGRTMIN, &action, nullptr);
}

int signal_thread(pid_t pid, pid_t tid, uid_t uid, int signum, sigval payload) {
#ifdef __APPLE__
    errno = ENOSYS;
    return -1;
#else
    siginfo_t info;
    memset(&info, '\0', sizeof(info));
    info.si_signo = signum;
    info.si_code = SI_QUEUE;
    info.si_pid = pid;
    info.si_uid = uid;
    info.si_value = payload;
    return syscall(SYS_rt_tgsigqueueinfo, pid, tid, signum, &info);
#endif
}

std::string get_stack_trace_for_thread(int tid, int timeout_ms) {
    static bool sighandler_installed = false;
    if (!sighandler_installed) {
        if (!install_stack_trace_sighandler()) {
            auto msg = strings::Substitute("install stack trace signal handler failed, error: $0 tid: $1",
                                           strerror(errno), tid);
            LOG(WARNING) << msg;
            return msg;
        }
        sighandler_installed = true;
    }
    const auto pid = getpid();
    const auto uid = getuid();
    auto stack_trace_id = g_stack_trace_id.fetch_add(1);
    StackTraceTaskMapSharedPtr tasks = std::make_shared<StackTraceTaskMap>();
    tasks->emplace(tid, StackTraceTask());
    g_running_stack_trace.insert_or_assign(stack_trace_id, tasks);
    DeferOp defer([stack_trace_id]() { g_running_stack_trace.erase(stack_trace_id); });
    union sigval payload;
    payload.sival_int = stack_trace_id;
    auto err = signal_thread(pid, tid, uid, SIGRTMIN, payload);
    if (0 != err) {
        auto msg = strings::Substitute("collect stack trace failed, signal thread error: $0 tid: $1", strerror(errno),
                                       tid);
        LOG(WARNING) << msg;
        return msg;
    }
    int timeout_us = timeout_ms * 1000;
    auto& task = (*tasks)[tid];
    while (!task.done) {
        usleep(1000);
        timeout_us -= 1000;
        if (timeout_us <= 0) {
            auto msg = strings::Substitute("collect stack trace timeout tid: $0", tid);
            LOG(WARNING) << msg;
            return msg;
        }
    }
    std::string ret =
            fmt::format("Stack trace id: {}, tid: {} cid:{} \n{}", stack_trace_id, tid, task.id, task.to_string());
    return ret;
}

std::string get_stack_trace_for_threads_with_pattern(const std::vector<int>& tids, const std::string& pattern,
                                                     int timeout_ms, const std::string& line_prefix = "") {
    int64_t start_us = MonotonicMicros();
    static bool sighandler_installed = false;
    if (!sighandler_installed) {
        if (!install_stack_trace_sighandler()) {
            auto msg = strings::Substitute("install stack trace signal handler failed, error: $0", strerror(errno));
            LOG(WARNING) << msg;
            return msg;
        }
        sighandler_installed = true;
    }
    const auto pid = getpid();
    const auto uid = getuid();
    auto stack_trace_id = g_stack_trace_id.fetch_add(1);
    if (tids.empty()) {
        return fmt::format(
                "{}stack trace id {}, total 0 threads, 0 identical groups, finish 0 threads, total cost {} us",
                line_prefix, stack_trace_id, MonotonicMicros() - start_us);
    }
    StackTraceTaskMapSharedPtr tasks = std::make_shared<StackTraceTaskMap>();
    for (int i = 0; i < tids.size(); ++i) {
        tasks->emplace(tids[i], StackTraceTask());
    }
    g_running_stack_trace.insert_or_assign(stack_trace_id, tasks);
    DeferOp defer([stack_trace_id]() { g_running_stack_trace.erase(stack_trace_id); });
    for (int i = 0; i < tids.size(); ++i) {
        union sigval payload;
        payload.sival_int = stack_trace_id;
        auto err = signal_thread(pid, tids[i], uid, SIGRTMIN, payload);
        if (0 != err) {
            auto msg = strings::Substitute("collect stack trace failed, signal thread error: $0 tid: $1",
                                           strerror(errno), tids[i]);
            LOG(WARNING) << msg;
        }
    }
    int timeout_us = timeout_ms * 1000;
    while (true) {
        usleep(1000);
        int done = 0;
        for (int i = 0; i < tids.size(); ++i) {
            if ((*tasks)[tids[i]].done) {
                ++done;
            }
        }
        // ignore jemalloc_bg_thd thread, it cannot be signal/sampled
        if (done >= tids.size() - 1) {
            break;
        }
        timeout_us -= 1000;
        if (timeout_us <= 0) {
            auto msg = strings::Substitute("collect stack trace timeout $0/$1 done", done, tids.size());
            LOG(WARNING) << msg;
            break;
        }
    }

    int64_t task_done_count = 0;
    int64_t max_task_cost_us = 0;
    int64_t min_task_cost_us = std::numeric_limits<int64_t>::max();
    int64_t total_task_cost_us = 0;
    // group threads with same stack trace together
    std::unordered_map<StackTraceTask, std::vector<int>, StackTraceTaskHash> task_map;
    for (int i = 0; i < tids.size(); ++i) {
        auto& task = (*tasks)[tids[i]];
        if (task.done) {
            task_map[task].push_back(tids[i]);
            task_done_count += 1;
            max_task_cost_us = std::max(max_task_cost_us, task.cost_us);
            min_task_cost_us = std::min(min_task_cost_us, task.cost_us);
            total_task_cost_us += task.cost_us;
        }
    }
    std::string ret;
    int64_t total_symbolize_cost_us = 0;
    for (auto& e : task_map) {
        int64_t ts = MonotonicMicros();
        std::string stack_trace = e.first.to_string(line_prefix);
        total_symbolize_cost_us += MonotonicMicros() - ts;
        if (!pattern.empty() && stack_trace.find(pattern) == std::string::npos) {
            continue;
        }
        if (e.second.size() == 1) {
            int tid = e.second[0];
            ret += strings::Substitute("$0tid: $1:$2\n", line_prefix, tid, get_thread_name(tid));
        } else {
            ret += strings::Substitute("$0$1 tids: ", line_prefix, e.second.size());
            for (size_t i = 0; i < e.second.size(); i++) {
                int tid = e.second[i];
                if (i > 0) {
                    ret += ", ";
                }
                ret += std::to_string(tid);
                ret += ":";
                ret += get_thread_name(tid);
            }
            ret += "\n";
        }
        ret += stack_trace;
        ret += "\n";
    }
    int64_t avg_task_cost_us = 0;
    if (task_done_count == 0) {
        min_task_cost_us = 0;
        max_task_cost_us = 0;
    } else {
        avg_task_cost_us = total_task_cost_us / task_done_count;
    }
    ret += fmt::format(
            "{}stack trace id {}, total {} threads, {} identical groups, finish {} threads, total cost {} us, "
            "symbolize cost {} us,"
            " thread block(avg/min/max) {}/{}/{} us, ",
            line_prefix, stack_trace_id, tids.size(), task_map.size(), task_done_count, (MonotonicMicros() - start_us),
            total_symbolize_cost_us, avg_task_cost_us, min_task_cost_us, max_task_cost_us);
    return ret;
}

std::string get_stack_trace_for_threads(const std::vector<int>& tids, int timeout_ms) {
    return get_stack_trace_for_threads_with_pattern(tids, "", timeout_ms);
}

std::string get_stack_trace_for_all_threads_with_prefix(const std::string& line_prefix) {
    return get_stack_trace_for_threads_with_pattern(get_thread_id_list(), "", 3000, line_prefix);
}

std::string get_stack_trace_for_all_threads() {
    return get_stack_trace_for_all_threads_with_prefix("");
}

std::string get_stack_trace_for_function(const std::string& function_pattern) {
    return get_stack_trace_for_threads_with_pattern(get_thread_id_list(), function_pattern, 3000);
}

std::vector<int> get_thread_id_list() {
    std::vector<int> thread_id_list;
    auto dir = opendir("/proc/self/task");
    if (dir == nullptr) {
        return thread_id_list;
    }
    struct dirent* entry;
    while ((entry = readdir(dir)) != nullptr) {
        if (entry->d_type == DT_DIR) {
            int tid = atoi(entry->d_name);
            if (tid != 0) {
                thread_id_list.push_back(tid);
            }
        }
    }
    closedir(dir);
    return thread_id_list;
}

std::string get_thread_name(int tid) {
    std::string self_path = "/proc/self/task/" + std::to_string(tid) + "/comm";
    std::ifstream self_file(self_path);
    if (self_file.is_open()) {
        std::string name;
        std::getline(self_file, name);
        if (!name.empty()) {
            return name;
        }
    }
    return "unknown";
}

} // namespace starrocks
