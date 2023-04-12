// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "util/stack_util.h"

#include <dirent.h>
#include <fmt/format.h>
#include <sys/syscall.h>

#include "gutil/strings/substitute.h"
#include "runtime/current_thread.h"
#include "util/time.h"

namespace google::glog_internal_namespace_ {
void DumpStackTraceToString(std::string* stacktrace);
} // namespace google::glog_internal_namespace_

// import hidden stack trace functions from glog
namespace google {
int GetStackTrace(void** result, int max_depth, int skip_count);
bool Symbolize(void* pc, char* out, int out_size);
} // namespace google

namespace starrocks {

std::string get_stack_trace() {
    std::string s;
    google::glog_internal_namespace_::DumpStackTraceToString(&s);
    return s;
}

struct StackTraceTask {
    static constexpr int kMaxStackDepth = 64;
    void* addrs[kMaxStackDepth];
    int depth{0};
    bool done = false;
    string to_string() const {
        string ret;
        for (int i = 0; i < depth; ++i) {
            char line[2048];
            char buf[1024];
            if (google::Symbolize(addrs[i], buf, sizeof(buf))) {
                snprintf(line, 2048, "  %16p  %s\n", addrs[i], buf);
            } else {
                snprintf(line, 2048, "  %16p  (unknown)\n", addrs[i]);
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

void get_stack_trace_sighandler(int signum, siginfo_t* siginfo, void* ucontext) {
    auto task = reinterpret_cast<StackTraceTask*>(siginfo->si_value.sival_ptr);
    task->depth = google::GetStackTrace(task->addrs, StackTraceTask::kMaxStackDepth, 2);
    task->done = true;
}

bool install_stack_trace_sighandler() {
    struct sigaction action;
    memset(&action, 0, sizeof(action));
    action.sa_sigaction = get_stack_trace_sighandler;
    action.sa_flags = SA_RESTART | SA_SIGINFO;
    return 0 == sigaction(SIGRTMIN, &action, nullptr);
}

int signal_thread(pid_t pid, pid_t tid, uid_t uid, int signum, sigval payload) {
    siginfo_t info;
    memset(&info, '\0', sizeof(info));
    info.si_signo = signum;
    info.si_code = SI_QUEUE;
    info.si_pid = pid;
    info.si_uid = uid;
    info.si_value = payload;
    return syscall(SYS_rt_tgsigqueueinfo, pid, tid, signum, &info);
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
    StackTraceTask task;
    const auto pid = getpid();
    const auto uid = getuid();
    union sigval payload;
    payload.sival_ptr = &task;
    auto err = signal_thread(pid, tid, uid, SIGRTMIN, payload);
    if (0 != err) {
        auto msg = strings::Substitute("collect stack trace failed, signal thread error: $0 tid: $1", strerror(errno),
                                       tid);
        LOG(WARNING) << msg;
        return msg;
    }
    int timeout_us = timeout_ms * 1000;
    while (!task.done) {
        usleep(1000);
        timeout_us -= 1000;
        if (timeout_us <= 0) {
            auto msg = strings::Substitute("collect stack trace timeout tid: $0", tid);
            LOG(WARNING) << msg;
            return msg;
        }
    }
    std::string ret = "Stack trace tid: " + std::to_string(tid) + "\n" + task.to_string();
    LOG(INFO) << ret;
    return ret;
}

std::string get_stack_trace_for_threads_with_pattern(const std::vector<int>& tids, const string& pattern,
                                                     int timeout_ms) {
    static bool sighandler_installed = false;
    if (!sighandler_installed) {
        if (!install_stack_trace_sighandler()) {
            auto msg = strings::Substitute("install stack trace signal handler failed, error: $0", strerror(errno));
            LOG(WARNING) << msg;
            return msg;
        }
        sighandler_installed = true;
    }
    vector<StackTraceTask> tasks(tids.size());
    const auto pid = getpid();
    const auto uid = getuid();
    for (int i = 0; i < tids.size(); ++i) {
        union sigval payload;
        payload.sival_ptr = &tasks[i];
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
            if (tasks[i].done) {
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
    // group threads with same stack trace together
    std::unordered_map<StackTraceTask, std::vector<int>, StackTraceTaskHash> task_map;
    for (int i = 0; i < tids.size(); ++i) {
        if (tasks[i].done) {
            task_map[tasks[i]].push_back(tids[i]);
        }
    }
    string ret;
    for (auto& e : task_map) {
        string stack_trace = e.first.to_string();
        if (!pattern.empty() && stack_trace.find(pattern) == string::npos) {
            continue;
        }
        if (e.second.size() == 1) {
            ret += strings::Substitute("tid: $0\n", e.second[0]);
        } else {
            ret += strings::Substitute("$0 tids: ", e.second.size());
            for (size_t i = 0; i < e.second.size(); i++) {
                if (i > 0) {
                    ret += ",";
                }
                ret += std::to_string(e.second[i]);
            }
            ret += "\n";
        }
        ret += stack_trace;
        ret += "\n";
    }
    ret += strings::Substitute("total $0 threads, $1 identical groups", tids.size(), task_map.size());
    return ret;
}

std::string get_stack_trace_for_threads(const std::vector<int>& tids, int timeout_ms) {
    return get_stack_trace_for_threads_with_pattern(tids, "", timeout_ms);
}

std::string get_stack_trace_for_all_threads() {
    return get_stack_trace_for_threads(get_thread_id_list(), 3000);
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

} // namespace starrocks
