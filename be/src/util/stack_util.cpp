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

#include <cxxabi.h>
#include <dirent.h>
#include <fmt/format.h>
#include <sys/syscall.h>

#include <exception>

#include "common/config.h"
#include "gutil/strings/join.h"
#include "gutil/strings/split.h"
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

class ExceptionStackContext {
public:
    static ExceptionStackContext* get_instance() {
        static ExceptionStackContext context;
        return &context;
    }
    // as exception' name is not large, so we can think there are no exceptions.
    static std::string get_exception_name(const void* info) {
        auto* exception_info = (std::type_info*)info;
        int demangle_status;
        char* demangled_exception_name;
        std::string exception_name = "unknown";
        if (exception_info != nullptr) {
            // Demangle the name of the exception using the GNU C++ ABI:
            demangled_exception_name = abi::__cxa_demangle(exception_info->name(), nullptr, nullptr, &demangle_status);
            if (demangled_exception_name != nullptr) {
                exception_name = std::string(demangled_exception_name);
                // Free the memory from __cxa_demangle():
                free(demangled_exception_name);
            } else {
                // NOTE: if the demangle fails, we do nothing, so the
                // non-demangled name will be printed. That's ok.
                exception_name = std::string(exception_info->name());
            }
        }
        return exception_name;
    }
    bool prefix_in_black_list(const string& exception) {
        for (auto const& str : _black_list) {
            if (exception.rfind(str, 0) == 0) {
                return true;
            }
        }
        return false;
    }

    bool prefix_in_white_list(const string& exception) {
        for (auto const& str : _white_list) {
            if (exception.rfind(str, 0) == 0) {
                return true;
            }
        }
        return false;
    }
    int get_level() { return _level; }

private:
    ExceptionStackContext() {
        _level = starrocks::config::exception_stack_level;
        // other values mean the default value.
        if (_level < -1 || _level > 2) {
            _level = 1;
        }
        _white_list = strings::Split(starrocks::config::exception_stack_white_list, ",");
        _black_list = strings::Split(starrocks::config::exception_stack_black_list, ",");
    }
    ~ExceptionStackContext() = default;
    std::vector<string> _white_list;
    std::vector<string> _black_list;
    int _level;
};

// wrap libc's _cxa_throw that must not throw exceptions again, otherwise causing crash.
#ifdef __clang__
void __wrap___cxa_throw(void* thrown_exception, std::type_info* info, void (*dest)(void*)) {
#elif defined(__GNUC__)
void __wrap___cxa_throw(void* thrown_exception, void* info, void (*dest)(void*)) {
#endif
    auto print_level = ExceptionStackContext::get_instance()->get_level();
    if (print_level != 0) {
        // to avoid recursively throwing std::bad_alloc exception when check memory limit in memory tracker.
        SCOPED_SET_CATCHED(false);
        string exception_name = ExceptionStackContext::get_exception_name((void*)info);
        if ((print_level == 1 && ExceptionStackContext::get_instance()->prefix_in_white_list(exception_name)) ||
            print_level == -1 ||
            (print_level == 2 && !ExceptionStackContext::get_instance()->prefix_in_black_list(exception_name))) {
            auto query_id = CurrentThread::current().query_id();
            auto fragment_instance_id = CurrentThread::current().fragment_instance_id();
            auto stack = fmt::format("{}, query_id={}, fragment_instance_id={} throws exception: {}, trace:\n {} \n",
                                     ToStringFromUnixMicros(GetCurrentTimeMicros()).c_str(), print_id(query_id).c_str(),
                                     print_id(fragment_instance_id).c_str(), exception_name.c_str(),
                                     get_stack_trace().c_str());
#ifdef BE_TEST
            // tests check message from stderr.
            std::cerr << stack << std::endl;
#endif
            LOG(WARNING) << stack;
        }
    }
    // call the real __cxa_throw():
#ifdef __clang__
    __real___cxa_throw(thrown_exception, info, dest);
#elif defined(__GNUC__)
    __real___cxa_throw(thrown_exception, info, dest);
#endif
}

} // namespace starrocks
