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

#include <tuple>

#include "common/config.h"
#include "gutil/strings/join.h"
#include "gutil/strings/split.h"
#include "gutil/strings/substitute.h"
#include "runtime/current_thread.h"
#include "testutil/sync_point.h"
#include "util/time.h"

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
    static constexpr int kMaxStackDepth = 64;
    void* addrs[kMaxStackDepth];
    int depth{0};
    bool done = false;
    int64_t cost_us = 0;
    string to_string(const std::string& line_prefix = "") const {
        string ret;
        for (int i = 0; i < depth; ++i) {
            char line[2048];
            char buf[1024];
            bool success = false;
            // symbolize costs a lot of time, so mock it in test mode
#ifndef BE_TEST
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

void get_stack_trace_sighandler(int signum, siginfo_t* siginfo, void* ucontext) {
    int64_t start_us = MonotonicMicros();
    auto task = reinterpret_cast<StackTraceTask*>(siginfo->si_value.sival_ptr);
    task->depth = google::glog_internal_namespace_::GetStackTrace(task->addrs, StackTraceTask::kMaxStackDepth, 2);
    // get_stack_trace_for_thread first checks done flag then gets the cost.
    // To ensure the cost is valid, set cost before done flag
    task->cost_us = MonotonicMicros() - start_us;
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

    int64_t task_done_count = 0;
    int64_t max_task_cost_us = 0;
    int64_t min_task_cost_us = std::numeric_limits<int64_t>::max();
    int64_t total_task_cost_us = 0;
    // group threads with same stack trace together
    std::unordered_map<StackTraceTask, std::vector<int>, StackTraceTaskHash> task_map;
    for (int i = 0; i < tids.size(); ++i) {
        if (tasks[i].done) {
            task_map[tasks[i]].push_back(tids[i]);
            task_done_count += 1;
            max_task_cost_us = std::max(max_task_cost_us, tasks[i].cost_us);
            min_task_cost_us = std::min(min_task_cost_us, tasks[i].cost_us);
            total_task_cost_us += tasks[i].cost_us;
        }
    }
    string ret;
    int64_t total_symbolize_cost_us = 0;
    for (auto& e : task_map) {
        int64_t ts = MonotonicMicros();
        string stack_trace = e.first.to_string(line_prefix);
        total_symbolize_cost_us += MonotonicMicros() - ts;
        if (!pattern.empty() && stack_trace.find(pattern) == string::npos) {
            continue;
        }
        if (e.second.size() == 1) {
            ret += strings::Substitute("$0tid: $1\n", line_prefix, e.second[0]);
        } else {
            ret += strings::Substitute("$0$1 tids: ", line_prefix, e.second.size());
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
    int64_t avg_task_cost_us = 0;
    if (task_done_count == 0) {
        min_task_cost_us = 0;
        max_task_cost_us = 0;
    } else {
        avg_task_cost_us = total_task_cost_us / task_done_count;
    }
    ret += fmt::format(
            "{}total {} threads, {} identical groups, finish {} threads, total cost {} us, symbolize cost {} us,"
            " thread block(avg/min/max) {}/{}/{} us, ",
            line_prefix, tids.size(), task_map.size(), task_done_count, (MonotonicMicros() - start_us),
            total_symbolize_cost_us, avg_task_cost_us, min_task_cost_us, max_task_cost_us);
    return ret;
}

std::string get_stack_trace_for_threads(const std::vector<int>& tids, int timeout_ms) {
    return get_stack_trace_for_threads_with_pattern(tids, "", timeout_ms);
}

std::string get_stack_trace_for_all_threads(const std::string& line_prefix) {
    return get_stack_trace_for_threads_with_pattern(get_thread_id_list(), "", 3000, line_prefix);
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

#if defined(ADDRESS_SANITIZER)
    __interceptor___cxa_throw(thrown_exception, info, dest);
#else
    __real___cxa_throw(thrown_exception, info, dest);
#endif
}

} // namespace starrocks
