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
#include <fmt/format.h>

#include <exception>

#include "common/config.h"
#include "gutil/strings/split.h"
#include "runtime/current_thread.h"
#include "util/time.h"

namespace google::glog_internal_namespace_ {
void DumpStackTraceToString(std::string* stacktrace);
} // namespace google::glog_internal_namespace_

namespace starrocks {

std::string get_stack_trace() {
    std::string s;
    google::glog_internal_namespace_::DumpStackTraceToString(&s);
    return s;
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
