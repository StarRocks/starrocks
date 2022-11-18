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

#include <string>

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

// as exception' name is not large, so we can think there are no exceptions.
std::string get_exception_name(const void* info) {
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

#if defined(__GNUC__)
// wrap libc's _cxa_throw that must not throw exceptions again, otherwise causing crash.
void __wrap___cxa_throw(void* thrown_exception, void* info, void (*dest)(void*)) {
    auto query_id = CurrentThread::current().query_id();
    auto fragment_instance_id = CurrentThread::current().fragment_instance_id();
    auto stack = fmt::format("{}, query_id={}, fragment_instance_id={} throws exception: {}, trace:\n {} \n",
                             ToStringFromUnixMicros(GetCurrentTimeMicros()).c_str(), print_id(query_id).c_str(),
                             print_id(fragment_instance_id).c_str(), get_exception_name(info).c_str(),
                             get_stack_trace().c_str());

    std::cerr << stack << std::endl;
    LOG(ERROR) << stack;

    // call the real __cxa_throw():
    __real___cxa_throw(thrown_exception, info, dest);
}
#endif
} // namespace starrocks
