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
#include <dlfcn.h>

#include "runtime/current_thread.h"

namespace google::glog_internal_namespace_ {
void DumpStackTraceToString(std::string* stacktrace);
} // namespace google::glog_internal_namespace_

namespace starrocks {

std::string get_stack_trace() {
    std::string s;
    google::glog_internal_namespace_::DumpStackTraceToString(&s);
    return s;
}

void __wrap___cxa_throw(void* thrown_exception, void* infov, void (*dest)(void*)) {
    std::type_info* info = (std::type_info*)infov;
    auto query_id = CurrentThread::current().query_id();
    auto fragment_instance_id = CurrentThread::current().fragment_instance_id();
    fprintf(stderr, "query_id=%s, fragment_instance_id=%s throws exceptions, trace:\n %s \n",
            print_id(query_id).c_str(), print_id(fragment_instance_id).c_str(), get_stack_trace().c_str());

    // call the real __cxa_throw():
    static void (*const rethrow)(void*, void*, void (*)(void*)) __attribute__((noreturn)) =
            (void (*)(void*, void*, void (*)(void*)))dlsym(RTLD_NEXT, "__cxa_throw");
    rethrow(thrown_exception, info, dest);
}

} // namespace starrocks
