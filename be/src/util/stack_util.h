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

#pragma once

#include <string>
#include <typeinfo>
#include <vector>

namespace starrocks {

// Returns the stack trace as a string from the current location.
// Note: there is a libc bug that causes this not to work on 64 bit machines
// for recursive calls.
std::string get_stack_trace();

std::vector<int> get_thread_id_list();
bool install_stack_trace_sighandler();
std::string get_stack_trace_for_thread(int tid, int timeout_ms);
std::string get_stack_trace_for_threads(const std::vector<int>& tids, int timeout_ms);
std::string get_stack_trace_for_all_threads();
// get all thread stack trace, and filter by function pattern
std::string get_stack_trace_for_function(const std::string& function_pattern);

// wrap libc's _cxa_throw to print stack trace of exceptions
extern "C" {
#ifdef __clang__
void __real___cxa_throw(void* thrown_exception, std::type_info* info, void (*dest)(void*));
__attribute__((no_sanitize("address"))) void __wrap___cxa_throw(void* thrown_exception, std::type_info* info,
                                                                void (*dest)(void*));
#elif defined(__GNUC__)
void __real___cxa_throw(void* thrown_exception, void* infov, void (*dest)(void*));
__attribute__((no_sanitize("address"))) void __wrap___cxa_throw(void* thrown_exception, void* infov,
                                                                void (*dest)(void*));
#endif
}

} // namespace starrocks
