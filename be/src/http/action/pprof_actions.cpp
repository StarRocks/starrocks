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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/http/action/pprof_actions.cpp

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

#include "http/action/pprof_actions.h"

#include <gperftools/profiler.h>

#include <fstream>
#include <iostream>
#include <mutex>

#include "common/config.h"
#include "common/status.h"
#include "common/tracer.h"
#include "http/ev_http_server.h"
#include "http/http_channel.h"
#include "http/http_headers.h"
#include "http/http_request.h"
#include "jemalloc/jemalloc.h"
#include "util/bfd_parser.h"

namespace starrocks {

// pprof default sample time in seconds.
static const std::string SECOND_KEY = "seconds";
static const int kPprofDefaultSampleSecs = 30;

// Protect, only one thread can work
static std::mutex kPprofActionMutex;

void HeapAction::handle(HttpRequest* req) {
#if defined(ADDRESS_SANITIZER) || defined(LEAK_SANITIZER) || defined(THREAD_SANITIZER)
    (void)kPprofDefaultSampleSecs; // Avoid unused variable warning.

    std::string str = "Heap profiling is not available with address sanitizer builds.";

    HttpChannel::send_reply(req, str);
#else
    (void)kPprofDefaultSampleSecs; // Avoid unused variable warning.

    std::lock_guard<std::mutex> lock(kPprofActionMutex);
    std::string str;
    std::stringstream tmp_prof_file_name;
    tmp_prof_file_name << config::pprof_profile_dir << "/heap_profile." << getpid() << "." << rand();

    // NOTE: Use fname to make the content which fname_cstr references to is still valid
    // when je_mallctl is executing
    auto fname = tmp_prof_file_name.str();
    const char* fname_cstr = fname.c_str();
    if (je_mallctl("prof.dump", nullptr, nullptr, &fname_cstr, sizeof(const char*)) == 0) {
        std::ifstream f(fname_cstr);
        str = std::string(std::istreambuf_iterator<char>(f), std::istreambuf_iterator<char>());
    } else {
        str = "dump jemalloc prof file failed";
    }
    HttpChannel::send_reply(req, str);
#endif
}

void GrowthAction::handle(HttpRequest* req) {
#if defined(ADDRESS_SANITIZER) || defined(LEAK_SANITIZER) || defined(THREAD_SANITIZER)
    std::string str = "Growth profiling is not available with address sanitizer builds.";
    HttpChannel::send_reply(req, str);
#else
    std::string str =
            "Growth profiling is not available with jemalloc builds.You can set the `--base` flag to jeprof to compare "
            "the results of two Heap Profiling";
    HttpChannel::send_reply(req, str);
#endif
}

void ProfileAction::handle(HttpRequest* req) {
#if defined(ADDRESS_SANITIZER) || defined(LEAK_SANITIZER) || defined(THREAD_SANITIZER)
    std::string str = "CPU profiling is not available with address sanitizer builds.";
    HttpChannel::send_reply(req, str);
#else
    std::lock_guard<std::mutex> lock(kPprofActionMutex);
    auto scoped_span = trace::Scope(Tracer::Instance().start_trace("http_handle_profile"));

    int seconds = kPprofDefaultSampleSecs;
    const std::string& seconds_str = req->param(SECOND_KEY);
    if (!seconds_str.empty()) {
        seconds = std::atoi(seconds_str.c_str());
    }

    std::ostringstream tmp_prof_file_name;
    // Build a temporary file name that is hopefully unique.
    tmp_prof_file_name << config::pprof_profile_dir << "/starrocks_profile." << getpid() << "." << rand();
    ProfilerStart(tmp_prof_file_name.str().c_str());
    sleep(seconds);
    ProfilerStop();
    std::ifstream prof_file(tmp_prof_file_name.str().c_str(), std::ios::in);
    std::stringstream ss;
    if (!prof_file.is_open()) {
        ss << "Unable to open cpu profile: " << tmp_prof_file_name.str();
        std::string str = ss.str();
        HttpChannel::send_reply(req, str);
        return;
    }
    ss << prof_file.rdbuf();
    prof_file.close();
    std::string str = ss.str();

    HttpChannel::send_reply(req, str);
#endif
}

void CmdlineAction::handle(HttpRequest* req) {
    FILE* fp = fopen("/proc/self/cmdline", "r");
    if (fp == nullptr) {
        std::string str = "Unable to open file: /proc/self/cmdline";

        HttpChannel::send_reply(req, str);
        return;
    }
    char buf[1024];
    if (fscanf(fp, "%s ", buf) != 1) {
        strcpy(buf, "read cmdline failed");
    }
    fclose(fp);
    std::string str = buf;

    HttpChannel::send_reply(req, str);
}

void SymbolAction::handle(HttpRequest* req) {
    // TODO: Implement symbol resolution. Without this, the binary needs to be passed
    // to pprof to resolve all symbols.
    if (req->method() == HttpMethod::GET) {
        std::stringstream ss;
        ss << "num_symbols: " << _parser->num_symbols();
        std::string str = ss.str();

        HttpChannel::send_reply(req, str);
        return;
    } else if (req->method() == HttpMethod::HEAD) {
        HttpChannel::send_reply(req);
        return;
    } else if (req->method() == HttpMethod::POST) {
        std::string request = req->get_request_body();
        // parse address
        std::string result;
        const char* ptr = request.c_str();
        const char* end = request.c_str() + request.size();
        while (ptr < end && *ptr != '\0') {
            std::string file_name;
            std::string func_name;
            unsigned int lineno = 0;
            const char* old_ptr = ptr;
            if (!_parser->decode_address(ptr, &ptr, &file_name, &func_name, &lineno)) {
                result.append(old_ptr, ptr - old_ptr);
                result.push_back('\t');
                result.append(func_name);
                result.push_back('\n');
            }
            if (ptr < end && *ptr == '+') {
                ptr++;
            }
        }

        HttpChannel::send_reply(req, result);
    }
}

} // namespace starrocks
