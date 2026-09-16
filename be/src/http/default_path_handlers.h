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

#include <cstdio>
#include <optional>
#include <string>
#include <string_view>

#include "http/web_page_handler.h"

namespace starrocks {

class RuntimeEnv;
class MemTracker;

// Adds a set of default path handlers to the HTTP service to display
// logs and configuration flags
void add_default_path_handlers(WebPageHandler* web_page_handler, const RuntimeEnv& runtime_env);

void proc_profile_handler(const WebPageHandler::ArgumentMap& args, std::stringstream* output);

// Validates the `opts` /memz was asked for against the set malloc_stats_print() understands,
// and returns what to hand it. `requested` being absent means the caller did not ask, which
// yields the page's default of "a" -- omit the per-arena statistics.
//
// Returns nullopt when `requested` holds a character jemalloc does not recognise. It ignores
// those silently, so a typo would otherwise look like it took effect.
std::optional<std::string> parse_jemalloc_stats_opts(std::optional<std::string_view> requested);

class MemTrackerWebPageHandler {
public:
    static void handle(const RuntimeEnv& runtime_env, MemTracker* mem_tracker, const WebPageHandler::ArgumentMap& args,
                       std::stringstream* output);
};
} // namespace starrocks
