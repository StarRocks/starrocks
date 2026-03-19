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

#pragma once

#include <memory>
#include <string>

namespace google_breakpad {
class MinidumpDescriptor;
class ExceptionHandler;
} // namespace google_breakpad

namespace starrocks {
class Minidump {
public:
    static void init();

private:
    Minidump();

    static Minidump& get_instance();
    void check_and_rotate_minidumps(int, const std::string&);
    static bool dump_callback(const google_breakpad::MinidumpDescriptor& descriptor, void* context, bool succeeded);
    static void handle_signal(int signal);
    static bool filter_callback(void* context);

    std::unique_ptr<google_breakpad::ExceptionHandler> _minidump;
    const std::string _minidump_dir;
};

} // namespace starrocks
