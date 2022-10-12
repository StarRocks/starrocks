// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

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
