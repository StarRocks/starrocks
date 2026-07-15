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

#include "exprs/udf/python/env.h"

#include <fcntl.h>
#include <gtest/gtest.h>
#include <unistd.h>

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <optional>
#include <string>
#include <unordered_map>

#include "base/testutil/assert.h"
#include "common/config_path_fwd.h"
#include "common/config_udf_fwd.h"
#include "exprs/udf/python/callstub.h"
#include "platform/python/env.h"
#include "types/type_descriptor.h"

namespace starrocks {

class PyWorkerManagerEnvTest : public testing::Test {
public:
    void SetUp() override {
        _test_dir = std::filesystem::current_path() / ("test_py_worker_env_" + std::to_string(getpid()));
        std::filesystem::remove_all(_test_dir);
        std::filesystem::create_directories(_test_dir);

        auto& registry = global_python_env_registry();
        _saved_envs = registry._envs;
        registry._envs.clear();

        if (const char* starrocks_home = std::getenv("STARROCKS_HOME"); starrocks_home != nullptr) {
            _saved_starrocks_home = starrocks_home;
        }
        _saved_local_library_dir = config::local_library_dir;
        _saved_create_child_worker_timeout_ms = config::create_child_worker_timeout_ms;
        _saved_report_python_worker_error = config::report_python_worker_error;

        _starrocks_home = _test_dir / "starrocks_home";
        config::local_library_dir = (_test_dir / "local_library_dir").string();
        config::create_child_worker_timeout_ms = 5000;
        config::report_python_worker_error = true;
        std::filesystem::create_directories(_starrocks_home / "lib/py-packages");
        std::filesystem::create_directories(config::local_library_dir);
        setenv("STARROCKS_HOME", _starrocks_home.c_str(), 1);
    }

    void TearDown() override {
        auto& registry = global_python_env_registry();
        registry._envs = _saved_envs;

        config::local_library_dir = _saved_local_library_dir;
        config::create_child_worker_timeout_ms = _saved_create_child_worker_timeout_ms;
        config::report_python_worker_error = _saved_report_python_worker_error;

        if (_saved_starrocks_home.has_value()) {
            setenv("STARROCKS_HOME", _saved_starrocks_home->c_str(), 1);
        } else {
            unsetenv("STARROCKS_HOME");
        }

        if (_leaked_fd >= 0) {
            close(_leaked_fd);
        }
        std::filesystem::remove_all(_test_dir);
    }

protected:
    void create_fake_python_env() {
        _python_env = _test_dir / "python_env";
        auto bin_dir = _python_env / "bin";
        std::filesystem::create_directories(bin_dir);
        auto python_path = bin_dir / "python3";

        std::ofstream python(python_path);
        python << "#!/bin/sh\n"
               << "if [ -e /proc/self/fd/" << _leaked_fd << " ] || [ -e /dev/fd/" << _leaked_fd << " ]; then\n"
               << "  printf 'leaked fd " << _leaked_fd << "'\n"
               << "else\n"
               << "  printf 'Pywork start success'\n"
               << "fi\n"
               << "/bin/sleep 30\n";
        python.close();

        std::filesystem::permissions(python_path,
                                     std::filesystem::perms::owner_exec | std::filesystem::perms::group_exec |
                                             std::filesystem::perms::others_exec,
                                     std::filesystem::perm_options::add);
        ASSERT_OK(global_python_env_registry().init({_python_env.string()}));
    }

    std::filesystem::path _test_dir;
    std::filesystem::path _starrocks_home;
    std::filesystem::path _python_env;
    std::unordered_map<std::string, PythonEnv> _saved_envs;
    std::optional<std::string> _saved_starrocks_home;
    std::string _saved_local_library_dir;
    int32_t _saved_create_child_worker_timeout_ms = 0;
    bool _saved_report_python_worker_error = false;
    int _leaked_fd = -1;
};

TEST_F(PyWorkerManagerEnvTest, fork_py_worker_closes_inherited_descriptors) {
    auto leaked_file = _test_dir / "leaked_fd";
    int fd = open(leaked_file.c_str(), O_CREAT | O_RDWR, 0600);
    ASSERT_GE(fd, 0);
    _leaked_fd = fcntl(fd, F_DUPFD, 100);
    close(fd);
    ASSERT_GE(_leaked_fd, 100);
    ASSERT_NO_FATAL_FAILURE(create_fake_python_env());

    std::unique_ptr<LocalPyWorker> child_process;
    ASSERT_OK(PyWorkerManager::getInstance()._fork_py_worker(&child_process));
    ASSERT_NE(nullptr, child_process);
    child_process->terminate_and_wait();
}

// A RemotePyWorker (external-worker/service_url mode) has no local process lifecycle: it must never
// kill a process or unlink a socket file it does not own, and its lifecycle hooks must be safe no-ops.
TEST_F(PyWorkerManagerEnvTest, remote_worker_has_no_process_lifecycle) {
    auto sentinel = std::filesystem::path(config::local_library_dir) / "pyworker_-1";
    {
        std::ofstream f(sentinel);
        f << "keep";
    }
    ASSERT_TRUE(std::filesystem::exists(sentinel));

    auto worker = std::make_shared<RemotePyWorker>("grpc+tcp://example:8815");
    ASSERT_EQ("grpc+tcp://example:8815", worker->url());
    ASSERT_FALSE(worker->is_dead());
    ASSERT_FALSE(worker->expired());
    worker->touch();                 // no-op
    worker->terminate_and_wait();    // must be a no-op, not crash / not signal any process
    worker->mark_dead();
    ASSERT_TRUE(worker->is_dead());
    worker.reset();                  // destructor must not touch any process or socket

    ASSERT_TRUE(std::filesystem::exists(sentinel)) << "a remote worker must never unlink a socket file";
}

// The Flight descriptor must carry the zip checksum (so an external worker can verify a package it
// downloads itself), but must NOT carry service_url (that is BE-side routing only).
TEST_F(PyWorkerManagerEnvTest, descriptor_serializes_checksum_not_service_url) {
    PyFunctionDescriptor desc;
    desc.driver_id = 0;
    desc.symbol = "echo";
    desc.location = "inline";
    desc.input_type = "scalar";
    desc.content = "def echo(x): return x";
    desc.service_url = "grpc+tcp://example:8815";
    desc.checksum = "deadbeefchecksum";
    desc.return_type = TYPE_INT_DESC;

    auto json = desc.to_json_string();
    ASSERT_OK(json.status());
    const std::string& s = json.value();
    EXPECT_NE(std::string::npos, s.find("\"checksum\""));
    EXPECT_NE(std::string::npos, s.find("deadbeefchecksum"));
    EXPECT_NE(std::string::npos, s.find("\"content\""));
    EXPECT_EQ(std::string::npos, s.find("service_url"));
}

} // namespace starrocks
