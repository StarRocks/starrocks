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

#include <fmt/format.h>

#include <atomic>
#include <condition_variable>
#include <filesystem>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <vector>

#include "common/config.h"
#include "common/statusor.h"

namespace starrocks {

struct PythonEnv {
    std::string home;
    std::string get_python_path() const { return fmt::format("{}/bin/python3", home); }
};

class ArrowFlightWithRW;
class PyFunctionDescriptor;

template <typename Callable>
void lock_free_call_once(std::atomic<bool>& once, Callable&& func) {
    bool expected = false;
    if (once.compare_exchange_strong(expected, true)) {
        func();
    }
}

class PyWorker {
public:
    PyWorker(pid_t pid) : _pid(pid) {}
    ~PyWorker() { terminate_and_wait(); }

    void terminate();

    void wait();

    void terminate_and_wait() {
        lock_free_call_once(_once, [this]() {
            terminate();
            wait();
            remove_unix_socket();
        });
    }
    void remove_unix_socket();

    const std::string url() { return _url; }
    void set_url(std::string url) { _url = std::move(url); }

    void touch() { _last_touch_time = MonotonicSeconds(); }
    bool expired() { return MonotonicSeconds() - _last_touch_time > config::python_worker_expire_time_sec; }

    void mark_dead() { _is_dead = true; }
    bool is_dead() { return _is_dead; }

private:
    std::atomic<bool> _once{};
    bool _is_dead{};
    std::string _url;
    pid_t _pid = -1;
    int64_t _last_touch_time = 0;
};

class PyWorkerManager {
public:
    using WorkerClientPtr = std::shared_ptr<ArrowFlightWithRW>;

    static PyWorkerManager& getInstance() {
        static PyWorkerManager instance;
        return instance;
    }

    StatusOr<WorkerClientPtr> get_client(const PyFunctionDescriptor& func_desc);

    static std::string unix_socket(pid_t pid) {
        std::string unix_socket = fmt::format("grpc+unix://{}/pyworker_{}", config::local_library_dir, pid);
        return unix_socket;
    }

    static std::string unix_socket_path(pid_t pid) {
        std::string unix_socket_path = fmt::format("{}/pyworker_{}", config::local_library_dir, pid);
        return unix_socket_path;
    }

    static std::string bootstrap() {
        const char* server_main = "flight_server.py";
        return fmt::format("{}/lib/py-packages/{}", getenv("STARROCKS_HOME"), server_main);
    }

    void cleanup_expired_worker();

private:
    Status _fork_py_worker(std::unique_ptr<PyWorker>* child_process);
    StatusOr<std::shared_ptr<PyWorker>> _acquire_worker(int32_t driver_id, size_t reusable, std::string* url);

    const size_t max_worker_per_driver = 2;
    std::mutex _mutex;
    std::unordered_map<int32_t, std::vector<std::shared_ptr<PyWorker>>> _processes;
};

// TODO: support config PYTHONPATH
class PythonEnvManager {
public:
    ~PythonEnvManager() { close(); }

    Status init(const std::vector<std::string>& envs) {
        for (const auto& env : envs) {
            std::filesystem::path path = env;
            if (!std::filesystem::is_directory(path)) {
                return Status::InvalidArgument(fmt::format("unsupported python env: {} not a directory", env));
            }

            if (!std::filesystem::exists(path / "bin/python3")) {
                return Status::InvalidArgument(fmt::format("unsupported python env: {} not found python", env));
            }

            PythonEnv python_env;
            python_env.home = env;
            _envs[env] = python_env;
        }
        return Status::OK();
    }

    static PythonEnvManager& getInstance() {
        static PythonEnvManager instance;
        return instance;
    }

    StatusOr<PythonEnv> getDefault() {
        if (_envs.empty()) {
            return Status::InternalError("not found avaliable env");
        }
        return _envs.begin()->second;
    }

    void start_background_cleanup_thread();
    void close();

private:
    bool _running = false;
    std::unique_ptr<std::thread> _cleanup_thread;
    // readyonly after init
    std::unordered_map<std::string, PythonEnv> _envs;
};
} // namespace starrocks
