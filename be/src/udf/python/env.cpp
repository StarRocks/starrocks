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

#include "udf/python/env.h"

#include <dirent.h>
#include <fmt/core.h>
#include <fmt/format.h>
#include <poll.h>
#include <sys/poll.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <unordered_set>

#include "butil/fd_guard.h"
#include "butil/fd_utility.h"
#include "common/config.h"
#include "util/defer_op.h"
#include "util/misc.h"
#include "util/slice.h"

namespace starrocks {

void PyWorker::terminate() {
    if (_pid != -1) {
        kill(_pid, SIGKILL);
    }
}

void PyWorker::wait() {
    if (_pid != -1) {
        int status;
        waitpid(_pid, &status, 0);
        _pid = -1;
    }
}

static Status close_all_fd_except(const std::unordered_set<int>& fds) {
    DIR* dir = opendir("/proc/self/fd");
    auto defer = DeferOp([&dir]() {
        if (dir != nullptr) {
            closedir(dir);
        }
    });

    if (dir == nullptr) {
        return Status::InternalError(fmt::format("open /proc/self/fd error {}", std::strerror(errno)));
    }

    int dir_fd = dirfd(dir);
    if (dir_fd < 0) {
        return Status::InternalError(fmt::format("syscall dirfd error {}", std::strerror(errno)));
    }

    struct dirent* entry;
    while ((entry = readdir(dir)) != nullptr) {
        if (entry->d_type == DT_LNK) {
            int fd = atoi(entry->d_name);
            if (fd >= 0 && fd != dir_fd && fds.count(fd) == 0) {
                close(fd);
            }
        }
    }

    return Status::OK();
}

void PyWorker::remove_unix_socket() {
    unlink(PyWorkerManager::unix_socket_path(_pid).c_str());
}

Status PyWorkerManager::_fork_py_worker(std::unique_ptr<PyWorker>* child_process) {
    ASSIGN_OR_RETURN(auto py_env, PythonEnvManager::getInstance().getDefault());

    std::string python_path = py_env.get_python_path();
    int pipefd[2];

    if (pipe(pipefd) == -1) {
        return Status::InternalError(fmt::format("create pipe error:{}", std::strerror(errno)));
    }
    butil::make_non_blocking(pipefd[0]);

    pid_t cpid = fork();
    if (cpid == -1) {
        return Status::InternalError(fmt::format("fork worker error:{}", std::strerror(errno)));
    } else if (cpid == 0) {
        dup2(pipefd[1], STDOUT_FILENO);
        if (config::report_python_worker_error) {
            dup2(pipefd[1], STDERR_FILENO);
        }
        // change dir
        if (chdir(config::local_library_dir.c_str()) != 0) {
            std::cout << "change dir failed:" << std::strerror(errno) << std::endl;
            exit(-1);
        }
        // run child process
        // close all resource
        std::unordered_set<int> reserved_fd{0, 1, 2, pipefd[0]};
        auto status = close_all_fd_except(reserved_fd);
        if (!status.ok()) {
            std::cout << "close fd failed:" << status.to_string() << std::endl;
            exit(-1);
        }

        pid_t self_pid = getpid();
        std::string str_pid = std::to_string(self_pid);
        char command[] = "python3";
        std::string script = PyWorkerManager::bootstrap();
        std::string unix_socket = PyWorkerManager::unix_socket(self_pid);
        std::string python_home_env = fmt::format("PYTHONHOME={}", py_env.home);
        char* const args[] = {command, script.data(), unix_socket.data(), nullptr};
        char* const envs[] = {python_home_env.data(), nullptr};
        // exec flight server
        if (execvpe(python_path.c_str(), args, envs)) {
            std::cout << "execvp failed:" << std::strerror(errno) << std::endl;
            exit(-1);
        }

    } else {
        close(pipefd[1]);
        butil::fd_guard guard(pipefd[0]);
        *child_process = std::make_unique<PyWorker>(cpid);

        pollfd fds[1];
        fds[0].fd = pipefd[0];
        fds[0].events = POLLIN;

        // wait util worker start
        int32_t poll_timeout = config::create_child_worker_timeout_ms;
        int ret = poll(fds, 1, poll_timeout);
        if (ret == -1) {
            return Status::InternalError(fmt::format("poll error:{}", std::strerror(errno)));
        } else if (ret == 0) {
            (*child_process)->terminate_and_wait();
            return Status::InternalError(fmt::format("create worker timeout, cost {}ms", poll_timeout));
        }

        char buffer[4096];
        size_t buffer_size = sizeof(buffer);
        char* cursor = buffer;
        while (buffer_size > 0) {
            ssize_t n = read(pipefd[0], cursor, buffer_size);
            // -1 errorno should be EAGAIN
            if (n == 0 || n == -1) break;
            buffer_size = buffer_size - n;
        }
        Slice result(buffer, sizeof(buffer) - buffer_size);
        if (result != Slice("Pywork start success\n")) {
            (*child_process)->terminate_and_wait();
            return Status::InternalError(fmt::format("worker start failed:{}", result.to_string()));
        }
        (*child_process)->set_url(PyWorkerManager::unix_socket(cpid));
    }
    return Status::OK();
}

StatusOr<std::shared_ptr<PyWorker>> PyWorkerManager::_acquire_worker(int32_t driver_id, size_t reusable,
                                                                     std::string* url) {
    if (!reusable) {
        std::unique_ptr<PyWorker> child_process;
        RETURN_IF_ERROR(_fork_py_worker(&child_process));
        *url = child_process->url();
        return child_process;
    }
    std::shared_ptr<PyWorker> worker;
    {
        // try to find a worker from pool
        std::lock_guard guard(_mutex);
        auto& workers = _processes[driver_id];
        if (workers.size() > max_worker_per_driver) {
            worker = workers[rand() % max_worker_per_driver];
        }
    }
    if (worker && worker->is_dead()) {
        worker->terminate_and_wait();
    }
    if (worker != nullptr && !worker->is_dead()) {
        *url = worker->url();
        worker->touch();
        return worker;
    }

    std::unique_ptr<PyWorker> uniq_worker;
    RETURN_IF_ERROR(_fork_py_worker(&uniq_worker));
    *url = uniq_worker->url();
    worker = std::move(uniq_worker);

    {
        // add to pool
        std::lock_guard guard(_mutex);
        _processes[driver_id].push_back(worker);
    }

    worker->touch();
    return worker;
}

void PyWorkerManager::cleanup_expired_worker() {
    {
        // iterate all workers and remove expired worker
        std::lock_guard guard(_mutex);
        for (auto& pair : _processes) {
            auto& workers = pair.second;
            workers.erase(std::remove_if(workers.begin(), workers.end(),
                                         [](const std::shared_ptr<PyWorker>& worker) {
                                             if (worker->expired() || worker->is_dead()) {
                                                 return true;
                                             }
                                             return false;
                                         }),
                          workers.end());
        }
    }
}

void PythonEnvManager::start_background_cleanup_thread() {
    _running = true;
    // TODO: port the task to common task pool
    _cleanup_thread = std::make_unique<std::thread>([this]() {
        while (_running) {
            PyWorkerManager::getInstance().cleanup_expired_worker();
            nap_sleep(60, [&]() { return !_running; });
        }
    });
}
void PythonEnvManager::close() {
    _running = false;
    if (_cleanup_thread != nullptr) {
        _cleanup_thread->join();
        _cleanup_thread.reset();
    }
}
} // namespace starrocks