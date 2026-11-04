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

#include <dirent.h>
#include <fmt/core.h>
#include <fmt/format.h>
#include <poll.h>
#include <spawn.h>
#include <sys/poll.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <csignal>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <unordered_set>

#include "base/random/random.h"
#include "base/string/slice.h"
#include "base/utility/defer_op.h"
#include "butil/fd_guard.h"
#include "butil/fd_utility.h"
#include "common/config_path_fwd.h"
#include "common/config_udf_fwd.h"
#include "common/util/misc.h"
#include "platform/python/env.h"

namespace starrocks {

bool LocalPyWorker::expired() {
    return MonotonicSeconds() - _last_touch_time > config::python_worker_expire_time_sec;
}

void LocalPyWorker::terminate() {
    if (_pid != -1) {
        if (_own_pgroup) {
            // Group-kill: the child leads its own process group (pgid == _pid), so
            // this reaps its descendants too -- a worker that spawned processes of
            // its own, or a wrapper whose real worker is a separate pid, would
            // otherwise be orphaned by kill(_pid).
            if (kill(-_pid, SIGKILL) != 0 && errno == ESRCH) {
                kill(_pid, SIGKILL);
            }
        } else {
            kill(_pid, SIGKILL);
        }
    }
}

void LocalPyWorker::wait() {
    if (_pid != -1) {
        int status;
        waitpid(_pid, &status, 0);
        _pid = -1;
    }
}

void LocalPyWorker::remove_unix_socket() {
    if (!_sock_path.empty()) {
        unlink(_sock_path.c_str());
    }
}

std::string PyWorkerManager::socket_dir() {
    return fmt::format("{}/pyworker", config::local_library_dir);
}

Status PyWorkerManager::ensure_socket_dir() {
    std::string dir = socket_dir();
    if (mkdir(dir.c_str(), 0700) != 0 && errno != EEXIST) {
        return Status::InternalError(fmt::format("create pyworker socket dir {} error: {}", dir, std::strerror(errno)));
    }
    // Tighten permissions to the BE user even if the directory pre-existed with
    // a looser mode: any local user able to reach the socket can drive the
    // worker's Flight server, which executes arbitrary UDF code.
    if (chmod(dir.c_str(), 0700) != 0) {
        return Status::InternalError(fmt::format("chmod pyworker socket dir {} error: {}", dir, std::strerror(errno)));
    }
    return Status::OK();
}

std::string PyWorkerManager::new_socket_path() {
    // A process-local counter keeps names unique among live workers; the random
    // suffix avoids colliding with a stale socket left behind by a prior BE run.
    static std::atomic<uint64_t> seq{0};
    uint64_t n = seq.fetch_add(1, std::memory_order_relaxed);
    uint32_t r = Random::GetTLSInstance()->Next();
    return fmt::format("{}/pyworker_{}_{:08x}", socket_dir(), n, r);
}

namespace {

// The launcher installed through PyWorkerManager::set_launch_hook, if any.
PyWorkerManager::LaunchHook& launch_hook() {
    static PyWorkerManager::LaunchHook hook = nullptr;
    return hook;
}

// posix_spawn takes NULL-terminated arrays; these borrow the strings' buffers, so the
// vector they were built from must outlive the spawn.
std::vector<const char*> c_string_array(const std::vector<std::string>& strings) {
    std::vector<const char*> array;
    array.reserve(strings.size() + 1);
    for (const auto& s : strings) {
        array.push_back(s.c_str());
    }
    array.push_back(nullptr);
    return array;
}

// Ask posix_spawn to put the child in a process group of its own (pgid == child pid), so
// terminate() can group-kill and reap the whole subtree, not just this one process.
// Returns whether the attributes were accepted.
bool spawn_in_own_process_group(posix_spawnattr_t* attrs) {
    short flags = 0;
    posix_spawnattr_getflags(attrs, &flags);
    return posix_spawnattr_setpgroup(attrs, 0) == 0 &&
           posix_spawnattr_setflags(attrs, static_cast<short>(flags | POSIX_SPAWN_SETPGROUP)) == 0;
}

} // namespace

void PyWorkerManager::set_launch_hook(LaunchHook hook) {
    launch_hook() = hook;
}

Status PyWorkerManager::_fork_py_worker(std::unique_ptr<LocalPyWorker>* child_process) {
    ASSIGN_OR_RETURN(auto py_env, global_python_env_registry().getDefault());

    std::string python_path = py_env.get_python_path();
    int pipefd[2];

    if (pipe(pipefd) == -1) {
        return Status::InternalError(fmt::format("create pipe error:{}", std::strerror(errno)));
    }
    butil::fd_guard guard(pipefd[0]);
    butil::make_non_blocking(pipefd[0]);

    pid_t pid;
    posix_spawn_file_actions_t actions;
    posix_spawn_file_actions_init(&actions);
    auto cleanup_action = DeferOp([&actions]() { posix_spawn_file_actions_destroy(&actions); });

    posix_spawn_file_actions_adddup2(&actions, pipefd[1], STDOUT_FILENO);
    if (config::report_python_worker_error) {
        posix_spawn_file_actions_adddup2(&actions, pipefd[1], STDERR_FILENO);
    }
    posix_spawn_file_actions_addclose(&actions, pipefd[0]);

    posix_spawnattr_t attrs;
    posix_spawnattr_init(&attrs);
    auto cleanup_attr = DeferOp([&attrs]() { posix_spawnattr_destroy(&attrs); });

#ifdef __APPLE__
    posix_spawnattr_setflags(&attrs, POSIX_SPAWN_CLOEXEC_DEFAULT);
#else
    DIR* dir = opendir("/proc/self/fd");
    auto defer = DeferOp([&dir]() {
        if (dir != nullptr) {
            closedir(dir);
        }
    });

    if (dir == nullptr) {
        return Status::InternalError(fmt::format("open /proc/self/fd error {}", std::strerror(errno)));
    }

    {
        int dir_fd = dirfd(dir);
        if (dir_fd < 0) {
            return Status::InternalError(fmt::format("syscall dirfd error {}", std::strerror(errno)));
        }
    }

    struct dirent* entry;
    while ((entry = readdir(dir)) != nullptr) {
        if (entry->d_type == DT_LNK || entry->d_type == DT_UNKNOWN) {
            int fd = atoi(entry->d_name);
            if (fd >= 3 && fd != pipefd[0] && fd != pipefd[1]) {
                posix_spawn_file_actions_addclose(&actions, fd);
            }
        }
    }
#endif

    RETURN_IF_ERROR(ensure_socket_dir());
    std::string script = PyWorkerManager::bootstrap();
    std::string sock_path = new_socket_path();
    std::string sock_url = "grpc+unix://" + sock_path;

    LaunchSpec spec{python_path, {"python3", script, sock_url}, {fmt::format("PYTHONHOME={}", py_env.home)}};
    if (auto hook = launch_hook()) {
        ASSIGN_OR_RETURN(spec, hook(LaunchRequest{python_path, script, sock_url, py_env.home}));
    }
    std::vector<const char*> argv = c_string_array(spec.argv);
    std::vector<const char*> envp = c_string_array(spec.envp);

    bool own_pgroup = spawn_in_own_process_group(&attrs);
    int rc = posix_spawnp(&pid, spec.exe.c_str(), &actions, &attrs, const_cast<char* const*>(argv.data()),
                          const_cast<char* const*>(envp.data()));
    close(pipefd[1]);

    if (rc != 0) {
        return Status::InternalError(fmt::format("posix_spawnp failed: {}", std::strerror(rc)));
    }

    // The worker owns its socket path from here on, so every failure and cleanup path
    // below removes it -- even if the worker bound it before failing to start.
    *child_process = std::make_unique<LocalPyWorker>(pid, sock_path, own_pgroup);

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

    const char* success_message = "Pywork start success";
    char buffer[4096];
    size_t buffer_size = sizeof(buffer);
    char* cursor = buffer;
    do {
        ssize_t n = read(pipefd[0], cursor, buffer_size);
        if (n == 0) {
            break;
        } else if (n == -1) {
            if (poll(fds, 1, 100) == -1) break;
        } else {
            buffer_size -= n;
            cursor += n;
            if (Slice(buffer, cursor - buffer).starts_with(success_message)) {
                break;
            }
        }
    } while (buffer_size > 0);

    Slice result(buffer, sizeof(buffer) - buffer_size);
    if (!result.starts_with(success_message)) {
        (*child_process)->terminate_and_wait();
        return Status::InternalError(fmt::format("worker start failed:{}", result.to_string()));
    }
    (*child_process)->set_url(sock_url);

    return Status::OK();
}

StatusOr<std::shared_ptr<PyWorker>> PyWorkerManager::_acquire_worker(int32_t driver_id, size_t reusable,
                                                                     std::string* url) {
    if (!reusable) {
        std::unique_ptr<LocalPyWorker> child_process;
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
            worker = workers[ThreadLocalRandomUniform(static_cast<int32_t>(max_worker_per_driver))];
        }
    }
    if (worker && worker->is_dead()) {
        worker->terminate_and_wait();
        std::lock_guard guard(_mutex);
        auto& workers = _processes[driver_id];
        workers.erase(std::remove(workers.begin(), workers.end(), worker), workers.end());
    }
    if (worker != nullptr && !worker->is_dead()) {
        *url = worker->url();
        worker->touch();
        return worker;
    }

    std::unique_ptr<LocalPyWorker> uniq_worker;
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
    std::vector<std::shared_ptr<PyWorker>> to_destroy;
    {
        std::lock_guard guard(_mutex);
        // iterate all workers and remove expired worker
        for (auto& pair : _processes) {
            auto& workers = pair.second;

            auto partition_it = std::partition(
                    workers.begin(), workers.end(),
                    [](const std::shared_ptr<PyWorker>& worker) { return !(worker->expired() || worker->is_dead()); });

            to_destroy.insert(to_destroy.end(), std::make_move_iterator(partition_it),
                              std::make_move_iterator(workers.end()));

            workers.erase(partition_it, workers.end());
        }
    }

    for (auto& worker : to_destroy) {
        worker->terminate_and_wait();
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
