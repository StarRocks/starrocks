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
#include <fcntl.h>
#include <fmt/core.h>
#include <fmt/format.h>
#include <poll.h>
#include <sched.h>
#include <spawn.h>
#include <sys/poll.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <csignal>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <memory>
#include <mutex>
#include <sstream>
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
#include "common/logging.h"
#include "common/util/misc.h"
#include "platform/python/env.h"

namespace starrocks {

bool LocalPyWorker::expired() {
    return MonotonicSeconds() - _last_touch_time > config::python_worker_expire_time_sec;
}

void LocalPyWorker::terminate() {
    if (_pid != -1) {
        kill(_pid, SIGKILL);
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

// How the worker process is launched: the program to exec, its argv, and an
// explicit environment (an empty envp vector means "empty environment").
struct SpawnSpec {
    std::string exe;
    std::vector<std::string> argv;
    std::vector<std::string> envp;
};

// CAP_SYS_ADMIN is capability bit 21. Reads this process's effective set.
bool has_cap_sys_admin() {
    std::ifstream status("/proc/self/status");
    std::string line;
    while (std::getline(status, line)) {
        if (line.rfind("CapEff:", 0) == 0) {
            uint64_t caps = std::strtoull(line.c_str() + 7, nullptr, 16);
            return (caps >> 21) & 1ULL;
        }
    }
    return false;
}

// Probe (once, cached) whether an unprivileged user namespace can be created.
// Launch nsjail (which creates the userns) via posix_spawn rather than an
// in-process fork()+unshare(): fork() is unreliable from the large multi-GB BE
// process, whereas posix_spawn is exactly how the worker itself is launched.
// Only a user namespace is created here (all other namespaces and procfs are
// disabled) so /bin/true runs directly on the host filesystem.
bool unprivileged_userns_available(const std::string& nsjail_path) {
    static std::atomic<int> cached{-1};
    int v = cached.load(std::memory_order_acquire);
    if (v >= 0) {
        return v != 0;
    }
    // This is a fixed capability check (not operator-tunable policy), so the flags
    // are inline: create only a user namespace, disable everything else, run /bin/true.
    const char* argv[] = {"nsjail",
                          "-Mo",
                          "-q",
                          "-l",
                          "/dev/null",
                          "--user",
                          "99999",
                          "--group",
                          "99999",
                          "--disable_clone_newns",
                          "--disable_clone_newnet",
                          "--disable_clone_newpid",
                          "--disable_clone_newipc",
                          "--disable_clone_newuts",
                          "--disable_clone_newcgroup",
                          "--disable_proc",
                          "--",
                          "/bin/true",
                          nullptr};
    const char* envp[] = {nullptr};
    posix_spawn_file_actions_t fa;
    posix_spawn_file_actions_init(&fa);
    posix_spawn_file_actions_addopen(&fa, STDOUT_FILENO, "/dev/null", O_WRONLY, 0);
    posix_spawn_file_actions_addopen(&fa, STDERR_FILENO, "/dev/null", O_WRONLY, 0);
    pid_t pid;
    int rc = posix_spawnp(&pid, nsjail_path.c_str(), &fa, nullptr, const_cast<char* const*>(argv),
                          const_cast<char* const*>(envp));
    posix_spawn_file_actions_destroy(&fa);
    int result = 0;
    if (rc == 0) {
        int status = 0;
        while (waitpid(pid, &status, 0) < 0 && errno == EINTR) {
        }
        result = (WIFEXITED(status) && WEXITSTATUS(status) == 0) ? 1 : 0;
    }
    cached.store(result, std::memory_order_release);
    return result != 0;
}

std::string render_placeholders(std::string content, const std::string& python_home, const std::string& pypkgs,
                                const std::string& socket_dir, const std::string& conf_dir) {
    auto replace_all = [&](const std::string& from, const std::string& to) {
        for (size_t p = content.find(from); p != std::string::npos; p = content.find(from, p + to.size())) {
            content.replace(p, from.size(), to);
        }
    };
    replace_all("{{PYTHON_HOME}}", python_home);
    replace_all("{{PYPKGS}}", pypkgs);
    replace_all("{{SOCKET_DIR}}", socket_dir);
    replace_all("{{CONF}}", conf_dir);
    return content;
}

// Render an nsjail config template (substituting the deployment paths) into a
// file under the socket dir, once per template path. nsjail loads it via --config,
// so all isolation policy stays in the (operator-editable) config file.
StatusOr<std::string> ensure_rendered_config(const std::string& template_path, const std::string& python_home,
                                             const std::string& pypkgs, const std::string& socket_dir) {
    static std::mutex mu;
    static std::unordered_map<std::string, std::string> cache;
    std::lock_guard<std::mutex> guard(mu);
    if (auto it = cache.find(template_path); it != cache.end()) {
        return it->second;
    }
    std::ifstream in(template_path);
    if (!in) {
        return Status::InternalError(fmt::format("cannot read nsjail config template '{}'", template_path));
    }
    std::stringstream ss;
    ss << in.rdbuf();
    std::string conf_dir = std::filesystem::path(template_path).parent_path().string();
    std::string content = render_placeholders(ss.str(), python_home, pypkgs, socket_dir, conf_dir);

    std::string rendered =
            fmt::format("{}/{}.rendered", socket_dir, std::filesystem::path(template_path).filename().string());
    std::ofstream out(rendered, std::ios::trunc);
    if (!out || !(out << content) || (out.close(), !out.good())) {
        return Status::InternalError(fmt::format("cannot write rendered nsjail config '{}'", rendered));
    }
    cache.emplace(template_path, rendered);
    return rendered;
}

// Build the program + argv + env used to launch the worker, applying the
// configured sandbox. Returns an error only when the sandbox is required
// (fail-closed) but cannot be established.
StatusOr<SpawnSpec> build_spawn_spec(const std::string& python_path, const std::string& script,
                                     const std::string& sock_url, const std::string& python_home) {
    SpawnSpec direct{python_path, {"python3", script, sock_url}, {fmt::format("PYTHONHOME={}", python_home)}};

    const std::string& sandbox = config::python_udf_sandbox;
    if (sandbox.empty() || sandbox == "off") {
        return direct;
    }
    if (sandbox != "seccomp" && sandbox != "nsjail") {
        return Status::InvalidArgument(fmt::format("unknown python_udf_sandbox '{}'", sandbox));
    }

    const std::string& nsjail = config::python_udf_nsjail_path;
    if (nsjail.empty() || !std::filesystem::exists(nsjail)) {
        if (config::python_udf_sandbox_required) {
            return Status::InternalError(fmt::format(
                    "python UDF sandbox '{}' required but nsjail binary not found at '{}'", sandbox, nsjail));
        }
        LOG(WARNING) << "python UDF sandbox '" << sandbox << "' requested but nsjail not found at '" << nsjail
                     << "'; running python UDF WITHOUT a sandbox";
        return direct;
    }

    // Decide whether full namespace isolation is available (nsjail mode only).
    bool full_ns = false;
    bool privileged = false;
    if (sandbox == "nsjail") {
        const std::string& mode = config::python_udf_sandbox_mode;
        if (mode == "rootless") {
            // Operator asserts unprivileged user namespaces are available; trust the
            // config instead of probing.
            full_ns = true;
        } else if (mode == "privileged") {
            // Operator asserts CAP_SYS_ADMIN is available; trust the config.
            full_ns = true;
            privileged = true;
        } else { // auto: detect. Prefer rootless (unprivileged userns), else CAP_SYS_ADMIN.
            bool userns = unprivileged_userns_available(nsjail);
            bool cap = has_cap_sys_admin();
            LOG(INFO) << "python UDF nsjail sandbox auto-detect: unprivileged_userns=" << userns
                      << " cap_sys_admin=" << cap;
            if (userns) {
                full_ns = true;
            } else if (cap) {
                full_ns = true;
                privileged = true;
            }
        }
        if (!full_ns) {
            if (config::python_udf_sandbox_required) {
                return Status::InternalError(
                        "python UDF nsjail sandbox required but neither unprivileged user namespaces "
                        "nor CAP_SYS_ADMIN are available");
            }
            LOG(WARNING) << "python UDF nsjail namespace isolation unavailable "
                            "(no unprivileged userns, no CAP_SYS_ADMIN); degrading to seccomp-only";
        }
    }

    // All isolation policy lives in the nsjail config file; the BE only renders
    // the deployment paths into it and passes it via --config.
    const std::string& template_path =
            full_ns ? config::python_udf_nsjail_config : config::python_udf_nsjail_seccomp_config;
    std::string pypkgs = std::filesystem::path(script).parent_path().string();
    ASSIGN_OR_RETURN(std::string cfg,
                     ensure_rendered_config(template_path, python_home, pypkgs, PyWorkerManager::socket_dir()));

    // Send nsjail's own logs to a file (not the worker's stdout/stderr pipe) so the
    // "Pywork start success" readiness handshake is not corrupted, while still keeping
    // the diagnostics for troubleshooting a failed sandbox launch.
    std::string nsjail_log = PyWorkerManager::socket_dir() + "/nsjail.log";
    std::vector<std::string> argv = {"nsjail", "-Mo", "-l", nsjail_log, "--config", cfg};
    // Privileged mode reuses the full-namespace config but drops the user namespace.
    if (privileged) {
        argv.emplace_back("--disable_clone_newuser");
    }
    argv.emplace_back("--");
    argv.push_back(python_path);
    argv.push_back(script);
    argv.push_back(sock_url);

    return SpawnSpec{nsjail, std::move(argv), {}};
}

} // namespace

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

    // Apply the configured sandbox: this may wrap the interpreter in nsjail, or
    // return the interpreter directly when the sandbox is off/unavailable.
    ASSIGN_OR_RETURN(SpawnSpec spec, build_spawn_spec(python_path, script, sock_url, py_env.home));

    std::vector<const char*> argv;
    argv.reserve(spec.argv.size() + 1);
    for (const auto& a : spec.argv) {
        argv.push_back(a.c_str());
    }
    argv.push_back(nullptr);
    std::vector<const char*> envp;
    envp.reserve(spec.envp.size() + 1);
    for (const auto& e : spec.envp) {
        envp.push_back(e.c_str());
    }
    envp.push_back(nullptr);

    int rc = posix_spawnp(&pid, spec.exe.c_str(), &actions, &attrs, const_cast<char* const*>(argv.data()),
                          const_cast<char* const*>(envp.data()));
    close(pipefd[1]);

    if (rc != 0) {
        return Status::InternalError(fmt::format("posix_spawnp failed: {}", std::strerror(rc)));
    }

    *child_process = std::make_unique<LocalPyWorker>(pid);
    // Record the socket path up front so it is removed on every failure/cleanup
    // path below, even if the worker bound the socket before failing to start.
    (*child_process)->set_sock_path(sock_path);

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
