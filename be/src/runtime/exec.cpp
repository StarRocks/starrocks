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

#include <fcntl.h>
#include <spawn.h>
#include <sys/poll.h>
#include <sys/time.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <cstring>
#include <string>
#include <vector>

extern char** environ;

namespace starrocks {
std::string lite_exec(const std::vector<std::string>& argv_vec, int timeout_ms) {
    std::string output;

    std::vector<char*> argv;

    argv.reserve(argv_vec.size() + 1);
    for (auto& s : argv_vec) {
        argv.push_back(const_cast<char*>(s.c_str()));
    }
    argv.push_back(nullptr);

    // create pipe
    int pipefd[2];
    if (pipe(pipefd) != 0) {
        return std::string("pipe failed: ") + std::strerror(errno);
    }
    // make read end non-blocking for robust poll/read
    int flags = fcntl(pipefd[0], F_GETFL, 0);
    if (flags != -1) fcntl(pipefd[0], F_SETFL, flags | O_NONBLOCK);

    // spawn file actions
    posix_spawn_file_actions_t actions;
    posix_spawn_file_actions_init(&actions);

    // In child:
    //  - dup pipefd[1] -> STDOUT
    //  - dup pipefd[1] -> STDERR
    //  - close pipefd[0] (read end)
    posix_spawn_file_actions_adddup2(&actions, pipefd[1], STDOUT_FILENO);
    posix_spawn_file_actions_adddup2(&actions, pipefd[1], STDERR_FILENO);
    posix_spawn_file_actions_addclose(&actions, pipefd[0]);

    pid_t pid;
    int rc = posix_spawnp(&pid, argv[0], &actions, nullptr, argv.data(), environ);
    posix_spawn_file_actions_destroy(&actions);

    // close write end in parent
    close(pipefd[1]);

    if (rc != 0) {
        close(pipefd[0]);
        return std::string("posix_spawnp failed: ") + std::strerror(rc);
    }

    // poll + read loop with timeout
    pollfd pfd;
    pfd.fd = pipefd[0];
    pfd.events = POLLIN;

    auto start = std::chrono::steady_clock::now();
    int remaining = timeout_ms;
    bool timed_out = false;
    char buf[4096];

    while (true) {
        int pr = poll(&pfd, 1, remaining);
        if (pr < 0) {
            if (errno == EINTR) {
                auto now = std::chrono::steady_clock::now();
                remaining =
                        timeout_ms -
                        static_cast<int>(std::chrono::duration_cast<std::chrono::milliseconds>(now - start).count());
                if (remaining < 0) remaining = 0;
                continue;
            } else
                // poll error
                break;
        } else if (pr == 0) {
            timed_out = true;
            break;
        } else {
            ssize_t n = read(pipefd[0], buf, sizeof(buf));
            if (n > 0) {
                output.append(buf, n);
            } else if (n == 0) {
                break;
            } else if (errno != EAGAIN && errno != EINTR) {
                break;
            }
        }
        auto now = std::chrono::steady_clock::now();
        remaining = timeout_ms -
                    static_cast<int>(std::chrono::duration_cast<std::chrono::milliseconds>(now - start).count());
        if (remaining <= 0) {
            timed_out = true;
            break;
        }
    }

    if (timed_out) {
        kill(pid, SIGKILL);
    }

    int status = 0;
    if (waitpid(pid, &status, 0) == -1) {
        close(pipefd[0]);
        output.append("\nwaitpid failed: " + std::string(strerror(errno)));
        return output;
    }
    close(pipefd[0]);

    if (WIFEXITED(status)) {
        int code = WEXITSTATUS(status);
        if (code != 0) output.append("\nexit_code: " + std::to_string(code));
    } else if (WIFSIGNALED(status)) {
        int sig = WTERMSIG(status);
        output.append("\nkilled_by_signal: " + std::to_string(sig));
    }

    if (timed_out) {
        output.append("\nexec: timeout");
    }

    return output;
}
} // namespace starrocks
