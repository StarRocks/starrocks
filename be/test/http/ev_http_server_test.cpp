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

#include "http/ev_http_server.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

#include "base/testutil/assert.h"
#include "http/http_channel.h"
#include "http/http_client.h"
#include "http/http_handler.h"
#include "http/http_method.h"
#include "http/http_request.h"

namespace starrocks {

// Echoes the worker thread id into the response body so the caller can
// verify that requests genuinely fan out across the worker pool — the
// SO_REUSEPORT load-balancing hash should distribute connections across
// listening fds, and each fd's owner is a distinct libevent worker
// thread.
class WorkerEchoHandler : public HttpHandler {
public:
    void handle(HttpRequest* req) override {
        std::string body =
                "worker_thread_id=" + std::to_string(std::hash<std::thread::id>{}(std::this_thread::get_id()));
        HttpChannel::send_reply(req, body);
        ++served_count;
    }

    std::atomic<int> served_count{0};
};

// All cases in this fixture exercise the multi-worker SO_REUSEPORT
// startup / serve / shutdown path that PR #72956 introduced. With
// num_workers >= 2, workers 1..N-1 each call `listen_with_reuseport`
// and serve traffic on their own listening fd; worker 0 keeps the
// shared `_server_fd`.
class EvHttpServerTest : public ::testing::Test {
protected:
    void SetUp() override { _handler = std::make_unique<WorkerEchoHandler>(); }

    std::unique_ptr<EvHttpServer> make_server(int num_workers) {
        auto server = std::make_unique<EvHttpServer>(0 /* port — kernel-assigned */, num_workers);
        EXPECT_TRUE(server->register_handler(GET, "/echo", _handler.get()));
        return server;
    }

    std::unique_ptr<WorkerEchoHandler> _handler;
};

// Multi-worker startup + serve + clean shutdown.
//
// Exercises:
//  - per-worker `listen_with_reuseport` socket creation (workers 1..N-1)
//  - successful `bind()` / `listen()` on the per-worker fd
//  - `evhttp_accept_socket` on the per-worker fd
//  - request handling on per-worker libevent loop
//  - `stop()`'s locked snapshot of `_worker_fds`
//  - `join()`'s close of every worker fd
TEST_F(EvHttpServerTest, multi_worker_serve_and_shutdown) {
    auto server = make_server(4);
    ASSERT_OK(server->start());
    int port = server->get_real_port();
    ASSERT_GT(port, 0);

    std::string base = "http://127.0.0.1:" + std::to_string(port);

    // Send a batch of requests — at least one per worker. The kernel
    // SO_REUSEPORT hash distributes connections across listeners, so a
    // few of these should land on workers 1..3 (the ones that own a
    // SO_REUSEPORT fd) rather than worker 0 (shared fd).
    constexpr int N_REQUESTS = 16;
    for (int i = 0; i < N_REQUESTS; ++i) {
        HttpClient client;
        ASSERT_OK(client.init(base + "/echo"));
        client.set_method(GET);
        std::string resp;
        ASSERT_OK(client.execute(&resp));
        ASSERT_NE(std::string::npos, resp.find("worker_thread_id="));
    }
    ASSERT_EQ(N_REQUESTS, _handler->served_count.load());

    // Clean shutdown — must not hang. The PR #72956 review surfaced a
    // race where stop() walked _worker_fds without taking _rw_lock; the
    // fix snapshots under the lock. This call exercises the locked path.
    server->stop();
    server->join();
}

// Single-worker server: should bypass SO_REUSEPORT entirely (every
// connection lands on `_server_fd`). Verifies the legacy / fallback
// path still works after the SO_REUSEPORT changes.
TEST_F(EvHttpServerTest, single_worker_uses_shared_fd) {
    auto server = make_server(1);
    ASSERT_OK(server->start());
    int port = server->get_real_port();
    ASSERT_GT(port, 0);

    std::string base = "http://127.0.0.1:" + std::to_string(port);
    HttpClient client;
    ASSERT_OK(client.init(base + "/echo"));
    client.set_method(GET);
    std::string resp;
    ASSERT_OK(client.execute(&resp));
    ASSERT_NE(std::string::npos, resp.find("worker_thread_id="));

    server->stop();
    server->join();
}

// Validates that two SO_REUSEPORT-enabled EvHttpServers asking for `port=0`
// each get a kernel-assigned port that does not collide with the other —
// the auto-assignment path must not accidentally fold s2 into s1's
// reuseport group at the same port.
TEST_F(EvHttpServerTest, port_auto_assignment_distinct) {
    auto s1 = make_server(2);
    auto s2 = make_server(2);
    ASSERT_OK(s1->start());
    ASSERT_OK(s2->start());
    EXPECT_NE(s1->get_real_port(), s2->get_real_port());
    EXPECT_GT(s1->get_real_port(), 0);
    EXPECT_GT(s2->get_real_port(), 0);

    // Round-trip a request against each server before tearing down. Without
    // it stop() can race with worker startup: event_base_loopbreak() called
    // before the worker's event_base_loop() starts iterating is dropped,
    // because libevent resets event_break to 0 on every loop entry. Once
    // the listener is then shutdown(), accept() returns EINVAL on every
    // wake and the loop spins (caught by the gtest 5-minute timeout). The
    // request guarantees the worker has entered dispatch before we stop.
    auto warm = [](EvHttpServer* server) {
        HttpClient client;
        std::string url = "http://127.0.0.1:" + std::to_string(server->get_real_port()) + "/echo";
        ASSERT_OK(client.init(url));
        client.set_method(GET);
        std::string resp;
        ASSERT_OK(client.execute(&resp));
    };
    warm(s1.get());
    warm(s2.get());

    s1->stop();
    s2->stop();
    s1->join();
    s2->join();
}

// Stress: rapid-fire concurrent connections against an 8-worker server
// — verifies the SO_REUSEPORT load-balancing path stays correct under
// concurrent accept(), and that none of the workers' listening fds
// leak or hang.
TEST_F(EvHttpServerTest, concurrent_connections_multi_worker) {
    auto server = make_server(8);
    ASSERT_OK(server->start());
    int port = server->get_real_port();
    std::string base = "http://127.0.0.1:" + std::to_string(port);

    constexpr int N_THREADS = 8;
    constexpr int REQS_PER_THREAD = 8;
    std::vector<std::thread> clients;
    std::atomic<int> ok_count{0};
    for (int t = 0; t < N_THREADS; ++t) {
        clients.emplace_back([&]() {
            for (int i = 0; i < REQS_PER_THREAD; ++i) {
                HttpClient c;
                if (!c.init(base + "/echo").ok()) continue;
                c.set_method(GET);
                std::string r;
                if (c.execute(&r).ok()) ++ok_count;
            }
        });
    }
    for (auto& th : clients) th.join();
    EXPECT_EQ(N_THREADS * REQS_PER_THREAD, ok_count.load());
    EXPECT_EQ(N_THREADS * REQS_PER_THREAD, _handler->served_count.load());

    server->stop();
    server->join();
}

} // namespace starrocks
