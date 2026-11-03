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

#include <fcntl.h>
#include <gtest/gtest.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "http/http_channel.h"
#include "http/http_client.h"
#include "http/http_handler.h"
#include "http/http_method.h"
#include "http/http_request.h"
#include "testutil/assert.h"
#include "testutil/sync_point.h"
#include "util/defer_op.h"

namespace starrocks {

class EvHttpServerTestHandler : public HttpHandler {
public:
    void handle(HttpRequest* req) override { HttpChannel::send_reply(req, "ok"); }
};

// evhttp_accept_socket() hands the listening fd to libevent (LEV_OPT_CLOSE_ON_FREE),
// so evhttp_free() closes it. join() must not leave any fd that libevent will close
// again: once join() has closed _server_fd, its number is the lowest free fd and the
// next socket()/open() in the process (e.g. a brpc health check) reuses it. The sync
// point grabs the freed fd numbers right before evhttp_free() and checks they survive.
static void verify_join_does_not_close_foreign_fds(int num_workers) {
    std::atomic<int> workers_in_dispatch{0};
    std::vector<int> foreign_fds;
    SyncPoint::GetInstance()->SetCallBack("EvHttpServer::worker:in_dispatch",
                                          [&](void*) { workers_in_dispatch.fetch_add(1); });
    SyncPoint::GetInstance()->SetCallBack("EvHttpServer::join:before_evhttp_free", [&](void*) {
        for (int i = 0; i < num_workers + 1; ++i) {
            int fd = ::socket(AF_INET, SOCK_STREAM, 0);
            if (fd >= 0) foreign_fds.push_back(fd);
        }
    });
    SyncPoint::GetInstance()->EnableProcessing();
    DeferOp defer([]() {
        SyncPoint::GetInstance()->DisableProcessing();
        SyncPoint::GetInstance()->ClearCallBack("EvHttpServer::worker:in_dispatch");
        SyncPoint::GetInstance()->ClearCallBack("EvHttpServer::join:before_evhttp_free");
    });

    EvHttpServerTestHandler handler;
    auto server = std::make_unique<EvHttpServer>(0, num_workers);
    ASSERT_TRUE(server->register_handler(GET, "/echo", &handler));
    ASSERT_OK(server->start());

    // Every worker must be running its event loop before stop(): a loopbreak issued before a worker enters
    // event_base_loop() is lost, and that worker would then spin forever on the shut down listen socket.
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
    while (workers_in_dispatch.load() < num_workers && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    ASSERT_EQ(num_workers, workers_in_dispatch.load());

    server->stop();
    server->join();

    ASSERT_EQ(num_workers + 1, foreign_fds.size());
    for (int fd : foreign_fds) {
        EXPECT_NE(-1, ::fcntl(fd, F_GETFD)) << "fd " << fd << " was closed by EvHttpServer::join()";
        ::close(fd);
    }
}

TEST(EvHttpServerTest, join_does_not_close_foreign_fds_single_worker) {
    verify_join_does_not_close_foreign_fds(1);
}

TEST(EvHttpServerTest, join_does_not_close_foreign_fds_multi_worker) {
    verify_join_does_not_close_foreign_fds(4);
}

// --------- AuthVerifier wiring (added with the HTTP-auth framework) ---------
//
// The auth tests use a dedicated handler that records invocations and a
// configurable need_auth() so we can exercise the four interesting paths:
//   1) verifier returns nullopt          -> handler is invoked
//   2) verifier returns AuthVerifyFailure -> handler is NOT invoked, response
//      carries the verifier-supplied status/headers/body verbatim
//   3) handler->need_auth() == false      -> verifier is bypassed entirely
//   4) no verifier wired                  -> handler runs unconditionally
class AuthProbeHandler : public HttpHandler {
public:
    explicit AuthProbeHandler(bool need_auth) : _need_auth(need_auth) {}
    void handle(HttpRequest* req) override {
        ++invocations;
        HttpChannel::send_reply(req, "handler-ok");
    }
    bool need_auth() const override { return _need_auth; }

    std::atomic<int> invocations{0};

private:
    bool _need_auth;
};

TEST(EvHttpServerAuthTest, verifier_returns_nullopt_dispatches_handler) {
    auto handler = std::make_unique<AuthProbeHandler>(true);
    auto server = std::make_unique<EvHttpServer>(0, 1);
    server->register_handler(GET, "/probe", handler.get());

    std::atomic<int> verifier_calls{0};
    server->set_auth_verifier(
            [&](HttpRequest*, HttpHandler::RequiredPrivilege) -> std::optional<EvHttpServer::AuthVerifyFailure> {
                ++verifier_calls;
                return std::nullopt;
            });

    ASSERT_OK(server->start());
    int port = server->get_real_port();
    std::string url = "http://127.0.0.1:" + std::to_string(port) + "/probe";

    HttpClient client;
    ASSERT_OK(client.init(url));
    client.set_method(GET);
    std::string resp;
    ASSERT_OK(client.execute(&resp));
    EXPECT_EQ("handler-ok", resp);
    EXPECT_EQ(1, verifier_calls.load());
    EXPECT_EQ(1, handler->invocations.load());

    server->stop();
    server->join();
}

TEST(EvHttpServerAuthTest, verifier_returns_failure_short_circuits_handler) {
    auto handler = std::make_unique<AuthProbeHandler>(true);
    auto server = std::make_unique<EvHttpServer>(0, 1);
    server->register_handler(GET, "/probe", handler.get());

    std::atomic<int> verifier_calls{0};
    server->set_auth_verifier(
            [&](HttpRequest*, HttpHandler::RequiredPrivilege) -> std::optional<EvHttpServer::AuthVerifyFailure> {
                ++verifier_calls;
                EvHttpServer::AuthVerifyFailure f;
                f.http_status = HttpStatus::UNAUTHORIZED;
                f.www_authenticate = "Basic realm=\"\"";
                f.body = "{\"status\":\"FAILED\",\"code\":\"1\",\"message\":\"denied\"}";
                return f;
            });

    ASSERT_OK(server->start());
    int port = server->get_real_port();
    std::string url = "http://127.0.0.1:" + std::to_string(port) + "/probe";

    HttpClient client;
    ASSERT_OK(client.init(url));
    client.set_method(GET);
    std::string resp;
    // HttpClient::init() sets CURLOPT_FAILONERROR, so the verifier's 401 short-circuit makes
    // execute() fail and the body is not delivered. The points under test are that the verifier
    // ran and the handler was bypassed (not the JSON body shaping, which FAILONERROR hides).
    EXPECT_FALSE(client.execute(&resp).ok());
    EXPECT_EQ(1, verifier_calls.load());
    EXPECT_EQ(0, handler->invocations.load());

    server->stop();
    server->join();
}

TEST(EvHttpServerAuthTest, handler_need_auth_false_bypasses_verifier) {
    auto handler = std::make_unique<AuthProbeHandler>(false);
    auto server = std::make_unique<EvHttpServer>(0, 1);
    server->register_handler(GET, "/probe", handler.get());

    std::atomic<int> verifier_calls{0};
    server->set_auth_verifier(
            [&](HttpRequest*, HttpHandler::RequiredPrivilege) -> std::optional<EvHttpServer::AuthVerifyFailure> {
                ++verifier_calls;
                // Verifier would reject if it ran, so seeing the handler run
                // is a strong signal that the verifier was bypassed.
                EvHttpServer::AuthVerifyFailure f;
                f.http_status = HttpStatus::UNAUTHORIZED;
                f.body = "should not be sent";
                return f;
            });

    ASSERT_OK(server->start());
    int port = server->get_real_port();
    std::string url = "http://127.0.0.1:" + std::to_string(port) + "/probe";

    HttpClient client;
    ASSERT_OK(client.init(url));
    client.set_method(GET);
    std::string resp;
    ASSERT_OK(client.execute(&resp));
    EXPECT_EQ("handler-ok", resp);
    EXPECT_EQ(0, verifier_calls.load());
    EXPECT_EQ(1, handler->invocations.load());

    server->stop();
    server->join();
}

TEST(EvHttpServerAuthTest, no_verifier_wired_runs_handler) {
    auto handler = std::make_unique<AuthProbeHandler>(true);
    auto server = std::make_unique<EvHttpServer>(0, 1);
    server->register_handler(GET, "/probe", handler.get());
    // intentionally no set_auth_verifier()

    ASSERT_OK(server->start());
    int port = server->get_real_port();
    std::string url = "http://127.0.0.1:" + std::to_string(port) + "/probe";

    HttpClient client;
    ASSERT_OK(client.init(url));
    client.set_method(GET);
    std::string resp;
    ASSERT_OK(client.execute(&resp));
    EXPECT_EQ("handler-ok", resp);
    EXPECT_EQ(1, handler->invocations.load());

    server->stop();
    server->join();
}

} // namespace starrocks
