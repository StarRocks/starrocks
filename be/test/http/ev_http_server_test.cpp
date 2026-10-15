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
#include <string>
#include <thread>
#include <vector>

#include "http/http_channel.h"
#include "http/http_handler.h"
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

} // namespace starrocks
