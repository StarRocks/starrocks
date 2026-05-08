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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/http/ev_http_server.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "http/ev_http_server.h"

#include <event2/buffer.h>
#include <event2/bufferevent.h>
#include <event2/event.h>
#include <event2/http.h>
#include <event2/http_struct.h>
#include <event2/keyvalq_struct.h>

#ifdef __APPLE__
#include "starrocks_macos_libevent_shims.h"
#endif

#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstring>
#include <memory>
#include <sstream>
#include <utility>

#include "base/brpc/brpc.h"
#include "base/system/errno.h"
#include "common/config.h"
#include "common/config_ingest_fwd.h"
#include "common/logging.h"
#include "common/system/backend_options.h"
#include "common/thread/thread.h"
#include "common/util/debug_util.h"
#include "http/http_channel.h"
#include "http/http_handler.h"
#include "http/http_headers.h"
#include "http/http_request.h"

namespace starrocks {

static int on_connection(struct evhttp_request* req, void* param);

static void on_chunked(struct evhttp_request* ev_req, void* param) {
#ifdef __APPLE__
    // macOS: use evhttp_request_get_user_data API
    auto* request = (HttpRequest*)evhttp_request_get_user_data(ev_req);
#else
    // Linux: directly access on_free_cb_arg field
    auto* request = (HttpRequest*)ev_req->on_free_cb_arg;
#endif
    request->handler()->on_chunk_data(request);
}

static void on_free(struct evhttp_request* ev_req, void* arg) {
    auto* request = (HttpRequest*)arg;
    delete request;
}

static void on_request(struct evhttp_request* ev_req, void* arg) {
#ifdef __APPLE__
    auto* server = (EvHttpServer*)arg;
    auto* request = (HttpRequest*)evhttp_request_get_user_data(ev_req);
    if (request == nullptr) {
        // Older libevent lacks evhttp_set_newreqcb(). In that case, defer the
        // per-request initialization until the generic callback runs.
        if (on_connection(ev_req, server) < 0 || server->on_header(ev_req) < 0) {
            return;
        }
        request = (HttpRequest*)evhttp_request_get_user_data(ev_req);
        if (request == nullptr) {
            return;
        }
        if (request->handler()->request_will_be_read_progressively()) {
            request->handler()->on_chunk_data(request);
        }
    }
#else
    // Linux: directly access on_free_cb_arg field
    auto request = (HttpRequest*)ev_req->on_free_cb_arg;
#endif
    if (request == nullptr) {
        // In this case, request's on_header return -1
        return;
    }
    request->handler()->handle(request);
}

static int on_header(struct evhttp_request* ev_req, void* param) {
    auto* server = (EvHttpServer*)ev_req->on_complete_cb_arg;
    return server->on_header(ev_req);
}

// param is pointer of EvHttpServer
static int on_connection(struct evhttp_request* req, void* param) {
    evhttp_request_set_header_cb(req, on_header);
    // only used on_complete_cb's argument
    evhttp_request_set_on_complete_cb(req, nullptr, param);
    return 0;
}

// Create a TCP listening socket with SO_REUSEADDR + SO_REUSEPORT, bound to
// `point`. Returns -1 on any failure (and `errno` is set). When SO_REUSEPORT
// is set on every listener for the same {addr, port}, the kernel load-balances
// new connections across them — workers no longer thundering-herd on a single
// shared listen fd's accept queue lock.
static int listen_with_reuseport(const butil::EndPoint& point) {
#ifndef SO_REUSEPORT
    errno = ENOTSUP;
    return -1;
#else
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        return -1;
    }
    int yes = 1;
    if (::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) != 0) {
        int saved = errno;
        ::close(fd);
        errno = saved;
        return -1;
    }
    if (::setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &yes, sizeof(yes)) != 0) {
        int saved = errno;
        ::close(fd);
        errno = saved;
        return -1;
    }
    sockaddr_in addr;
    std::memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(point.port);
    addr.sin_addr = point.ip;
    if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        int saved = errno;
        ::close(fd);
        errno = saved;
        return -1;
    }
    if (::listen(fd, 65535) != 0) {
        int saved = errno;
        ::close(fd);
        errno = saved;
        return -1;
    }
    return fd;
#endif
}

EvHttpServer::EvHttpServer(int port, int num_workers) : _port(port), _num_workers(num_workers), _real_port(0) {
    _host = BackendOptions::get_service_bind_address();
    DCHECK_GT(_num_workers, 0);
    auto res = pthread_rwlock_init(&_rw_lock, nullptr);
    DCHECK_EQ(res, 0);
}

EvHttpServer::EvHttpServer(std::string host, int port, int num_workers)
        : _host(std::move(host)), _port(port), _num_workers(num_workers), _real_port(0) {
    DCHECK_GT(_num_workers, 0);
    auto res = pthread_rwlock_init(&_rw_lock, nullptr);
    DCHECK_EQ(res, 0);
}

EvHttpServer::~EvHttpServer() {
    pthread_rwlock_destroy(&_rw_lock);
}

Status EvHttpServer::start() {
    // bind to
    RETURN_IF_ERROR(_bind());

    // Resolve the bind endpoint once, on the main thread, for per-worker
    // SO_REUSEPORT listeners. _bind() has already populated _real_port (incl.
    // the port==0 auto-assign case).
    butil::EndPoint listen_point;
    bool reuseport_enabled = false;
#ifdef SO_REUSEPORT
    if (config::enable_http_server_so_reuseport) {
        if (butil::str2endpoint(_host.c_str(), _real_port, &listen_point) == 0) {
            reuseport_enabled = true;
        } else {
            LOG(WARNING) << "Failed to resolve listen endpoint " << _host << ":" << _real_port
                         << " for SO_REUSEPORT mode; falling back to shared-fd accept";
        }
    }
#endif

    for (int i = 0; i < _num_workers; ++i) {
        auto worker = [this, listen_point, reuseport_enabled, i]() {
            struct event_base* base = event_base_new();
            if (base == nullptr) {
                LOG(WARNING) << "Couldn't create an event_base.";
                return;
            }
            pthread_rwlock_wrlock(&_rw_lock);
            _event_bases.push_back(base);
            pthread_rwlock_unlock(&_rw_lock);

            /* Create a new evhttp object to handle requests. */
            struct evhttp* http = evhttp_new(base);
            if (http == nullptr) {
                LOG(WARNING) << "Couldn't create an evhttp.";
                return;
            }

            pthread_rwlock_wrlock(&_rw_lock);
            _https.push_back(http);
            pthread_rwlock_unlock(&_rw_lock);

#if defined(__APPLE__) && !defined(STARROCKS_HAVE_EVHTTP_SET_NEWREQCB)
            // Old libevent can only initialize requests in the generic callback,
            // so cap pre-read bodies before any handler-specific header checks.
            evhttp_set_max_body_size(http, static_cast<ev_ssize_t>(config::streaming_load_max_mb) * 1024 * 1024);
#endif

            // Worker 0 reuses _server_fd from _bind(); workers 1..N-1 create
            // their own SO_REUSEPORT listeners so the kernel load-balances
            // accepts in-kernel rather than waking all workers on every
            // connection (the cause of native_queued_spin_lock_slowpath
            // contention seen in perf when be_http_num_workers is large).
            int worker_fd = _server_fd;
            if (reuseport_enabled && i > 0) {
                int new_fd = listen_with_reuseport(listen_point);
                if (new_fd < 0) {
                    LOG(WARNING) << "Worker " << i << ": SO_REUSEPORT listen failed, "
                                 << "falling back to shared fd: " << errno_to_string(errno);
                } else if (butil::make_non_blocking(new_fd) < 0) {
                    LOG(WARNING) << "Worker " << i << ": failed to set non-blocking on reuseport fd, "
                                 << "falling back to shared fd: " << errno_to_string(errno);
                    ::close(new_fd);
                } else {
                    pthread_rwlock_wrlock(&_rw_lock);
                    _worker_fds.push_back(new_fd);
                    pthread_rwlock_unlock(&_rw_lock);
                    worker_fd = new_fd;
                }
            }

            auto res = evhttp_accept_socket(http, worker_fd);
            if (res < 0) {
                LOG(WARNING) << "evhttp accept socket failed"
                             << ", error:" << errno_to_string(errno);
                return;
            }

            evhttp_set_newreqcb(http, on_connection, this);
            evhttp_set_gencb(http, on_request, this);

            event_base_dispatch(base);
        };
        _workers.emplace_back(worker);
        Thread::set_thread_name(_workers.back(), "http_server");
    }
    return Status::OK();
}

void EvHttpServer::stop() {
    // break the base to stop the event
    for (auto base : _event_bases) {
        event_base_loopbreak(base);
    }

    // shutdown the socket to wake up the epoll_wait
    shutdown(_server_fd, SHUT_RDWR);
    // Per-worker SO_REUSEPORT listeners need their own shutdown, otherwise
    // their event_base_dispatch will not unblock from epoll_wait and join()
    // will hang.
    for (int fd : _worker_fds) {
        ::shutdown(fd, SHUT_RDWR);
    }
}

void EvHttpServer::join() {
    for (auto& thread : _workers) {
        if (thread.joinable()) {
            thread.join();
        }
    }

    // close the socket at last
    close(_server_fd);
    for (int fd : _worker_fds) {
        ::close(fd);
    }
    _worker_fds.clear();

    // free the evhttp and event_base
    for (auto http : _https) {
        evhttp_free(http);
    }

    for (auto base : _event_bases) {
        event_base_free(base);
    }
}

Status EvHttpServer::_bind() {
    butil::EndPoint point;
    auto res = butil::str2endpoint(_host.c_str(), _port, &point);
    if (res < 0) {
        std::stringstream ss;
        ss << "convert address failed, host=" << _host << ", port=" << _port;
        return Status::InternalError(ss.str());
    }
    // reuse_addr arg is removed in brpc 0.9.7 and use gflag instead.
    // default reuse_addr is true and reuse_port is false.
    _server_fd = butil::tcp_listen(point);
    if (_server_fd < 0) {
        std::stringstream ss;
        ss << "Failed to listen port. port: " << _port << ", error: " << errno_to_string(errno);
        return Status::InternalError(ss.str());
    }
    if (_port == 0) {
        struct sockaddr_in addr;
        socklen_t socklen = sizeof(addr);
        const int rc = getsockname(_server_fd, (struct sockaddr*)&addr, &socklen);
        if (rc == 0) {
            _real_port = ntohs(addr.sin_port);
        }
    } else {
        _real_port = _port;
    }
#ifdef SO_REUSEPORT
    // Mark _server_fd as SO_REUSEPORT before any worker tries to bind another
    // listener to the same {addr, real_port}. The kernel only permits multiple
    // listening sockets on the same address when *all* of them carry
    // SO_REUSEPORT — without setting it on the primary fd, every worker's
    // bind() would fail with EADDRINUSE.
    if (config::enable_http_server_so_reuseport) {
        int yes = 1;
        if (::setsockopt(_server_fd, SOL_SOCKET, SO_REUSEPORT, &yes, sizeof(yes)) != 0) {
            LOG(WARNING) << "Failed to set SO_REUSEPORT on primary listener; per-worker listen sockets "
                         << "will be skipped: " << errno_to_string(errno);
        }
    }
#endif
    res = butil::make_non_blocking(_server_fd);
    if (res < 0) {
        std::stringstream ss;
        ss << "Failed to generate a non-blocking socket. error: " << errno_to_string(errno);
        return Status::InternalError(ss.str());
    }
    return Status::OK();
}

bool EvHttpServer::register_handler(const HttpMethod& method, const std::string& path, HttpHandler* handler) {
    if (handler == nullptr) {
        LOG(WARNING) << "dummy handler for http method " << method << " with path " << path;
        return false;
    }

    bool result = true;
    pthread_rwlock_wrlock(&_rw_lock);
    PathTrie<HttpHandler*>* root = nullptr;
    switch (method) {
    case GET:
        root = &_get_handlers;
        break;
    case PUT:
        root = &_put_handlers;
        break;
    case POST:
        root = &_post_handlers;
        break;
    case DELETE:
        root = &_delete_handlers;
        break;
    case HEAD:
        root = &_head_handlers;
        break;
    case OPTIONS:
        root = &_options_handlers;
        break;
    default:
        LOG(WARNING) << "unknown HTTP method, method=" << method;
        result = false;
    }
    if (result) {
        result = root->insert(path, handler);
    }
    pthread_rwlock_unlock(&_rw_lock);

    return result;
}

void EvHttpServer::register_static_file_handler(HttpHandler* handler) {
    DCHECK(handler != nullptr);
    DCHECK(_static_file_handler == nullptr);
    pthread_rwlock_wrlock(&_rw_lock);
    _static_file_handler = handler;
    pthread_rwlock_unlock(&_rw_lock);
}

int EvHttpServer::on_header(struct evhttp_request* ev_req) {
    std::unique_ptr<HttpRequest> request(new HttpRequest(ev_req));
    auto res = request->init_from_evhttp();
    if (res < 0) {
        return -1;
    }
    auto handler = _find_handler(request.get());
    if (handler == nullptr) {
        evhttp_remove_header(evhttp_request_get_input_headers(ev_req), HttpHeaders::EXPECT);
        HttpChannel::send_reply(request.get(), HttpStatus::NOT_FOUND, "Not Found");
        return 0;
    }
    // set handler before call on_header, because handler_ctx will set in on_header
    request->set_handler(handler);
    res = handler->on_header(request.get());
    if (res < 0) {
        // reply has already sent by handler's on_header
        evhttp_remove_header(evhttp_request_get_input_headers(ev_req), HttpHeaders::EXPECT);
        return 0;
    }

    // If request body would be big(greater than 1GB),
    // it is better that request_will_be_read_progressively is set true,
    // this can make body read in chunk, not in total
    if (handler->request_will_be_read_progressively()) {
        evhttp_request_set_chunked_cb(ev_req, on_chunked);
    }

    // Free request when evhttp_request is done; also stash pointer
    HttpRequest* req_ptr = request.release();
#ifdef __APPLE__
    // macOS: use evhttp_request_set_user_data API for storing pointer
    evhttp_request_set_user_data(ev_req, req_ptr);
#endif
    // Both platforms: set on_free callback with req_ptr as argument
    // On Linux, this also sets on_free_cb_arg which is accessed directly
    evhttp_request_set_on_free_cb(ev_req, on_free, req_ptr);
    return 0;
}

HttpHandler* EvHttpServer::_find_handler(HttpRequest* req) {
    auto& path = req->raw_path();

    HttpHandler* handler = nullptr;

    pthread_rwlock_rdlock(&_rw_lock);
    switch (req->method()) {
    case GET:
        _get_handlers.retrieve(path, &handler, req->params());
        // Static file handler is a fallback handler
        if (handler == nullptr) {
            handler = _static_file_handler;
        }
        break;
    case PUT:
        _put_handlers.retrieve(path, &handler, req->params());
        break;
    case POST:
        _post_handlers.retrieve(path, &handler, req->params());
        break;
    case DELETE:
        _delete_handlers.retrieve(path, &handler, req->params());
        break;
    case HEAD:
        _head_handlers.retrieve(path, &handler, req->params());
        break;
    case OPTIONS:
        _options_handlers.retrieve(path, &handler, req->params());
        break;
    default:
        LOG(WARNING) << "unknown HTTP method, method=" << req->method();
        break;
    }
    pthread_rwlock_unlock(&_rw_lock);
    return handler;
}

} // namespace starrocks
