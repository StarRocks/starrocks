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

#include <arpa/inet.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <cctype>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <mutex>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

namespace starrocks::testutil {

struct ReceivedRequest {
    std::string method;
    std::string target;
    // Header names in lower case.
    std::map<std::string, std::string> headers;
    std::string body;

    std::string header(const std::string& name) const {
        auto it = headers.find(name);
        return it == headers.end() ? std::string() : it->second;
    }
};

namespace detail {

inline std::string to_lower(std::string_view value) {
    std::string result(value);
    std::transform(result.begin(), result.end(), result.begin(), [](unsigned char c) { return std::tolower(c); });
    return result;
}

inline bool send_all(int fd, std::string_view data) {
#ifdef MSG_NOSIGNAL
    constexpr int kFlags = MSG_NOSIGNAL;
#else
    constexpr int kFlags = 0;
#endif
    while (!data.empty()) {
        ssize_t written = ::send(fd, data.data(), data.size(), kFlags);
        if (written <= 0) {
            return false;
        }
        data.remove_prefix(static_cast<size_t>(written));
    }
    return true;
}

} // namespace detail

// A loopback HTTP/1.1 server that answers each request with 200 after a delay that the test chooses for it. It
// reads the whole request before the delay and answers "Expect: 100-continue" at once, like object storage that is
// slow to process a request it has already received. Connections stay open between requests.
class DelayingHttpServer {
public:
    // Delay in milliseconds before answering the index-th request the server receives, counted from 0. A negative
    // value closes the connection without answering.
    using DelayFn = std::function<int64_t(size_t index, const ReceivedRequest& request)>;

    explicit DelayingHttpServer(DelayFn delay_ms) : _delay_ms(std::move(delay_ms)) {
        _listener = ::socket(AF_INET, SOCK_STREAM, 0);
        if (_listener < 0) {
            ADD_FAILURE() << "socket() failed, errno=" << errno;
            return;
        }
        int enabled = 1;
        (void)::setsockopt(_listener, SOL_SOCKET, SO_REUSEADDR, &enabled, sizeof(enabled));
        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        address.sin_port = 0;
        if (::bind(_listener, reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0 ||
            ::listen(_listener, 64) != 0) {
            ADD_FAILURE() << "bind() or listen() failed, errno=" << errno;
            return;
        }
        socklen_t length = sizeof(address);
        if (::getsockname(_listener, reinterpret_cast<sockaddr*>(&address), &length) != 0) {
            ADD_FAILURE() << "getsockname() failed, errno=" << errno;
            return;
        }
        _port = ntohs(address.sin_port);
        _acceptor = std::thread([this] { accept_loop(); });
    }

    ~DelayingHttpServer() { stop(); }

    DelayingHttpServer(const DelayingHttpServer&) = delete;
    DelayingHttpServer& operator=(const DelayingHttpServer&) = delete;

    int port() const { return _port; }

    std::string url(std::string_view path) const {
        return "http://127.0.0.1:" + std::to_string(_port) + std::string(path);
    }

    std::vector<ReceivedRequest> requests() const {
        std::lock_guard lock(_mutex);
        return _requests;
    }

private:
    void accept_loop() {
        while (true) {
            int fd = ::accept(_listener, nullptr, nullptr);
            if (fd < 0) {
                if (errno == EINTR) {
                    continue;
                }
                return;
            }
            std::lock_guard lock(_mutex);
            if (_stopping) {
                ::close(fd);
                return;
            }
            _connections.push_back(fd);
            _workers.emplace_back([this, fd] { serve(fd); });
        }
    }

    void serve(int fd) {
        std::string buffer;
        while (std::optional<ReceivedRequest> request = read_request(fd, &buffer)) {
            size_t index = 0;
            {
                std::lock_guard lock(_mutex);
                index = _requests.size();
                _requests.push_back(*request);
            }
            const int64_t delay_ms = _delay_ms(index, *request);
            if (delay_ms < 0 || (delay_ms > 0 && !sleep_unless_stopped(delay_ms))) {
                break;
            }
            const std::string body = request->method == "GET" ? "hello" : "";
            const std::string response =
                    "HTTP/1.1 200 OK\r\nETag: \"etag\"\r\nContent-Length: " + std::to_string(body.size()) + "\r\n\r\n" +
                    body;
            // Fails when the client has given up on the request and closed the connection.
            if (!detail::send_all(fd, response)) {
                break;
            }
        }
        {
            std::lock_guard lock(_mutex);
            _connections.erase(std::remove(_connections.begin(), _connections.end(), fd), _connections.end());
        }
        ::close(fd);
    }

    // Reads one request from the connection. `buffer` keeps the bytes received after it.
    static std::optional<ReceivedRequest> read_request(int fd, std::string* buffer) {
        char chunk[4096];
        size_t header_end = std::string::npos;
        while ((header_end = buffer->find("\r\n\r\n")) == std::string::npos) {
            ssize_t bytes = ::recv(fd, chunk, sizeof(chunk), 0);
            if (bytes <= 0) {
                return std::nullopt;
            }
            buffer->append(chunk, static_cast<size_t>(bytes));
        }

        ReceivedRequest request;
        std::istringstream head(buffer->substr(0, header_end));
        buffer->erase(0, header_end + 4);
        std::string line;
        if (!std::getline(head, line)) {
            return std::nullopt;
        }
        std::istringstream request_line(line);
        if (!(request_line >> request.method >> request.target)) {
            return std::nullopt;
        }
        while (std::getline(head, line)) {
            if (!line.empty() && line.back() == '\r') {
                line.pop_back();
            }
            size_t colon = line.find(':');
            if (colon == std::string::npos) {
                continue;
            }
            std::string value = line.substr(colon + 1);
            value.erase(0, value.find_first_not_of(' '));
            request.headers[detail::to_lower(std::string_view(line).substr(0, colon))] = value;
        }

        if (detail::to_lower(request.header("expect")) == "100-continue" &&
            !detail::send_all(fd, "HTTP/1.1 100 Continue\r\n\r\n")) {
            return std::nullopt;
        }
        const std::string content_length = request.header("content-length");
        const size_t body_length = content_length.empty() ? 0 : std::stoul(content_length);
        while (buffer->size() < body_length) {
            ssize_t bytes = ::recv(fd, chunk, sizeof(chunk), 0);
            if (bytes <= 0) {
                return std::nullopt;
            }
            buffer->append(chunk, static_cast<size_t>(bytes));
        }
        request.body = buffer->substr(0, body_length);
        buffer->erase(0, body_length);
        return request;
    }

    // Returns false when the server stops before the delay is over.
    bool sleep_unless_stopped(int64_t delay_ms) {
        std::unique_lock lock(_mutex);
        return !_stopped.wait_for(lock, std::chrono::milliseconds(delay_ms), [this] { return _stopping; });
    }

    void stop() {
        {
            std::lock_guard lock(_mutex);
            if (_stopping) {
                return;
            }
            _stopping = true;
            for (int fd : _connections) {
                (void)::shutdown(fd, SHUT_RDWR);
            }
        }
        _stopped.notify_all();
        if (_listener >= 0) {
            (void)::shutdown(_listener, SHUT_RDWR);
        }
        if (_acceptor.joinable()) {
            _acceptor.join();
        }
        if (_listener >= 0) {
            ::close(_listener);
            _listener = -1;
        }
        std::vector<std::thread> workers;
        {
            std::lock_guard lock(_mutex);
            workers.swap(_workers);
        }
        for (auto& worker : workers) {
            worker.join();
        }
    }

    const DelayFn _delay_ms;
    int _listener = -1;
    int _port = 0;
    std::thread _acceptor;

    mutable std::mutex _mutex;
    std::condition_variable _stopped;
    bool _stopping = false;
    std::vector<ReceivedRequest> _requests;
    std::vector<int> _connections;
    std::vector<std::thread> _workers;
};

} // namespace starrocks::testutil
