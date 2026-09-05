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

#include "platform/llm/ai_http_client.h"

#include <arpa/inet.h>
#include <curl/curl.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/rsa.h>
#include <openssl/ssl.h>
#include <openssl/x509.h>
#include <openssl/x509v3.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <barrier>
#include <cctype>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <functional>
#include <future>
#include <iterator>
#include <limits>
#include <memory>
#include <mutex>
#include <numeric>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include "base/concurrency/countdown_latch.h"
#include "base/testutil/sync_point.h"
#include "base/time/time.h"
#include "base/utility/defer_op.h"
#include "platform/llm/ai_lifecycle.h"

namespace starrocks {
namespace {

using namespace std::chrono_literals;

struct FakeMemoryContextState;
thread_local FakeMemoryContextState* tls_physical_scope = nullptr;

struct FakeMemoryContextState {
    static void retain(void* opaque) noexcept {
        static_cast<FakeMemoryContextState*>(opaque)->references.fetch_add(1, std::memory_order_relaxed);
    }

    static void release_owner(void* opaque) noexcept {
        static_cast<FakeMemoryContextState*>(opaque)->references.fetch_sub(1, std::memory_order_relaxed);
    }

    static bool reserve_bytes(void* opaque, size_t bytes) noexcept {
        auto* state = static_cast<FakeMemoryContextState*>(opaque);
        try {
            return !state->reserve || state->reserve(bytes);
        } catch (...) {
            return false;
        }
    }

    static void release_bytes(void* opaque, size_t bytes) noexcept {
        auto* state = static_cast<FakeMemoryContextState*>(opaque);
        try {
            if (state->release) {
                state->release(bytes);
            }
        } catch (...) {
        }
    }

    static void run(void* opaque, AIMemoryContext::Action action, void* action_context) {
        auto* state = static_cast<FakeMemoryContextState*>(opaque);
        struct RestoreScope {
            explicit RestoreScope(FakeMemoryContextState* next) : previous(tls_physical_scope) {
                tls_physical_scope = next;
            }
            ~RestoreScope() { tls_physical_scope = previous; }
            FakeMemoryContextState* previous;
        } restore(state);
        state->entries.fetch_add(1, std::memory_order_relaxed);
        action(action_context);
        state->exits.fetch_add(1, std::memory_order_relaxed);
    }

    AIMemoryContext context() {
        return AIMemoryContext::create(this, &FakeMemoryContextState::reserve_bytes,
                                       &FakeMemoryContextState::release_bytes, &FakeMemoryContextState::run,
                                       &FakeMemoryContextState::retain, &FakeMemoryContextState::release_owner);
    }

    std::function<bool(size_t)> reserve;
    std::function<void(size_t)> release;
    std::atomic<int> references{0};
    std::atomic<int> entries{0};
    std::atomic<int> exits{0};
};

class ResponseChunkHandshake {
public:
    bool wait_for_client_to_consume_first_chunk() {
        std::unique_lock lock(_mutex);
        if (!_cv.wait_for(lock, 5s, [this] { return _client_consumed_first_chunk; })) {
            _timed_out = true;
            _cv.notify_all();
            return false;
        }
        _server_acknowledged_first_chunk = true;
        _cv.notify_all();
        return true;
    }

    void client_consumed_first_chunk() {
        std::unique_lock lock(_mutex);
        _client_consumed_first_chunk = true;
        _cv.notify_all();
        if (!_cv.wait_for(lock, 5s, [this] { return _server_acknowledged_first_chunk || _timed_out; })) {
            _timed_out = true;
            _cv.notify_all();
        }
    }

    bool succeeded() {
        std::lock_guard lock(_mutex);
        return _client_consumed_first_chunk && _server_acknowledged_first_chunk && !_timed_out;
    }

private:
    std::mutex _mutex;
    std::condition_variable _cv;
    bool _client_consumed_first_chunk = false;
    bool _server_acknowledged_first_chunk = false;
    bool _timed_out = false;
};

std::string lowercase(std::string_view value) {
    std::string result(value);
    std::transform(result.begin(), result.end(), result.begin(), [](unsigned char c) { return std::tolower(c); });
    return result;
}

struct CapturedHttpRequest {
    std::string method;
    std::string target;
    std::unordered_map<std::string, std::string> headers;
    std::string body;
};

bool send_all(int socket, std::string_view data) {
    while (!data.empty()) {
#ifdef MSG_NOSIGNAL
        constexpr int flags = MSG_NOSIGNAL;
#else
        constexpr int flags = 0;
#endif
        ssize_t written = ::send(socket, data.data(), data.size(), flags);
        if (written <= 0) {
            return false;
        }
        data.remove_prefix(static_cast<size_t>(written));
    }
    return true;
}

template <typename Reader>
std::optional<CapturedHttpRequest> read_http_request_with(Reader reader) {
    std::string wire;
    char buffer[4096];
    size_t header_end = std::string::npos;
    while ((header_end = wire.find("\r\n\r\n")) == std::string::npos && wire.size() < 128 * 1024) {
        ssize_t bytes = reader(buffer, sizeof(buffer));
        if (bytes <= 0) {
            return std::nullopt;
        }
        wire.append(buffer, static_cast<size_t>(bytes));
    }
    if (header_end == std::string::npos) {
        return std::nullopt;
    }

    CapturedHttpRequest request;
    std::istringstream headers(wire.substr(0, header_end));
    std::string line;
    if (!std::getline(headers, line)) {
        return std::nullopt;
    }
    if (!line.empty() && line.back() == '\r') {
        line.pop_back();
    }
    std::istringstream request_line(line);
    std::string version;
    if (!(request_line >> request.method >> request.target >> version)) {
        return std::nullopt;
    }

    size_t content_length = 0;
    while (std::getline(headers, line)) {
        if (!line.empty() && line.back() == '\r') {
            line.pop_back();
        }
        size_t colon = line.find(':');
        if (colon == std::string::npos) {
            continue;
        }
        std::string name = lowercase(std::string_view(line).substr(0, colon));
        std::string value = line.substr(colon + 1);
        while (!value.empty() && value.front() == ' ') {
            value.erase(value.begin());
        }
        request.headers.emplace(std::move(name), std::move(value));
    }
    auto content_length_it = request.headers.find("content-length");
    if (content_length_it != request.headers.end()) {
        content_length = static_cast<size_t>(std::stoull(content_length_it->second));
    }
    request.body = wire.substr(header_end + 4);
    while (request.body.size() < content_length) {
        ssize_t bytes = reader(buffer, sizeof(buffer));
        if (bytes <= 0) {
            return std::nullopt;
        }
        request.body.append(buffer, static_cast<size_t>(bytes));
    }
    request.body.resize(content_length);
    return request;
}

std::optional<CapturedHttpRequest> read_http_request(int socket) {
    timeval timeout{5, 0};
    (void)setsockopt(socket, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    return read_http_request_with([&](char* data, size_t size) { return ::recv(socket, data, size, 0); });
}

void send_http_response(int socket, int status, const std::vector<AIHttpHeader>& headers, std::string_view body) {
    std::string wire = "HTTP/1.1 " + std::to_string(status) +
                       " Test\r\nContent-Length: " + std::to_string(body.size()) + "\r\nConnection: close\r\n";
    for (const auto& header : headers) {
        wire.append(header.name).append(": ").append(header.value).append("\r\n");
    }
    wire.append("\r\n").append(body);
    (void)send_all(socket, wire);
}

void set_socket_timeouts(int socket) {
    timeval timeout{2, 0};
    (void)setsockopt(socket, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    (void)setsockopt(socket, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
}

class LoopbackHttpServer {
public:
    using Handler = std::function<void(int, const CapturedHttpRequest&)>;

    explicit LoopbackHttpServer(Handler handler) : _handler(std::move(handler)) {
        _listener = ::socket(AF_INET, SOCK_STREAM, 0);
        EXPECT_GE(_listener, 0);
        if (_listener < 0) {
            return;
        }
        int enabled = 1;
        (void)setsockopt(_listener, SOL_SOCKET, SO_REUSEADDR, &enabled, sizeof(enabled));
#ifdef SO_NOSIGPIPE
        (void)setsockopt(_listener, SOL_SOCKET, SO_NOSIGPIPE, &enabled, sizeof(enabled));
#endif
        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        address.sin_port = 0;
        EXPECT_EQ(0, ::bind(_listener, reinterpret_cast<sockaddr*>(&address), sizeof(address)));
        EXPECT_EQ(0, ::listen(_listener, 32));
        socklen_t length = sizeof(address);
        EXPECT_EQ(0, ::getsockname(_listener, reinterpret_cast<sockaddr*>(&address), &length));
        _port = ntohs(address.sin_port);
        _thread = std::thread([this] { accept_loop(); });
    }

    ~LoopbackHttpServer() {
        _stopping.store(true);
        if (_listener >= 0) {
            (void)::shutdown(_listener, SHUT_RDWR);
        }
        if (_thread.joinable()) {
            _thread.join();
        }
        if (_listener >= 0) {
            (void)::close(_listener);
            _listener = -1;
        }
        for (auto& worker : _workers) {
            if (worker.joinable()) {
                worker.join();
            }
        }
    }

    std::string url(std::string_view target) const {
        return "http://127.0.0.1:" + std::to_string(_port) + std::string(target);
    }

    uint16_t port() const { return _port; }
    int connections() const { return _connections.load(); }

private:
    void accept_loop() {
        while (!_stopping.load()) {
            pollfd descriptor{_listener, POLLIN, 0};
            int ready = ::poll(&descriptor, 1, 20);
            if (ready <= 0 || (descriptor.revents & POLLIN) == 0) {
                continue;
            }
            int connection = ::accept(_listener, nullptr, nullptr);
            if (connection < 0) {
                continue;
            }
            set_socket_timeouts(connection);
#ifdef SO_NOSIGPIPE
            int enabled = 1;
            (void)setsockopt(connection, SOL_SOCKET, SO_NOSIGPIPE, &enabled, sizeof(enabled));
#endif
            ++_connections;
            _workers.emplace_back([this, connection] {
                auto request = read_http_request(connection);
                if (request.has_value()) {
                    _handler(connection, *request);
                }
                (void)::shutdown(connection, SHUT_RDWR);
                (void)::close(connection);
            });
        }
    }

    Handler _handler;
    int _listener = -1;
    uint16_t _port = 0;
    std::atomic<bool> _stopping{false};
    std::atomic<int> _connections{0};
    std::thread _thread;
    std::vector<std::thread> _workers;
};

EVP_PKEY* generate_rsa_key() {
    EVP_PKEY_CTX* context = EVP_PKEY_CTX_new_id(EVP_PKEY_RSA, nullptr);
    if (context == nullptr) {
        return nullptr;
    }
    EVP_PKEY* key = nullptr;
    bool ok = EVP_PKEY_keygen_init(context) > 0 && EVP_PKEY_CTX_set_rsa_keygen_bits(context, 2048) > 0 &&
              EVP_PKEY_keygen(context, &key) > 0;
    EVP_PKEY_CTX_free(context);
    if (!ok) {
        EVP_PKEY_free(key);
        return nullptr;
    }
    return key;
}

bool add_certificate_extension(X509* certificate, X509* issuer, int nid, const char* value) {
    X509V3_CTX context;
    X509V3_set_ctx(&context, issuer, certificate, nullptr, nullptr, 0);
    X509_EXTENSION* extension = X509V3_EXT_conf_nid(nullptr, &context, nid, const_cast<char*>(value));
    if (extension == nullptr) {
        return false;
    }
    bool ok = X509_add_ext(certificate, extension, -1) == 1;
    X509_EXTENSION_free(extension);
    return ok;
}

X509* generate_ca_certificate(EVP_PKEY* key) {
    X509* certificate = X509_new();
    if (certificate == nullptr) {
        return nullptr;
    }
    X509_NAME* subject = X509_get_subject_name(certificate);
    const unsigned char common_name[] = "StarRocks AI HTTP Test CA";
    bool ok = X509_set_version(certificate, 2) == 1 && ASN1_INTEGER_set(X509_get_serialNumber(certificate), 1) == 1 &&
              X509_gmtime_adj(X509_getm_notBefore(certificate), -3600) != nullptr &&
              X509_gmtime_adj(X509_getm_notAfter(certificate), 86400) != nullptr &&
              X509_set_pubkey(certificate, key) == 1 &&
              X509_NAME_add_entry_by_txt(subject, "CN", MBSTRING_ASC, common_name, -1, -1, 0) == 1 &&
              X509_set_issuer_name(certificate, subject) == 1 &&
              add_certificate_extension(certificate, certificate, NID_basic_constraints, "critical,CA:TRUE") &&
              add_certificate_extension(certificate, certificate, NID_key_usage, "critical,keyCertSign,cRLSign") &&
              X509_sign(certificate, key, EVP_sha256()) > 0;
    if (!ok) {
        X509_free(certificate);
        return nullptr;
    }
    return certificate;
}

X509* generate_server_certificate(EVP_PKEY* key, X509* ca_certificate, EVP_PKEY* ca_key) {
    X509* certificate = X509_new();
    if (certificate == nullptr) {
        return nullptr;
    }
    X509_NAME* subject = X509_get_subject_name(certificate);
    const unsigned char common_name[] = "localhost";
    bool ok = X509_set_version(certificate, 2) == 1 && ASN1_INTEGER_set(X509_get_serialNumber(certificate), 2) == 1 &&
              X509_gmtime_adj(X509_getm_notBefore(certificate), -3600) != nullptr &&
              X509_gmtime_adj(X509_getm_notAfter(certificate), 86400) != nullptr &&
              X509_set_pubkey(certificate, key) == 1 &&
              X509_NAME_add_entry_by_txt(subject, "CN", MBSTRING_ASC, common_name, -1, -1, 0) == 1 &&
              X509_set_issuer_name(certificate, X509_get_subject_name(ca_certificate)) == 1 &&
              add_certificate_extension(certificate, ca_certificate, NID_basic_constraints, "critical,CA:FALSE") &&
              add_certificate_extension(certificate, ca_certificate, NID_key_usage,
                                        "critical,digitalSignature,keyEncipherment") &&
              add_certificate_extension(certificate, ca_certificate, NID_ext_key_usage, "serverAuth") &&
              add_certificate_extension(certificate, ca_certificate, NID_subject_alt_name, "DNS:localhost") &&
              X509_sign(certificate, ca_key, EVP_sha256()) > 0;
    if (!ok) {
        X509_free(certificate);
        return nullptr;
    }
    return certificate;
}

class TestTlsMaterial {
public:
    TestTlsMaterial() {
        _ca_key = generate_rsa_key();
        if (_ca_key != nullptr) {
            _ca_certificate = generate_ca_certificate(_ca_key);
        }
        _server_key = generate_rsa_key();
        if (_server_key != nullptr && _ca_certificate != nullptr) {
            _server_certificate = generate_server_certificate(_server_key, _ca_certificate, _ca_key);
        }
    }

    ~TestTlsMaterial() {
        X509_free(_server_certificate);
        EVP_PKEY_free(_server_key);
        X509_free(_ca_certificate);
        EVP_PKEY_free(_ca_key);
    }

    TestTlsMaterial(const TestTlsMaterial&) = delete;
    TestTlsMaterial& operator=(const TestTlsMaterial&) = delete;

    bool valid() const {
        return _ca_key != nullptr && _ca_certificate != nullptr && _server_key != nullptr &&
               _server_certificate != nullptr;
    }
    X509* ca_certificate() const { return _ca_certificate; }
    X509* server_certificate() const { return _server_certificate; }
    EVP_PKEY* server_key() const { return _server_key; }

private:
    EVP_PKEY* _ca_key = nullptr;
    X509* _ca_certificate = nullptr;
    EVP_PKEY* _server_key = nullptr;
    X509* _server_certificate = nullptr;
};

class TemporaryCaBundle {
public:
    explicit TemporaryCaBundle(X509* certificate) {
        char path[] = "/tmp/starrocks_ai_http_ca.XXXXXX";
        int descriptor = ::mkstemp(path);
        if (descriptor < 0) {
            return;
        }
        FILE* file = ::fdopen(descriptor, "w");
        if (file == nullptr) {
            (void)::close(descriptor);
            (void)::unlink(path);
            return;
        }
        bool written = PEM_write_X509(file, certificate) == 1;
        bool closed = ::fclose(file) == 0;
        if (written && closed) {
            _path = path;
        } else {
            (void)::unlink(path);
        }
    }

    ~TemporaryCaBundle() {
        if (!_path.empty()) {
            (void)::unlink(_path.c_str());
        }
    }

    TemporaryCaBundle(const TemporaryCaBundle&) = delete;
    TemporaryCaBundle& operator=(const TemporaryCaBundle&) = delete;

    const std::string& path() const { return _path; }

private:
    std::string _path;
};

bool ssl_send_all(SSL* ssl, std::string_view data) {
    while (!data.empty()) {
        int chunk = static_cast<int>(std::min<size_t>(data.size(), std::numeric_limits<int>::max()));
        int written = SSL_write(ssl, data.data(), chunk);
        if (written <= 0) {
            return false;
        }
        data.remove_prefix(static_cast<size_t>(written));
    }
    return true;
}

void send_tls_http_response(SSL* ssl, int status, std::string_view body) {
    std::string wire = "HTTP/1.1 " + std::to_string(status) + " TLS\r\nContent-Length: " + std::to_string(body.size()) +
                       "\r\nConnection: close\r\n\r\n" + std::string(body);
    (void)ssl_send_all(ssl, wire);
}

class LoopbackTlsServer {
public:
    using Handler = std::function<void(SSL*, const CapturedHttpRequest&)>;

    LoopbackTlsServer(const TestTlsMaterial& material, Handler handler) : _handler(std::move(handler)) {
        _context = SSL_CTX_new(TLS_server_method());
        EXPECT_NE(nullptr, _context);
        if (_context == nullptr) {
            return;
        }
        EXPECT_EQ(1, SSL_CTX_use_certificate(_context, material.server_certificate()));
        EXPECT_EQ(1, SSL_CTX_use_PrivateKey(_context, material.server_key()));
        EXPECT_EQ(1, SSL_CTX_check_private_key(_context));
        _listener = ::socket(AF_INET, SOCK_STREAM, 0);
        EXPECT_GE(_listener, 0);
        if (_listener < 0) {
            return;
        }
        int enabled = 1;
        (void)setsockopt(_listener, SOL_SOCKET, SO_REUSEADDR, &enabled, sizeof(enabled));
#ifdef SO_NOSIGPIPE
        (void)setsockopt(_listener, SOL_SOCKET, SO_NOSIGPIPE, &enabled, sizeof(enabled));
#endif
        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        address.sin_port = 0;
        EXPECT_EQ(0, ::bind(_listener, reinterpret_cast<sockaddr*>(&address), sizeof(address)));
        EXPECT_EQ(0, ::listen(_listener, 16));
        socklen_t length = sizeof(address);
        EXPECT_EQ(0, ::getsockname(_listener, reinterpret_cast<sockaddr*>(&address), &length));
        _port = ntohs(address.sin_port);
        _thread = std::thread([this] { accept_loop(); });
    }

    ~LoopbackTlsServer() {
        _stopping.store(true);
        if (_listener >= 0) {
            (void)::shutdown(_listener, SHUT_RDWR);
        }
        if (_thread.joinable()) {
            _thread.join();
        }
        if (_listener >= 0) {
            (void)::close(_listener);
            _listener = -1;
        }
        for (auto& worker : _workers) {
            if (worker.joinable()) {
                worker.join();
            }
        }
        SSL_CTX_free(_context);
    }

    std::string url(std::string_view host, std::string_view target) const {
        return "https://" + std::string(host) + ":" + std::to_string(_port) + std::string(target);
    }

private:
    void accept_loop() {
        while (!_stopping.load()) {
            pollfd descriptor{_listener, POLLIN, 0};
            int ready = ::poll(&descriptor, 1, 20);
            if (ready <= 0 || (descriptor.revents & POLLIN) == 0) {
                continue;
            }
            int connection = ::accept(_listener, nullptr, nullptr);
            if (connection < 0) {
                continue;
            }
            set_socket_timeouts(connection);
#ifdef SO_NOSIGPIPE
            int enabled = 1;
            (void)setsockopt(connection, SOL_SOCKET, SO_NOSIGPIPE, &enabled, sizeof(enabled));
#endif
            _workers.emplace_back([this, connection] {
                SSL* ssl = SSL_new(_context);
                if (ssl != nullptr) {
                    (void)SSL_set_fd(ssl, connection);
                    if (SSL_accept(ssl) == 1) {
                        auto request = read_http_request_with(
                                [&](char* data, size_t size) { return SSL_read(ssl, data, static_cast<int>(size)); });
                        if (request.has_value()) {
                            _handler(ssl, *request);
                        }
                    }
                    SSL_free(ssl);
                }
                (void)::shutdown(connection, SHUT_RDWR);
                (void)::close(connection);
            });
        }
    }

    Handler _handler;
    SSL_CTX* _context = nullptr;
    int _listener = -1;
    uint16_t _port = 0;
    std::atomic<bool> _stopping{false};
    std::thread _thread;
    std::vector<std::thread> _workers;
};

TestTlsMaterial& test_tls_material() {
    static TestTlsMaterial material;
    return material;
}

TemporaryCaBundle& test_ca_bundle() {
    static TemporaryCaBundle bundle(test_tls_material().ca_certificate());
    return bundle;
}

class ScopedEnvironment {
public:
    ScopedEnvironment(std::string name, std::optional<std::string> value) : _name(std::move(name)) {
        const char* previous = std::getenv(_name.c_str());
        if (previous != nullptr) {
            _previous = previous;
        }
        if (value.has_value()) {
            (void)setenv(_name.c_str(), value->c_str(), 1);
        } else {
            (void)unsetenv(_name.c_str());
        }
    }

    ~ScopedEnvironment() {
        if (_previous.has_value()) {
            (void)setenv(_name.c_str(), _previous->c_str(), 1);
        } else {
            (void)unsetenv(_name.c_str());
        }
    }

private:
    std::string _name;
    std::optional<std::string> _previous;
};

[[noreturn]] void run_reentrant_shutdown_scenario(bool reenter_from_release_hook) {
    (void)::alarm(5);
    CountDownLatch server_arrived(1);
    CountDownLatch release_server(1);
    LoopbackHttpServer server([&](int, const CapturedHttpRequest&) {
        server_arrived.count_down();
        release_server.wait();
    });
    DeferOp release_server_on_unwind([&] { release_server.count_down(); });
    auto client_result = AIHttpClient::create();
    if (!client_result.ok()) {
        _exit(10);
    }
    std::unique_ptr<AIHttpClient> client = std::move(client_result).value();

    AIHttpRequest request;
    request.url = server.url("/reentrant-shutdown");
    request.headers = {{"Authorization", "Bearer test-token"}, {"Content-Type", "application/json"}};
    request.body = R"({"prompt":"hello"})";
    request.request_deadline_ns = MonotonicNanos() + 5'000'000'000L;
    request.connect_timeout_ms = 100;
    request.max_response_bytes = 1024;
    const int64_t query_deadline_ns = request.request_deadline_ns;
    request.lifecycle = [query_deadline_ns] {
        return AIQueryLifecycleSnapshot{.monotonic_deadline_ns = query_deadline_ns};
    };
    std::atomic<int> reentrant_calls{0};
    FakeMemoryContextState memory;
    if (reenter_from_release_hook) {
        memory.release = [&](size_t) {
            ++reentrant_calls;
            client->shutdown();
        };
        request.memory = memory.context();
    }
    Status submit_status = client->submit(std::move(request), [&](AIHttpResult) {
        if (!reenter_from_release_hook) {
            ++reentrant_calls;
            client->shutdown();
        }
    });
    if (!submit_status.ok()) {
        _exit(11);
    }
    if (!server_arrived.wait_for(2s)) {
        _exit(12);
    }

    std::thread external_shutdown([&] { client->shutdown(); });
    external_shutdown.join();
    release_server.count_down();
    client.reset();
    (void)::alarm(0);
    _exit(reentrant_calls.load() == 1 ? 0 : 13);
}

class AIHttpClientTest : public testing::Test {
protected:
    static void SetUpTestSuite() { ASSERT_EQ(CURLE_OK, curl_global_init(CURL_GLOBAL_ALL)); }

    static AIHttpRequest valid_request() {
        AIHttpRequest request;
        request.url = "http://127.0.0.1:1/v1/chat/completions?tenant=test";
        request.headers = {{"Authorization", "Bearer test-token"}, {"Content-Type", "application/json"}};
        request.body = R"({"prompt":"hello"})";
        request.request_deadline_ns = MonotonicNanos() + 5'000'000'000L;
        request.connect_timeout_ms = 100;
        request.max_response_bytes = 1024;
        const int64_t query_deadline_ns = request.request_deadline_ns;
        request.lifecycle = [query_deadline_ns] {
            return AIQueryLifecycleSnapshot{.monotonic_deadline_ns = query_deadline_ns};
        };
        return request;
    }

    static std::unique_ptr<AIHttpClient> create_client() {
        auto result = AIHttpClient::create();
        EXPECT_TRUE(result.ok()) << result.status();
        return result.ok() ? std::move(result).value() : nullptr;
    }

    static AIHttpResult submit_and_wait(AIHttpClient* client, AIHttpRequest request) {
        auto promise = std::make_shared<std::promise<AIHttpResult>>();
        std::future<AIHttpResult> future = promise->get_future();
        Status status = client->submit(
                std::move(request), [promise](AIHttpResult result) mutable { promise->set_value(std::move(result)); });
        EXPECT_TRUE(status.ok()) << status;
        if (!status.ok() || future.wait_for(5s) != std::future_status::ready) {
            ADD_FAILURE() << "AI HTTP callback did not complete";
            return AIHttpNoResponse{AIHttpNoResponseCode::UNKNOWN};
        }
        return future.get();
    }
};

static_assert(!std::is_copy_constructible_v<AIHttpRequest>);
static_assert(std::is_nothrow_move_constructible_v<AIHttpRequest>);
static_assert(!std::is_copy_constructible_v<AIHttpResponseBody>);
static_assert(std::is_nothrow_move_constructible_v<AIHttpResponseBody>);
static_assert(!std::is_copy_constructible_v<AIHttpResult>);
static_assert(std::is_nothrow_move_constructible_v<AIHttpResult>);

TEST_F(AIHttpClientTest, MemoryContextRestoresAmbientStateWhenActionThrows) {
    FakeMemoryContextState ambient;
    FakeMemoryContextState physical;
    tls_physical_scope = &ambient;
    auto memory = physical.context();

    EXPECT_THROW(memory.run_in_physical_scope(
                         [](void* context) {
                             EXPECT_EQ(context, tls_physical_scope);
                             throw std::bad_alloc();
                         },
                         &physical),
                 std::bad_alloc);

    EXPECT_EQ(&ambient, tls_physical_scope);
    EXPECT_EQ(1, physical.entries.load());
    EXPECT_EQ(0, physical.exits.load());
    tls_physical_scope = nullptr;
}

TEST_F(AIHttpClientTest, RejectsInvalidUrlsSynchronouslyWithoutCallback) {
    auto client = create_client();
    ASSERT_NE(nullptr, client);

    std::vector<std::string> invalid_urls = {
            "",
            "ftp://localhost/model",
            "http:///model",
            "http://localhost/model#secret",
            "http://u:p@localhost/model",
            "http://localhost/\nkey",
    };
    invalid_urls.emplace_back(std::string("https://localhost/model") + '\0' + "tail");
    for (const auto& url : invalid_urls) {
        SCOPED_TRACE(testing::Message() << "url_size=" << url.size() << " url=" << url);
        auto request = valid_request();
        request.url = url;
        auto callback_count = std::make_shared<std::atomic<int>>(0);
        Status status = client->submit(std::move(request), [callback_count](AIHttpResult) { ++*callback_count; });
        EXPECT_TRUE(status.is_invalid_argument()) << status;
        EXPECT_EQ(0, callback_count->load());
        EXPECT_EQ(std::string::npos, status.to_string().find("secret"));
    }
}

TEST_F(AIHttpClientTest, RejectsInvalidHeadersSynchronouslyWithoutLeakingValues) {
    auto client = create_client();
    ASSERT_NE(nullptr, client);

    const std::vector<std::vector<AIHttpHeader>> invalid_headers = {
            {{"Bad Header", "value"}},
            {{"X-Test", "line\rbreak"}},
            {{"X-Test", "line\nbreak"}},
            {{"X-Test", std::string("nul\0value", 9)}},
            {{"Authorization", ""}},
            {{"authorization", "   "}},
            {{"Content-Length", "7"}},
            {{"Transfer-Encoding", "chunked"}},
            {{"Expect", "100-continue"}},
            {{"Host", "secret.invalid"}},
            {{"Proxy-Authorization", "Bearer proxy-secret"}},
            {{"X-Duplicate", "first"}, {"x-duplicate", "second-secret"}},
    };
    for (const auto& headers : invalid_headers) {
        auto request = valid_request();
        request.headers = headers;
        std::atomic<int> callback_count{0};
        Status status = client->submit(std::move(request), [&](AIHttpResult) { ++callback_count; });
        EXPECT_TRUE(status.is_invalid_argument()) << status;
        EXPECT_EQ(0, callback_count.load());
        EXPECT_EQ(std::string::npos, status.to_string().find("secret"));
        EXPECT_EQ(std::string::npos, status.to_string().find("Bearer"));
    }
}

TEST_F(AIHttpClientTest, RejectsInvalidLimitsSynchronously) {
    auto client = create_client();
    ASSERT_NE(nullptr, client);

    std::vector<AIHttpRequest> invalid_requests;
    auto request = valid_request();
    request.request_deadline_ns = -1;
    invalid_requests.emplace_back(std::move(request));
    request = valid_request();
    request.connect_timeout_ms = -1;
    invalid_requests.emplace_back(std::move(request));
    request = valid_request();
    request.max_response_bytes = 0;
    invalid_requests.emplace_back(std::move(request));
    request = valid_request();
    request.lifecycle = {};
    invalid_requests.emplace_back(std::move(request));

    for (auto& invalid_request : invalid_requests) {
        std::atomic<int> callback_count{0};
        Status status = client->submit(std::move(invalid_request), [&](AIHttpResult) { ++callback_count; });
        EXPECT_TRUE(status.is_invalid_argument()) << status;
        EXPECT_EQ(0, callback_count.load());
    }

    auto valid = valid_request();
    EXPECT_TRUE(client->submit(std::move(valid), {}).is_invalid_argument());
}

TEST_F(AIHttpClientTest, SubmitRejectionsDestroyCallbackInPhysicalScopeWithoutFiring) {
    auto client = create_client();
    ASSERT_NE(nullptr, client);

    struct CallbackOwner {
        CallbackOwner(FakeMemoryContextState* expected_scope, std::atomic<bool>* destroyed_in_scope)
                : expected_scope(expected_scope), destroyed_in_scope(destroyed_in_scope) {}
        ~CallbackOwner() { destroyed_in_scope->store(tls_physical_scope == expected_scope, std::memory_order_relaxed); }
        FakeMemoryContextState* expected_scope;
        std::atomic<bool>* destroyed_in_scope;
    };

    auto assert_rejection = [&](AIHttpRequest request, FakeMemoryContextState* memory, const Status& expected_status) {
        std::atomic<bool> callback_destroyed_in_scope{false};
        std::atomic<int> callback_count{0};
        request.memory = memory->context();
        auto callback_owner = std::make_shared<CallbackOwner>(memory, &callback_destroyed_in_scope);
        std::weak_ptr<CallbackOwner> weak_callback_owner = callback_owner;
        AIHttpCallback callback = [callback_owner = std::move(callback_owner), &callback_count](AIHttpResult) {
            ++callback_count;
        };

        Status status = client->submit(std::move(request), std::move(callback));
        EXPECT_EQ(expected_status.code(), status.code()) << status;
        EXPECT_EQ(0, callback_count.load());
        EXPECT_TRUE(weak_callback_owner.expired());
        EXPECT_TRUE(callback_destroyed_in_scope.load(std::memory_order_relaxed));
    };

    {
        auto request = valid_request();
        request.url.clear();
        FakeMemoryContextState memory;
        std::atomic<int> validation_result_in_scope{0};
        std::atomic<int> validation_status_outside_scope{0};
        auto* sync_point = SyncPoint::GetInstance();
        sync_point->EnableProcessing();
        sync_point->SetCallBack("AIHttpClientImpl::submit:validation_result:in_physical_scope", [&](void*) {
            EXPECT_EQ(&memory, tls_physical_scope);
            ++validation_result_in_scope;
        });
        sync_point->SetCallBack("AIHttpClientImpl::submit:validation_status:outside_physical_scope", [&](void*) {
            EXPECT_NE(&memory, tls_physical_scope);
            ++validation_status_outside_scope;
        });
        DeferOp cleanup([&] {
            sync_point->ClearCallBack("AIHttpClientImpl::submit:validation_result:in_physical_scope");
            sync_point->ClearCallBack("AIHttpClientImpl::submit:validation_status:outside_physical_scope");
            sync_point->DisableProcessing();
        });

        assert_rejection(std::move(request), &memory, Status::InvalidArgument(""));
        EXPECT_EQ(1, validation_result_in_scope.load());
        EXPECT_EQ(1, validation_status_outside_scope.load());
    }

    {
        auto request = valid_request();
        const size_t body_size = request.body.size();
        std::atomic<size_t> reserve_calls{0};
        std::atomic<size_t> release_calls{0};
        FakeMemoryContextState memory;
        memory.reserve = [&](size_t bytes) {
            EXPECT_GT(bytes, body_size);
            ++reserve_calls;
            return false;
        };
        memory.release = [&](size_t) { ++release_calls; };

        assert_rejection(std::move(request), &memory, Status::MemoryLimitExceeded(""));
        EXPECT_EQ(1, reserve_calls.load());
        EXPECT_EQ(0, release_calls.load());
    }

    {
        auto request = valid_request();
        std::atomic<size_t> reserve_calls{0};
        std::atomic<size_t> release_calls{0};
        FakeMemoryContextState memory;
        memory.reserve = [&](size_t) {
            ++reserve_calls;
            return true;
        };
        memory.release = [&](size_t) {
            EXPECT_NE(&memory, tls_physical_scope);
            ++release_calls;
        };
        auto* sync_point = SyncPoint::GetInstance();
        sync_point->EnableProcessing();
        sync_point->SetCallBack("AIHttpClientImpl::submit:before_attempt_allocation",
                                [](void*) { throw std::bad_alloc(); });
        DeferOp cleanup([&] {
            sync_point->ClearCallBack("AIHttpClientImpl::submit:before_attempt_allocation");
            sync_point->DisableProcessing();
        });

        assert_rejection(std::move(request), &memory, Status::MemoryLimitExceeded(""));
        EXPECT_EQ(1, reserve_calls.load());
        EXPECT_EQ(1, release_calls.load());
    }
}

TEST_F(AIHttpClientTest, AcceptedRequestReleasesRequestMemoryAndCompletesExactlyOnce) {
    std::mutex mutex;
    std::condition_variable cv;
    int callback_count = 0;
    size_t reserved = 0;
    size_t released = 0;
    FakeMemoryContextState memory;
    memory.reserve = [&](size_t bytes) {
        std::lock_guard lock(mutex);
        reserved += bytes;
        return true;
    };
    memory.release = [&](size_t bytes) {
        std::lock_guard lock(mutex);
        released += bytes;
    };

    auto client = create_client();
    ASSERT_NE(nullptr, client);

    auto request = valid_request();
    request.memory = memory.context();

    ASSERT_TRUE(client->submit(std::move(request), [&](AIHttpResult result) {
                          EXPECT_TRUE(std::holds_alternative<AIHttpNoResponse>(result));
                          std::lock_guard lock(mutex);
                          ++callback_count;
                          cv.notify_all();
                      }).ok());

    {
        std::unique_lock lock(mutex);
        ASSERT_TRUE(cv.wait_for(lock, 5s, [&] { return callback_count == 1; }));
    }
    client->shutdown();

    std::lock_guard lock(mutex);
    EXPECT_EQ(1, callback_count);
    EXPECT_GT(reserved, 0);
    EXPECT_EQ(reserved, released);
}

TEST_F(AIHttpClientTest, AttemptResponseAndCrossThreadBodyCleanupUseMemoryContextButCallbackDoesNot) {
    FakeMemoryContextState physical;
    FakeMemoryContextState destroy_thread_ambient;
    LoopbackHttpServer server([](int socket, const CapturedHttpRequest&) {
        send_http_response(socket, 200, {{"Retry-After", "7"}}, std::string(512, 'p'));
    });
    auto client = create_client();
    ASSERT_NE(nullptr, client);

    struct CallbackOwner {
        CallbackOwner(FakeMemoryContextState* expected_scope, std::atomic<bool>* destroyed_in_scope)
                : expected_scope(expected_scope), destroyed_in_scope(destroyed_in_scope) {}
        ~CallbackOwner() { destroyed_in_scope->store(tls_physical_scope == expected_scope, std::memory_order_relaxed); }
        FakeMemoryContextState* expected_scope;
        std::atomic<bool>* destroyed_in_scope;
    };

    std::atomic<bool> append_in_scope{false};
    std::atomic<bool> attempt_destroyed_in_scope{false};
    std::atomic<bool> body_destroyed_in_scope{false};
    std::atomic<bool> callback_destroyed_in_scope{false};
    std::atomic<int> queued_nodes_scoped{0};
    std::atomic<int> active_nodes_scoped{0};
    std::atomic<int> callback_clear_scoped{0};
    std::atomic<int> accepted_request_source_clear_scoped{0};
    std::atomic<int> accepted_callback_source_clear_scoped{0};
    std::atomic<int> response_source_clear_scoped{0};
    std::atomic<int> retry_after_source_clear_scoped{0};
    auto* sync_point = SyncPoint::GetInstance();
    sync_point->SetCallBack("AIHttpClientImpl::write_callback:after_response_append", [&](void*) {
        append_in_scope.store(tls_physical_scope == &physical, std::memory_order_relaxed);
    });
    sync_point->SetCallBack("AIHttpClientImpl::destroy_attempt:in_physical_scope", [&](void*) {
        attempt_destroyed_in_scope.store(tls_physical_scope == &physical, std::memory_order_relaxed);
    });
    sync_point->SetCallBack("AIHttpResponseBody::_release:in_physical_scope", [&](void*) {
        body_destroyed_in_scope.store(tls_physical_scope == &physical, std::memory_order_relaxed);
    });
    sync_point->SetCallBack("AIHttpClientImpl::invoke_callback:callback_cleared:in_physical_scope", [&](void*) {
        if (tls_physical_scope == &physical) {
            callback_clear_scoped.fetch_add(1, std::memory_order_relaxed);
        }
    });
    sync_point->SetCallBack("AIHttpClientImpl::submit:accepted_request_source_cleared:in_physical_scope",
                            [&](void* value) {
                                auto* source = static_cast<AIHttpRequest*>(value);
                                if (tls_physical_scope == &physical && source->url.empty() && source->headers.empty() &&
                                    source->body.empty() && !source->lifecycle && !source->memory) {
                                    accepted_request_source_clear_scoped.fetch_add(1, std::memory_order_relaxed);
                                }
                            });
    sync_point->SetCallBack("AIHttpClientImpl::submit:accepted_callback_source_cleared:in_physical_scope",
                            [&](void* value) {
                                auto* source = static_cast<AIHttpCallback*>(value);
                                if (tls_physical_scope == &physical && !*source) {
                                    accepted_callback_source_clear_scoped.fetch_add(1, std::memory_order_relaxed);
                                }
                            });
    sync_point->SetCallBack("AIHttpClientImpl::finish_response:response_source_cleared:in_physical_scope",
                            [&](void* value) {
                                auto* source = static_cast<std::string*>(value);
                                if (tls_physical_scope == &physical && source->empty() &&
                                    source->capacity() <= std::string().capacity()) {
                                    response_source_clear_scoped.fetch_add(1, std::memory_order_relaxed);
                                }
                            });
    sync_point->SetCallBack("AIHttpClientImpl::finish_response:retry_after_source_cleared:in_physical_scope",
                            [&](void* value) {
                                auto* source = static_cast<std::optional<std::string>*>(value);
                                if (tls_physical_scope == &physical && !source->has_value()) {
                                    retry_after_source_clear_scoped.fetch_add(1, std::memory_order_relaxed);
                                }
                            });
    for (const char* point : {"AIHttpClientImpl::queued_node_allocated:in_physical_scope",
                              "AIHttpClientImpl::queued_node_deallocated:in_physical_scope"}) {
        sync_point->SetCallBack(point, [&](void*) {
            if (tls_physical_scope == &physical) {
                queued_nodes_scoped.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }
    for (const char* point : {"AIHttpClientImpl::active_node_allocated:in_physical_scope",
                              "AIHttpClientImpl::active_node_deallocated:in_physical_scope"}) {
        sync_point->SetCallBack(point, [&](void*) {
            if (tls_physical_scope == &physical) {
                active_nodes_scoped.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }
    sync_point->EnableProcessing();
    DeferOp cleanup([&] {
        sync_point->DisableProcessing();
        sync_point->ClearCallBack("AIHttpClientImpl::write_callback:after_response_append");
        sync_point->ClearCallBack("AIHttpClientImpl::destroy_attempt:in_physical_scope");
        sync_point->ClearCallBack("AIHttpResponseBody::_release:in_physical_scope");
        sync_point->ClearCallBack("AIHttpClientImpl::invoke_callback:callback_cleared:in_physical_scope");
        sync_point->ClearCallBack("AIHttpClientImpl::submit:accepted_request_source_cleared:in_physical_scope");
        sync_point->ClearCallBack("AIHttpClientImpl::submit:accepted_callback_source_cleared:in_physical_scope");
        sync_point->ClearCallBack("AIHttpClientImpl::finish_response:response_source_cleared:in_physical_scope");
        sync_point->ClearCallBack("AIHttpClientImpl::finish_response:retry_after_source_cleared:in_physical_scope");
        sync_point->ClearCallBack("AIHttpClientImpl::queued_node_allocated:in_physical_scope");
        sync_point->ClearCallBack("AIHttpClientImpl::queued_node_deallocated:in_physical_scope");
        sync_point->ClearCallBack("AIHttpClientImpl::active_node_allocated:in_physical_scope");
        sync_point->ClearCallBack("AIHttpClientImpl::active_node_deallocated:in_physical_scope");
        sync_point->ClearTrace();
    });

    std::atomic<int> reserve_calls{0};
    std::atomic<int> release_calls{0};
    std::atomic<bool> request_released_after_cleanup{false};
    std::atomic<bool> response_released_after_cleanup{false};
    std::promise<AIHttpResult> completion;
    std::future<AIHttpResult> future = completion.get_future();
    auto callback_owner = std::make_shared<CallbackOwner>(&physical, &callback_destroyed_in_scope);
    std::weak_ptr<CallbackOwner> weak_callback_owner = callback_owner;
    auto request = valid_request();
    request.url = server.url("/physical-scope");
    physical.reserve = [&](size_t) {
        EXPECT_NE(&physical, tls_physical_scope);
        ++reserve_calls;
        return true;
    };
    physical.release = [&](size_t) {
        EXPECT_NE(&physical, tls_physical_scope);
        const int release_ordinal = ++release_calls;
        if (release_ordinal == 1) {
            request_released_after_cleanup.store(attempt_destroyed_in_scope.load(std::memory_order_relaxed),
                                                 std::memory_order_relaxed);
        } else if (release_ordinal == 2) {
            response_released_after_cleanup.store(body_destroyed_in_scope.load(std::memory_order_relaxed),
                                                  std::memory_order_relaxed);
        }
    };
    request.memory = physical.context();

    ASSERT_TRUE(client->submit(std::move(request), [callback_owner = std::move(callback_owner), &physical,
                                                    &attempt_destroyed_in_scope, &request_released_after_cleanup,
                                                    &completion](AIHttpResult result) {
                          EXPECT_NE(&physical, tls_physical_scope);
                          EXPECT_TRUE(attempt_destroyed_in_scope.load(std::memory_order_relaxed));
                          EXPECT_TRUE(request_released_after_cleanup.load(std::memory_order_relaxed));
                          completion.set_value(std::move(result));
                      }).ok());
    ASSERT_EQ(std::future_status::ready, future.wait_for(5s));
    AIHttpResult result = future.get();
    ASSERT_TRUE(std::holds_alternative<AIHttpResponse>(result));
    EXPECT_TRUE(append_in_scope.load(std::memory_order_relaxed));
    EXPECT_EQ(2, queued_nodes_scoped.load(std::memory_order_relaxed));
    EXPECT_EQ(2, active_nodes_scoped.load(std::memory_order_relaxed));
    EXPECT_EQ(2, reserve_calls.load());
    EXPECT_EQ(1, release_calls.load());

    std::thread destroyer([&result, &destroy_thread_ambient] {
        tls_physical_scope = &destroy_thread_ambient;
        result = AIHttpNoResponse{AIHttpNoResponseCode::UNKNOWN};
        EXPECT_EQ(&destroy_thread_ambient, tls_physical_scope);
        tls_physical_scope = nullptr;
    });
    destroyer.join();
    client->shutdown();

    EXPECT_TRUE(body_destroyed_in_scope.load(std::memory_order_relaxed));
    EXPECT_TRUE(response_released_after_cleanup.load(std::memory_order_relaxed));
    EXPECT_TRUE(weak_callback_owner.expired());
    EXPECT_TRUE(callback_destroyed_in_scope.load(std::memory_order_relaxed));
    EXPECT_EQ(1, callback_clear_scoped.load(std::memory_order_relaxed));
    EXPECT_EQ(1, accepted_request_source_clear_scoped.load(std::memory_order_relaxed));
    EXPECT_EQ(1, accepted_callback_source_clear_scoped.load(std::memory_order_relaxed));
    EXPECT_EQ(1, response_source_clear_scoped.load(std::memory_order_relaxed));
    EXPECT_EQ(1, retry_after_source_clear_scoped.load(std::memory_order_relaxed));
    EXPECT_EQ(2, release_calls.load());
    EXPECT_EQ(0, physical.references.load());
}

TEST_F(AIHttpClientTest, IdleWaitWakesForSubmitAndShutdownAndShutdownRemainsIdempotent) {
    std::mutex idle_mutex;
    std::condition_variable idle_cv;
    int idle_waits = 0;
    auto* sync_point = SyncPoint::GetInstance();
    sync_point->EnableProcessing();
    sync_point->SetCallBack("AIHttpClientImpl::run:before_idle_wait", [&](void*) {
        {
            std::lock_guard lock(idle_mutex);
            ++idle_waits;
        }
        idle_cv.notify_all();
    });
    DeferOp cleanup([&] {
        sync_point->ClearCallBack("AIHttpClientImpl::run:before_idle_wait");
        sync_point->DisableProcessing();
    });

    LoopbackHttpServer server(
            [](int socket, const CapturedHttpRequest&) { send_http_response(socket, 200, {}, "idle-wakeup"); });
    auto client = create_client();
    ASSERT_NE(nullptr, client);

    auto wait_for_idle = [&](int expected) {
        std::unique_lock lock(idle_mutex);
        return idle_cv.wait_for(lock, 1s, [&] { return idle_waits >= expected; });
    };
    ASSERT_TRUE(wait_for_idle(1));
    std::this_thread::sleep_for(80ms);
    {
        std::lock_guard lock(idle_mutex);
        EXPECT_EQ(1, idle_waits) << "an idle client must block instead of periodically polling";
    }

    auto request = valid_request();
    request.url = server.url("/idle-wakeup");
    AIHttpResult result = submit_and_wait(client.get(), std::move(request));
    ASSERT_TRUE(std::holds_alternative<AIHttpResponse>(result));
    EXPECT_EQ("idle-wakeup", std::get<AIHttpResponse>(result).body.data());
    ASSERT_TRUE(wait_for_idle(2));
    std::this_thread::sleep_for(80ms);
    {
        std::lock_guard lock(idle_mutex);
        EXPECT_EQ(2, idle_waits);
    }

    const auto shutdown_started = std::chrono::steady_clock::now();
    client->shutdown();
    EXPECT_LT(std::chrono::steady_clock::now() - shutdown_started, 1s);
    client->shutdown();

    std::atomic<int> callback_count{0};
    Status status = client->submit(valid_request(), [&](AIHttpResult) { ++callback_count; });
    EXPECT_TRUE(status.is_shutdown()) << status;
    EXPECT_EQ(0, callback_count.load());
}

TEST_F(AIHttpClientTest, ShutdownRejectionReleaseHookCanReenterSubmitWithoutMutexDeadlock) {
    auto client = create_client();
    ASSERT_NE(nullptr, client);
    client->shutdown();

    std::promise<Status> nested_status_promise;
    std::future<Status> nested_status = nested_status_promise.get_future();
    std::thread nested_thread;
    bool nested_completed_inside_release = false;
    auto request = valid_request();
    FakeMemoryContextState memory;
    memory.release = [&](size_t) {
        nested_thread = std::thread(
                [&] { nested_status_promise.set_value(client->submit(valid_request(), [](AIHttpResult) {})); });
        // A timeout makes the old lock inversion fail without hanging: returning from this hook releases the outer
        // submit's mutex, so the worker can still complete and be joined below.
        nested_completed_inside_release = nested_status.wait_for(1s) == std::future_status::ready;
    };
    request.memory = memory.context();

    Status status = client->submit(std::move(request), [](AIHttpResult) {});
    if (nested_thread.joinable()) {
        nested_thread.join();
    }

    EXPECT_TRUE(status.is_shutdown()) << status;
    EXPECT_TRUE(nested_completed_inside_release);
    ASSERT_EQ(std::future_status::ready, nested_status.wait_for(0s));
    Status reentrant_status = nested_status.get();
    EXPECT_TRUE(reentrant_status.is_shutdown()) << reentrant_status;
}

TEST_F(AIHttpClientTest, SynchronousValidationStatusDoesNotContainRequestSecrets) {
    auto client = create_client();
    ASSERT_NE(nullptr, client);

    auto request = valid_request();
    request.url = "http://url-userinfo-secret@localhost/model?query-secret=1";
    request.headers = {{"Authorization", "Bearer authorization-secret"}};
    request.body = "body-secret";
    request.connect_timeout_ms = -1;
    Status status = client->submit(std::move(request), [](AIHttpResult) {});
    ASSERT_FALSE(status.ok());
    for (const char* secret : {"url-userinfo-secret", "query-secret", "authorization-secret", "body-secret"}) {
        EXPECT_EQ(std::string::npos, status.to_string().find(secret));
    }
}

TEST_F(AIHttpClientTest, PostsFullPathQueryHeadersAndBodyAndCapturesRetryAfter) {
    std::promise<CapturedHttpRequest> captured_promise;
    std::future<CapturedHttpRequest> captured = captured_promise.get_future();
    LoopbackHttpServer server([&](int socket, const CapturedHttpRequest& request) {
        captured_promise.set_value(request);
        send_http_response(socket, 201, {{"rEtRy-AfTeR", "17"}}, "response-body");
    });
    auto client = create_client();
    ASSERT_NE(nullptr, client);

    auto request = valid_request();
    request.url = server.url("/v1/chat/completions?tenant=a%20b&key=value");
    request.headers.emplace_back("X-Request-Tag", "tag-value");
    request.body = R"({"prompt":"full body"})";
    AIHttpResult result = submit_and_wait(client.get(), std::move(request));

    ASSERT_TRUE(std::holds_alternative<AIHttpResponse>(result));
    const auto& response = std::get<AIHttpResponse>(result);
    EXPECT_EQ(201, response.status_code);
    EXPECT_EQ("response-body", response.body.data());
    ASSERT_TRUE(response.retry_after.has_value());
    EXPECT_EQ("17", *response.retry_after);

    ASSERT_EQ(std::future_status::ready, captured.wait_for(1s));
    CapturedHttpRequest received = captured.get();
    EXPECT_EQ("POST", received.method);
    EXPECT_EQ("/v1/chat/completions?tenant=a%20b&key=value", received.target);
    EXPECT_EQ("Bearer test-token", received.headers["authorization"]);
    EXPECT_EQ("application/json", received.headers["content-type"]);
    EXPECT_EQ("tag-value", received.headers["x-request-tag"]);
    EXPECT_EQ(R"({"prompt":"full body"})", received.body);
    EXPECT_FALSE(received.headers.contains("expect"));
}

TEST_F(AIHttpClientTest, RedirectIsReturnedWithoutFollowingLocation) {
    LoopbackHttpServer redirected([](int socket, const CapturedHttpRequest&) {
        send_http_response(socket, 200, {}, "must-not-be-requested");
    });
    LoopbackHttpServer source([&](int socket, const CapturedHttpRequest&) {
        send_http_response(socket, 302, {{"Location", redirected.url("/redirect-target")}}, "redirect-body");
    });
    auto client = create_client();
    ASSERT_NE(nullptr, client);
    auto request = valid_request();
    request.url = source.url("/redirect-source");

    AIHttpResult result = submit_and_wait(client.get(), std::move(request));
    ASSERT_TRUE(std::holds_alternative<AIHttpResponse>(result));
    EXPECT_EQ(302, std::get<AIHttpResponse>(result).status_code);
    EXPECT_EQ("redirect-body", std::get<AIHttpResponse>(result).body.data());
    EXPECT_EQ(0, redirected.connections());
}

TEST_F(AIHttpClientTest, EnvironmentProxyIsIgnored) {
    LoopbackHttpServer proxy(
            [](int socket, const CapturedHttpRequest&) { send_http_response(socket, 502, {}, "proxy-used"); });
    LoopbackHttpServer endpoint(
            [](int socket, const CapturedHttpRequest&) { send_http_response(socket, 200, {}, "direct"); });
    ScopedEnvironment http_proxy("http_proxy", proxy.url(""));
    ScopedEnvironment upper_http_proxy("HTTP_PROXY", proxy.url(""));
    ScopedEnvironment all_proxy("ALL_PROXY", proxy.url(""));
    ScopedEnvironment no_proxy("NO_PROXY", std::nullopt);
    auto client = create_client();
    ASSERT_NE(nullptr, client);
    auto request = valid_request();
    request.url = endpoint.url("/direct");

    AIHttpResult result = submit_and_wait(client.get(), std::move(request));
    ASSERT_TRUE(std::holds_alternative<AIHttpResponse>(result));
    EXPECT_EQ("direct", std::get<AIHttpResponse>(result).body.data());
    EXPECT_EQ(0, proxy.connections());
}

TEST_F(AIHttpClientTest, UsesValidatedDnsSnapshotForConnection) {
    LoopbackHttpServer endpoint(
            [](int socket, const CapturedHttpRequest&) { send_http_response(socket, 200, {}, "pinned"); });
    auto client = create_client();
    ASSERT_NE(nullptr, client);
    auto request = valid_request();
    request.url = "http://ai-pinned.invalid:" + std::to_string(endpoint.port()) + "/pinned";
    request.resolved_endpoint = std::make_shared<const ResolvedHttpEndpoint>(ResolvedHttpEndpoint{
            .host = "ai-pinned.invalid",
            .port = endpoint.port(),
            .addresses = {"127.0.0.1"},
    });

    AIHttpResult result = submit_and_wait(client.get(), std::move(request));

    ASSERT_TRUE(std::holds_alternative<AIHttpResponse>(result));
    EXPECT_EQ(200, std::get<AIHttpResponse>(result).status_code);
    EXPECT_EQ("pinned", std::get<AIHttpResponse>(result).body.data());
    EXPECT_EQ(1, endpoint.connections());
    client->shutdown();
}

TEST_F(AIHttpClientTest, NumericIpv6EndpointDoesNotUseUnsupportedCurlResolveHostSyntax) {
    const ResolvedHttpEndpoint endpoint{
            .host = "2001:4860:4860::8888",
            .port = 443,
            .addresses = {"2001:4860:4860::8888"},
    };

    EXPECT_FALSE(http_endpoint_needs_dns_pinning(endpoint));
    Status status = validate_resolved_http_endpoint("https://[2001:4860:4860::8888]/v1/chat/completions", endpoint);
    EXPECT_TRUE(status.ok()) << status;

    ResolvedHttpEndpoint mismatched = endpoint;
    mismatched.addresses = {"2001:4860:4860::8844"};
    EXPECT_TRUE(validate_resolved_http_endpoint("https://[2001:4860:4860::8888]/v1/chat/completions", mismatched)
                        .is_invalid_argument());
}

TEST_F(AIHttpClientTest, ResponseCapDiscardsAndReleasesPartialResponseMemory) {
    ResponseChunkHandshake handshake;
    LoopbackHttpServer server([&](int socket, const CapturedHttpRequest&) {
        std::string headers = "HTTP/1.1 200 OK\r\nContent-Length: 12\r\nConnection: close\r\n\r\n";
        ASSERT_TRUE(send_all(socket, headers));
        ASSERT_TRUE(send_all(socket, "1"));
        if (!handshake.wait_for_client_to_consume_first_chunk()) {
            return;
        }
        (void)send_all(socket, "23456789012");
    });
    auto client = create_client();
    ASSERT_NE(nullptr, client);
    FakeMemoryContextState memory;
    std::atomic<bool> attempt_destroyed_in_scope{false};
    auto* sync_point = SyncPoint::GetInstance();
    sync_point->SetCallBack("AIHttpClientImpl::write_callback:after_response_append", [&](void* value) {
        auto* response = static_cast<std::string*>(value);
        if (response->size() == 1) {
            handshake.client_consumed_first_chunk();
        }
    });
    sync_point->SetCallBack("AIHttpClientImpl::destroy_attempt:in_physical_scope", [&](void*) {
        attempt_destroyed_in_scope.store(tls_physical_scope == &memory, std::memory_order_relaxed);
    });
    sync_point->EnableProcessing();
    DeferOp cleanup([&] {
        sync_point->DisableProcessing();
        sync_point->ClearCallBack("AIHttpClientImpl::write_callback:after_response_append");
        sync_point->ClearCallBack("AIHttpClientImpl::destroy_attempt:in_physical_scope");
        sync_point->ClearTrace();
    });

    auto request = valid_request();
    request.url = server.url("/large");
    request.max_response_bytes = 1;
    std::mutex mutex;
    std::vector<size_t> reserves;
    std::vector<size_t> releases;
    memory.reserve = [&](size_t bytes) {
        std::lock_guard lock(mutex);
        reserves.emplace_back(bytes);
        return true;
    };
    memory.release = [&](size_t bytes) {
        std::lock_guard lock(mutex);
        releases.emplace_back(bytes);
    };
    request.memory = memory.context();

    AIHttpResult result = submit_and_wait(client.get(), std::move(request));
    ASSERT_TRUE(std::holds_alternative<AIHttpNoResponse>(result));
    client->shutdown();
    EXPECT_EQ(AIHttpNoResponseCode::RESPONSE_CAP, std::get<AIHttpNoResponse>(result).code);
    EXPECT_TRUE(handshake.succeeded());
    EXPECT_TRUE(attempt_destroyed_in_scope.load(std::memory_order_relaxed));
    EXPECT_EQ(memory.entries.load(), memory.exits.load());
    EXPECT_EQ(0, memory.references.load());
    std::lock_guard lock(mutex);
    ASSERT_EQ(2, reserves.size());
    EXPECT_EQ(1, reserves[1]) << "only the first response chunk should be reserved";
    ASSERT_EQ(2, releases.size());
    EXPECT_EQ(reserves[1], releases[0]);
    EXPECT_EQ(reserves[0], releases[1]);
}

TEST_F(AIHttpClientTest, ResponseReservationFailureIsTypedAndDiscardsBody) {
    LoopbackHttpServer server(
            [](int socket, const CapturedHttpRequest&) { send_http_response(socket, 200, {}, "response-secret"); });
    auto client = create_client();
    ASSERT_NE(nullptr, client);
    FakeMemoryContextState memory;
    std::atomic<bool> response_appended_in_scope{false};
    std::atomic<bool> attempt_destroyed_in_scope{false};
    auto* sync_point = SyncPoint::GetInstance();
    sync_point->SetCallBack("AIHttpClientImpl::write_callback:after_response_append", [&](void*) {
        response_appended_in_scope.store(tls_physical_scope == &memory, std::memory_order_relaxed);
    });
    sync_point->SetCallBack("AIHttpClientImpl::destroy_attempt:in_physical_scope", [&](void*) {
        attempt_destroyed_in_scope.store(tls_physical_scope == &memory, std::memory_order_relaxed);
    });
    sync_point->EnableProcessing();
    DeferOp cleanup([&] {
        sync_point->DisableProcessing();
        sync_point->ClearCallBack("AIHttpClientImpl::write_callback:after_response_append");
        sync_point->ClearCallBack("AIHttpClientImpl::destroy_attempt:in_physical_scope");
        sync_point->ClearTrace();
    });
    auto request = valid_request();
    request.url = server.url("/memory-limit");
    std::atomic<int> reserve_calls{0};
    std::atomic<int> release_calls{0};
    memory.reserve = [&](size_t) { return ++reserve_calls == 1; };
    memory.release = [&](size_t) { ++release_calls; };
    request.memory = memory.context();

    AIHttpResult result = submit_and_wait(client.get(), std::move(request));
    ASSERT_TRUE(std::holds_alternative<AIHttpNoResponse>(result));
    client->shutdown();
    EXPECT_EQ(AIHttpNoResponseCode::MEMORY_LIMIT, std::get<AIHttpNoResponse>(result).code);
    EXPECT_TRUE(response_appended_in_scope.load(std::memory_order_relaxed));
    EXPECT_TRUE(attempt_destroyed_in_scope.load(std::memory_order_relaxed));
    EXPECT_EQ(2, reserve_calls.load());
    EXPECT_EQ(1, release_calls.load()) << "the physically appended but unlabelled response must not be released";
    EXPECT_EQ(memory.entries.load(), memory.exits.load());
    EXPECT_EQ(0, memory.references.load());
}

TEST_F(AIHttpClientTest, ResponseAppendFailureCreatesNoLogicalReservation) {
    LoopbackHttpServer server(
            [](int socket, const CapturedHttpRequest&) { send_http_response(socket, 200, {}, "response-secret"); });
    auto client = create_client();
    ASSERT_NE(nullptr, client);
    FakeMemoryContextState memory;
    std::atomic<bool> attempt_destroyed_in_scope{false};
    auto* sync_point = SyncPoint::GetInstance();
    sync_point->SetCallBack("AIHttpClientImpl::write_callback:before_response_append",
                            [](void*) { throw std::bad_alloc(); });
    sync_point->SetCallBack("AIHttpClientImpl::destroy_attempt:in_physical_scope", [&](void*) {
        attempt_destroyed_in_scope.store(tls_physical_scope == &memory, std::memory_order_relaxed);
    });
    sync_point->EnableProcessing();
    DeferOp cleanup([&] {
        sync_point->DisableProcessing();
        sync_point->ClearCallBack("AIHttpClientImpl::write_callback:before_response_append");
        sync_point->ClearCallBack("AIHttpClientImpl::destroy_attempt:in_physical_scope");
        sync_point->ClearTrace();
    });

    std::mutex mutex;
    std::vector<size_t> reserves;
    std::vector<size_t> releases;
    auto request = valid_request();
    request.url = server.url("/append-memory-limit");
    memory.reserve = [&](size_t bytes) {
        std::lock_guard lock(mutex);
        reserves.emplace_back(bytes);
        return true;
    };
    memory.release = [&](size_t bytes) {
        std::lock_guard lock(mutex);
        releases.emplace_back(bytes);
    };
    request.memory = memory.context();

    AIHttpResult result = submit_and_wait(client.get(), std::move(request));
    ASSERT_TRUE(std::holds_alternative<AIHttpNoResponse>(result));
    client->shutdown();
    EXPECT_EQ(AIHttpNoResponseCode::MEMORY_LIMIT, std::get<AIHttpNoResponse>(result).code);
    EXPECT_TRUE(attempt_destroyed_in_scope.load(std::memory_order_relaxed));
    EXPECT_EQ(memory.exits.load() + 1, memory.entries.load()) << "the injected append action is the sole throw";
    EXPECT_EQ(0, memory.references.load());
    std::lock_guard lock(mutex);
    ASSERT_EQ(1, reserves.size());
    ASSERT_EQ(1, releases.size());
    EXPECT_EQ(reserves[0], releases[0]);
}

TEST_F(AIHttpClientTest, TwoChunkReservationFailureReleasesOnlyRequestAndOwnedFirstChunk) {
    constexpr std::string_view first_chunk = "1";
    constexpr std::string_view second_chunk = "2";
    ResponseChunkHandshake handshake;
    LoopbackHttpServer server([&](int socket, const CapturedHttpRequest&) {
        std::string headers =
                "HTTP/1.1 200 OK\r\nContent-Length: " + std::to_string(first_chunk.size() + second_chunk.size()) +
                "\r\nConnection: close\r\n\r\n";
        ASSERT_TRUE(send_all(socket, headers));
        ASSERT_TRUE(send_all(socket, first_chunk));
        if (!handshake.wait_for_client_to_consume_first_chunk()) {
            return;
        }
        (void)send_all(socket, second_chunk);
    });
    auto client = create_client();
    ASSERT_NE(nullptr, client);
    FakeMemoryContextState memory;
    std::atomic<bool> attempt_destroyed_in_scope{false};
    auto* sync_point = SyncPoint::GetInstance();
    std::mutex mutex;
    std::vector<size_t> appended_sizes;
    sync_point->SetCallBack("AIHttpClientImpl::write_callback:after_response_append", [&](void* value) {
        auto* response = static_cast<std::string*>(value);
        {
            std::lock_guard lock(mutex);
            appended_sizes.emplace_back(response->size());
        }
        if (response->size() == first_chunk.size()) {
            handshake.client_consumed_first_chunk();
        }
    });
    sync_point->SetCallBack("AIHttpClientImpl::destroy_attempt:in_physical_scope", [&](void*) {
        attempt_destroyed_in_scope.store(tls_physical_scope == &memory, std::memory_order_relaxed);
    });
    sync_point->EnableProcessing();
    DeferOp cleanup([&] {
        sync_point->DisableProcessing();
        sync_point->ClearCallBack("AIHttpClientImpl::write_callback:after_response_append");
        sync_point->ClearCallBack("AIHttpClientImpl::destroy_attempt:in_physical_scope");
        sync_point->ClearTrace();
    });

    std::vector<size_t> reserves;
    std::vector<size_t> releases;
    auto request = valid_request();
    request.url = server.url("/two-chunk-memory-limit");
    memory.reserve = [&](size_t bytes) {
        std::lock_guard lock(mutex);
        const bool accepted = reserves.size() < 2;
        reserves.emplace_back(bytes);
        return accepted;
    };
    memory.release = [&](size_t bytes) {
        std::lock_guard lock(mutex);
        releases.emplace_back(bytes);
    };
    request.memory = memory.context();

    AIHttpResult result = submit_and_wait(client.get(), std::move(request));
    ASSERT_TRUE(std::holds_alternative<AIHttpNoResponse>(result));
    client->shutdown();
    EXPECT_EQ(AIHttpNoResponseCode::MEMORY_LIMIT, std::get<AIHttpNoResponse>(result).code);
    EXPECT_TRUE(handshake.succeeded());
    EXPECT_TRUE(attempt_destroyed_in_scope.load(std::memory_order_relaxed));
    EXPECT_EQ(memory.entries.load(), memory.exits.load());
    EXPECT_EQ(0, memory.references.load());
    std::lock_guard lock(mutex);
    ASSERT_EQ(2, appended_sizes.size());
    EXPECT_EQ(first_chunk.size(), appended_sizes[0]);
    EXPECT_EQ(first_chunk.size() + second_chunk.size(), appended_sizes[1]);
    ASSERT_EQ(3, reserves.size());
    EXPECT_EQ(first_chunk.size(), reserves[1]);
    EXPECT_EQ(second_chunk.size(), reserves[2]);
    ASSERT_EQ(2, releases.size());
    EXPECT_EQ(reserves[1], releases[0]);
    EXPECT_EQ(reserves[0], releases[1]);
}

TEST_F(AIHttpClientTest, SuccessfulResponseReservationLivesUntilMovedBodyOwnerIsDestroyed) {
    LoopbackHttpServer server([](int socket, const CapturedHttpRequest&) {
        send_http_response(socket, 200, {{"Retry-After", "7"}}, std::string(512, 'o'));
    });
    auto client = create_client();
    ASSERT_NE(nullptr, client);
    std::mutex mutex;
    std::vector<size_t> reserves;
    std::vector<size_t> releases;
    auto request = valid_request();
    request.url = server.url("/owned");
    FakeMemoryContextState memory;
    memory.reserve = [&](size_t bytes) {
        std::lock_guard lock(mutex);
        reserves.emplace_back(bytes);
        return true;
    };
    memory.release = [&](size_t bytes) {
        std::lock_guard lock(mutex);
        releases.emplace_back(bytes);
    };
    request.memory = memory.context();

    std::optional<AIHttpResult> owner(submit_and_wait(client.get(), std::move(request)));
    // The future becomes ready from inside the I/O-thread callback. Join that thread before the stack-backed fake
    // context can be destroyed; the response result must remain the sole owner of its reservation afterwards.
    client->shutdown();
    ASSERT_TRUE(std::holds_alternative<AIHttpResponse>(*owner));
    {
        std::lock_guard lock(mutex);
        ASSERT_GE(reserves.size(), 2);
        ASSERT_EQ(1, releases.size());
        EXPECT_EQ(reserves.front(), releases.front());
    }
    std::optional<AIHttpResult> moved_owner(std::move(*owner));
    const auto& moved_from_response = std::get<AIHttpResponse>(*owner);
    EXPECT_TRUE(moved_from_response.body.data().empty());
    EXPECT_LE(moved_from_response.body.data().capacity(), std::string().capacity());
    EXPECT_FALSE(moved_from_response.retry_after.has_value());
    ASSERT_TRUE(std::holds_alternative<AIHttpResponse>(*moved_owner));
    EXPECT_EQ("7", std::get<AIHttpResponse>(*moved_owner).retry_after);
    owner.reset();
    {
        std::lock_guard lock(mutex);
        EXPECT_EQ(1, releases.size());
    }
    moved_owner.reset();
    {
        std::lock_guard lock(mutex);
        ASSERT_EQ(2, releases.size());
        EXPECT_EQ(std::accumulate(std::next(reserves.begin()), reserves.end(), size_t{0}), releases.back());
    }
    EXPECT_EQ(0, memory.references.load());
    EXPECT_EQ(memory.entries.load(), memory.exits.load());
}

TEST_F(AIHttpClientTest, ResponseBodyFreesDataBeforeReleaseHookOnMoveAssignmentAndDestruction) {
    std::atomic<int> request_index{0};
    LoopbackHttpServer server([&](int socket, const CapturedHttpRequest&) {
        char fill = request_index.fetch_add(1) == 0 ? 'a' : 'b';
        send_http_response(socket, 200, {}, std::string(512, fill));
    });
    auto client = create_client();
    ASSERT_NE(nullptr, client);

    struct ReleaseObservation {
        AIHttpResponseBody* owner = nullptr;
        bool saw_empty_body = false;
        int observed_releases = 0;
        FakeMemoryContextState memory;
    } first_observation, second_observation;
    auto make_request = [&](std::string_view path, ReleaseObservation* observation) {
        auto request = valid_request();
        request.url = server.url(path);
        observation->memory.release = [observation](size_t) {
            if (observation->owner != nullptr) {
                observation->saw_empty_body = observation->owner->data().empty();
                ++observation->observed_releases;
            }
        };
        request.memory = observation->memory.context();
        return request;
    };

    std::optional<AIHttpResult> first(
            submit_and_wait(client.get(), make_request("/release-order/first", &first_observation)));
    std::optional<AIHttpResult> second(
            submit_and_wait(client.get(), make_request("/release-order/second", &second_observation)));
    client->shutdown();
    ASSERT_TRUE(std::holds_alternative<AIHttpResponse>(*first));
    ASSERT_TRUE(std::holds_alternative<AIHttpResponse>(*second));
    auto& first_body = std::get<AIHttpResponse>(*first).body;
    auto& second_body = std::get<AIHttpResponse>(*second).body;

    first_observation.owner = &first_body;
    first_body = std::move(second_body);
    EXPECT_TRUE(first_observation.saw_empty_body);
    EXPECT_EQ(1, first_observation.observed_releases);

    second_observation.owner = &first_body;
    first.reset();
    EXPECT_TRUE(second_observation.saw_empty_body);
    EXPECT_EQ(1, second_observation.observed_releases);
    second.reset();
}

TEST_F(AIHttpClientTest, PartialTransferReturnsTypedNoResponseAndNoRetryAfterData) {
    LoopbackHttpServer server([](int socket, const CapturedHttpRequest&) {
        std::string partial =
                "HTTP/1.1 503 Partial\r\nContent-Length: 20\r\nRetry-After: 19\r\nConnection: close\r\n\r\nshort";
        ASSERT_TRUE(send_all(socket, partial));
    });
    auto client = create_client();
    ASSERT_NE(nullptr, client);
    auto request = valid_request();
    request.url = server.url("/partial");

    AIHttpResult result = submit_and_wait(client.get(), std::move(request));
    ASSERT_TRUE(std::holds_alternative<AIHttpNoResponse>(result));
    EXPECT_EQ(AIHttpNoResponseCode::PARTIAL_TRANSFER, std::get<AIHttpNoResponse>(result).code);
}

TEST_F(AIHttpClientTest, DuplicateOrOversizedRetryAfterIsNotExposed) {
    std::atomic<int> request_index{0};
    LoopbackHttpServer server([&](int socket, const CapturedHttpRequest&) {
        if (request_index++ == 0) {
            std::string response =
                    "HTTP/1.1 429 Limited\r\nContent-Length: 0\r\nRetry-After: 1\r\n"
                    "retry-after: 2\r\nConnection: close\r\n\r\n";
            ASSERT_TRUE(send_all(socket, response));
        } else {
            send_http_response(socket, 429, {{"Retry-After", std::string(257, '9')}}, "");
        }
    });
    auto client = create_client();
    ASSERT_NE(nullptr, client);
    for (int i = 0; i < 2; ++i) {
        auto request = valid_request();
        request.url = server.url("/retry-after/" + std::to_string(i));
        AIHttpResult result = submit_and_wait(client.get(), std::move(request));
        ASSERT_TRUE(std::holds_alternative<AIHttpResponse>(result));
        EXPECT_FALSE(std::get<AIHttpResponse>(result).retry_after.has_value());
    }
}

TEST_F(AIHttpClientTest, RetryAfterAcceptsHttpDateButRejectsProviderText) {
    std::atomic<int> request_index{0};
    LoopbackHttpServer server([&](int socket, const CapturedHttpRequest&) {
        if (request_index++ == 0) {
            send_http_response(socket, 429, {{"Retry-After", "Wed, 21 Oct 2037 07:28:00 GMT"}}, "");
        } else {
            send_http_response(socket, 429, {{"Retry-After", "provider-secret-message"}}, "");
        }
    });
    auto client = create_client();
    ASSERT_NE(nullptr, client);

    auto request = valid_request();
    request.url = server.url("/http-date");
    AIHttpResult http_date_result = submit_and_wait(client.get(), std::move(request));
    ASSERT_TRUE(std::holds_alternative<AIHttpResponse>(http_date_result));
    ASSERT_TRUE(std::get<AIHttpResponse>(http_date_result).retry_after.has_value());
    EXPECT_EQ("Wed, 21 Oct 2037 07:28:00 GMT", *std::get<AIHttpResponse>(http_date_result).retry_after);

    request = valid_request();
    request.url = server.url("/provider-text");
    AIHttpResult text_result = submit_and_wait(client.get(), std::move(request));
    ASSERT_TRUE(std::holds_alternative<AIHttpResponse>(text_result));
    EXPECT_FALSE(std::get<AIHttpResponse>(text_result).retry_after.has_value());
}

TEST_F(AIHttpClientTest, RetryAfterRejectsNonImfOrInvalidHttpDates) {
    const std::vector<std::string> invalid_dates = {
            "1994 Nov 6",
            "Sun, 06 Nov 1994 08:49:37",
            "Sun, 30 Feb 1994 08:49:37 GMT",
            "Mon, 06 Nov 1994 08:49:37 GMT",
            "Sun, 06 Nov 1994 24:49:37 GMT",
    };
    std::atomic<size_t> request_index{0};
    LoopbackHttpServer server([&](int socket, const CapturedHttpRequest&) {
        size_t index = request_index.fetch_add(1);
        ASSERT_LT(index, invalid_dates.size());
        send_http_response(socket, 429, {{"Retry-After", invalid_dates[index]}}, "");
    });
    auto client = create_client();
    ASSERT_NE(nullptr, client);

    for (size_t index = 0; index < invalid_dates.size(); ++index) {
        SCOPED_TRACE(invalid_dates[index]);
        auto request = valid_request();
        request.url = server.url("/invalid-http-date/" + std::to_string(index));
        AIHttpResult result = submit_and_wait(client.get(), std::move(request));
        ASSERT_TRUE(std::holds_alternative<AIHttpResponse>(result));
        EXPECT_FALSE(std::get<AIHttpResponse>(result).retry_after.has_value());
    }
}

TEST_F(AIHttpClientTest, RunsMultipleRequestsConcurrentlyOnOneClient) {
    constexpr int request_count = 8;
    std::mutex server_mutex;
    std::condition_variable server_cv;
    int arrived = 0;
    bool release = false;
    LoopbackHttpServer server([&](int socket, const CapturedHttpRequest&) {
        std::unique_lock lock(server_mutex);
        ++arrived;
        server_cv.notify_all();
        server_cv.wait(lock, [&] { return release || arrived == request_count; });
        lock.unlock();
        send_http_response(socket, 200, {}, "concurrent");
    });
    auto client = create_client();
    ASSERT_NE(nullptr, client);

    std::mutex result_mutex;
    std::condition_variable result_cv;
    int callbacks = 0;
    for (int i = 0; i < request_count; ++i) {
        auto request = valid_request();
        request.url = server.url("/concurrent/" + std::to_string(i));
        ASSERT_TRUE(client->submit(std::move(request), [&](AIHttpResult result) {
                              EXPECT_TRUE(std::holds_alternative<AIHttpResponse>(result));
                              std::lock_guard lock(result_mutex);
                              ++callbacks;
                              result_cv.notify_all();
                          }).ok());
    }

    {
        std::unique_lock lock(server_mutex);
        bool all_arrived = server_cv.wait_for(lock, 2s, [&] { return arrived == request_count; });
        EXPECT_TRUE(all_arrived) << "arrived=" << arrived;
        release = true;
        server_cv.notify_all();
    }
    std::unique_lock lock(result_mutex);
    EXPECT_TRUE(result_cv.wait_for(lock, 2s, [&] { return callbacks == request_count; }));
    EXPECT_EQ(request_count, callbacks);
}

TEST_F(AIHttpClientTest, ActiveCancellationIsObservedWithinPollQuantum) {
    CountDownLatch accepted(1);
    CountDownLatch release_server(1);
    LoopbackHttpServer server([&](int, const CapturedHttpRequest&) {
        accepted.count_down();
        release_server.wait();
    });
    DeferOp release_server_on_exit([&] { release_server.count_down(); });
    auto client = create_client();
    ASSERT_NE(nullptr, client);
    auto cancelled = std::make_shared<std::atomic<bool>>(false);
    auto request = valid_request();
    request.url = server.url("/cancel");
    const int64_t query_deadline_ns = request.request_deadline_ns;
    request.lifecycle = [cancelled, query_deadline_ns] {
        return AIQueryLifecycleSnapshot{.cancelled = cancelled->load(), .monotonic_deadline_ns = query_deadline_ns};
    };
    auto promise = std::make_shared<std::promise<AIHttpResult>>();
    std::future<AIHttpResult> result = promise->get_future();
    ASSERT_TRUE(client->submit(std::move(request), [promise](AIHttpResult value) mutable {
                          promise->set_value(std::move(value));
                      }).ok());
    ASSERT_TRUE(accepted.wait_for(1s));

    auto start = std::chrono::steady_clock::now();
    cancelled->store(true);
    bool completed_promptly = result.wait_for(250ms) == std::future_status::ready;
    EXPECT_TRUE(completed_promptly);
    if (!completed_promptly) {
        client->shutdown();
    }
    release_server.count_down();
    ASSERT_EQ(std::future_status::ready, result.wait_for(1s));
    AIHttpResult value = result.get();
    if (completed_promptly) {
        ASSERT_TRUE(std::holds_alternative<AIHttpNoResponse>(value));
        EXPECT_EQ(AIHttpNoResponseCode::CANCELLATION, std::get<AIHttpNoResponse>(value).code);
        EXPECT_LT(std::chrono::steady_clock::now() - start, 300ms);
    }
}

TEST_F(AIHttpClientTest, LiveLifecycleAndImmutableDeadlineAreObservedWithoutWaitingForServerData) {
    CountDownLatch release_server(1);
    LoopbackHttpServer server([&](int, const CapturedHttpRequest&) { release_server.wait(); });
    DeferOp release_server_on_exit([&] { release_server.count_down(); });
    auto client = create_client();
    ASSERT_NE(nullptr, client);
    auto request = valid_request();
    request.url = server.url("/deadline");
    const int64_t original_query_deadline_ns = MonotonicNanos() + 500'000'000L;
    auto live_query_deadline_ns = std::make_shared<std::atomic<int64_t>>(original_query_deadline_ns);
    auto lifecycle_observed = std::make_shared<CountDownLatch>(1);
    request.request_deadline_ns = MonotonicNanos() + 2'000'000'000L;
    request.lifecycle = [live_query_deadline_ns, lifecycle_observed] {
        lifecycle_observed->count_down();
        return AIQueryLifecycleSnapshot{.monotonic_deadline_ns = live_query_deadline_ns->load()};
    };

    auto promise = std::make_shared<std::promise<AIHttpResult>>();
    std::future<AIHttpResult> result = promise->get_future();
    ASSERT_TRUE(client->submit(std::move(request), [promise](AIHttpResult value) mutable {
                          promise->set_value(std::move(value));
                      }).ok());
    ASSERT_TRUE(lifecycle_observed->wait_for(250ms)) << "HTTP attempt did not observe Query lifecycle";

    live_query_deadline_ns->store(MonotonicNanos() + 2'000'000'000L);
    EXPECT_EQ(std::future_status::timeout, result.wait_for(600ms))
            << "the HTTP attempt must re-read an extended Query deadline";

    live_query_deadline_ns->store(MonotonicNanos() - 1);
    ASSERT_EQ(std::future_status::ready, result.wait_for(250ms));
    release_server.count_down();
    AIHttpResult value = result.get();
    ASSERT_TRUE(std::holds_alternative<AIHttpNoResponse>(value));
    EXPECT_EQ(AIHttpNoResponseCode::DEADLINE, std::get<AIHttpNoResponse>(value).code);

    const AIQueryLifecycleProbe throwing = []() -> AIQueryLifecycleSnapshot { throw std::runtime_error("sentinel"); };
    EXPECT_EQ(AILifecycleState::CANCELLED, observe_ai_lifecycle(throwing, 0, MonotonicNanos()).state);
    EXPECT_EQ(AILifecycleState::DEADLINE_EXCEEDED,
              observe_ai_lifecycle([] { return AIQueryLifecycleSnapshot{}; }, 0, MonotonicNanos()).state);
}

TEST_F(AIHttpClientTest, ShutdownCompletesEveryAcceptedAttemptExactlyOnce) {
    constexpr int request_count = 24;
    CountDownLatch release_server(1);
    LoopbackHttpServer server([&](int, const CapturedHttpRequest&) { release_server.wait(); });
    DeferOp release_server_on_exit([&] { release_server.count_down(); });
    CountDownLatch callbacks_completed(request_count);
    std::array<std::atomic<int>, request_count> callbacks{};
    auto client = create_client();
    ASSERT_NE(nullptr, client);
    for (int i = 0; i < request_count; ++i) {
        auto request = valid_request();
        request.url = server.url("/shutdown/" + std::to_string(i));
        ASSERT_TRUE(client->submit(std::move(request), [&, i](AIHttpResult result) {
                              EXPECT_TRUE(std::holds_alternative<AIHttpNoResponse>(result));
                              if (std::holds_alternative<AIHttpNoResponse>(result)) {
                                  EXPECT_EQ(AIHttpNoResponseCode::SHUTDOWN, std::get<AIHttpNoResponse>(result).code);
                              }
                              ++callbacks[i];
                              callbacks_completed.count_down();
                          }).ok());
    }

    client->shutdown();
    release_server.count_down();
    ASSERT_TRUE(callbacks_completed.wait_for(2s));
    for (const auto& count : callbacks) {
        EXPECT_EQ(1, count.load());
    }
}

TEST_F(AIHttpClientTest, ActiveShutdownReleaseHookCanReenterShutdownWithoutDeadlock) {
    testing::FLAGS_gtest_death_test_style = "threadsafe";
    ASSERT_EXIT({ run_reentrant_shutdown_scenario(true); }, testing::ExitedWithCode(0), "");
}

TEST_F(AIHttpClientTest, ActiveShutdownCallbackCanReenterShutdownWithoutDeadlock) {
    testing::FLAGS_gtest_death_test_style = "threadsafe";
    ASSERT_EXIT({ run_reentrant_shutdown_scenario(false); }, testing::ExitedWithCode(0), "");
}

TEST_F(AIHttpClientTest, SubmitShutdownRaceNeverLosesOrDuplicatesAcceptedCallback) {
    constexpr int submitter_count = 8;
    auto client = create_client();
    ASSERT_NE(nullptr, client);
    std::array<Status, submitter_count> outcomes;
    std::array<std::atomic<int>, submitter_count> callbacks{};
    std::barrier start(submitter_count + 1);
    auto* sync_point = SyncPoint::GetInstance();
    sync_point->EnableProcessing();
    sync_point->SetCallBack("AIHttpClientImpl::submit:before_attempt_allocation",
                            [&](void*) { start.arrive_and_wait(); });
    DeferOp cleanup([&] {
        sync_point->ClearCallBack("AIHttpClientImpl::submit:before_attempt_allocation");
        sync_point->DisableProcessing();
    });

    std::vector<std::thread> submitters;
    submitters.reserve(submitter_count);
    for (int i = 0; i < submitter_count; ++i) {
        submitters.emplace_back([&, i] {
            auto request = valid_request();
            outcomes[i] = client->submit(std::move(request), [&, i](AIHttpResult) { ++callbacks[i]; });
        });
    }
    start.arrive_and_wait();
    client->shutdown();
    for (auto& submitter : submitters) {
        submitter.join();
    }

    for (int i = 0; i < submitter_count; ++i) {
        if (outcomes[i].ok()) {
            EXPECT_EQ(1, callbacks[i].load());
        } else {
            EXPECT_TRUE(outcomes[i].is_shutdown()) << outcomes[i];
            EXPECT_EQ(0, callbacks[i].load());
        }
    }
}

TEST_F(AIHttpClientTest, ThrowingCallbackDoesNotStopLaterCompletions) {
    LoopbackHttpServer server(
            [](int socket, const CapturedHttpRequest&) { send_http_response(socket, 200, {}, "ok"); });
    auto client = create_client();
    ASSERT_NE(nullptr, client);
    auto first = valid_request();
    first.url = server.url("/throws");
    ASSERT_TRUE(
            client->submit(std::move(first), [](AIHttpResult) { throw std::runtime_error("callback-secret"); }).ok());

    auto second = valid_request();
    second.url = server.url("/after-throw");
    AIHttpResult result = submit_and_wait(client.get(), std::move(second));
    ASSERT_TRUE(std::holds_alternative<AIHttpResponse>(result));
    EXPECT_EQ("ok", std::get<AIHttpResponse>(result).body.data());
}

TEST_F(AIHttpClientTest, TrustedLocalTlsCertificateCompletesHttpResponse) {
    ASSERT_TRUE(test_tls_material().valid());
    ASSERT_FALSE(test_ca_bundle().path().empty());
    LoopbackTlsServer server(test_tls_material(), [](SSL* ssl, const CapturedHttpRequest&) {
        send_tls_http_response(ssl, 200, "trusted-tls");
    });
    auto client_result = AIHttpClient::create({test_ca_bundle().path()});
    ASSERT_TRUE(client_result.ok()) << client_result.status();
    auto client = std::move(client_result).value();
    auto request = valid_request();
    request.url = server.url("localhost", "/trusted");

    AIHttpResult result = submit_and_wait(client.get(), std::move(request));
    ASSERT_TRUE(std::holds_alternative<AIHttpResponse>(result));
    EXPECT_EQ(200, std::get<AIHttpResponse>(result).status_code);
    EXPECT_EQ("trusted-tls", std::get<AIHttpResponse>(result).body.data());
}

TEST_F(AIHttpClientTest, UnknownLocalTlsCertificateIsVerificationFailure) {
    ASSERT_TRUE(test_tls_material().valid());
    LoopbackTlsServer server(test_tls_material(), [](SSL* ssl, const CapturedHttpRequest&) {
        send_tls_http_response(ssl, 200, "must-not-complete");
    });
    auto client = create_client();
    ASSERT_NE(nullptr, client);
    auto request = valid_request();
    request.url = server.url("localhost", "/unknown-ca");

    AIHttpResult result = submit_and_wait(client.get(), std::move(request));
    ASSERT_TRUE(std::holds_alternative<AIHttpNoResponse>(result));
    EXPECT_EQ(AIHttpNoResponseCode::TLS_VERIFICATION, std::get<AIHttpNoResponse>(result).code);
}

TEST_F(AIHttpClientTest, LocalTlsHostnameMismatchIsVerificationFailure) {
    ASSERT_TRUE(test_tls_material().valid());
    ASSERT_FALSE(test_ca_bundle().path().empty());
    LoopbackTlsServer server(test_tls_material(), [](SSL* ssl, const CapturedHttpRequest&) {
        send_tls_http_response(ssl, 200, "must-not-complete");
    });
    auto client_result = AIHttpClient::create({test_ca_bundle().path()});
    ASSERT_TRUE(client_result.ok()) << client_result.status();
    auto client = std::move(client_result).value();
    auto request = valid_request();
    request.url = server.url("127.0.0.1", "/hostname-mismatch");

    AIHttpResult result = submit_and_wait(client.get(), std::move(request));
    ASSERT_TRUE(std::holds_alternative<AIHttpNoResponse>(result));
    EXPECT_EQ(AIHttpNoResponseCode::TLS_VERIFICATION, std::get<AIHttpNoResponse>(result).code);
}

} // namespace
} // namespace starrocks
