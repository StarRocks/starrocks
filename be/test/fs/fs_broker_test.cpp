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

#include "fs/fs_broker.h"

#include <brpc/uri.h>
#include <gtest/gtest.h>

#include <map>
#include <memory>

#include "fs/fs_memory.h"
#include "gen_cpp/FileBrokerService_types.h"
#include "gen_cpp/TFileBrokerService.h"
#include "gutil/strings/substitute.h"
#include "testutil/assert.h"
#include "util/thrift_client.h"

namespace starrocks {

class MockBrokerServer {
public:
    explicit MockBrokerServer(MemoryFileSystem* fs) : _fs(fs) {}

    std::string url_path(const std::string& url) {
        brpc::URI uri;
        CHECK_EQ(0, uri.SetHttpURL(url)) << uri.status();
        return uri.path();
    }

    void listPath(TBrokerListResponse& response, const TBrokerListPathRequest& request) {
        std::string path = url_path(request.path);
        TBrokerFileStatus status;
        StatusOr<bool> status_or = _fs->is_directory(path);
        if (status_or.status().is_not_found()) {
            response.opStatus.__set_statusCode(TBrokerOperationStatusCode::FILE_NOT_FOUND);
            return;
        } else if (!status_or.ok()) {
            response.opStatus.__set_statusCode(TBrokerOperationStatusCode::TARGET_STORAGE_SERVICE_ERROR);
            return;
        }
        bool is_dir = status_or.value();
        uint64_t size = is_dir ? 0 : _fs->get_file_size(path).value();
        status.__set_isSplitable(false);
        if (request.__isset.fileNameOnly && request.fileNameOnly) {
            status.__set_path(path.substr(path.rfind('/') + 1));
        } else {
            status.__set_path(path);
        }
        status.__set_isDir(is_dir);
        status.__set_size(size);
        response.files.emplace_back(std::move(status));
        response.opStatus.__set_statusCode(TBrokerOperationStatusCode::OK);
    }

    void deletePath(TBrokerOperationStatus& response, const TBrokerDeletePathRequest& request) {
        std::string path = url_path(request.path);
        if (_is_file(path)) {
            CHECK(_rm_file(path));
            response.__set_statusCode(TBrokerOperationStatusCode::OK);
        } else if (_is_dir(path)) {
            CHECK(_rm_dir(path));
            response.__set_statusCode(TBrokerOperationStatusCode::OK);
        } else {
            response.__set_statusCode(TBrokerOperationStatusCode::FILE_NOT_FOUND);
        }
    }

    void checkPathExist(TBrokerCheckPathExistResponse& response, const TBrokerCheckPathExistRequest& request) {
        response.opStatus.__set_statusCode(TBrokerOperationStatusCode::OK);
        response.isPathExist = _exists(url_path(request.path));
    }

    void openReader(TBrokerOpenReaderResponse& response, const TBrokerOpenReaderRequest& request) {
        std::string path = url_path(request.path);
        auto res = _fs->new_random_access_file(path);
        if (res.ok()) {
            TBrokerFD fd;
            fd.__set_high(0);
            fd.__set_low(++_next_fd);
            response.__set_fd(fd);
            response.__set_size(0);
            response.opStatus.__set_statusCode(TBrokerOperationStatusCode::OK);
            _readers[fd.low] = std::move(res).value();
        } else if (res.status().is_not_found()) {
            response.opStatus.__set_statusCode(TBrokerOperationStatusCode::FILE_NOT_FOUND);
        } else {
            // I don't know how real broker handle this case, just return an non-OK status here.
            response.opStatus.__set_statusCode(TBrokerOperationStatusCode::INVALID_ARGUMENT);
        }
    }

    void pread(TBrokerReadResponse& response, const TBrokerPReadRequest& request) {
        auto iter = _readers.find(request.fd.low);
        if (iter == _readers.end()) {
            response.opStatus.__set_statusCode(TBrokerOperationStatusCode::INVALID_ARGUMENT);
            return;
        }
        response.data.resize(request.length);
        auto& f = iter->second;
        auto res = f->read_at(request.offset, response.data.data(), response.data.size());
        if (!res.ok()) {
            response.opStatus.__set_statusCode(TBrokerOperationStatusCode::INVALID_INPUT_OFFSET);
        } else if (*res == 0) {
            response.data.resize(*res);
            response.opStatus.__set_statusCode(TBrokerOperationStatusCode::END_OF_FILE);
        } else {
            response.data.resize(*res);
            response.opStatus.__set_statusCode(TBrokerOperationStatusCode::OK);
        }
    }

    void closeReader(TBrokerOperationStatus& response, const TBrokerCloseReaderRequest& request) {
        auto iter = _readers.find(request.fd.low);
        if (iter == _readers.end()) {
            response.__set_statusCode(TBrokerOperationStatusCode::INVALID_ARGUMENT);
        } else {
            _readers.erase(iter);
            response.__set_statusCode(TBrokerOperationStatusCode::OK);
        }
    }

    void openWriter(TBrokerOpenWriterResponse& response, const TBrokerOpenWriterRequest& request) {
        std::string path = url_path(request.path);
        WritableFileOptions opts{.mode = FileSystem::MUST_CREATE};
        auto res = _fs->new_writable_file(opts, path);
        if (!res.ok()) {
            response.opStatus.__set_statusCode(TBrokerOperationStatusCode::INVALID_INPUT_FILE_PATH);
        } else {
            TBrokerFD fd;
            fd.__set_high(0);
            fd.__set_low(++_next_fd);
            _writers[fd.low] = std::move(res).value();
            response.__set_fd(fd);
            response.opStatus.__set_statusCode(TBrokerOperationStatusCode::OK);
        }
    }

    void pwrite(TBrokerOperationStatus& response, const TBrokerPWriteRequest& request) {
        auto iter = _writers.find(request.fd.low);
        if (iter == _writers.end()) {
            response.__set_statusCode(TBrokerOperationStatusCode::INVALID_ARGUMENT);
            response.__set_message("Invalid FD");
            return;
        }
        auto& f = iter->second;
        if (f->size() != request.offset) {
            response.__set_statusCode(TBrokerOperationStatusCode::INVALID_INPUT_OFFSET);
            return;
        }
        Status st = f->append(request.data);
        if (st.ok()) {
            response.__set_statusCode(TBrokerOperationStatusCode::OK);
        } else {
            response.__set_statusCode(TBrokerOperationStatusCode::TARGET_STORAGE_SERVICE_ERROR);
        }
    }

    void closeWriter(TBrokerOperationStatus& response, const TBrokerCloseWriterRequest& request) {
        auto iter = _writers.find(request.fd.low);
        if (iter == _writers.end()) {
            response.__set_statusCode(TBrokerOperationStatusCode::INVALID_ARGUMENT);
        } else {
            _writers.erase(iter);
            response.__set_statusCode(TBrokerOperationStatusCode::OK);
        }
    }

private:
    bool _is_dir(const std::string& path) {
        const auto status_or = _fs->is_directory(path);
        return status_or.ok() && status_or.value();
    }
    bool _is_file(const std::string& path) {
        const auto status_or = _fs->is_directory(path);
        return status_or.ok() && !status_or.value();
    }
    bool _exists(const std::string& path) { return _is_dir(path) || _is_file(path); }
    bool _rm_file(const std::string& path) { return _fs->delete_file(path).ok(); }
    bool _rm_dir(const std::string& path) { return _fs->delete_dir(path).ok(); }

    MemoryFileSystem* _fs;
    int64_t _next_fd = 0;
    std::map<int64_t, std::unique_ptr<RandomAccessFile>> _readers;
    std::map<int64_t, std::unique_ptr<WritableFile>> _writers;
};

class MockBrokerServiceClient : public TFileBrokerServiceClient {
public:
    using TBinaryProtocol = apache::thrift::protocol::TBinaryProtocol;
    using TBufferedTransport = apache::thrift::transport::TBufferedTransport;
    using TSocket = apache::thrift::transport::TSocket;

    MockBrokerServiceClient()
            : TFileBrokerServiceClient(std::make_shared<TBinaryProtocol>(
                      std::make_shared<TBufferedTransport>(std::make_shared<TSocket>("127.0.0.1", 12345)))) {}

    [[maybe_unused]] void disconnect_from_broker() { _server = nullptr; }
    [[maybe_unused]] void connect_to_broker(MockBrokerServer* server) { _server = server; }

    [[maybe_unused]] void listPath(TBrokerListResponse& response, const TBrokerListPathRequest& request) override {
        _check_connection();
        _server->listPath(response, request);
    }
    [[maybe_unused]] void deletePath(TBrokerOperationStatus& response,
                                     const TBrokerDeletePathRequest& request) override {
        _check_connection();
        _server->deletePath(response, request);
    }
    [[maybe_unused]] void checkPathExist(TBrokerCheckPathExistResponse& response,
                                         const TBrokerCheckPathExistRequest& request) override {
        _check_connection();
        _server->checkPathExist(response, request);
    }
    [[maybe_unused]] void openReader(TBrokerOpenReaderResponse& response,
                                     const TBrokerOpenReaderRequest& request) override {
        _check_connection();
        _server->openReader(response, request);
    }
    [[maybe_unused]] void pread(TBrokerReadResponse& response, const TBrokerPReadRequest& request) override {
        _check_connection();
        _server->pread(response, request);
    }
    [[maybe_unused]] void closeReader(TBrokerOperationStatus& response,
                                      const TBrokerCloseReaderRequest& request) override {
        _check_connection();
        _server->closeReader(response, request);
    }
    [[maybe_unused]] void openWriter(TBrokerOpenWriterResponse& response,
                                     const TBrokerOpenWriterRequest& request) override {
        _check_connection();
        _server->openWriter(response, request);
    }
    [[maybe_unused]] void pwrite(TBrokerOperationStatus& response, const TBrokerPWriteRequest& request) override {
        _check_connection();
        _server->pwrite(response, request);
    }
    [[maybe_unused]] void closeWriter(TBrokerOperationStatus& response,
                                      const TBrokerCloseWriterRequest& request) override {
        _check_connection();
        _server->closeWriter(response, request);
    }

private:
    void _check_connection() {
        if (_server == nullptr) throw apache::thrift::transport::TTransportException();
    }

    MockBrokerServer* _server = nullptr;
};

class EnvBrokerTest : public ::testing::Test {
public:
    EnvBrokerTest() = default;

protected:
    void SetUp() override {
        TNetworkAddress addr;
        addr.__set_hostname("127.0.0.1");
        addr.__set_port(456);
        BrokerFileSystem fs(addr, {});
        _fs = fs;

        _buff.resize(4096);
        _fs_mem = new MemoryFileSystem();
        CHECK_OK(_fs_mem->create_dir("/tmp"));
        _server = new MockBrokerServer(_fs_mem);
        _client = new MockBrokerServiceClient();
        _client->connect_to_broker(_server);
        BrokerFileSystem::TEST_set_broker_client(_client);
    }

    void TearDown() override {
        BrokerFileSystem::TEST_set_broker_client(nullptr);
        delete _client;
        delete _server;
        delete _fs_mem;
    }

    MemoryFileSystem* _fs_mem = nullptr;
    BrokerFileSystem _fs{TNetworkAddress(), {}};
    MockBrokerServiceClient* _client = nullptr;
    MockBrokerServer* _server = nullptr;
    std::vector<char> _buff;
};

std::string read(std::unique_ptr<SequentialFile>& f, int len) {
    std::string s(len, '\0');
    ASSIGN_OR_ABORT(auto nread, f->read(s.data(), s.size()));
    s.resize(nread);
    return s;
}

std::string read(std::unique_ptr<RandomAccessFile>& f, size_t off, size_t len) {
    std::string s(len, '\0');
    ASSIGN_OR_ABORT(auto nread, f->read_at(off, s.data(), s.size()));
    s.resize(nread);
    return s;
}

std::string read_full(std::unique_ptr<RandomAccessFile>& f, size_t off, size_t len) {
    std::string s = read(f, off, len);
    CHECK_EQ(len, s.size());
    return s;
}

uint64_t filesize(std::unique_ptr<RandomAccessFile>& f) {
    ASSIGN_OR_ABORT(uint64_t len, f->get_size());
    return len;
}

// NOLINTNEXTLINE
TEST_F(EnvBrokerTest, test_open_non_exist_file) {
    const std::string url = "/xyz/xxx.txt";

    // Check the specific (mocked) error code is meaningless, because
    // I don't know what it is.
    ASSERT_FALSE(_fs.new_sequential_file(url).ok());
    ASSERT_FALSE(_fs.new_random_access_file(url).ok());
}

// NOLINTNEXTLINE
TEST_F(EnvBrokerTest, test_write_file) {
    const std::string path = "/tmp/1.txt";

    auto f = *_fs.new_writable_file(path);
    ASSERT_OK(f->append("first line\n"));
    ASSERT_OK(f->append("second line\n"));
    f->close();
    std::string content;
    ASSERT_OK(_fs_mem->read_file(path, &content));
    ASSERT_EQ("first line\nsecond line\n", content);
}

// NOLINTNEXTLINE
TEST_F(EnvBrokerTest, test_sequential_read) {
    const std::string path = "/tmp/1.txt";
    const std::string content = "abcdefghijklmnopqrstuvwxyz0123456789";
    ASSERT_OK(_fs_mem->create_file(path));
    ASSERT_OK(_fs_mem->append_file(path, content));

    auto f = *_fs.new_sequential_file(path);
    ASSERT_EQ("", read(f, 0));
    ASSERT_EQ("a", read(f, 1));
    ASSERT_EQ("bcdefghij", read(f, 9));
    ASSERT_EQ("klmnopqrstuvwxyz0123456789", read(f, 100));
    ASSERT_EQ("", read(f, 1));
}

// NOLINTNEXTLINE
TEST_F(EnvBrokerTest, test_random_read) {
    const std::string path = "/tmp/1.txt";
    const std::string content = "abcdefghijklmnopqrstuvwxyz0123456789";
    ASSERT_OK(_fs_mem->create_file(path));
    ASSERT_OK(_fs_mem->append_file(path, content));

    auto f = *_fs.new_random_access_file(path);
    ASSERT_EQ(content.size(), filesize(f));
    ASSERT_EQ("", read(f, 0, 0));
    ASSERT_EQ("", read(f, 10, 0));
    ASSERT_EQ(content.substr(10, 5), read(f, 10, 5));
    ASSERT_EQ(content.substr(0, 9), read(f, 0, 9));
    ASSERT_EQ(content.substr(1), read(f, 1, 100));
    ASSERT_EQ("a", read(f, 0, 1));

    ASSERT_FALSE(f->read_at_fully(content.size(), _buff.data(), 1).ok());

    f.reset();
}

// NOLINTNEXTLINE
TEST_F(EnvBrokerTest, test_write_exist_file) {
    const std::string path = "/tmp/1.txt";
    ASSERT_OK(_fs_mem->create_file(path));
    ASSERT_OK(_fs_mem->append_file(path, "old content"));

    auto opts = WritableFileOptions{.mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
    ASSERT_TRUE(_fs.new_writable_file(opts, path).status().is_not_supported());

    opts = WritableFileOptions{.mode = FileSystem::MUST_CREATE};
    ASSERT_TRUE(_fs.new_writable_file(opts, path).status().is_already_exist());

    opts = WritableFileOptions{.mode = FileSystem::MUST_EXIST};
    ASSERT_TRUE(_fs.new_writable_file(opts, path).status().is_not_supported());

    opts = WritableFileOptions{.mode = FileSystem::CREATE_OR_OPEN};
    ASSERT_TRUE(_fs.new_writable_file(opts, path).status().is_not_supported());

    // Assume the file still exists.
    std::string content;
    ASSERT_OK(_fs_mem->read_file(path, &content));
    ASSERT_EQ("old content", content);
}

// NOLINTNEXTLINE
TEST_F(EnvBrokerTest, test_delete_file) {
    const std::string path = "/tmp/1.txt";

    ASSERT_FALSE(_fs.delete_file(path).ok());

    ASSERT_OK(_fs_mem->create_file(path));
    ASSERT_OK(_fs_mem->append_file(path, "file content"));
    ASSERT_OK(_fs.delete_file(path));
    std::string content;
    EXPECT_TRUE(_fs_mem->read_file(path, &content).is_not_found());
}

// NOLINTNEXTLINE
TEST_F(EnvBrokerTest, test_check_path_exist) {
    const std::string path = "/tmp/1.txt";

    ASSERT_TRUE(_fs.path_exists(path).is_not_found());

    ASSERT_OK(_fs_mem->create_file(path));
    ASSERT_OK(_fs_mem->append_file(path, "file content"));
    ASSERT_OK(_fs.path_exists(path));
}

// NOLINTNEXTLINE
TEST_F(EnvBrokerTest, test_is_directory) {
    ASSERT_OK(_fs_mem->create_file("/tmp/1.txt"));
    ASSERT_TRUE(_fs.is_directory("/xy").status().is_not_found());

    auto status_or = _fs.is_directory("/tmp");
    ASSERT_OK(status_or.status());
    ASSERT_TRUE(status_or.value());

    status_or = _fs.is_directory("/tmp/1.txt");
    ASSERT_OK(status_or.status());
    ASSERT_FALSE(status_or.value());
}

// NOLINTNEXTLINE
TEST_F(EnvBrokerTest, test_link_file) {
    const std::string old_path = "/tmp";
    const std::string new_path = "/ttt";
    ASSERT_TRUE(_fs.link_file(old_path, new_path).is_not_supported());
}

// NOLINTNEXTLINE
TEST_F(EnvBrokerTest, test_rename) {
    const std::string old_path = "/tmp";
    const std::string new_path = "/ttt";
    ASSERT_TRUE(_fs.rename_file(old_path, new_path).is_not_supported());
}

// NOLINTNEXTLINE
TEST_F(EnvBrokerTest, test_get_modified_time) {
    const std::string path = "/tmp";
    ASSERT_TRUE(_fs.get_file_modified_time(path).status().is_not_supported());
}

// NOLINTNEXTLINE
TEST_F(EnvBrokerTest, test_canonicalize) {
    std::string s;
    ASSERT_TRUE(_fs.canonicalize("//a//b/", &s).is_not_supported());
}

// NOLINTNEXTLINE
TEST_F(EnvBrokerTest, test_create_dir) {
    ASSERT_TRUE(_fs.create_dir("//a//b/").is_not_supported());
}

// NOLINTNEXTLINE
TEST_F(EnvBrokerTest, test_create_dir_if_missing) {
    bool created;
    ASSERT_TRUE(_fs.create_dir_if_missing("//a//b/", &created).is_not_supported());
}

// NOLINTNEXTLINE
TEST_F(EnvBrokerTest, test_delete_dir) {
    ASSERT_TRUE(_fs.delete_dir("//a//b/").is_not_supported());
}

// NOLINTNEXTLINE
TEST_F(EnvBrokerTest, test_sync_dir) {
    ASSERT_TRUE(_fs.sync_dir("//a//b/").is_not_supported());
}

// NOLINTNEXTLINE
TEST_F(EnvBrokerTest, test_get_children) {
    std::vector<std::string> children;
    ASSERT_TRUE(_fs.get_children("//a//b/", &children).is_not_supported());
}

} // namespace starrocks
