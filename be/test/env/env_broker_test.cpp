// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "env/env_broker.h"

#include <brpc/uri.h>
#include <gtest/gtest.h>

#include <map>
#include <memory>

#include "env/env_memory.h"
#include "gen_cpp/FileBrokerService_types.h"
#include "gen_cpp/TFileBrokerService.h"
#include "gutil/strings/substitute.h"
#include "util/thrift_client.h"

namespace starrocks {

#define CHECK_OK(stmt)                                \
    do {                                              \
        Status __status = (stmt);                     \
        CHECK(__status.ok()) << __status.to_string(); \
    } while (0)

class MockBrokerServer {
public:
    explicit MockBrokerServer(EnvMemory* env) : _env(env) {}

    std::string url_path(const std::string& url) {
        brpc::URI uri;
        CHECK_EQ(0, uri.SetHttpURL(url)) << uri.status();
        return uri.path();
    }

    void listPath(TBrokerListResponse& response, const TBrokerListPathRequest& request) {
        std::string path = url_path(request.path);
        TBrokerFileStatus status;
        bool is_dir = false;
        Status st = _env->is_directory(path, &is_dir);
        if (st.is_not_found()) {
            response.opStatus.__set_statusCode(TBrokerOperationStatusCode::FILE_NOT_FOUND);
            return;
        } else if (!st.ok()) {
            response.opStatus.__set_statusCode(TBrokerOperationStatusCode::TARGET_STORAGE_SERVICE_ERROR);
            return;
        }
        uint64_t size = 0;
        (void)_env->get_file_size(path, &size);
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
        std::unique_ptr<RandomAccessFile> f;
        Status st = _env->new_random_access_file(path, &f);
        if (st.ok()) {
            TBrokerFD fd;
            fd.__set_high(0);
            fd.__set_low(++_next_fd);
            response.__set_fd(fd);
            response.__set_size(0);
            response.opStatus.__set_statusCode(TBrokerOperationStatusCode::OK);
            _readers[fd.low] = std::move(f);
        } else if (st.is_not_found()) {
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
        Slice buff(response.data);
        auto& f = iter->second;
        Status st = f->read(request.offset, &buff);
        response.data.resize(buff.size);
        if (!st.ok()) {
            response.opStatus.__set_statusCode(TBrokerOperationStatusCode::INVALID_INPUT_OFFSET);
        } else if (buff.size == 0) {
            response.opStatus.__set_statusCode(TBrokerOperationStatusCode::END_OF_FILE);
        } else {
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
        RandomRWFileOptions opts{.mode = Env::MUST_CREATE};
        std::unique_ptr<RandomRWFile> f;
        Status st = _env->new_random_rw_file(opts, path, &f);
        if (!st.ok()) {
            response.opStatus.__set_statusCode(TBrokerOperationStatusCode::INVALID_INPUT_FILE_PATH);
        } else {
            TBrokerFD fd;
            fd.__set_high(0);
            fd.__set_low(++_next_fd);
            _writers[fd.low] = std::move(f);
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
        uint64_t size = 0;
        CHECK(f->size(&size).ok());
        if (size != request.offset) {
            response.__set_statusCode(TBrokerOperationStatusCode::INVALID_INPUT_OFFSET);
            return;
        }
        Status st = f->write_at(request.offset, request.data);
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
        bool is_dir = false;
        return _env->is_directory(path, &is_dir).ok() && is_dir;
    }
    bool _is_file(const std::string& path) {
        bool is_dir = false;
        return _env->is_directory(path, &is_dir).ok() && !is_dir;
    }
    bool _exists(const std::string& path) { return _is_dir(path) || _is_file(path); }
    bool _rm_file(const std::string& path) { return _env->delete_file(path).ok(); }
    bool _rm_dir(const std::string& path) { return _env->delete_dir(path).ok(); }

    EnvMemory* _env;
    int64_t _next_fd = 0;
    std::map<int64_t, std::unique_ptr<RandomAccessFile>> _readers;
    std::map<int64_t, std::unique_ptr<RandomRWFile>> _writers;
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
        EnvBroker env(addr, {});
        _env = env;

        _buff.resize(4096);
        _env_mem = new EnvMemory();
        CHECK_OK(_env_mem->create_dir("/tmp"));
        _server = new MockBrokerServer(_env_mem);
        _client = new MockBrokerServiceClient();
        _client->connect_to_broker(_server);
        EnvBroker::TEST_set_broker_client(_client);
    }

    void TearDown() override {
        EnvBroker::TEST_set_broker_client(nullptr);
        delete _client;
        delete _server;
        delete _env_mem;
    }

    EnvMemory* _env_mem = nullptr;
    EnvBroker _env{TNetworkAddress(), {}};
    MockBrokerServiceClient* _client = nullptr;
    MockBrokerServer* _server = nullptr;
    std::vector<char> _buff;
};

#define ASSERT_OK(stmt)                             \
    do {                                            \
        Status __st = (stmt);                       \
        ASSERT_TRUE(__st.ok()) << __st.to_string(); \
    } while (0)

std::string read(std::unique_ptr<SequentialFile>& f, int len) {
    std::string s(len, '\0');
    Slice buff(s.data(), s.size());
    Status st = f->read(&buff);
    CHECK(st.ok()) << st.to_string();
    s.resize(buff.size);
    return s;
}

std::string read(std::unique_ptr<RandomAccessFile>& f, size_t off, size_t len) {
    std::string s(len, '\0');
    Slice buff(s.data(), s.size());
    Status st = f->read(off, &buff);
    CHECK(st.ok()) << st.to_string();
    s.resize(buff.size);
    return s;
}

std::string read_full(std::unique_ptr<RandomAccessFile>& f, size_t off, size_t len) {
    std::string s = read(f, off, len);
    CHECK_EQ(len, s.size());
    return s;
}

uint64_t filesize(std::unique_ptr<RandomAccessFile>& f) {
    uint64_t len = 0;
    CHECK(f->size(&len).ok());
    return len;
}

// NOLINTNEXTLINE
TEST_F(EnvBrokerTest, test_open_non_exist_file) {
    const std::string url = "/xyz/xxx.txt";

    std::unique_ptr<SequentialFile> f0;
    std::unique_ptr<RandomAccessFile> f1;
    // Check the specific (mocked) error code is meaningless, because
    // I don't know what it is.
    ASSERT_FALSE(_env.new_sequential_file(url, &f0).ok());
    ASSERT_FALSE(_env.new_random_access_file(url, &f1).ok());
}

// NOLINTNEXTLINE
TEST_F(EnvBrokerTest, test_write_file) {
    const std::string path = "/tmp/1.txt";

    std::unique_ptr<WritableFile> f;
    ASSERT_OK(_env.new_writable_file(path, &f));
    ASSERT_OK(f->append("first line\n"));
    ASSERT_OK(f->append("second line\n"));
    f->close();
    std::string content;
    ASSERT_OK(_env_mem->read_file(path, &content));
    ASSERT_EQ("first line\nsecond line\n", content);
}

// NOLINTNEXTLINE
TEST_F(EnvBrokerTest, test_sequential_read) {
    const std::string path = "/tmp/1.txt";
    const std::string content = "abcdefghijklmnopqrstuvwxyz0123456789";
    ASSERT_OK(_env_mem->create_file(path));
    ASSERT_OK(_env_mem->append_file(path, content));

    std::unique_ptr<SequentialFile> f;
    ASSERT_OK(_env.new_sequential_file(path, &f));
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
    ASSERT_OK(_env_mem->create_file(path));
    ASSERT_OK(_env_mem->append_file(path, content));

    std::unique_ptr<RandomAccessFile> f;
    ASSERT_OK(_env.new_random_access_file(path, &f));
    ASSERT_EQ(content.size(), filesize(f));
    ASSERT_EQ("", read(f, 0, 0));
    ASSERT_EQ("", read(f, 10, 0));
    ASSERT_EQ(content.substr(10, 5), read(f, 10, 5));
    ASSERT_EQ(content.substr(0, 9), read(f, 0, 9));
    ASSERT_EQ(content.substr(1), read(f, 1, 100));
    ASSERT_EQ("a", read(f, 0, 1));

    Slice s(_buff.data(), 1);
    ASSERT_FALSE(f->read_at(content.size(), s).ok());

    f.reset();
}

// NOLINTNEXTLINE
TEST_F(EnvBrokerTest, test_write_exist_file) {
    const std::string path = "/tmp/1.txt";
    ASSERT_OK(_env_mem->create_file(path));
    ASSERT_OK(_env_mem->append_file(path, "old content"));

    std::unique_ptr<WritableFile> f;
    ASSERT_TRUE(_env.new_writable_file(path, &f).is_not_supported());

    auto opts = WritableFileOptions{.mode = Env::CREATE_OR_OPEN_WITH_TRUNCATE};
    ASSERT_TRUE(_env.new_writable_file(opts, path, &f).is_not_supported());

    opts = WritableFileOptions{.mode = Env::MUST_CREATE};
    ASSERT_TRUE(_env.new_writable_file(opts, path, &f).is_already_exist());

    opts = WritableFileOptions{.mode = Env::MUST_EXIST};
    ASSERT_TRUE(_env.new_writable_file(opts, path, &f).is_not_supported());

    opts = WritableFileOptions{.mode = Env::CREATE_OR_OPEN};
    ASSERT_TRUE(_env.new_writable_file(opts, path, &f).is_not_supported());

    // Assume the file still exists.
    std::string content;
    ASSERT_OK(_env_mem->read_file(path, &content));
    ASSERT_EQ("old content", content);
}

// NOLINTNEXTLINE
TEST_F(EnvBrokerTest, test_delete_file) {
    const std::string path = "/tmp/1.txt";

    ASSERT_FALSE(_env.delete_file(path).ok());

    ASSERT_OK(_env_mem->create_file(path));
    ASSERT_OK(_env_mem->append_file(path, "file content"));
    ASSERT_OK(_env.delete_file(path));
    std::string content;
    EXPECT_TRUE(_env_mem->read_file(path, &content).is_not_found());
}

// NOLINTNEXTLINE
TEST_F(EnvBrokerTest, test_check_path_exist) {
    const std::string path = "/tmp/1.txt";

    ASSERT_TRUE(_env.path_exists(path).is_not_found());

    ASSERT_OK(_env_mem->create_file(path));
    ASSERT_OK(_env_mem->append_file(path, "file content"));
    ASSERT_OK(_env.path_exists(path));
}

// NOLINTNEXTLINE
TEST_F(EnvBrokerTest, test_is_directory) {
    ASSERT_OK(_env_mem->create_file("/tmp/1.txt"));

    bool is_dir = false;
    ASSERT_TRUE(_env.is_directory("/xy", &is_dir).is_not_found());
    ASSERT_OK(_env.is_directory("/tmp", &is_dir));
    ASSERT_TRUE(is_dir);
    ASSERT_TRUE(_env.is_directory("/tmp/1.txt", &is_dir).ok());
    ASSERT_FALSE(is_dir);
}

// NOLINTNEXTLINE
TEST_F(EnvBrokerTest, test_link_file) {
    const std::string old_path = "/tmp";
    const std::string new_path = "/ttt";
    ASSERT_TRUE(_env.link_file(old_path, new_path).is_not_supported());
}

// NOLINTNEXTLINE
TEST_F(EnvBrokerTest, test_rename) {
    const std::string old_path = "/tmp";
    const std::string new_path = "/ttt";
    ASSERT_TRUE(_env.rename_file(old_path, new_path).is_not_supported());
}

// NOLINTNEXTLINE
TEST_F(EnvBrokerTest, test_get_modified_time) {
    const std::string path = "/tmp";
    uint64_t ts;
    ASSERT_TRUE(_env.get_file_modified_time(path, &ts).is_not_supported());
}

// NOLINTNEXTLINE
TEST_F(EnvBrokerTest, test_canonicalize) {
    std::string s;
    ASSERT_TRUE(_env.canonicalize("//a//b/", &s).is_not_supported());
}

// NOLINTNEXTLINE
TEST_F(EnvBrokerTest, test_create_dir) {
    ASSERT_TRUE(_env.create_dir("//a//b/").is_not_supported());
}

// NOLINTNEXTLINE
TEST_F(EnvBrokerTest, test_create_dir_if_missing) {
    bool created;
    ASSERT_TRUE(_env.create_dir_if_missing("//a//b/", &created).is_not_supported());
}

// NOLINTNEXTLINE
TEST_F(EnvBrokerTest, test_delete_dir) {
    ASSERT_TRUE(_env.delete_dir("//a//b/").is_not_supported());
}

// NOLINTNEXTLINE
TEST_F(EnvBrokerTest, test_sync_dir) {
    ASSERT_TRUE(_env.sync_dir("//a//b/").is_not_supported());
}

// NOLINTNEXTLINE
TEST_F(EnvBrokerTest, test_get_children) {
    std::vector<std::string> children;
    ASSERT_TRUE(_env.get_children("//a//b/", &children).is_not_supported());
}

} // namespace starrocks
