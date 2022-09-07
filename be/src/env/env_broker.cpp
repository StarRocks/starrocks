// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "env/env_broker.h"

#include <brpc/uri.h>

#include <chrono>
#include <memory>
#include <string>
#include <thread>

#include "env/env.h"
#include "gen_cpp/FileBrokerService_types.h"
#include "gen_cpp/TFileBrokerService.h"
#include "gutil/strings/substitute.h"
#include "runtime/broker_mgr.h"
#include "runtime/client_cache.h"
#include "runtime/exec_env.h"
#include "util/coding.h"

namespace starrocks {

using BrokerServiceClient = TFileBrokerServiceClient;

#ifdef BE_TEST
namespace {
TFileBrokerServiceClient* g_broker_client = nullptr;
}
void EnvBroker::TEST_set_broker_client(TFileBrokerServiceClient* client) {
    g_broker_client = client;
}
const std::string& get_client_id(const TNetworkAddress& /*broker_addr*/) {
    static std::string s_client_id("test_client_id");
    return s_client_id;
}
#else
const std::string& get_client_id(const TNetworkAddress& broker_addr) {
    return ExecEnv::GetInstance()->broker_mgr()->get_client_id(broker_addr);
}
#endif

inline BrokerServiceClientCache* client_cache() {
    return ExecEnv::GetInstance()->broker_client_cache();
}

static Status to_status(const TBrokerOperationStatus& st) {
    switch (st.statusCode) {
    case TBrokerOperationStatusCode::OK:
        return Status::OK();
    case TBrokerOperationStatusCode::END_OF_FILE:
        return Status::EndOfFile(st.message);
    case TBrokerOperationStatusCode::NOT_AUTHORIZED:
        return Status::IOError("No broker permission, " + st.message);
    case TBrokerOperationStatusCode::DUPLICATE_REQUEST:
        return Status::InternalError("Duplicate broker request, " + st.message);
    case TBrokerOperationStatusCode::INVALID_INPUT_OFFSET:
        return Status::InvalidArgument("Invalid broker offset, " + st.message);
    case TBrokerOperationStatusCode::INVALID_ARGUMENT:
        return Status::InvalidArgument("Invalid broker argument, " + st.message);
    case TBrokerOperationStatusCode::INVALID_INPUT_FILE_PATH:
        return Status::NotFound("Invalid broker file path, " + st.message);
    case TBrokerOperationStatusCode::FILE_NOT_FOUND:
        return Status::NotFound("Broker file not found, " + st.message);
    case TBrokerOperationStatusCode::TARGET_STORAGE_SERVICE_ERROR:
        return Status::InternalError("Broker storage service error, " + st.message);
    case TBrokerOperationStatusCode::OPERATION_NOT_SUPPORTED:
        return Status::NotSupported("Broker operation not supported, " + st.message);
    }
    return Status::InternalError("Unknown broker error, " + st.message);
}

template <typename Method, typename Request, typename Response>
static Status call_method(const TNetworkAddress& broker, Method method, const Request& request, Response* response,
                          int retry_count = 1, int timeout_ms = DEFAULT_TIMEOUT_MS) {
    Status status;
    TFileBrokerServiceClient* client;
#ifndef BE_TEST
    BrokerServiceConnection conn(client_cache(), broker, timeout_ms, &status);
    if (!status.ok()) {
        LOG(WARNING) << "Fail to get broker client: " << status;
        return status;
    }
    client = conn.get();
#else
    client = g_broker_client;
#endif

    while (true) {
        try {
            (client->*method)(*response, request);
            return Status::OK();
        } catch (apache::thrift::transport::TTransportException& e) {
#ifndef BE_TEST
            RETURN_IF_ERROR(conn.reopen());
            client = conn.get();
#endif
            if (retry_count-- > 0) {
                std::this_thread::sleep_for(std::chrono::seconds(1));
            } else {
                return Status::ThriftRpcError(e.what());
            }
        } catch (apache::thrift::TException& e) {
            return Status::ThriftRpcError(e.what());
        }
    }
}

// This function will *NOT* return EOF status.
static Status broker_pread(void* buff, const TNetworkAddress& broker, const TBrokerFD& fd, int64_t offset,
                           int64_t* length) {
    int64_t bytes_read = 0;
    while (bytes_read < *length) {
        TBrokerPReadRequest request;
        TBrokerReadResponse response;

        request.__set_version(TBrokerVersion::VERSION_ONE);
        request.__set_fd(fd);
        request.__set_offset(offset + bytes_read);
        request.__set_length(*length - bytes_read);

        RETURN_IF_ERROR(call_method(broker, &BrokerServiceClient::pread, request, &response));

        if (response.opStatus.statusCode == TBrokerOperationStatusCode::END_OF_FILE) {
            break;
        } else if (response.opStatus.statusCode != TBrokerOperationStatusCode::OK) {
            return to_status(response.opStatus);
        } else if (response.data.empty()) {
            break;
        }
        memcpy((char*)buff + bytes_read, response.data.data(), response.data.size());
        bytes_read += static_cast<int64_t>(response.data.size());
    }
    *length = bytes_read;
    return Status::OK();
}

static void broker_close_reader(const TNetworkAddress& broker, const TBrokerFD& fd) {
    TBrokerCloseReaderRequest request;
    TBrokerOperationStatus response;

    request.__set_version(TBrokerVersion::VERSION_ONE);
    request.__set_fd(fd);

    Status st = call_method(broker, &BrokerServiceClient::closeReader, request, &response);
    LOG_IF(WARNING, !st.ok()) << "Fail to close broker reader, " << st.to_string();
}

static Status broker_close_writer(const TNetworkAddress& broker, const TBrokerFD& fd, int timeout_ms) {
    TBrokerCloseWriterRequest request;
    TBrokerOperationStatus response;

    request.__set_version(TBrokerVersion::VERSION_ONE);
    request.__set_fd(fd);

    Status st = call_method(broker, &BrokerServiceClient::closeWriter, request, &response, 1, timeout_ms);
    if (!st.ok()) {
        LOG(WARNING) << "Fail to close broker writer: " << st;
        return st;
    }
    if (response.statusCode != TBrokerOperationStatusCode::OK) {
        LOG(WARNING) << "Fail to close broker writer: " << response.message;
        return to_status(response);
    }
    return Status::OK();
}

class BrokerRandomAccessFile : public RandomAccessFile {
public:
    BrokerRandomAccessFile(const TNetworkAddress& broker, std::string path, const TBrokerFD& fd, int64_t size)
            : _broker(broker), _path(std::move(path)), _fd(fd), _size(size) {}

    ~BrokerRandomAccessFile() override { broker_close_reader(_broker, _fd); }

    // Return OK if reached end of file in order to be compatible with posix env.
    StatusOr<int64_t> read_at(int64_t offset, void* data, int64_t size) const override {
        Status st = broker_pread(data, _broker, _fd, offset, &size);
        if (st.ok()) {
            return size;
        }
        LOG_IF(WARNING, !st.ok()) << "Fail to read " << _path << ", " << st.message();
        return st;
    }

    Status read_at_fully(int64_t offset, void* data, int64_t size) const override {
        int64_t nread = size;
        RETURN_IF_ERROR(broker_pread(data, _broker, _fd, offset, &nread));
        if (nread < size) {
            LOG(WARNING) << "Fail to read from " << _path << ", partial read expect=" << size << " real=" << nread;
            return Status::IOError("Partial read");
        }
        return Status::OK();
    }

    Status readv_at(uint64_t offset, const Slice* res, size_t res_cnt) const override {
        for (size_t i = 0; i < res_cnt; i++) {
            RETURN_IF_ERROR(read_at_fully(offset, res[i].data, res[i].size));
            offset += res[i].size;
        }
        return Status::OK();
    }

    Status size(uint64_t* size) const override {
        *size = _size;
        return Status::OK();
    }

    const std::string& filename() const override { return _path; }

private:
    TNetworkAddress _broker;
    std::string _path;
    TBrokerFD _fd;
    int64_t _size;
};

class BrokerSequentialFile : public SequentialFile {
public:
    explicit BrokerSequentialFile(std::unique_ptr<RandomAccessFile> random_file) : _file(std::move(random_file)) {}

    ~BrokerSequentialFile() override = default;

    StatusOr<int64_t> read(void* data, int64_t size) override {
        ASSIGN_OR_RETURN(auto nread, _file->read_at(_offset, data, size));
        _offset += nread;
        return nread;
    }

    Status skip(uint64_t n) override {
        _offset += n;
        return Status::OK();
    }

    const std::string& filename() const override { return _file->filename(); }

private:
    std::unique_ptr<RandomAccessFile> _file;
    size_t _offset = 0;
};

class BrokerWritableFile : public WritableFile {
public:
    BrokerWritableFile(const TNetworkAddress& broker, std::string path, const TBrokerFD& fd, size_t offset,
                       int timeout_ms)
            : _broker(broker), _path(std::move(path)), _fd(fd), _offset(offset), _timeout_ms(timeout_ms) {}

    ~BrokerWritableFile() override { (void)BrokerWritableFile::close(); }

    Status append(const Slice& data) override {
        TBrokerPWriteRequest request;
        TBrokerOperationStatus response;
        request.__set_version(TBrokerVersion::VERSION_ONE);
        request.__set_fd(_fd);
        request.__set_offset(static_cast<int64_t>(_offset));
        request.__set_data(data.to_string());

        Status st = call_method(_broker, &BrokerServiceClient::pwrite, request, &response, 0, _timeout_ms);
        if (!st.ok()) {
            LOG(WARNING) << "Fail to append " << _path << ": " << st;
            return st;
        }
        if (response.statusCode != TBrokerOperationStatusCode::OK) {
            LOG(WARNING) << "Fail to append " << _path << ": " << response.message;
            return to_status(response);
        }
        _offset += data.size;
        return Status::OK();
    }

    Status appendv(const Slice* data, size_t cnt) override {
        for (size_t i = 0; i < cnt; i++) {
            RETURN_IF_ERROR(append(data[i]));
        }
        return Status::OK();
    }

    Status pre_allocate(uint64_t size) override { return Status::NotSupported("BrokerWritableFile::pre_allocate"); }

    Status close() override {
        if (_closed) {
            return Status::OK();
        }
        Status st = broker_close_writer(_broker, _fd, _timeout_ms);
        _closed = true;
        return st;
    }

    Status flush(FlushMode mode) override { return Status::OK(); }

    Status sync() override {
        LOG(WARNING) << "Ignored sync " << _path;
        return Status::OK();
    }

    uint64_t size() const override { return _offset; }

    const std::string& filename() const override { return _path; }

private:
    TNetworkAddress _broker;
    std::string _path;
    TBrokerFD _fd;
    size_t _offset;
    bool _closed = false;
    int _timeout_ms = DEFAULT_TIMEOUT_MS;
};

StatusOr<std::unique_ptr<SequentialFile>> EnvBroker::new_sequential_file(const std::string& path) {
    ASSIGN_OR_RETURN(auto random_file, new_random_access_file(path));
    return std::make_unique<BrokerSequentialFile>(std::move(random_file));
}

StatusOr<std::unique_ptr<RandomAccessFile>> EnvBroker::new_random_access_file(const std::string& path) {
    return new_random_access_file(RandomAccessFileOptions(), path);
}

StatusOr<std::unique_ptr<RandomAccessFile>> EnvBroker::new_random_access_file(const RandomAccessFileOptions& opts,
                                                                              const std::string& path) {
    TBrokerOpenReaderRequest request;
    TBrokerOpenReaderResponse response;
    request.__set_path(path);
    request.__set_clientId(get_client_id(_broker_addr));
    request.__set_startOffset(0);
    request.__set_version(TBrokerVersion::VERSION_ONE);
    request.__set_properties(_properties);

    Status st = call_method(_broker_addr, &BrokerServiceClient::openReader, request, &response);
    if (!st.ok()) {
        LOG(WARNING) << "Fail to open " << path << ": " << st;
        return st;
    }
    if (response.opStatus.statusCode != TBrokerOperationStatusCode::OK) {
        LOG(WARNING) << "Fail to open " << path << ": " << response.opStatus.message;
        return to_status(response.opStatus);
    }

    // Get file size
    uint64_t size;
    RETURN_IF_ERROR(_get_file_size(path, &size));
    return std::make_unique<BrokerRandomAccessFile>(_broker_addr, path, response.fd, size);
}

StatusOr<std::unique_ptr<WritableFile>> EnvBroker::new_writable_file(const std::string& path) {
    return new_writable_file(WritableFileOptions(), path);
}

StatusOr<std::unique_ptr<WritableFile>> EnvBroker::new_writable_file(const WritableFileOptions& opts,
                                                                     const std::string& path) {
    if (opts.mode == CREATE_OR_OPEN_WITH_TRUNCATE) {
        if (auto st = _path_exists(path); st.ok()) {
            return Status::NotSupported("Cannot truncate a file by broker");
        }
    } else if (opts.mode == MUST_CREATE) {
        if (auto st = _path_exists(path); st.ok()) {
            return Status::AlreadyExist(path);
        }
    } else if (opts.mode == MUST_EXIST) {
        return Status::NotSupported("Open with MUST_EXIST not supported by broker");
    } else if (opts.mode == CREATE_OR_OPEN) {
        if (auto st = _path_exists(path); st.ok()) {
            return Status::NotSupported("Cannot write an already exists file through broker");
        }
    } else {
        auto msg = strings::Substitute("Unsupported open mode $0", opts.mode);
        return Status::NotSupported(msg);
    }

    TBrokerOpenWriterRequest request;
    TBrokerOpenWriterResponse response;

    request.__set_path(path);
    request.__set_version(TBrokerVersion::VERSION_ONE);
    request.__set_openMode(TBrokerOpenMode::APPEND);
    request.__set_clientId(get_client_id(_broker_addr));
    request.__set_properties(_properties);

    Status st = call_method(_broker_addr, &BrokerServiceClient::openWriter, request, &response, 1, _timeout_ms);
    if (!st.ok()) {
        LOG(WARNING) << "Fail to open " << path << ": " << st;
        return st;
    }

    if (response.opStatus.statusCode != TBrokerOperationStatusCode::OK) {
        LOG(WARNING) << "Fail to open " << path << ": " << response.opStatus.message;
        return to_status(response.opStatus);
    }

    return std::make_unique<BrokerWritableFile>(_broker_addr, path, response.fd, 0, _timeout_ms);
}

StatusOr<std::unique_ptr<RandomRWFile>> EnvBroker::new_random_rw_file(const std::string& path) {
    return Status::NotSupported("EnvBroker::new_random_rw_file");
}

StatusOr<std::unique_ptr<RandomRWFile>> EnvBroker::new_random_rw_file(const RandomRWFileOptions& opts,
                                                                      const std::string& path) {
    return Status::NotSupported("BrokerEnv::new_random_rw_file");
}

Status EnvBroker::path_exists(const std::string& path) {
    return _path_exists(path);
}

Status EnvBroker::_path_exists(const std::string& path) {
    TBrokerCheckPathExistRequest request;
    TBrokerCheckPathExistResponse response;
    request.__set_properties(_properties);
    request.__set_path(path);
    request.__set_version(TBrokerVersion::VERSION_ONE);
    RETURN_IF_ERROR(call_method(_broker_addr, &BrokerServiceClient::checkPathExist, request, &response));
    if (response.opStatus.statusCode != TBrokerOperationStatusCode::OK) {
        return to_status(response.opStatus);
    }
    return response.isPathExist ? Status::OK() : Status::NotFound(path);
}

Status EnvBroker::get_children(const std::string& dir, std::vector<std::string>* file) {
    return Status::NotSupported("EnvBroker::get_children");
}

Status EnvBroker::iterate_dir(const std::string& dir, const std::function<bool(const char*)>& cb) {
    std::vector<std::string> files;
    RETURN_IF_ERROR(get_children(dir, &files));
    for (const auto& f : files) {
        if (!cb(f.c_str())) {
            break;
        }
    }
    return Status::OK();
}

Status EnvBroker::delete_file(const std::string& path) {
    return _delete_file(path);
}

Status EnvBroker::_delete_file(const std::string& path) {
    TBrokerDeletePathRequest request;
    TBrokerOperationStatus response;
    request.__set_version(TBrokerVersion::VERSION_ONE);
    request.__set_path(path);
    request.__set_properties(_properties);

    Status st = call_method(_broker_addr, &BrokerServiceClient::deletePath, request, &response);
    if (!st.ok()) {
        LOG(WARNING) << "Fail to delete " << path << ": " << st.message();
        return st;
    }
    st = to_status(response);
    if (st.ok()) {
        LOG(INFO) << "Deleted " << path;
    } else {
        LOG(WARNING) << "Fail to delete " << path << ": " << st.message();
    }
    return st;
}

Status EnvBroker::create_dir(const std::string& dirname) {
    return Status::NotSupported("BrokerEnv::create_dir");
}

Status EnvBroker::create_dir_if_missing(const std::string& dirname, bool* created) {
    return Status::NotSupported("BrokerEnv::create_dir_if_missing");
}

Status EnvBroker::delete_dir(const std::string& dirname) {
    return Status::NotSupported("BrokerEnv::delete_dir");
}

Status EnvBroker::sync_dir(const std::string& dirname) {
    return Status::NotSupported("BrokerEnv::sync_dir");
}

Status EnvBroker::is_directory(const std::string& path, bool* is_dir) {
    TBrokerFileStatus stat;
    RETURN_IF_ERROR(_list_file(path, &stat));
    *is_dir = stat.isDir;
    return Status::OK();
}

Status EnvBroker::canonicalize(const std::string& path, std::string* file) {
    return Status::NotSupported("BrokerEnv::canonicalize");
}

Status EnvBroker::_get_file_size(const std::string& path, uint64_t* size) {
    TBrokerFileStatus stat;
    Status st = _list_file(path, &stat);
    *size = stat.size;
    return st;
}

Status EnvBroker::get_file_size(const std::string& path, uint64_t* size) {
    return _get_file_size(path, size);
}

Status EnvBroker::get_file_modified_time(const std::string& path, uint64_t* file_mtime) {
    return Status::NotSupported("BrokerEnv::get_file_modified_time");
}

Status EnvBroker::rename_file(const std::string& src, const std::string& target) {
    return Status::NotSupported("BrokerEnv::rename_file");
}

Status EnvBroker::link_file(const std::string& old_path, const std::string& new_path) {
    return Status::NotSupported("BrokerEnv::link_file");
}

Status EnvBroker::_list_file(const std::string& path, TBrokerFileStatus* stat) {
    TBrokerListPathRequest request;
    TBrokerListResponse response;
    request.__set_version(TBrokerVersion::VERSION_ONE);
    request.__set_fileNameOnly(true);
    request.__set_isRecursive(false);
    request.__set_path(path);
    request.__set_properties(_properties);
    RETURN_IF_ERROR(call_method(_broker_addr, &BrokerServiceClient::listPath, request, &response));
    if (response.opStatus.statusCode == TBrokerOperationStatusCode::FILE_NOT_FOUND ||
        response.opStatus.statusCode == TBrokerOperationStatusCode::NOT_AUTHORIZED) {
        return Status::NotFound(path);
    } else if (response.opStatus.statusCode == TBrokerOperationStatusCode::OK) {
        if (response.files.size() != 1) {
            return Status::InternalError(strings::Substitute("unexpected file list size=$0", response.files.size()));
        }
        swap(*stat, response.files[0]);
        return Status::OK();
    } else {
        return to_status(response.opStatus);
    }
}

} // namespace starrocks
