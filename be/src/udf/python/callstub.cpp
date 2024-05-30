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

#include "udf/python/callstub.h"

#include <dirent.h>
#include <fmt/core.h>
#include <fmt/format.h>
#include <poll.h>
#include <sys/poll.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cstring>
#include <memory>
#include <string>
#include <unordered_set>

#include "arrow/buffer.h"
#include "arrow/flight/client.h"
#include "arrow/type.h"
#include "butil/fd_guard.h"
#include "common/config.h"
#include "common/status.h"
#include "common/statusor.h"
#include "exprs/base64.h"
#include "rapidjson/document.h"
#include "rapidjson/rapidjson.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"
#include "runtime/types.h"
#include "udf/python/env.h"
#include "util/arrow/row_batch.h"
#include "util/arrow/utils.h"
#include "util/defer_op.h"
#include "util/slice.h"

#define RETURN_IF_ARROW_ERROR(expr)    \
    do {                               \
        auto status = to_status(expr); \
        if (!status.ok()) {            \
            return status;             \
        }                              \
    } while (0)

namespace starrocks {

class ChildProcess {
public:
    ChildProcess(pid_t pid) : _pid(pid) {}
    ~ChildProcess() { terminate_and_wait(); }

    void terminate() {
        if (_pid != -1) {
            kill(_pid, SIGKILL);
        }
    }

    void wait() {
        if (_pid != -1) {
            waitpid(_pid, nullptr, 0);
            remove_unix_socket();
            _pid = -1;
        }
    }

    void terminate_and_wait() {
        if (_pid != -1) {
            terminate();
            wait();
            remove_unix_socket();
        }
    }
    void remove_unix_socket();

private:
    pid_t _pid = -1;
};

using ArrowFlightClient = arrow::flight::FlightClient;

class ArrowFlightWithRW {
public:
    using FlightStreamWriter = arrow::flight::FlightStreamWriter;
    using FlightStreamReader = arrow::flight::FlightStreamReader;

    Status init(const std::string& uri_string, const PyFunctionDescriptor& func_desc);
    StatusOr<std::shared_ptr<arrow::RecordBatch>> rpc(arrow::RecordBatch& batch);

    void close();

private:
    bool _begin = false;
    std::unique_ptr<ArrowFlightClient> _arrow_client;
    std::unique_ptr<FlightStreamWriter> _writer;
    std::unique_ptr<FlightStreamReader> _reader;
};

Status ArrowFlightWithRW::init(const std::string& uri_string, const PyFunctionDescriptor& func_desc) {
    using namespace arrow::flight;
    Location location;
    RETURN_IF_ARROW_ERROR(location.Parse(uri_string, &location));
    RETURN_IF_ARROW_ERROR(ArrowFlightClient::Connect(location, &_arrow_client));
    ASSIGN_OR_RETURN(auto command, func_desc.to_json_string());
    FlightDescriptor descriptor = FlightDescriptor::Command(command);
    RETURN_IF_ARROW_ERROR(_arrow_client->DoExchange(descriptor, &_writer, &_reader));
    return Status::OK();
}

StatusOr<std::shared_ptr<arrow::RecordBatch>> ArrowFlightWithRW::rpc(arrow::RecordBatch& batch) {
    if (!_begin) {
        RETURN_IF_ARROW_ERROR(_writer->Begin(batch.schema()));
        _begin = true;
    }
    RETURN_IF_ARROW_ERROR(_writer->WriteRecordBatch(batch));
    arrow::flight::FlightStreamChunk stream_chunk;
    RETURN_IF_ARROW_ERROR(_reader->Next(&stream_chunk));
    return stream_chunk.data;
}

void ArrowFlightWithRW::close() {
    if (_writer != nullptr) {
        WARN_IF_ERROR(to_status(_writer->Close()), "arrow flight rpc close error:");
    }
}

class PyWorkerHandle {
public:
    PyWorkerHandle(std::unique_ptr<ArrowFlightWithRW> arrow_client, std::shared_ptr<ChildProcess> process)
            : _arrow_client(std::move(arrow_client)), _process(std::move(process)) {}

    ArrowFlightWithRW* client() const { return _arrow_client.get(); }

private:
    std::unique_ptr<ArrowFlightWithRW> _arrow_client;
    std::shared_ptr<ChildProcess> _process;
};
using PyWorkerPtr = std::shared_ptr<PyWorkerHandle>;

class PyWorkerManager {
public:
    static StatusOr<PyWorkerPtr> create_worker(const PyFunctionDescriptor& func_desc);

    static std::string unix_socket(pid_t pid) {
        std::string unix_socket = fmt::format("grpc+unix://{}/pyworker_{}", config::local_library_dir, pid);
        return unix_socket;
    }

    static std::string unix_socket_path(pid_t pid) {
        std::string unix_socket_path = fmt::format("{}/pyworker_{}", config::local_library_dir, pid);
        return unix_socket_path;
    }

    static std::string bootstrap() {
        const char* server_main = "flight_server.py";
        return fmt::format("{}/lib/py-packages/{}", getenv("STARROCKS_HOME"), server_main);
    }

private:
    static Status _fork_py_worker(std::unique_ptr<ChildProcess>* child_process, std::string* unix_socket);
};

static Status close_all_fd_except(const std::unordered_set<int>& fds) {
    DIR* dir = opendir("/proc/self/fd");
    auto defer = DeferOp([&dir]() {
        if (dir != nullptr) {
            closedir(dir);
        }
    });

    if (dir == nullptr) {
        return Status::InternalError(fmt::format("open /proc/self/fd error {}", std::strerror(errno)));
    }

    int dir_fd = dirfd(dir);
    if (dir_fd < 0) {
        return Status::InternalError(fmt::format("syscall dirfd error {}", std::strerror(errno)));
    }

    struct dirent* entry;
    while ((entry = readdir(dir)) != nullptr) {
        if (entry->d_type == DT_LNK) {
            int fd = atoi(entry->d_name);
            if (fd >= 0 && fd != dir_fd && fds.count(fd) == 0) {
                close(fd);
            }
        }
    }

    return Status::OK();
}

void ChildProcess::remove_unix_socket() {
    unlink(PyWorkerManager::unix_socket_path(_pid).c_str());
}

StatusOr<PyWorkerPtr> PyWorkerManager::create_worker(const PyFunctionDescriptor& func_desc) {
    std::unique_ptr<ChildProcess> child_process;
    std::string url;
    RETURN_IF_ERROR(_fork_py_worker(&child_process, &url));
    auto arrow_client = std::make_unique<ArrowFlightWithRW>();
    RETURN_IF_ERROR(arrow_client->init(url, func_desc));
    return std::make_shared<PyWorkerHandle>(std::move(arrow_client), std::move(child_process));
}

Status PyWorkerManager::_fork_py_worker(std::unique_ptr<ChildProcess>* child_process, std::string* unix_socket) {
    ASSIGN_OR_RETURN(auto py_env, PythonEnvManager::getInstance().getDefault());

    std::string python_path = py_env.get_python_path();
    int pipefd[2];

    if (pipe(pipefd) == -1) {
        return Status::InternalError(fmt::format("create pipe error:{}", std::strerror(errno)));
    }

    pid_t cpid = fork();
    if (cpid == -1) {
        return Status::InternalError(fmt::format("fork worker error:{}", std::strerror(errno)));
    } else if (cpid == 0) {
        dup2(pipefd[1], STDOUT_FILENO);
        if (config::report_python_worker_error) {
            dup2(pipefd[1], STDERR_FILENO);
        }
        // change dir
        if (chdir(config::local_library_dir.c_str()) != 0) {
            std::cout << "change dir failed:" << std::strerror(errno) << std::endl;
            exit(-1);
        }
        // run child process
        // close all resource
        std::unordered_set<int> reserved_fd{0, 1, 2, pipefd[0]};
        auto status = close_all_fd_except(reserved_fd);
        if (!status.ok()) {
            std::cout << "close fd failed:" << status.to_string() << std::endl;
            exit(-1);
        }

        pid_t self_pid = getpid();
        std::string str_pid = std::to_string(self_pid);
        char command[] = "python3";
        std::string script = PyWorkerManager::bootstrap();
        std::string unix_socket = PyWorkerManager::unix_socket(self_pid);
        std::string python_home_env = fmt::format("PYTHONHOME={}", py_env.home);
        char* const args[] = {command, script.data(), unix_socket.data(), nullptr};
        char* const envs[] = {python_home_env.data(), nullptr};
        // exec flight server
        if (execvpe(python_path.c_str(), args, envs)) {
            std::cout << "execvp failed:" << std::strerror(errno) << std::endl;
            exit(-1);
        }

    } else {
        close(pipefd[1]);
        butil::fd_guard guard(pipefd[0]);
        *child_process = std::make_unique<ChildProcess>(cpid);

        pollfd fds[1];
        fds[0].fd = pipefd[0];
        fds[0].events = POLLIN;

        // wait util worker start
        int ret = poll(fds, 1, config::create_child_worker_timeout_ms);
        if (ret == -1) {
            return Status::InternalError(fmt::format("poll error:{}", std::strerror(errno)));
        } else if (ret == 0) {
            (*child_process)->terminate_and_wait();
            return Status::InternalError("create worker timeout");
        }

        char buffer[4096];
        ssize_t n = read(pipefd[0], buffer, sizeof(buffer));
        Slice result(buffer, n);
        if (result != Slice("Pywork start success\n")) {
            (*child_process)->terminate_and_wait();
            return Status::InternalError(fmt::format("worker start failed:{}", result.to_string()));
        }
        *unix_socket = PyWorkerManager::unix_socket(cpid);
    }
    return Status::OK();
}

StatusOr<std::shared_ptr<arrow::RecordBatch>> ArrowFlightFuncCallStub::do_evaluate(RecordBatch&& batch) {
    size_t num_rows = batch.num_rows();
    ASSIGN_OR_RETURN(auto result_batch, _py_worker_handle->client()->rpc(batch));
    if (result_batch->num_rows() != num_rows) {
        return Status::InternalError(
                fmt::format("unexpected result batch rows from UDF:{} expect:{}", result_batch->num_rows(), num_rows));
    }
    return result_batch;
}

StatusOr<std::shared_ptr<arrow::Schema>> convert_type_to_schema(const TypeDescriptor& typedesc) {
    // conver to field
    arrow::SchemaBuilder schema_builder;
    std::shared_ptr<arrow::Field> field;
    RETURN_IF_ERROR(convert_to_arrow_field(typedesc, "result", true, &field));
    RETURN_IF_ARROW_ERROR(schema_builder.AddField(field));
    std::shared_ptr<arrow::Schema> schema;
    auto result_schema = schema_builder.Finish();
    RETURN_IF_ARROW_ERROR(std::move(result_schema).Value(&schema));
    return schema;
}

StatusOr<std::string> PyFunctionDescriptor::to_json_string() const {
    rapidjson::Document doc;
    doc.SetObject();
    auto& allocator = doc.GetAllocator();

    // Adding basic string properties
    doc.AddMember("symbol", rapidjson::Value().SetString(symbol.c_str(), allocator), allocator);
    doc.AddMember("location", rapidjson::Value().SetString(location.c_str(), allocator), allocator);
    doc.AddMember("input_type", rapidjson::Value().SetString(input_type.c_str(), allocator), allocator);
    doc.AddMember("content", rapidjson::Value().SetString(content.c_str(), content.size(), allocator), allocator);

    {
        // serialize return type schema
        ASSIGN_OR_RETURN(auto schema, convert_type_to_schema(return_type));
        auto serialized_schema_result = arrow::ipc::SerializeSchema(*schema);
        std::shared_ptr<arrow::Buffer> serialized_schema;
        RETURN_IF_ARROW_ERROR(std::move(serialized_schema_result).Value(&serialized_schema));
        const uint8_t* data = serialized_schema->data();
        size_t serialized_size = serialized_schema->size();
        int base64_length = (size_t)(4.0 * ceil((double)serialized_size / 3.0)) + 1;
        char p[base64_length];
        int len = base64_encode2((unsigned char*)data, serialized_size, (unsigned char*)p);
        doc.AddMember("return_type", rapidjson::Value().SetString(p, len, allocator), allocator);
    }

    // Convert document to string
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    doc.Accept(writer);

    auto value = std::string(buffer.GetString(), buffer.GetSize());

    return value;
}

std::unique_ptr<UDFCallStub> build_py_call_stub(FunctionContext* context, const PyFunctionDescriptor& func_desc) {
    auto worker_with_st = PyWorkerManager::create_worker(func_desc);
    if (!worker_with_st.ok()) {
        return create_error_call_stub(worker_with_st.status());
    }
    auto worker = worker_with_st.value();
    return std::make_unique<ArrowFlightFuncCallStub>(context, worker);
}
} // namespace starrocks