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

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <atomic>
#include <cstdlib>
#include <filesystem>
#include <string_view>

#include "base/failpoint/fail_point.h"
#include "base/testutil/assert.h"
#include "base/utility/defer_op.h"
#include "column/chunk.h"
#include "column/fixed_length_column.h"
#include "connector/common/partition_chunk_writer.h"
#include "formats/io/async_flush_stream_poller.h"
#include "formats/parquet/parquet_file_writer.h"
#include "fs/fs_memory.h"
#include "fs/writable_file_wrapper.h"
#include "fs_ext/hdfs/fs_hdfs.h"
#include "runtime/runtime_state.h"

namespace starrocks::connector {

DECLARE_FAIL_POINT(parquet_chunk_writer_init_failed);

namespace {

class CloseFailingFile final : public WritableFileWrapper {
public:
    explicit CloseFailingFile(std::unique_ptr<WritableFile> file)
            : WritableFileWrapper(file.release(), kTakesOwnership) {}

    Status close() override {
        RETURN_IF_ERROR(WritableFileWrapper::close());
        return Status::IOError("injected close failure");
    }
};

class WriterInitTestFileSystem final : public MemoryFileSystem {
public:
    explicit WriterInitTestFileSystem(FileSystem* fs) : _fs(fs) {}

    StatusOr<std::unique_ptr<WritableFile>> new_writable_file(const WritableFileOptions& options,
                                                              const std::string& path) override {
        auto write_options = options;
        // The POSIX test writes an unaligned Parquet buffer when closing the first file.
        if (_fs->type() == FileSystem::POSIX) {
            write_options.direct_write = false;
        }
        ASSIGN_OR_RETURN(auto file, _fs->new_writable_file(write_options, path));
        if (fail_close) {
            return std::make_unique<CloseFailingFile>(std::move(file));
        }
        return file;
    }

    Status delete_file(const std::string& path) override {
        deleted_path = path;
        if (fail_delete) {
            return Status::IOError("injected delete failure");
        }
        return _fs->delete_file(path);
    }

    bool fail_close = false;
    bool fail_delete = false;
    std::string deleted_path;

private:
    FileSystem* _fs;
};

class CleanupWarning final : public google::LogSink {
public:
    CleanupWarning(std::string path, std::string_view reason) : _path(std::move(path)), _reason(reason) {
        google::AddLogSink(this);
    }
    ~CleanupWarning() override { google::RemoveLogSink(this); }

    void send(google::LogSeverity severity, const char*, const char*, int, const google::LogMessageTime&,
              const char* message, size_t length) override {
        std::string_view text(message, length);
        if (severity == google::GLOG_WARNING && text.find(_path) != text.npos && text.find(_reason) != text.npos) {
            seen = true;
        }
    }

    std::atomic<bool> seen = false;

private:
    const std::string _path;
    const std::string_view _reason;
};

class PartitionChunkWriterInitTest : public testing::TestWithParam<bool> {
protected:
    void SetUp() override {
        char path[] = "/tmp/partition_writer_init_XXXXXX";
        ASSERT_NE(mkdtemp(path), nullptr);
        _root = path;
        if (GetParam()) {
            _hadoop_fs = new_fs_hdfs(FSOptions());
        }
        _fs = std::make_shared<WriterInitTestFileSystem>(GetParam() ? _hadoop_fs.get() : FileSystem::Default());
        _state.init_mem_trackers(TUniqueId{}, nullptr);
    }

    void TearDown() override {
        if (!_root.empty()) {
            std::filesystem::remove_all(_root);
        }
    }

    void make_writer(TCompressionType::type compression) {
        auto evaluators = std::make_shared<std::vector<std::unique_ptr<ColumnEvaluator>>>();
        evaluators->push_back(std::make_unique<ColumnSlotIdEvaluator>(0, TypeDescriptor(TYPE_INT)));
        auto factory = std::make_shared<formats::ParquetFileWriterFactory>(
                _fs, compression, std::map<std::string, std::string>{}, std::vector<std::string>{"id"}, evaluators,
                std::nullopt, nullptr, &_state);
        auto location = std::make_shared<LocationProvider>(uri_prefix() + _root, "query", 0, 0, "parquet");
        auto ctx = std::make_shared<BufferPartitionChunkWriterContext>();
        ctx->file_writer_factory = factory;
        ctx->location_provider = location;
        // Rotate after the first chunk, whose buffered size exceeds the four-byte Parquet header.
        ctx->max_file_size = 5;
        ctx->is_default_partition = true;
        ASSERT_OK(factory->init());
        _writer = std::make_unique<BufferPartitionChunkWriter>("", std::vector<int8_t>{}, ctx);
        _writer->set_io_poller(&_poller);
        _writer->set_commit_callback([this](const CommitResult& result) { _committed = result.file_result.location; });
    }

    std::string uri_prefix() const { return GetParam() ? "file://" : ""; }

    std::string name(int index) const { return "query_0_0_" + std::to_string(index) + ".parquet"; }

    std::string file_path(int index) const { return uri_prefix() + _root + "/" + name(index); }

    void expect_files(int index, bool exist) const {
        EXPECT_EQ(exist, std::filesystem::exists(std::filesystem::path(_root) / name(index)));
        if (GetParam()) {
            EXPECT_EQ(exist, std::filesystem::exists(std::filesystem::path(_root) / ("." + name(index) + ".crc")));
        }
    }

    void expect_zlib_error(const Status& status) {
        EXPECT_TRUE(status.is_not_supported()) << status;
        EXPECT_EQ("not supported compression type ZLIB", status.message());
    }

    std::string _root;
    std::unique_ptr<FileSystem> _hadoop_fs;
    std::shared_ptr<WriterInitTestFileSystem> _fs;
    RuntimeState _state{TQueryGlobals{}};
    formats::AsyncFlushStreamPoller _poller;
    std::string _committed;
    std::unique_ptr<BufferPartitionChunkWriter> _writer;
};

TEST_P(PartitionChunkWriterInitTest, removes_first_file) {
    ASSERT_NO_FATAL_FAILURE(make_writer(TCompressionType::ZLIB));
    expect_zlib_error(_writer->init());
    expect_files(0, false);
    EXPECT_EQ(file_path(0), _fs->deleted_path);
    EXPECT_EQ(nullptr, _writer->file_writer());
}

TEST_P(PartitionChunkWriterInitTest, removes_rotated_file) {
    ASSERT_NO_FATAL_FAILURE(make_writer(TCompressionType::SNAPPY));
    ASSERT_OK(_writer->init());
    auto chunk = std::make_shared<Chunk>();
    auto ids = Int32Column::create();
    ids->append(7);
    chunk->append_column(std::move(ids), 0);
    ASSERT_OK(_writer->write(chunk));
    ASSERT_GE(_writer->get_written_bytes(), 5);

    PFailPointTriggerMode mode;
    mode.set_mode(FailPointTriggerModeType::ENABLE);
    fp_parquet_chunk_writer_init_failed.setMode(mode);
    DeferOp disable([&] {
        mode.set_mode(FailPointTriggerModeType::DISABLE);
        fp_parquet_chunk_writer_init_failed.setMode(mode);
    });
    auto status = _writer->write(chunk);

    EXPECT_TRUE(status.is_internal_error()) << status;
    EXPECT_EQ("Create file writer failed due to fail point", status.message());
    EXPECT_EQ(file_path(0), _committed);
    EXPECT_EQ(file_path(1), _fs->deleted_path);
    EXPECT_EQ(nullptr, _writer->file_writer());
    const auto [io_status, finished] = _poller.poll();
    EXPECT_OK(io_status);
    EXPECT_TRUE(finished);
    expect_files(0, true);
    expect_files(1, false);
}

TEST_P(PartitionChunkWriterInitTest, deletes_even_if_close_fails) {
    ASSERT_NO_FATAL_FAILURE(make_writer(TCompressionType::ZLIB));
    _fs->fail_close = true;
    CleanupWarning warning(file_path(0), "injected close failure");
    expect_zlib_error(_writer->init());
    EXPECT_TRUE(warning.seen);
    EXPECT_EQ(file_path(0), _fs->deleted_path);
    expect_files(0, false);
}

TEST_P(PartitionChunkWriterInitTest, warns_if_delete_fails) {
    ASSERT_NO_FATAL_FAILURE(make_writer(TCompressionType::ZLIB));
    _fs->fail_delete = true;
    CleanupWarning warning(file_path(0), "injected delete failure");
    expect_zlib_error(_writer->init());
    EXPECT_EQ(file_path(0), _fs->deleted_path);
    EXPECT_TRUE(warning.seen);
    expect_files(0, true);
}

INSTANTIATE_TEST_SUITE_P(Posix, PartitionChunkWriterInitTest, testing::Values(false));
INSTANTIATE_TEST_SUITE_P(HdfsFileSystemTest, PartitionChunkWriterInitTest, testing::Values(true));

} // namespace
} // namespace starrocks::connector
