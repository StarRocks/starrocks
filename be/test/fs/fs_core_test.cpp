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

#include <gtest/gtest.h>

#include <algorithm>

#include "base/testutil/assert.h"
#include "common/config_local_io_fwd.h"
#include "fs/fs.h"
#include "fs/fs_factory.h"
#include "io/core/array_input_stream.h"

namespace starrocks {

class MockWritableFile final : public WritableFile {
public:
    explicit MockWritableFile(std::string filename) : _filename(std::move(filename)) {}
    ~MockWritableFile() override = default;

    Status append(const Slice& data) override {
        _size += data.size;
        return Status::OK();
    }

    Status appendv(const Slice* data, size_t cnt) override {
        for (size_t i = 0; i < cnt; ++i) {
            _size += data[i].size;
        }
        return Status::OK();
    }

    Status pre_allocate(uint64_t size) override { return Status::OK(); }
    Status close() override { return Status::OK(); }
    Status flush(FlushMode mode) override { return Status::OK(); }
    Status sync() override { return Status::OK(); }
    uint64_t size() const override { return _size; }
    const std::string& filename() const override { return _filename; }

private:
    std::string _filename;
    uint64_t _size = 0;
};

TEST(FSCoreTest, random_access_file_from_sets_encryption_flag) {
    std::string content = "0123456789";
    auto stream = std::make_unique<io::ArrayInputStream>(content.data(), static_cast<int64_t>(content.size()));

    FileEncryptionInfo encryption_info;
    encryption_info.algorithm = EncryptionAlgorithmPB::AES_128;
    encryption_info.key = "0000000000000000";

    auto file = RandomAccessFile::from(std::move(stream), "mock-file", false, encryption_info);
    ASSERT_TRUE(file->is_encrypted());
}

TEST(FSCoreTest, file_write_history_tracks_open_and_close) {
    const int64_t old_history_size = config::file_write_history_size;
    config::file_write_history_size = 16;

    MockWritableFile file("fs_core_test_file");
    ASSERT_OK(file.append(Slice("abc")));

    FileSystem::on_file_write_open(&file);
    FileSystem::on_file_write_close(&file);

    std::vector<FileWriteStat> stats;
    FileSystem::get_file_write_history(&stats);

    auto it = std::find_if(stats.begin(), stats.end(), [](const FileWriteStat& stat) {
        return stat.path == "fs_core_test_file" && stat.size == 3 && stat.close_time >= stat.open_time;
    });
    ASSERT_TRUE(it != stats.end());

    config::file_write_history_size = old_history_size;
}

TEST(FSCoreTest, factory_uses_default_posix_registry) {
    ASSIGN_OR_ABORT(auto fs, FileSystemFactory::CreateSharedFromString("posix://"));
    ASSERT_EQ(FileSystem::POSIX, fs->type());
}

} // namespace starrocks
