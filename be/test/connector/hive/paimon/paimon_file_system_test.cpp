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

#include "connector/hive/paimon/paimon_file_system.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <string>
#include <vector>

#include "base/testutil/assert.h"
#include "base/url_coding.h"
#include "formats/scan_context.h"
#include "fs/fs.h"
#include "fs/fs_memory.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/table/source/data_split.h"
#include "paimon/table/source/split.h"

namespace starrocks {
namespace {

class ObjectStoreLikeMemoryFileSystem final : public MemoryFileSystem {
public:
    Status path_exists(const std::string& path) override {
        return Status::NotSupported("object stores do not implement path_exists");
    }
};

} // namespace

class PaimonFileSystemTest : public ::testing::Test {
protected:
    void SetUp() override {
        _local_fs = FileSystem::Default();
        (void)_local_fs->delete_dir_recursive(_root);
        ASSERT_OK(_local_fs->create_dir_recursive(_child_dir));

        ASSIGN_OR_ABORT(auto file, _local_fs->new_writable_file(_data_path));
        ASSERT_OK(file->append(_content));
        ASSERT_OK(file->close());
    }

    void TearDown() override { (void)_local_fs->delete_dir_recursive(_root); }

    const std::string _root = "./ut_dir/paimon_file_system_test";
    const std::string _child_dir = _root + "/child";
    const std::string _data_path = _root + "/data.bin";
    const std::string _content = "abcdefghij";
    FileSystem* _local_fs = nullptr;
};

TEST_F(PaimonFileSystemTest, OpenReadSeekAndReadAsync) {
    MemoryFileSystem memory_fs;
    ASSERT_OK(memory_fs.append_file("/data.bin", _content));

    FormatScannerStats fs_stats;
    FormatScannerStats app_stats;
    PaimonFileSystem file_system(&memory_fs, &fs_stats, &app_stats);

    auto open_result = file_system.Open("/data.bin");
    ASSERT_TRUE(open_result.ok()) << open_result.status().ToString();
    auto input = std::move(open_result).value();

    auto uri_result = input->GetUri();
    ASSERT_TRUE(uri_result.ok()) << uri_result.status().ToString();
    EXPECT_EQ("/data.bin", uri_result.value());

    auto length_result = input->Length();
    ASSERT_TRUE(length_result.ok()) << length_result.status().ToString();
    EXPECT_EQ(_content.size(), length_result.value());

    std::string sequential_buffer(4, '\0');
    auto read_result = input->Read(sequential_buffer.data(), sequential_buffer.size());
    ASSERT_TRUE(read_result.ok()) << read_result.status().ToString();
    EXPECT_EQ(4, read_result.value());
    EXPECT_EQ("abcd", sequential_buffer);

    auto position_result = input->GetPos();
    ASSERT_TRUE(position_result.ok()) << position_result.status().ToString();
    EXPECT_EQ(4, position_result.value());

    std::string positional_buffer(3, '\0');
    read_result = input->Read(positional_buffer.data(), positional_buffer.size(), 5);
    ASSERT_TRUE(read_result.ok()) << read_result.status().ToString();
    EXPECT_EQ(3, read_result.value());
    EXPECT_EQ("fgh", positional_buffer);

    position_result = input->GetPos();
    ASSERT_TRUE(position_result.ok()) << position_result.status().ToString();
    EXPECT_EQ(4, position_result.value());

    EXPECT_TRUE(input->Seek(2, paimon::FS_SEEK_SET).ok());
    EXPECT_TRUE(input->Seek(2, paimon::FS_SEEK_CUR).ok());
    position_result = input->GetPos();
    ASSERT_TRUE(position_result.ok()) << position_result.status().ToString();
    EXPECT_EQ(4, position_result.value());

    EXPECT_TRUE(input->Seek(-2, paimon::FS_SEEK_END).ok());
    position_result = input->GetPos();
    ASSERT_TRUE(position_result.ok()) << position_result.status().ToString();
    EXPECT_EQ(8, position_result.value());
    EXPECT_TRUE(input->Seek(-11, paimon::FS_SEEK_END).IsInvalid());

    std::string async_buffer(4, '\0');
    bool callback_invoked = false;
    paimon::Status async_status;
    input->ReadAsync(async_buffer.data(), async_buffer.size(), 1, [&](paimon::Status status) {
        callback_invoked = true;
        async_status = std::move(status);
    });
    EXPECT_TRUE(callback_invoked);
    EXPECT_TRUE(async_status.ok()) << async_status.ToString();
    EXPECT_EQ("bcde", async_buffer);

    std::string short_buffer(3, '\0');
    callback_invoked = false;
    input->ReadAsync(short_buffer.data(), short_buffer.size(), 9, [&](paimon::Status status) {
        callback_invoked = true;
        async_status = std::move(status);
    });
    EXPECT_TRUE(callback_invoked);
    EXPECT_TRUE(async_status.IsIOError()) << async_status.ToString();
    EXPECT_EQ('j', short_buffer[0]);

    EXPECT_EQ(4, fs_stats.io_count);
    EXPECT_EQ(12, fs_stats.bytes_read);
    EXPECT_EQ(fs_stats.io_count, app_stats.io_count);
    EXPECT_EQ(fs_stats.bytes_read, app_stats.bytes_read);
    EXPECT_TRUE(input->Close().ok());

    auto missing_result = file_system.Open("/missing.bin");
    EXPECT_FALSE(missing_result.ok());
    EXPECT_TRUE(missing_result.status().IsNotExist()) << missing_result.status().ToString();
}

TEST_F(PaimonFileSystemTest, GetStatusListAndExists) {
    PaimonFileSystem file_system(_local_fs, nullptr, nullptr);

    auto exists_result = file_system.Exists(_data_path);
    ASSERT_TRUE(exists_result.ok()) << exists_result.status().ToString();
    EXPECT_TRUE(exists_result.value());

    exists_result = file_system.Exists(_root + "/missing");
    ASSERT_TRUE(exists_result.ok()) << exists_result.status().ToString();
    EXPECT_FALSE(exists_result.value());

    auto file_status_result = file_system.GetFileStatus(_data_path);
    ASSERT_TRUE(file_status_result.ok()) << file_status_result.status().ToString();
    auto file_status = std::move(file_status_result).value();
    EXPECT_EQ(_data_path, file_status->GetPath());
    EXPECT_FALSE(file_status->IsDir());
    EXPECT_EQ(_content.size(), file_status->GetLen());
    ASSIGN_OR_ABORT(auto starrocks_modification_time, _local_fs->get_file_modified_time(_data_path));
    EXPECT_EQ(starrocks_modification_time * 1000, file_status->GetModificationTime());

    auto directory_status_result = file_system.GetFileStatus(_child_dir);
    ASSERT_TRUE(directory_status_result.ok()) << directory_status_result.status().ToString();
    auto directory_status = std::move(directory_status_result).value();
    EXPECT_EQ(_child_dir, directory_status->GetPath());
    EXPECT_TRUE(directory_status->IsDir());

    auto missing_status_result = file_system.GetFileStatus(_root + "/missing");
    EXPECT_FALSE(missing_status_result.ok());
    EXPECT_TRUE(missing_status_result.status().IsNotExist()) << missing_status_result.status().ToString();

    std::vector<std::unique_ptr<paimon::BasicFileStatus>> basic_statuses;
    auto list_status = file_system.ListDir(_root, &basic_statuses);
    ASSERT_TRUE(list_status.ok()) << list_status.ToString();
    std::sort(basic_statuses.begin(), basic_statuses.end(),
              [](const auto& lhs, const auto& rhs) { return lhs->GetPath() < rhs->GetPath(); });
    ASSERT_EQ(2, basic_statuses.size());
    EXPECT_EQ(_child_dir, basic_statuses[0]->GetPath());
    EXPECT_TRUE(basic_statuses[0]->IsDir());
    EXPECT_EQ(_data_path, basic_statuses[1]->GetPath());
    EXPECT_FALSE(basic_statuses[1]->IsDir());

    std::vector<std::unique_ptr<paimon::FileStatus>> detailed_statuses;
    list_status = file_system.ListFileStatus(_root, &detailed_statuses);
    ASSERT_TRUE(list_status.ok()) << list_status.ToString();
    std::sort(detailed_statuses.begin(), detailed_statuses.end(),
              [](const auto& lhs, const auto& rhs) { return lhs->GetPath() < rhs->GetPath(); });
    ASSERT_EQ(2, detailed_statuses.size());
    EXPECT_EQ(_child_dir, detailed_statuses[0]->GetPath());
    EXPECT_TRUE(detailed_statuses[0]->IsDir());
    EXPECT_EQ(_data_path, detailed_statuses[1]->GetPath());
    EXPECT_FALSE(detailed_statuses[1]->IsDir());
    EXPECT_EQ(_content.size(), detailed_statuses[1]->GetLen());

    detailed_statuses.clear();
    list_status = file_system.ListFileStatus(_data_path, &detailed_statuses);
    ASSERT_TRUE(list_status.ok()) << list_status.ToString();
    ASSERT_EQ(1, detailed_statuses.size());
    EXPECT_EQ(_data_path, detailed_statuses[0]->GetPath());
    EXPECT_EQ(_content.size(), detailed_statuses[0]->GetLen());

    EXPECT_TRUE(file_system.ListDir(_root, nullptr).IsInvalid());
    EXPECT_TRUE(file_system.ListFileStatus(_root, nullptr).IsInvalid());
}

TEST_F(PaimonFileSystemTest, RejectsWrites) {
    PaimonFileSystem file_system(_local_fs, nullptr, nullptr);

    auto create_result = file_system.Create(_root + "/new-file", false);
    EXPECT_FALSE(create_result.ok());
    EXPECT_TRUE(create_result.status().IsNotImplemented()) << create_result.status().ToString();
    EXPECT_TRUE(file_system.Mkdirs(_root + "/new-directory").IsNotImplemented());
    EXPECT_TRUE(file_system.Rename(_data_path, _root + "/renamed.bin").IsNotImplemented());
    EXPECT_TRUE(file_system.Delete(_data_path, false).IsNotImplemented());

    auto exists_result = file_system.Exists(_data_path);
    ASSERT_TRUE(exists_result.ok()) << exists_result.status().ToString();
    EXPECT_TRUE(exists_result.value());
}

TEST_F(PaimonFileSystemTest, ExistsFallsBackToDirectoryProbe) {
    ObjectStoreLikeMemoryFileSystem object_store;
    ASSERT_OK(object_store.append_file("/data.bin", _content));
    PaimonFileSystem file_system(&object_store, nullptr, nullptr);

    auto exists_result = file_system.Exists("/data.bin");
    ASSERT_TRUE(exists_result.ok()) << exists_result.status().ToString();
    EXPECT_TRUE(exists_result.value());

    exists_result = file_system.Exists("/missing.bin");
    ASSERT_TRUE(exists_result.ok()) << exists_result.status().ToString();
    EXPECT_FALSE(exists_result.value());

    auto open_result = file_system.Open("/data.bin", _content.size());
    ASSERT_TRUE(open_result.ok()) << open_result.status().ToString();
    auto length_result = open_result.value()->Length();
    ASSERT_TRUE(length_result.ok()) << length_result.status().ToString();
    EXPECT_EQ(_content.size(), length_result.value());
}

TEST(PaimonSplitCompatibilityTest, DeserializeJavaVersion8DataSplit) {
    // Generated by paimon-java and kept in paimon-cpp's version-8 DataSplit
    // compatibility corpus. This guards the FE DataSplit.serialize() -> BE
    // Split::Deserialize() wire contract independently of either serializer.
    const std::string encoded_split =
            "3sPSMCwZ7GYAAAAIAAAAAAAAAAQAAAAUAAAAAQAAAAAAAAAACgAAAAAAAAAAAAABAF5kYXRhL29yYy9wa19kdl9pbmRleF9pbl9k"
            "YXRhX3dpdGhfZXh0ZXJuYWwuZGIvcGtfZHZfaW5kZXhfaW5fZGF0YV93aXRoX2V4dGVybmFsL2YxPTEwL2J1Y2tldC0xAQAAAAIA"
            "AAAAAAAAAAEAAAKYAABADQAAAAAvAAAAqAAAAMEDAAAAAAAABQAAAAAAAAAcAAAA2AAAABwAAAD4AAAAeAAAABgBAACoAAAAkAEA"
            "AAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAUAAAAAAAAACAAAADgCAABfVnwqmQEAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
            "AAAAAAAAUQAAAEACAAAAAAAAAAAAAAAAAAAAAAAAZGF0YS03MmI2MmE1Zi1kNjk4LTRkYjUtYjUxYS0wNGMwZGMwMjc3MDItMC5v"
            "cmMAAAAAAgAAAAAAAAAAQWxleAAAAIQAAAAAAAAAAAAAAAAAAAACAAAAAAAAAABUb255AAAAhAAAAAAAAAAAAAAAAAAAAAAAAAAA"
            "HAAAACAAAAAcAAAAQAAAABgAAABgAAAAAAAAAgAAAAAAAAAAQWxleAAAAIQAAAAAAAAAAAAAAAAAAAACAAAAAAAAAABUb255AAAA"
            "hAAAAAAAAAAAAAAAAAIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAALAAAACAAAAAsAAAAUAAAACgAAACAAAAAAAAABAAA"
            "AAAAAAAAQWxleAAAAIQKAAAAAAAAAAAAAAAAAAAAMzMzMzMzKEAAAAAAAAAABAAAAAAAAAAAVG9ueQAAAIQKAAAAAAAAAAAAAAAA"
            "AAAAmpmZmZkZMUAAAAAABAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAARklMRTovdG1wL2V4"
            "dGVybmFsL2YxPTEwL2J1Y2tldC0xL2RhdGEtNzJiNjJhNWYtZDY5OC00ZGI1LWI1MWEtMDRjMGRjMDI3NzAyLTAub3JjAAAAAAAA"
            "AAEAAAABAQBORklMRTovdG1wL2V4dGVybmFsL2YxPTEwL2J1Y2tldC0xL2luZGV4LTQxOWU3YzZiLTljYWQtNDllOC05Y2QyLTYx"
            "ODc0NzFkZjk1NC0xAAAAAAAAAAEAAAAAAAAAFgAAAAAAAAABAAE=";

    std::string serialized_split;
    ASSERT_TRUE(base64_decode(encoded_split, &serialized_split));
    auto memory_pool = paimon::GetDefaultPool();
    auto split_result = paimon::Split::Deserialize(serialized_split.data(), serialized_split.size(), memory_pool);
    ASSERT_TRUE(split_result.ok()) << split_result.status().ToString();

    auto data_split = std::dynamic_pointer_cast<paimon::DataSplit>(split_result.value());
    ASSERT_NE(nullptr, data_split);
    EXPECT_EQ(1, data_split->Bucket());
    const auto files = data_split->GetFileList();
    ASSERT_EQ(1, files.size());
    EXPECT_EQ(961, files[0].file_size);

    auto serialize_result = paimon::Split::Serialize(split_result.value(), memory_pool);
    ASSERT_TRUE(serialize_result.ok()) << serialize_result.status().ToString();
    EXPECT_EQ(serialized_split, serialize_result.value());
}

} // namespace starrocks
