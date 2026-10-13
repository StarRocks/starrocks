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

#include "connector/hive/paimon/paimon_global_index_schema_guard.h"

#include <dlfcn.h>
#include <gtest/gtest.h>
#include <paimon/fs/file_system_factory.h>

#include <cstdlib>
#include <filesystem>
#include <string>

namespace starrocks {

class PaimonGlobalIndexSchemaGuardTest : public ::testing::Test {
public:
    static void SetUpTestSuite() {
        // The factory registry borrows objects from the plugin; keep it loaded
        // for the process lifetime, just as the native SDK does in production.
        ASSERT_NE(nullptr, dlopen("libpaimon_local_file_system.so", RTLD_NOW | RTLD_LOCAL));
    }

protected:
    void SetUp() override {
        std::string pattern = std::filesystem::temp_directory_path() / "paimon-schema-guard-XXXXXX";
        ASSERT_NE(nullptr, mkdtemp(pattern.data()));
        _root = pattern;
        std::filesystem::create_directory(_root + "/schema");
        auto fs = paimon::FileSystemFactory::Get("local", _root, {});
        ASSERT_TRUE(fs.ok()) << fs.status().ToString();
        _fs = std::move(fs).value();
        _path = _root + "/schema/schema-1";
        ASSERT_TRUE(_fs->WriteFile(_path, _schema, true).ok());
    }

    void TearDown() override {
        if (!_root.empty()) {
            std::filesystem::remove_all(_root);
        }
    }

    std::string _root;
    std::string _path;
    const std::string _schema = R"({"id":1,"timeMillis":1000,"fields":[]})";
    std::shared_ptr<paimon::FileSystem> _fs;
};

TEST_F(PaimonGlobalIndexSchemaGuardTest, ReadsOnlyValidatedBytesAfterUnderlyingFileChanges) {
    PaimonGlobalIndexSchemaGuard guard(_fs, _root + "/", 1, 1000);
    EXPECT_FALSE(guard.schema_validated());
    auto opened = guard.Open(_path);
    ASSERT_TRUE(opened.ok()) << opened.status().ToString();
    EXPECT_TRUE(guard.schema_validated());
    auto stream = std::move(opened).value();
    ASSERT_TRUE(_fs->WriteFile(_path, R"({"id":1,"timeMillis":2000})", true).ok());
    std::string bytes(_schema.size(), '\0');
    ASSERT_TRUE(stream->Read(bytes.data(), bytes.size()).ok());
    EXPECT_EQ(_schema, bytes);
    EXPECT_EQ(_path, stream->GetUri().value());
    EXPECT_EQ(_schema.size(), stream->Length().value());
    ASSERT_TRUE(stream->Seek(0, paimon::FS_SEEK_SET).ok());
    ASSERT_TRUE(stream->Read(bytes.data(), bytes.size(), 0).ok());
    EXPECT_EQ(_schema, bytes);
    EXPECT_EQ(0, stream->GetPos().value());
    bool called = false;
    stream->ReadAsync(bytes.data(), bytes.size(), 0, [&](paimon::Status status) {
        EXPECT_TRUE(status.ok());
        called = true;
    });
    EXPECT_TRUE(called);
    EXPECT_EQ(_schema, bytes);
    EXPECT_EQ(0, stream->GetPos().value());
    EXPECT_TRUE(stream->Close().ok());
    // A second Open must validate again, not reuse the earlier successful flag.
    EXPECT_FALSE(guard.Open(_path).ok());
}

TEST_F(PaimonGlobalIndexSchemaGuardTest, RejectsChangedAndMalformedSchemaIdentity) {
    for (const auto& contents :
         {R"({"id":2,"timeMillis":1000})", R"({"id":1,"timeMillis":1001})", R"({"id":"1","timeMillis":1000})",
          R"({"id":1,"timeMillis":null})", R"({"id":1})", R"([])", R"({)", ""}) {
        ASSERT_TRUE(_fs->WriteFile(_path, contents, true).ok());
        PaimonGlobalIndexSchemaGuard guard(_fs, _root, 1, 1000);
        EXPECT_FALSE(guard.Open(_path).ok()) << contents;
        EXPECT_FALSE(guard.schema_validated());
    }
    PaimonGlobalIndexSchemaGuard guard(_fs, _root, 1, 1000);
    EXPECT_FALSE(guard.Open(_root + "/schema/schema-2").ok());
    EXPECT_FALSE(guard.Open(_root + "/schema/schema-01").ok());
    ASSERT_TRUE(_fs->Delete(_path, false).ok());
    EXPECT_FALSE(guard.Open(_path).ok());
    EXPECT_FALSE(guard.schema_validated());
}

TEST_F(PaimonGlobalIndexSchemaGuardTest, DelegatesOtherFilesWithoutValidatingSchema) {
    PaimonGlobalIndexSchemaGuard guard(_fs, _root, 1, 1000);
    const std::string file = _root + "/index.bin";
    ASSERT_TRUE(guard.WriteFile(file, "index-data", true).ok());
    std::string bytes;
    ASSERT_TRUE(guard.ReadFile(file, &bytes).ok());
    EXPECT_EQ("index-data", bytes);
    EXPECT_TRUE(guard.Exists(file).value());
    EXPECT_EQ(10, guard.GetFileStatus(file).value()->GetLen());
    std::vector<std::unique_ptr<paimon::BasicFileStatus>> entries;
    EXPECT_TRUE(guard.ListDir(_root, &entries).ok());
    EXPECT_EQ(2, entries.size());
    std::vector<std::unique_ptr<paimon::FileStatus>> statuses;
    EXPECT_TRUE(guard.ListFileStatus(_root, &statuses).ok());
    EXPECT_EQ(2, statuses.size());
    EXPECT_TRUE(guard.Rename(file, file + ".renamed").ok());
    EXPECT_TRUE(guard.Delete(file + ".renamed", false).ok());
    EXPECT_TRUE(guard.Mkdirs(_root + "/empty").ok());
    EXPECT_FALSE(guard.schema_validated());
}

TEST_F(PaimonGlobalIndexSchemaGuardTest, RejectsUnboundedSchemaBuffer) {
    ASSERT_TRUE(_fs->WriteFile(_path, std::string(16 * 1024 * 1024 + 1, 'x'), true).ok());
    PaimonGlobalIndexSchemaGuard guard(_fs, _root, 1, 1000);
    EXPECT_FALSE(guard.Open(_path).ok());
    EXPECT_FALSE(guard.schema_validated());
}

} // namespace starrocks
