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

#include "storage/replication_utils.h"

#include <gtest/gtest.h>

#include "base/testutil/assert.h"
#include "fs/fs_factory.h"
#include "fs/fs_util.h"
#include "platform/key_cache.h"

namespace starrocks {

namespace {

void seed_test_encryption_keys() {
    EncryptionKeyPB pb;
    pb.set_id(EncryptionKey::DEFAULT_MASTER_KYE_ID);
    pb.set_type(EncryptionKeyTypePB::NORMAL_KEY);
    pb.set_algorithm(EncryptionAlgorithmPB::AES_128);
    pb.set_plain_key("0000000000000000");
    auto root_encryption_key = EncryptionKey::create_from_pb(pb).value();
    ASSIGN_OR_ABORT(auto encryption_key, root_encryption_key->generate_key());
    encryption_key->set_id(2);
    KeyCache::instance().add_key(root_encryption_key);
    KeyCache::instance().add_key(encryption_key);
}

} // namespace

class ReplicationUtilsTest : public testing::Test {
public:
    ReplicationUtilsTest() = default;
    ~ReplicationUtilsTest() override = default;

    void SetUp() override {
        (void)fs::remove_all(_test_dir);
        ASSERT_OK(fs::create_directories(_test_dir));
    }

    void TearDown() override { (void)fs::remove_all(_test_dir); }

protected:
    const std::string _test_dir = "./ut_dir/replication_utils";
};

TEST_F(ReplicationUtilsTest, test_convert_column_unique_ids) {
    std::vector<uint32_t> column_unique_ids = {1, 2};
    std::unordered_map<uint32_t, uint32_t> column_unique_id_map = {{1, 10}, {2, 20}};

    auto status = ReplicationUtils::convert_column_unique_ids(&column_unique_ids, column_unique_id_map);
    EXPECT_TRUE(status.ok()) << status;

    column_unique_id_map.erase(1);
    status = ReplicationUtils::convert_column_unique_ids(&column_unique_ids, column_unique_id_map);
    EXPECT_FALSE(status.ok()) << status;

    column_unique_id_map.clear();
    status = ReplicationUtils::convert_column_unique_ids(&column_unique_ids, column_unique_id_map);
    EXPECT_TRUE(status.ok()) << status;
}

TEST_F(ReplicationUtilsTest, EncryptedSourceConverterDecryptsBeforeTargetEncryption) {
    seed_test_encryption_keys();
    ASSIGN_OR_ABORT(auto source_pair, KeyCache::instance().create_encryption_meta_pair_using_current_kek());
    ASSIGN_OR_ABORT(auto target_pair, KeyCache::instance().create_encryption_meta_pair_using_current_kek());
    EXPECT_NE(source_pair.info.key, target_pair.info.key);

    const std::string plaintext = "source-plaintext-payload";
    const std::string source_path = _test_dir + "/converter-source";
    const std::string target_path = _test_dir + "/converter-target";
    ASSIGN_OR_ABORT(auto source_fs, FileSystemFactory::CreateSharedFromString(source_path));
    ASSIGN_OR_ABORT(auto target_fs, FileSystemFactory::CreateSharedFromString(target_path));

    WritableFileOptions source_write_opts{.sync_on_close = true,
                                          .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE,
                                          .encryption_info = source_pair.info};
    ASSIGN_OR_ABORT(auto source_file, source_fs->new_writable_file(source_write_opts, source_path));
    ASSERT_OK(source_file->append(plaintext));
    ASSERT_OK(source_file->close());

    WritableFileOptions target_write_opts{.sync_on_close = true,
                                          .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE,
                                          .encryption_info = target_pair.info};
    FileConverterCreatorFunc converter = [target_path, target_fs, target_write_opts](
                                                 const std::string& file_name,
                                                 uint64_t file_size) -> StatusOr<std::unique_ptr<FileStreamConverter>> {
        ASSIGN_OR_RETURN(auto target_file, target_fs->new_writable_file(target_write_opts, target_path));
        return std::make_unique<FileStreamConverter>(file_name, file_size, std::move(target_file));
    };

    RandomAccessFileOptions source_read_opts{.encryption_info = source_pair.info};
    ASSERT_OK(ReplicationUtils::download_lake_file_with_converter(source_path, "converter-source", plaintext.size(),
                                                                  source_fs, source_read_opts, converter));

    RandomAccessFileOptions target_read_opts{.encryption_info = target_pair.info};
    ASSIGN_OR_ABORT(auto target_file, target_fs->new_random_access_file(target_read_opts, target_path));
    ASSIGN_OR_ABORT(auto copied_plaintext, target_file->read_all());
    EXPECT_EQ(plaintext, copied_plaintext);

    ASSIGN_OR_ABORT(auto default_target_pair, KeyCache::instance().create_encryption_meta_pair_using_current_kek());
    EXPECT_NE(source_pair.info.key, default_target_pair.info.key);
    const std::string default_target_path = _test_dir + "/converter-default-source-options-target";
    ASSIGN_OR_ABORT(auto default_target_fs, FileSystemFactory::CreateSharedFromString(default_target_path));
    WritableFileOptions default_target_write_opts{.sync_on_close = true,
                                                  .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE,
                                                  .encryption_info = default_target_pair.info};
    FileConverterCreatorFunc default_converter =
            [default_target_path, default_target_fs, default_target_write_opts](
                    const std::string& file_name,
                    uint64_t file_size) -> StatusOr<std::unique_ptr<FileStreamConverter>> {
        ASSIGN_OR_RETURN(auto default_target_file,
                         default_target_fs->new_writable_file(default_target_write_opts, default_target_path));
        return std::make_unique<FileStreamConverter>(file_name, file_size, std::move(default_target_file));
    };

    ASSERT_OK(ReplicationUtils::download_lake_file_with_converter(source_path, "converter-source", plaintext.size(),
                                                                  source_fs, default_converter));

    RandomAccessFileOptions default_target_read_opts{.encryption_info = default_target_pair.info};
    ASSIGN_OR_ABORT(auto default_target_file,
                    default_target_fs->new_random_access_file(default_target_read_opts, default_target_path));
    ASSIGN_OR_ABORT(auto copied_without_source_opts, default_target_file->read_all());
    EXPECT_NE(plaintext, copied_without_source_opts);
}

TEST_F(ReplicationUtilsTest, EncryptedSourceSequentialCopyDecryptsBeforeTargetEncryption) {
    seed_test_encryption_keys();
    ASSIGN_OR_ABORT(auto source_pair, KeyCache::instance().create_encryption_meta_pair_using_current_kek());
    ASSIGN_OR_ABORT(auto target_pair, KeyCache::instance().create_encryption_meta_pair_using_current_kek());
    EXPECT_NE(source_pair.info.key, target_pair.info.key);

    const std::string plaintext = "source-plaintext-payload";
    const std::string source_path = _test_dir + "/sequential-source";
    const std::string target_path = _test_dir + "/sequential-target";
    ASSIGN_OR_ABORT(auto source_fs, FileSystemFactory::CreateSharedFromString(source_path));
    ASSIGN_OR_ABORT(auto target_fs, FileSystemFactory::CreateSharedFromString(target_path));

    WritableFileOptions source_write_opts{.sync_on_close = true,
                                          .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE,
                                          .encryption_info = source_pair.info};
    ASSIGN_OR_ABORT(auto source_file, source_fs->new_writable_file(source_write_opts, source_path));
    ASSERT_OK(source_file->append(plaintext));
    ASSERT_OK(source_file->close());

    SequentialFileOptions source_read_opts{.encryption_info = source_pair.info};
    WritableFileOptions target_write_opts{.sync_on_close = true,
                                          .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE,
                                          .encryption_info = target_pair.info};
    ASSERT_OK(fs::copy_file(source_path, source_fs, source_read_opts, target_path, target_fs, target_write_opts));

    RandomAccessFileOptions target_read_opts{.encryption_info = target_pair.info};
    ASSIGN_OR_ABORT(auto target_file, target_fs->new_random_access_file(target_read_opts, target_path));
    ASSIGN_OR_ABORT(auto copied_plaintext, target_file->read_all());
    EXPECT_EQ(plaintext, copied_plaintext);

    ASSIGN_OR_ABORT(auto default_target_pair, KeyCache::instance().create_encryption_meta_pair_using_current_kek());
    EXPECT_NE(source_pair.info.key, default_target_pair.info.key);
    const std::string default_target_path = _test_dir + "/sequential-default-source-options-target";
    ASSIGN_OR_ABORT(auto default_target_fs, FileSystemFactory::CreateSharedFromString(default_target_path));
    WritableFileOptions default_target_write_opts{.sync_on_close = true,
                                                  .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE,
                                                  .encryption_info = default_target_pair.info};
    ASSERT_OK(fs::copy_file(source_path, source_fs, default_target_path, default_target_fs, default_target_write_opts));

    RandomAccessFileOptions default_target_read_opts{.encryption_info = default_target_pair.info};
    ASSIGN_OR_ABORT(auto default_target_file,
                    default_target_fs->new_random_access_file(default_target_read_opts, default_target_path));
    ASSIGN_OR_ABORT(auto copied_without_source_opts, default_target_file->read_all());
    EXPECT_NE(plaintext, copied_without_source_opts);
}

} // namespace starrocks
