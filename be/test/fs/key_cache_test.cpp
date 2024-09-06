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

#include "fs/key_cache.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "script/script.h"

namespace starrocks {

// Test EncryptionKey constructor with EncryptionKeyPB and accessors
TEST(EncryptionKeyTest, ConstructorWithPB) {
    EncryptionKeyPB pb;
    pb.set_id(123);
    pb.set_parent_id(456);
    pb.set_type(EncryptionKeyTypePB::NORMAL_KEY);
    pb.set_algorithm(EncryptionAlgorithmPB::AES_128);
    pb.set_encrypted_key("0000000000000000");

    auto key = EncryptionKey::create_from_pb(pb).value();
    EXPECT_EQ(key->get_id(), 123);
    EXPECT_EQ(key->get_parent_id(), 456);
    EXPECT_EQ(key->type(), EncryptionKeyTypePB::NORMAL_KEY);
    EXPECT_EQ(key->algorithm(), EncryptionAlgorithmPB::AES_128);
    EXPECT_TRUE(key->has_parent());
}

TEST(EncryptionKeyTest, GenerateAndDecrypt) {
    EncryptionKeyPB pb;
    pb.set_id(EncryptionKey::DEFAULT_MASTER_KYE_ID);
    pb.set_type(EncryptionKeyTypePB::NORMAL_KEY);
    pb.set_algorithm(EncryptionAlgorithmPB::AES_128);
    pb.set_plain_key("0000000000000000");

    auto root_key = EncryptionKey::create_from_pb(pb).value();
    auto key = root_key->generate_key().value();
    ASSERT_TRUE(key->get_plain_key().ok());

    auto key2 = EncryptionKey::create_from_pb(key->pb()).value();
    ASSERT_TRUE(root_key->decrypt(key2.get()).ok());
    ASSERT_EQ(key->get_plain_key().value(), key2->get_plain_key().value());

    // tamper data then decrypt should be failed
    for (int i = 0; i < 100; i++) {
        auto pb = key->pb();
        auto mkey = pb.mutable_encrypted_key();
        (*mkey)[rand() % mkey->size()]++;
        auto key2 = EncryptionKey::create_from_pb(pb).value();
        auto st = root_key->decrypt(key2.get());
        LOG(INFO) << st;
        ASSERT_FALSE(st.ok());
    }
}

class KeyCacheTest : public testing::Test {};

TEST_F(KeyCacheTest, AddKey) {
    KeyCache cache;
    EncryptionKey* root = nullptr;
    {
        // add root key
        EncryptionKeyPB pb;
        pb.set_id(EncryptionKey::DEFAULT_MASTER_KYE_ID);
        pb.set_type(EncryptionKeyTypePB::NORMAL_KEY);
        pb.set_algorithm(EncryptionAlgorithmPB::AES_128);
        pb.set_plain_key("0000000000000000");
        auto key = EncryptionKey::create_from_pb(pb).value();
        root = key.get();
        cache.add_key(key);
        ASSERT_EQ(1, cache.size());
    }

    // add kek 2
    auto key = root->generate_key().value();
    ASSERT_EQ(EncryptionKey::DEFAULT_MASTER_KYE_ID, key->get_parent_id());
    key->set_id(2);
    cache.add_key(key);
    ASSERT_EQ(2, cache.size());
    std::string result;
    ASSERT_TRUE(execute_script("System.print(ExecEnv.key_cache_info())", result).ok());
}

static void wrap_unwrap_test(int num_level) {
    EncryptionKeyPB pb;
    pb.set_id(EncryptionKey::DEFAULT_MASTER_KYE_ID);
    pb.set_type(EncryptionKeyTypePB::NORMAL_KEY);
    pb.set_algorithm(EncryptionAlgorithmPB::AES_128);
    pb.set_plain_key("0000000000000000");
    auto cur = EncryptionKey::create_from_pb(pb).value();

    EncryptionMetaPB metaPb;
    *metaPb.add_key_hierarchy() = cur->pb();

    for (int level = 1; level < num_level; level++) {
        auto kek = cur->generate_key().value();
        kek->set_id(level + 1);
        *metaPb.add_key_hierarchy() = kek->pb();
        cur.swap(kek);
    }

    std::string encryption_meta;
    ASSERT_TRUE(metaPb.SerializeToString(&encryption_meta));

    KeyCache cache;
    ASSERT_EQ(0, cache.size());
    auto st = cache.create_encryption_meta_pair(encryption_meta);
    LOG_IF(WARNING, !st.ok()) << st.status();
    auto& epair = st.value();
    ASSERT_EQ(num_level, cache.size());
    auto st2 = cache.unwrap_encryption_meta(epair.encryption_meta);
    LOG_IF(WARNING, !st2.ok()) << st2.status();
    ASSERT_EQ(num_level, cache.size());
    auto& info = st2.value();
    ASSERT_EQ(epair.info.algorithm, info.algorithm);
    ASSERT_EQ(epair.info.key, info.key);
}

TEST_F(KeyCacheTest, WrapEncryptionMeta) {
    for (int i = 1; i < 10; i++) {
        LOG(INFO) << "test wrap_unwrap level " << i;
        wrap_unwrap_test(i);
    }
}

TEST_F(KeyCacheTest, RefreshKeys) {
    EncryptionKeyPB pb;
    pb.set_id(EncryptionKey::DEFAULT_MASTER_KYE_ID);
    pb.set_type(EncryptionKeyTypePB::NORMAL_KEY);
    pb.set_algorithm(EncryptionAlgorithmPB::AES_128);
    pb.set_plain_key("0000000000000000");
    auto root = EncryptionKey::create_from_pb(pb).value();

    // add kek
    auto kek = root->generate_key().value();
    ASSERT_EQ(EncryptionKey::DEFAULT_MASTER_KYE_ID, kek->get_parent_id());
    kek->set_id(2);

    EncryptionMetaPB metaPb;
    *metaPb.add_key_hierarchy() = root->pb();
    *metaPb.add_key_hierarchy() = kek->pb();
    std::vector<std::string> metas(1);
    ASSERT_TRUE(metaPb.SerializeToString(&metas[0]));

    KeyCache cache;
    ASSERT_EQ(0, cache.size());
    ASSERT_TRUE(cache.refresh_keys(metas).ok());
    ASSERT_EQ(2, cache.size());
    auto epair = cache.create_encryption_meta_pair_using_current_kek().value();
    auto info = cache.unwrap_encryption_meta(epair.encryption_meta).value();
    ASSERT_EQ(epair.info.algorithm, info.algorithm);
    ASSERT_EQ(epair.info.key, info.key);
    LOG(INFO) << cache.to_string();
}

} // namespace starrocks
