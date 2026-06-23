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

#include "platform/key_cache.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

<<<<<<< HEAD:be/test/fs/key_cache_test.cpp
#include "base/url_coding.h"
#include "script/script.h"

=======
>>>>>>> 21e9817633f... [Refactor] Move KeyCache to platform (#75185):be/test/platform/key_cache_test.cpp
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

struct KeySpec {
    EncryptionAlgorithmPB algorithm;
    std::string key;
};

StatusOr<KeySpec> get_key_spec_from_vault_response(const std::string& vault_get_response);

TEST(VaultTest, GetKeySpecFromVaultResponse) {
    std::string response =
            "{\"request_id\":\"657f959e-063a-e947-296a-97e7c9ea5776\",\"lease_id\":\"\",\"renewable\":false,\"lease_"
            "duration\":0,\"data\":{\"data\":{\"plain_key\":\"aes_128:3bozYSHPqtPi49TMQU1T4g==\"},\"metadata\":{"
            "\"created_time\":\"2024-07-18T15:23:24.115424002Z\",\"custom_metadata\":null,\"deletion_time\":\"\","
            "\"destroyed\":false,\"version\":1}},\"wrap_info\":null,\"warnings\":null,\"auth\":null,\"mount_type\":"
            "\"kv\"}";
    auto spec = get_key_spec_from_vault_response(response).value();
    ASSERT_EQ(spec.algorithm, EncryptionAlgorithmPB::AES_128);
    std::string key_encoded;
    base64_encode(spec.key, &key_encoded);
    ASSERT_EQ(key_encoded, "3bozYSHPqtPi49TMQU1T4g==");
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
    ASSERT_NE(cache.to_string().find("id:1"), std::string::npos);
    ASSERT_NE(cache.to_string().find("id:2"), std::string::npos);
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
