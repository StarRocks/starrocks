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

#include "fs/encrypt_file.h"

#include <gtest/gtest.h>

#include "io/core/array_input_stream.h"

namespace starrocks {

class EncryptionTest : public testing::Test {
protected:
    void SetUp() override {
        _plain_kek.assign(16, '\0');
        ssl_random_bytes(_plain_kek.data(), 16);
        _plain_key.assign(16, '\0');
        ssl_random_bytes(_plain_key.data(), 16);
    }

    std::string _plain_kek;
    std::string _plain_key;
};

TEST_F(EncryptionTest, WrapKeyBadAlgo) {
    auto result = wrap_key(NO_ENCRYPTION, _plain_kek, _plain_key);
    ASSERT_FALSE(result.ok());
    auto result2 = wrap_key(static_cast<EncryptionAlgorithmPB>(999), _plain_kek, _plain_key);
    ASSERT_FALSE(result2.ok());
}

TEST_F(EncryptionTest, WrapKeyAES128) {
    auto result = wrap_key(AES_128, _plain_kek, _plain_key);
    ASSERT_TRUE(result.ok());
    auto& encrypted_key = result.value();
    EXPECT_EQ(16 + 16 + 12, encrypted_key.length());
    ASSERT_FALSE(unwrap_key(NO_ENCRYPTION, _plain_kek, encrypted_key).ok());
    auto ret = unwrap_key(AES_128, _plain_kek, encrypted_key);
    ASSERT_TRUE(ret.ok());
    ASSERT_EQ(_plain_key, ret.value());
}

TEST_F(EncryptionTest, EncryptSeekableInputStreamIsEncrypted) {
    std::string content = "0123456789";
    auto stream = std::make_unique<io::ArrayInputStream>(content.data(), content.size());
    ASSERT_FALSE(stream->is_encrypted());

    std::unique_ptr<io::SeekableInputStream> seekable_stream = std::move(stream);
    EncryptSeekableInputStream encrypted(std::move(seekable_stream),
                                         {EncryptionAlgorithmPB::AES_128, "0000000000000000"});
    ASSERT_TRUE(encrypted.is_encrypted());
}

} // namespace starrocks
