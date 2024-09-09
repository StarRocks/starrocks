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

#pragma once

#include <atomic>

#include "common/statusor.h"
#include "fs/encryption.h"

namespace starrocks {

class KeyCache;

struct FileEncryptionPair {
    FileEncryptionInfo info;
    std::string encryption_meta;
};

class EncryptionKey {
public:
    static constexpr int64_t INVALID_KEY_ID = 0;
    static constexpr int64_t DEFAULT_MASTER_KYE_ID = 1;

    EncryptionKey();
    EncryptionKey(EncryptionKeyPB pb);

    virtual ~EncryptionKey();

    int64_t get_id() const { return _pb.id(); }

    void set_id(int64_t id) { _pb.set_id(id); }

    // key with an id should put in cache
    // key without id is usually one-time file encryption key, no need to put in cache
    bool has_id() const { return get_id() != INVALID_KEY_ID; }

    int64_t get_parent_id() const { return _pb.has_parent_id() ? _pb.parent_id() : INVALID_KEY_ID; }

    bool has_parent() const { return _pb.parent_id() != INVALID_KEY_ID; }

    EncryptionKeyTypePB type() const { return _pb.type(); }

    virtual EncryptionAlgorithmPB algorithm() const { return _pb.algorithm(); }

    const EncryptionKeyPB& pb() const { return _pb; }

    /**
     * @return each key should have an unique identifier, so it can be identified and searched in cache
     */
    virtual const std::string& identifier() const;

    virtual bool is_kek() const;

    virtual StatusOr<std::unique_ptr<EncryptionKey>> generate_key() const {
        return Status::NotSupported("generate_key not implemented");
    }

    virtual Status decrypt(EncryptionKey* child_key) const { return Status::NotSupported("decrypt not implemented"); }

    virtual bool is_decrypted() const = 0;

    virtual StatusOr<std::string> get_plain_key() const { return Status::NotSupported("no plain key"); }

    virtual std::string to_string() const = 0;

    static StatusOr<std::unique_ptr<EncryptionKey>> create_from_pb(EncryptionKeyPB pb);

protected:
    EncryptionKeyPB _pb;
};

class KeyCache {
public:
    static KeyCache& instance();

    KeyCache();

    StatusOr<FileEncryptionPair> create_encryption_meta_pair(const std::string& encryption_meta_prefix);

    StatusOr<FileEncryptionPair> create_encryption_meta_pair_using_current_kek();

    // create a FileEncryptionPair, with single plain key in key hierarchy, only used for tests
    StatusOr<FileEncryptionPair> create_plain_random_encryption_meta_pair();

    StatusOr<FileEncryptionInfo> unwrap_encryption_meta(const std::string& encryption_meta);

    Status refresh_keys_from_fe();

    Status refresh_keys(const std::vector<std::string>& key_metas);

    Status refresh_keys(const std::string& key_metas);

    size_t size() const;

    const EncryptionKey* get_key(const std::string& identifier);

    void add_key(std::unique_ptr<EncryptionKey>& key);

    std::string to_string() const;

private:
    Status _resolve_encryption_meta(const EncryptionMetaPB& metaPb, std::vector<const EncryptionKey*>& keys,
                                    std::vector<std::unique_ptr<EncryptionKey>>& owned_keys, bool cache_last_key);

    mutable std::mutex _lock;
    std::unordered_map<std::string, std::unique_ptr<EncryptionKey>> _identifier_to_keys;
    std::atomic<EncryptionKey*> _current_kek;
    std::atomic<EncryptionKey*> _default_root;
    int64_t _last_refresh_key_time{0};
};

} // namespace starrocks
