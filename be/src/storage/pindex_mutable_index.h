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

#include <memory>
#include <tuple>

#include "common/statusor.h"
#include "fs/fs.h"
#include "gen_cpp/persistent_index.pb.h"
#include "storage/edit_version.h"
#include "storage/pindex_common.h"
#include "storage/rowset/bloom_filter.h"
#include "storage/rowset/rowset.h"
#include "storage/storage_engine.h"
#include "util/phmap/phmap.h"
#include "util/phmap/phmap_dump.h"

namespace starrocks {

uint64_t key_index_hash(const void* data, size_t len);

class Tablet;
class Schema;
class Column;
class PrimaryKeyDump;
class PersistentIndex;

namespace lake {
class LakeLocalPersistentIndex;
}

class MutableIndex {
public:
    MutableIndex();
    virtual ~MutableIndex();

    // batch get
    // |keys|: key array as raw buffer
    // |values|: value array for return values
    // |not_found|: information of keys not found, which need to be further checked in next level
    // |num_found|: add the number of keys found to this argument
    // |idxes|: the target indexes of keys
    virtual Status get(const Slice* keys, IndexValue* values, KeysInfo* not_found, size_t* num_found,
                       const std::vector<size_t>& idxes) const = 0;

    // batch upsert and get old value
    // |keys|: key array as raw buffer
    // |values|: value array
    // |old_values|: return old values for updates, or set to NullValue for inserts
    // |not_found|: information of keys not found, which need to be further checked in next level
    // |num_found|: add the number of keys found to this argument
    // |idxes|: the target indexes of keys
    virtual Status upsert(const Slice* keys, const IndexValue* values, IndexValue* old_values, KeysInfo* not_found,
                          size_t* num_found, const std::vector<size_t>& idxes) = 0;

    // batch upsert
    // |keys|: key array as raw buffer
    // |values|: value array
    // |not_found|: information of keys not found, which need to be further checked in next level
    // |num_found|: add the number of keys found(or already exist) to this argument
    // |idxes|: the target indexes of keys
    virtual Status upsert(const Slice* keys, const IndexValue* values, KeysInfo* not_found, size_t* num_found,
                          const std::vector<size_t>& idxes) = 0;

    // batch insert
    // |keys|: key array as raw buffer
    // |values|: value array
    // |idxes|: the target indexes of keys
    virtual Status insert(const Slice* keys, const IndexValue* values, const std::vector<size_t>& idxes) = 0;

    // batch erase(delete)
    // |keys|: key array as raw buffer
    // |old_values|: return old values for updates, or set to NullValue if not exists
    // |not_found|: information of keys not found, which need to be further checked in next level
    // |num_found|: add the number of keys found to this argument
    // |idxes|: the target indexes of keys
    virtual Status erase(const Slice* keys, IndexValue* old_values, KeysInfo* not_found, size_t* num_found,
                         const std::vector<size_t>& idxes) = 0;

    // batch replace
    // |keys|: key array as raw buffer
    // |values|: new value array
    // |replace_idxes|: the idx array of the kv needed to be replaced
    virtual Status replace(const Slice* keys, const IndexValue* values, const std::vector<size_t>& replace_idxes) = 0;

    virtual Status append_wal(const Slice* keys, const IndexValue* values, const std::vector<size_t>& idxes,
                              std::unique_ptr<WritableFile>& index_file, uint64_t* page_size, uint32_t* checksum) = 0;

    // load wals
    // |n|: size of key/value array
    // |keys|: key array as raw buffer
    // |values|: value array
    virtual Status load_wals(size_t n, const Slice* keys, const IndexValue* values) = 0;

    // load snapshot
    virtual bool load_snapshot(phmap::BinaryInputArchive& ar) = 0;

    // load according meta
    virtual Status load(size_t& offset, std::unique_ptr<RandomAccessFile>& file) = 0;

    // get dump total size of hashmaps of shards
    virtual size_t dump_bound() = 0;

    virtual bool dump(phmap::BinaryOutputArchive& ar) = 0;

    // get all key-values pair references by shard, the result will remain valid until next modification
    // |nshard|: number of shard
    // |num_entry|: number of entries expected, it should be:
    //                 the num of KV entries if with_null == true
    //                 the num of KV entries excluding nulls if with_null == false
    // |with_null|: whether to include null entries
    // [not thread-safe]
    virtual std::vector<std::vector<KVRef>> get_kv_refs_by_shard(size_t nshard, size_t num_entry,
                                                                 bool with_null) const = 0;

    virtual Status flush_to_immutable_index(std::unique_ptr<ImmutableIndexWriter>& writer, size_t nshard,
                                            size_t npage_hint, size_t nbucket, bool with_null) const = 0;

    // get the number of entries in the index (including NullIndexValue)
    virtual size_t size() const = 0;

    virtual size_t usage() const = 0;

    virtual size_t capacity() = 0;

    virtual void reserve(size_t size) = 0;

    virtual void clear() = 0;

    virtual size_t memory_usage() = 0;

    virtual Status pk_dump(PrimaryKeyDump* dump, PrimaryIndexDumpPB* dump_pb) = 0;

    static StatusOr<std::unique_ptr<MutableIndex>> create(size_t key_size);

    static std::tuple<size_t, size_t> estimate_nshard_and_npage(const size_t total_kv_pairs_usage);

    static size_t estimate_nbucket(size_t key_size, size_t size, size_t nshard, size_t npage);
};

class ShardByLengthMutableIndex {
public:
    ShardByLengthMutableIndex() = default;

    ShardByLengthMutableIndex(const size_t key_size, const std::string& path) // NOLINT
            : _fixed_key_size(key_size), _path(path) {}

    ~ShardByLengthMutableIndex() {
        if (_index_file) {
            WARN_IF_ERROR(_index_file->close(), "Failed to close index file:" + _index_file->filename());
        }
    }

    Status init();

    uint64_t file_size() {
        if (_index_file != nullptr) {
            return _index_file->size();
        } else {
            return 0;
        }
    }

    // batch get
    // |n|: size of key/value array
    // |keys|: key array as raw buffer
    // |values|: value array for return values
    // |num_found|: add the number of keys found to this argument
    // |not_found_keys_info_by_key_size|: a map maintain the key size as key, and keys infos there're not found as value
    Status get(size_t n, const Slice* keys, IndexValue* values, size_t* num_found,
               std::map<size_t, KeysInfo>& not_found_keys_info_by_key_size);

    // batch upsert and get old value
    // |n|: size of key/value array
    // |keys|: key array as raw buffer
    // |values|: value array
    // |old_values|: return old values for updates, or set to NullValue for inserts
    // |num_found|: add the number of keys found to this argument
    // |not_found_keys_info_by_key_size|: a map maintain the key size as key, and keys infos there're not found as value
    Status upsert(size_t n, const Slice* keys, const IndexValue* values, IndexValue* old_values, size_t* num_found,
                  std::map<size_t, KeysInfo>& not_found_keys_info_by_key_size);

    // batch upsert
    // |n|: size of key/value array
    // |keys|: key array as raw buffer
    // |values|: value array
    // |num_found|: add the number of keys found(or already exist) to this argument
    // |not_found_keys_info_by_key_size|: a map maintain the key size as key, and keys infos there're not found as value
    Status upsert(size_t n, const Slice* keys, const IndexValue* values, size_t* num_found,
                  std::map<size_t, KeysInfo>& not_found_keys_info_by_key_size);

    // batch insert
    // |n|: size of key/value array
    // |keys|: key array as raw buffer
    // |values|: value array
    // |check_l1_key_sizes|: a set of key size need to be checked in l1.
    Status insert(size_t n, const Slice* keys, const IndexValue* values, std::set<size_t>& check_l1_key_sizes);

    // batch erase(delete)
    // |n|: size of key/value array
    // |keys|: key array as raw buffer
    // |old_values|: return old values for updates, or set to NullValue if not exists
    // |num_found|: add the number of keys found to this argument
    // |not_found_keys_info_by_key_size|: a map maintain the key size as key, and keys infos there're not found as value
    Status erase(size_t n, const Slice* keys, IndexValue* old_values, size_t* num_found,
                 std::map<size_t, KeysInfo>& not_found_keys_info_by_key_size);

    // batch replace
    // |keys|: key array as raw buffer
    // |values|: new value array
    // |idxes|: the idx array of the kv needed to be replaced
    Status replace(const Slice* keys, const IndexValue* values, const std::vector<size_t>& idxes);

    Status append_wal(size_t n, const Slice* keys, const IndexValue* values);
    Status append_wal(const Slice* keys, const IndexValue* values, const std::vector<size_t>& idxes);

    // load snapshot
    bool load_snapshot(phmap::BinaryInputArchive& ar, const std::set<uint32_t>& dumped_shard_idxes);

    // load according meta
    Status load(const MutableIndexMetaPB& meta);

    size_t dump_bound();

    bool dump(phmap::BinaryOutputArchive& ar, std::set<uint32_t>& dumped_shard_idxes);

    Status commit(MutableIndexMetaPB* meta, const EditVersion& version, const CommitType& type);

    // get all key-values pair references by shard, the result will remain valid until next modification
    // |num_entry|: number of entries expected, it should be:
    //                 the num of KV entries if with_null == true
    //                 the num of KV entries excluding nulls if with_null == false
    // |with_null|: whether to include null entries
    std::vector<std::pair<uint32_t, std::vector<std::vector<KVRef>>>> get_kv_refs_by_shard(size_t num_entry,
                                                                                           bool with_null);

    std::vector<std::vector<size_t>> split_keys_by_shard(size_t nshard, const Slice* keys, size_t idx_begin,
                                                         size_t idx_end);
    std::vector<std::vector<size_t>> split_keys_by_shard(size_t nshard, const Slice* keys,
                                                         const std::vector<size_t>& idxes);

    Status flush_to_immutable_index(const std::string& dir, const EditVersion& version, bool write_tmp_l1,
                                    bool keep_delete);

    // get the number of entries in the index (including NullIndexValue)
    size_t size();

    size_t capacity();

    size_t memory_usage();

    void clear();

    Status create_index_file(std::string& path);

    static StatusOr<std::unique_ptr<ShardByLengthMutableIndex>> create(size_t key_size, const std::string& path);

    Status pk_dump(PrimaryKeyDump* dump, PrimaryIndexDumpPB* dump_pb);

private:
    friend class PersistentIndex;
    friend class starrocks::lake::LakeLocalPersistentIndex;

    template <int N>
    void _init_loop_helper();

private:
    uint32_t _fixed_key_size = -1;
    uint64_t _offset = 0;
    uint64_t _page_size = 0;
    uint32_t _checksum = 0;
    std::string _path;
    std::unique_ptr<WritableFile> _index_file;
    std::shared_ptr<FileSystem> _fs;
    std::vector<std::unique_ptr<MutableIndex>> _shards;
    // TODO: confirm whether can be just one shard in a offset, which means shard size always be 1, it can simplify the manager of various shards.
    // <key size, <shard offset, shard size>>
    std::map<uint32_t, std::pair<uint32_t, uint32_t>> _shard_info_by_key_size;
};

struct StringHasher2 {
    uint64_t operator()(const std::string& s) const { return key_index_hash(s.data(), s.length() - kIndexValueSize); }
};

class EqualOnStringWithHash {
public:
    bool operator()(const std::string& lhs, const std::string& rhs) const {
        return memequal_padded(lhs.data(), lhs.size() - kIndexValueSize, rhs.data(), rhs.size() - kIndexValueSize);
    }
};

class SliceMutableIndex : public MutableIndex {
public:
    using KeyType = std::string;

    using WALKVSizeType = uint32_t;
    static constexpr size_t kWALKVSize = 4;
    static_assert(sizeof(WALKVSizeType) == kWALKVSize);
    static constexpr size_t kKeySizeMagicNum = 0;

    SliceMutableIndex() = default;
    ~SliceMutableIndex() override = default;

public:
    Status get(const Slice* keys, IndexValue* values, KeysInfo* not_found, size_t* num_found,
               const std::vector<size_t>& idxes) const override;

    Status upsert(const Slice* keys, const IndexValue* values, IndexValue* old_values, KeysInfo* not_found,
                  size_t* num_found, const std::vector<size_t>& idxes) override;

    Status upsert(const Slice* keys, const IndexValue* values, KeysInfo* not_found, size_t* num_found,
                  const std::vector<size_t>& idxes) override;

    Status insert(const Slice* keys, const IndexValue* values, const std::vector<size_t>& idxes) override;

    Status erase(const Slice* keys, IndexValue* old_values, KeysInfo* not_found, size_t* num_found,
                 const std::vector<size_t>& idxes) override;

    Status replace(const Slice* keys, const IndexValue* values, const std::vector<size_t>& idxes) override;

    Status append_wal(const Slice* keys, const IndexValue* values, const std::vector<size_t>& idxes,
                      std::unique_ptr<WritableFile>& index_file, uint64_t* page_size, uint32_t* checksum) override;

    Status load_wals(size_t n, const Slice* keys, const IndexValue* values) override;

    // return the dump file size if dump _set into a new file
    //  ｜--------    snapshot file      --------｜
    //  |  size_t ||   size_t  ||  char[]  | ... |   size_t  ||  char[]  |
    //  |total num|| data size ||  data    | ... | data size ||  data    |
    size_t dump_bound() override { return sizeof(size_t) * (1 + size()) + _total_kv_pairs_usage; }

    bool dump(phmap::BinaryOutputArchive& ar) override;

    Status pk_dump(PrimaryKeyDump* dump, PrimaryIndexDumpPB* dump_pb) override;

    bool load_snapshot(phmap::BinaryInputArchive& ar) override;

    Status load(size_t& offset, std::unique_ptr<RandomAccessFile>& file) override;

    std::vector<std::vector<KVRef>> get_kv_refs_by_shard(size_t nshard, size_t num_entry,
                                                         bool with_null) const override;

    Status flush_to_immutable_index(std::unique_ptr<ImmutableIndexWriter>& writer, size_t nshard, size_t npage_hint,
                                    size_t nbucket, bool with_null) const override;

    size_t size() const override { return _set.size(); }

    size_t usage() const override { return _total_kv_pairs_usage; }

    size_t capacity() override { return _set.capacity(); }

    void reserve(size_t size) override { _set.reserve(size); }

    void clear() override {
        _set.clear();
        _total_kv_pairs_usage = 0;
    }

    // TODO: more accurate estimation for phmap::flat_hash_set<std::string, ...
    size_t memory_usage() override {
        auto ret = capacity() * (1 + 32);
        if (size() > 0 && _total_kv_pairs_usage / size() > 15) {
            // std::string with size > 15 will alloc new memory for storage
            ret += _total_kv_pairs_usage;
            // an malloc extra cost estimation
            ret += size() * 8;
        }
        return ret;
    }

private:
    friend ShardByLengthMutableIndex;
    friend PersistentIndex;
    phmap::flat_hash_set<KeyType, StringHasher2, EqualOnStringWithHash> _set;
    size_t _total_kv_pairs_usage = 0;
};

} // namespace starrocks