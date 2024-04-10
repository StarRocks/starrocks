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

namespace starrocks {

class Tablet;
class Schema;
class Column;
class PrimaryKeyDump;
class PersistentIndex;

namespace lake {
class LakeLocalPersistentIndex;
}

class ImmutableIndex {
public:
    // batch get
    // |n|: size of key/value array
    // |keys|: key array as slice array
    // |not_found|: information of keys not found in upper level, which needs to be checked in this level
    // |values|: value array for return values
    // |num_found|: add the number of keys found in L1 to this argument
    // |key_size|: the key size of keys array
    // |stat|: used for collect statistic
    Status get(size_t n, const Slice* keys, KeysInfo& keys_info, IndexValue* values, KeysInfo* found_keys_info,
               size_t key_size, IOStat* stat = nullptr);

    // batch check key existence
    Status check_not_exist(size_t n, const Slice* keys, size_t key_size);

    // get Immutable index file size;
    uint64_t file_size() {
        if (_file != nullptr) {
            auto res = _file->get_size();
            DCHECK(res.ok()) << res.status(); // FIXME: no abort
            return *res;
        } else {
            return 0;
        }
    }

    void clear() {
        if (_file != nullptr) {
            _file.reset();
        }
    }

    void destroy() {
        if (_file != nullptr) {
            WARN_IF_ERROR(FileSystem::Default()->delete_file(_file->filename()),
                          "Failed to delete file" + _file->filename());
            _file.reset();
        }
    }

    size_t total_usage() {
        size_t usage = 0;
        for (const auto& shard : _shards) {
            usage += shard.data_size;
        }
        return usage;
    }

    // return total kv count of this immutable index
    size_t total_size() {
        size_t size = 0;
        for (const auto& shard : _shards) {
            size += shard.size;
        }
        return size;
    }

    size_t memory_usage() {
        size_t mem_usage = 0;
        for (auto& bf : _bf_vec) {
            mem_usage += bf->size();
        }
        return mem_usage;
    }

    std::string filename() const {
        if (_file != nullptr) {
            return _file->filename();
        } else {
            return "";
        }
    }

    EditVersion version() const { return _version; }

    bool has_bf() { return !_bf_vec.empty(); }

    static StatusOr<std::unique_ptr<ImmutableIndex>> load(std::unique_ptr<RandomAccessFile>&& index_rb,
                                                          bool load_bf_data);

    Status pk_dump(PrimaryKeyDump* dump, PrimaryIndexDumpPB* dump_pb);

private:
    friend class PersistentIndex;
    friend class starrocks::lake::LakeLocalPersistentIndex;
    friend class ImmutableIndexWriter;

    Status _get_fixlen_kvs_for_shard(std::vector<std::vector<KVRef>>& kvs_by_shard, size_t shard_idx,
                                     uint32_t shard_bits, std::unique_ptr<ImmutableIndexShard>* shard) const;

    Status _get_varlen_kvs_for_shard(std::vector<std::vector<KVRef>>& kvs_by_shard, size_t shard_idx,
                                     uint32_t shard_bits, std::unique_ptr<ImmutableIndexShard>* shard) const;

    // get all the kv refs of a single shard by `shard_idx`, and add them to `kvs_by_shard`, the shard number of
    // kvs_by_shard may be different from this object's own shard number
    // NOTE: used by PersistentIndex only
    Status _get_kvs_for_shard(std::vector<std::vector<KVRef>>& kvs_by_shard, size_t shard_idx, uint32_t shard_bits,
                              std::unique_ptr<ImmutableIndexShard>* shard) const;

    Status _get_in_fixlen_shard(size_t shard_idx, size_t n, const Slice* keys, const std::vector<KeyInfo>& keys_info,
                                IndexValue* values, KeysInfo* found_keys_info,
                                std::unique_ptr<ImmutableIndexShard>* shard) const;

    Status _get_in_varlen_shard(size_t shard_idx, size_t n, const Slice* keys, std::vector<KeyInfo>& keys_info,
                                IndexValue* values, KeysInfo* found_keys_info,
                                std::unique_ptr<ImmutableIndexShard>* shard) const;

    Status _split_keys_info_by_page(size_t shard_idx, std::vector<KeyInfo>& keys_info,
                                    std::map<size_t, std::vector<KeyInfo>>& keys_info_by_page) const;

    Status _get_in_fixlen_shard_by_page(size_t shard_idx, size_t n, const Slice* keys, IndexValue* values,
                                        KeysInfo* found_keys_info,
                                        std::map<size_t, std::vector<KeyInfo>>& keys_info_by_page,
                                        std::map<size_t, IndexPage>& pages) const;

    Status _get_in_varlen_shard_by_page(size_t shard_idx, size_t n, const Slice* keys, IndexValue* values,
                                        KeysInfo* found_keys_info,
                                        std::map<size_t, std::vector<KeyInfo>>& keys_info_by_page,
                                        std::map<size_t, IndexPage>& pages) const;

    Status _get_in_shard_by_page(size_t shard_idx, size_t n, const Slice* keys, IndexValue* values,
                                 KeysInfo* found_keys_info, std::map<size_t, std::vector<KeyInfo>>& keys_info_by_page,
                                 IOStat* stat) const;

    Status _get_in_shard(size_t shard_idx, size_t n, const Slice* keys, std::vector<KeyInfo>& keys_info,
                         IndexValue* values, KeysInfo* found_keys_info, IOStat* stat) const;

    Status _check_not_exist_in_fixlen_shard(size_t shard_idx, size_t n, const Slice* keys, const KeysInfo& keys_info,
                                            std::unique_ptr<ImmutableIndexShard>* shard) const;

    Status _check_not_exist_in_varlen_shard(size_t shard_idx, size_t n, const Slice* keys, const KeysInfo& keys_info,
                                            std::unique_ptr<ImmutableIndexShard>* shard) const;

    Status _check_not_exist_in_shard(size_t shard_idx, size_t n, const Slice* keys, const KeysInfo& keys_info) const;

    bool _need_bloom_filter(size_t idx_begin, size_t idx_end, std::vector<KeysInfo>& keys_info_by_shard) const;

    Status _prepare_bloom_filter(size_t idx_begin, size_t idx_end) const;

    bool _filter(size_t shard_idx, std::vector<KeyInfo>& keys_info, std::vector<KeyInfo>* res) const;

    std::unique_ptr<RandomAccessFile> _file;
    EditVersion _version;
    size_t _size = 0;

    struct ShardInfo {
        uint64_t offset;
        uint64_t bytes;
        uint32_t npage;
        uint32_t size; // kv count
        uint32_t key_size;
        uint32_t value_size;
        uint32_t nbucket;
        uint64_t data_size;
        uint64_t uncompressed_size;
    };

    std::vector<ShardInfo> _shards;
    std::map<size_t, std::pair<size_t, size_t>> _shard_info_by_length;
    mutable std::vector<std::unique_ptr<BloomFilter>> _bf_vec;
    std::vector<size_t> _bf_off;
    CompressionTypePB _compression_type;
};

} // namespace starrocks