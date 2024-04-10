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

#include "storage/pindex_mutable_index.h"

#include "io/io_profiler.h"
#include "storage/primary_key_dump.h"
#include "storage/update_manager.h"
#include "util/crc32c.h"
#include "util/faststring.h"
#include "util/filesystem_util.h"
#include "util/xxh3.h"

namespace starrocks {

static std::string get_l0_index_file_name(std::string& dir, const EditVersion& version) {
    return strings::Substitute("$0/index.l0.$1.$2", dir, version.major_number(), version.minor_number());
}

MutableIndex::MutableIndex() = default;

MutableIndex::~MutableIndex() = default;

template <size_t KeySize>
struct FixedKey {
    uint8_t data[KeySize];
};

template <size_t KeySize>
bool operator==(const FixedKey<KeySize>& lhs, const FixedKey<KeySize>& rhs) {
    return memcmp(lhs.data, rhs.data, KeySize) == 0;
}

template <size_t KeySize>
struct FixedKeyHash {
    uint64_t operator()(const FixedKey<KeySize>& k) const { return XXH3_64bits(k.data, KeySize); }
};

uint64_t key_index_hash(const void* data, size_t len) {
    return XXH3_64bits(data, len);
}

template <size_t KeySize>
class FixedMutableIndex : public MutableIndex {
public:
    using KeyType = FixedKey<KeySize>;
    FixedMutableIndex() = default;
    ~FixedMutableIndex() override = default;

    Status get(const Slice* keys, IndexValue* values, KeysInfo* not_found, size_t* num_found,
               const std::vector<size_t>& idxes) const override {
        size_t nfound = 0;
        for (const auto idx : idxes) {
            const auto& key = *reinterpret_cast<const KeyType*>(keys[idx].data);
            uint64_t hash = FixedKeyHash<KeySize>()(key);
            auto iter = _map.find(key, hash);
            if (iter == _map.end()) {
                values[idx] = NullIndexValue;
                not_found->key_infos.emplace_back((uint32_t)idx, hash);
            } else {
                values[idx] = iter->second;
                nfound += iter->second.get_value() != NullIndexValue;
            }
        }
        *num_found = nfound;
        return Status::OK();
    }

    Status upsert(const Slice* keys, const IndexValue* values, IndexValue* old_values, KeysInfo* not_found,
                  size_t* num_found, const std::vector<size_t>& idxes) override {
        size_t nfound = 0;
        for (const auto idx : idxes) {
            const auto& key = *reinterpret_cast<const KeyType*>(keys[idx].data);
            const auto value = values[idx];
            uint64_t hash = FixedKeyHash<KeySize>()(key);
            if (auto [it, inserted] = _map.emplace_with_hash(hash, key, value); inserted) {
                not_found->key_infos.emplace_back((uint32_t)idx, hash);
            } else {
                auto old_value = it->second;
                old_values[idx] = old_value;
                nfound += old_value.get_value() != NullIndexValue;
                it->second = value;
            }
        }
        *num_found = nfound;
        return Status::OK();
    }

    Status upsert(const Slice* keys, const IndexValue* values, KeysInfo* not_found, size_t* num_found,
                  const std::vector<size_t>& idxes) override {
        size_t nfound = 0;
        for (const auto idx : idxes) {
            const auto& key = *reinterpret_cast<const KeyType*>(keys[idx].data);
            const auto value = values[idx];
            uint64_t hash = FixedKeyHash<KeySize>()(key);
            if (auto [it, inserted] = _map.emplace_with_hash(hash, key, value); inserted) {
                not_found->key_infos.emplace_back((uint32_t)idx, hash);
            } else {
                auto old_value = it->second;
                nfound += old_value.get_value() != NullIndexValue;
                it->second = value;
            }
        }
        *num_found = nfound;
        return Status::OK();
    }

    Status insert(const Slice* keys, const IndexValue* values, const std::vector<size_t>& idxes) override {
        for (const auto idx : idxes) {
            const auto& key = *reinterpret_cast<const KeyType*>(keys[idx].data);
            const auto value = values[idx];
            uint64_t hash = FixedKeyHash<KeySize>()(key);
            if (auto [it, inserted] = _map.emplace_with_hash(hash, key, value); !inserted) {
                auto old = reinterpret_cast<uint64_t*>(&(it->second));
                auto old_rssid = (uint32_t)((*old) >> 32);
                auto old_rowid = (uint32_t)((*old) & ROWID_MASK);
                auto new_value = reinterpret_cast<uint64_t*>(const_cast<IndexValue*>(&value));
                std::string msg = strings::Substitute(
                        "FixedMutableIndex<$0> insert found duplicate key $1, new(rssid=$2 rowid=$3), old(rssid=$4 "
                        "rowid=$5)",
                        KeySize, hexdump((const char*)key.data, KeySize), (uint32_t)((*new_value) >> 32),
                        (uint32_t)((*new_value) & ROWID_MASK), old_rssid, old_rowid);
                LOG(WARNING) << msg;
                return Status::AlreadyExist(msg);
            }
        }
        return Status::OK();
    }

    Status erase(const Slice* keys, IndexValue* old_values, KeysInfo* not_found, size_t* num_found,
                 const std::vector<size_t>& idxes) override {
        size_t nfound = 0;
        for (const auto idx : idxes) {
            const auto& key = *reinterpret_cast<const KeyType*>(keys[idx].data);
            uint64_t hash = FixedKeyHash<KeySize>()(key);
            if (auto [it, inserted] = _map.emplace_with_hash(hash, key, IndexValue(NullIndexValue)); inserted) {
                old_values[idx] = NullIndexValue;
                not_found->key_infos.emplace_back((uint32_t)idx, hash);
            } else {
                old_values[idx] = it->second;
                nfound += it->second.get_value() != NullIndexValue;
                it->second = NullIndexValue;
            }
        }
        *num_found = nfound;
        return Status::OK();
    }

    Status replace(const Slice* keys, const IndexValue* values, const std::vector<size_t>& replace_idxes) override {
        for (unsigned long replace_idxe : replace_idxes) {
            const auto& key = *reinterpret_cast<const KeyType*>(keys[replace_idxe].data);
            const auto value = values[replace_idxe];
            uint64_t hash = FixedKeyHash<KeySize>()(key);
            if (auto [it, inserted] = _map.emplace_with_hash(hash, key, value); !inserted) {
                it->second = value;
            }
        }
        return Status::OK();
    }

    Status append_wal(const Slice* keys, const IndexValue* values, const std::vector<size_t>& idxes,
                      std::unique_ptr<WritableFile>& index_file, uint64_t* page_size, uint32_t* checksum) override {
        faststring fixed_buf;
        fixed_buf.reserve(sizeof(size_t) + sizeof(size_t) + idxes.size() * (KeySize + sizeof(IndexValue)));
        put_fixed32_le(&fixed_buf, KeySize);
        put_fixed32_le(&fixed_buf, idxes.size());
        for (const auto idx : idxes) {
            const auto value = (values != nullptr) ? values[idx] : IndexValue(NullIndexValue);
            fixed_buf.append(keys[idx].data, KeySize);
            put_fixed64_le(&fixed_buf, value.get_value());
        }
        RETURN_IF_ERROR(index_file->append(fixed_buf));
        *page_size += fixed_buf.size();
        // incremental calc crc32
        *checksum = crc32c::Extend(*checksum, (const char*)fixed_buf.data(), fixed_buf.size());
        return Status::OK();
    }

    Status load_wals(size_t n, const Slice* keys, const IndexValue* values) override {
        for (size_t i = 0; i < n; i++) {
            const auto& key = *reinterpret_cast<const KeyType*>(keys[i].data);
            const auto value = values[i];
            uint64_t hash = FixedKeyHash<KeySize>()(key);
            if (auto [it, inserted] = _map.emplace_with_hash(hash, key, value); !inserted) {
                it->second = value;
            }
        }
        return Status::OK();
    }

    bool load_snapshot(phmap::BinaryInputArchive& ar) override { return _map.load(ar); }

    Status load(size_t& offset, std::unique_ptr<RandomAccessFile>& file) override {
        size_t kv_header_size = 8;
        std::string buff;
        raw::stl_string_resize_uninitialized(&buff, kv_header_size);
        RETURN_IF_ERROR(file->read_at_fully(offset, buff.data(), buff.size()));
        uint32_t key_size = UNALIGNED_LOAD32(buff.data());
        DCHECK(key_size == KeySize);
        offset += kv_header_size;
        uint32_t nums = UNALIGNED_LOAD32(buff.data() + 4);
        const size_t kv_pair_size = KeySize + sizeof(IndexValue);
        while (nums > 0) {
            const size_t batch_num = (nums > 4096) ? 4096 : nums;
            raw::stl_string_resize_uninitialized(&buff, batch_num * kv_pair_size);
            RETURN_IF_ERROR(file->read_at_fully(offset, buff.data(), buff.size()));
            std::vector<Slice> keys;
            keys.reserve(batch_num);
            std::vector<IndexValue> values;
            values.reserve(batch_num);
            size_t buf_offset = 0;
            for (size_t i = 0; i < batch_num; ++i) {
                keys.emplace_back(buff.data() + buf_offset, KeySize);
                uint64_t value = UNALIGNED_LOAD64(buff.data() + buf_offset + KeySize);
                values.emplace_back(value);
                buf_offset += kv_pair_size;
            }
            RETURN_IF_ERROR(load_wals(batch_num, keys.data(), values.data()));
            offset += batch_num * kv_pair_size;
            nums -= batch_num;
        }
        return Status::OK();
    }

    // return the dump file size if dump _map into a new file
    // If _map is empty, _map.dump_bound() will  set empty hash set serialize_size larger
    // than sizeof(uint64_t) in order to improve count distinct streaming aggregate performance.
    // Howevevr, the real snapshot file will only wite a size_(type is size_t) into file. So we
    // will use `sizeof(size_t)` as return value.
    size_t dump_bound() override { return _map.empty() ? sizeof(size_t) : _map.dump_bound(); }

    bool dump(phmap::BinaryOutputArchive& ar) override { return _map.dump(ar); }

    Status pk_dump(PrimaryKeyDump* dump, PrimaryIndexDumpPB* dump_pb) override {
        for (const auto& each : _map) {
            RETURN_IF_ERROR(dump->add_pindex_kvs(
                    std::string_view(reinterpret_cast<const char*>(each.first.data), sizeof(KeyType)),
                    each.second.get_value(), dump_pb));
        }
        return dump->finish_pindex_kvs(dump_pb);
    }

    std::vector<std::vector<KVRef>> get_kv_refs_by_shard(size_t nshard, size_t num_entry,
                                                         bool with_null) const override {
        std::vector<std::vector<KVRef>> ret(nshard);
        uint32_t shard_bits = log2(nshard);
        for (auto i = 0; i < nshard; ++i) {
            ret[i].reserve(num_entry / nshard * 100 / 85);
        }
        auto hasher = FixedKeyHash<KeySize>();
        for (const auto& [key, value] : _map) {
            if (!with_null && value.get_value() == NullIndexValue) {
                continue;
            }
            IndexHash h(hasher(key));
            ret[h.shard(shard_bits)].emplace_back((uint8_t*)&key, h.hash, KeySize + kIndexValueSize);
        }
        return ret;
    }

    Status flush_to_immutable_index(std::unique_ptr<ImmutableIndexWriter>& writer, size_t nshard, size_t npage_hint,
                                    size_t nbucket, bool with_null) const override {
        if (nshard > 0) {
            const auto& kv_ref_by_shard = get_kv_refs_by_shard(nshard, size(), with_null);
            for (const auto& kvs : kv_ref_by_shard) {
                RETURN_IF_ERROR(writer->write_shard(KeySize, npage_hint, nbucket, kvs));
            }
        }
        return Status::OK();
    }

    size_t size() const override { return _map.size(); }

    size_t usage() const override { return (KeySize + kIndexValueSize) * _map.size(); }

    size_t capacity() override { return _map.capacity(); }

    void reserve(size_t size) override { _map.reserve(size); }

    void clear() override { _map.clear(); }

    size_t memory_usage() override { return _map.capacity() * (1 + (KeySize + 3) / 4 * 4 + kIndexValueSize); }

private:
    phmap::flat_hash_map<KeyType, IndexValue, FixedKeyHash<KeySize>> _map;
};

std::tuple<size_t, size_t> MutableIndex::estimate_nshard_and_npage(const size_t total_kv_pairs_usage) {
    // if size == 0, will return { nshard:1, npage:0 }, meaning an empty shard
    size_t cap = total_kv_pairs_usage * 100 / kDefaultUsagePercent;
    size_t nshard = 1;
    while (nshard * 1024 * 1024 < cap) {
        nshard *= 2;
        if (nshard == kShardMax) {
            break;
        }
    }
    size_t npage = npad(cap / nshard, kPageSize);
    return {nshard, npage};
}

size_t MutableIndex::estimate_nbucket(size_t key_size, size_t size, size_t nshard, size_t npage) {
    if (key_size != 0 && key_size < kLongKeySize) {
        return kBucketPerPage;
    }
    // if size == 0, return 1 or return kBucketPerPage?
    if (size == 0) {
        return 1;
    }
    size_t pad = nshard * npage * kRecordPerBucket;
    return std::min(kBucketPerPage, npad(size, pad));
}

Status SliceMutableIndex::get(const Slice* keys, IndexValue* values, KeysInfo* not_found, size_t* num_found,
                              const std::vector<size_t>& idxes) const {
    size_t nfound = 0;
    for (const auto idx : idxes) {
        std::string composite_key;
        const auto& skey = keys[idx];
        const auto value = values[idx];
        composite_key.reserve(skey.size + kIndexValueSize);
        composite_key.append(skey.data, skey.size);
        put_fixed64_le(&composite_key, value.get_value());
        uint64_t hash = StringHasher2()(composite_key);
        auto iter = _set.find(composite_key, hash);
        if (iter == _set.end()) {
            values[idx] = NullIndexValue;
            not_found->key_infos.emplace_back((uint32_t)idx, hash);
        } else {
            const auto& composite_key = *iter;
            auto value = UNALIGNED_LOAD64(composite_key.data() + composite_key.size() - kIndexValueSize);
            values[idx] = IndexValue(value);
            nfound += value != NullIndexValue;
        }
    }
    *num_found = nfound;
    return Status::OK();
}

Status SliceMutableIndex::upsert(const Slice* keys, const IndexValue* values, IndexValue* old_values,
                                 KeysInfo* not_found, size_t* num_found, const std::vector<size_t>& idxes) {
    size_t nfound = 0;
    for (const auto idx : idxes) {
        std::string composite_key;
        const auto& skey = keys[idx];
        const auto value = values[idx];
        composite_key.reserve(skey.size + kIndexValueSize);
        composite_key.append(skey.data, skey.size);
        put_fixed64_le(&composite_key, value.get_value());
        uint64_t hash = StringHasher2()(composite_key);
        if (auto [it, inserted] = _set.emplace_with_hash(hash, composite_key); inserted) {
            not_found->key_infos.emplace_back((uint32_t)idx, hash);
            _total_kv_pairs_usage += composite_key.size();
        } else {
            const auto& old_compose_key = *it;
            auto old_value = UNALIGNED_LOAD64(old_compose_key.data() + old_compose_key.size() - kIndexValueSize);
            old_values[idx] = old_value;
            nfound += old_value != NullIndexValue;
            _set.erase(it);
            _set.emplace_with_hash(hash, composite_key);
        }
    }
    *num_found = nfound;
    return Status::OK();
}

Status SliceMutableIndex::upsert(const Slice* keys, const IndexValue* values, KeysInfo* not_found, size_t* num_found,
                                 const std::vector<size_t>& idxes) {
    size_t nfound = 0;
    for (const auto idx : idxes) {
        std::string composite_key;
        const auto& skey = keys[idx];
        const auto value = values[idx];
        composite_key.reserve(skey.size + kIndexValueSize);
        composite_key.append(skey.data, skey.size);
        put_fixed64_le(&composite_key, value.get_value());
        uint64_t hash = StringHasher2()(composite_key);
        if (auto [it, inserted] = _set.emplace_with_hash(hash, composite_key); inserted) {
            not_found->key_infos.emplace_back((uint32_t)idx, hash);
            _total_kv_pairs_usage += composite_key.size();
        } else {
            const auto& old_compose_key = *it;
            const auto old_value = UNALIGNED_LOAD64(old_compose_key.data() + old_compose_key.size() - kIndexValueSize);
            nfound += old_value != NullIndexValue;
            // TODO: find a way to modify iterator directly, currently just erase then re-insert
            _set.erase(it);
            _set.emplace_with_hash(hash, composite_key);
        }
    }
    *num_found = nfound;
    return Status::OK();
}

Status SliceMutableIndex::insert(const Slice* keys, const IndexValue* values, const std::vector<size_t>& idxes) {
    for (const auto idx : idxes) {
        std::string composite_key;
        const auto& skey = keys[idx];
        const auto value = values[idx];
        composite_key.reserve(skey.size + kIndexValueSize);
        composite_key.append(skey.data, skey.size);
        put_fixed64_le(&composite_key, value.get_value());
        uint64_t hash = StringHasher2()(composite_key);
        if (auto [it, inserted] = _set.emplace_with_hash(hash, composite_key); inserted) {
            _total_kv_pairs_usage += composite_key.size();
        } else {
            auto& old_compose_key = *it;
            auto old_value = UNALIGNED_LOAD64(old_compose_key.data() + old_compose_key.size() - kIndexValueSize);
            auto old_rssid = (uint32_t)(old_value >> 32);
            auto old_rowid = (uint32_t)(old_value & ROWID_MASK);
            auto new_value = reinterpret_cast<uint64_t*>(const_cast<IndexValue*>(&value));
            std::string msg = strings::Substitute(
                    "SliceMutableIndex key_size=$0 insert found duplicate key $1, "
                    "new(rssid=$2 rowid=$3), old(rssid=$4 rowid=$5)",
                    skey.size, hexdump((const char*)skey.data, skey.size), (uint32_t)((*new_value) >> 32),
                    (uint32_t)((*new_value) & ROWID_MASK), old_rssid, old_rowid);
            LOG(WARNING) << msg;
            return Status::AlreadyExist(msg);
        }
    }
    return Status::OK();
}

Status SliceMutableIndex::erase(const Slice* keys, IndexValue* old_values, KeysInfo* not_found, size_t* num_found,
                                const std::vector<size_t>& idxes) {
    size_t nfound = 0;
    for (const auto idx : idxes) {
        std::string composite_key;
        const auto& skey = keys[idx];
        const auto value = NullIndexValue;
        composite_key.reserve(skey.size + kIndexValueSize);
        composite_key.append(skey.data, skey.size);
        put_fixed64_le(&composite_key, value);
        uint64_t hash = StringHasher2()(composite_key);
        if (auto [it, inserted] = _set.emplace_with_hash(hash, composite_key); inserted) {
            old_values[idx] = NullIndexValue;
            not_found->key_infos.emplace_back((uint32_t)idx, hash);
            _total_kv_pairs_usage += composite_key.size();
        } else {
            auto& old_compose_key = *it;
            auto old_value = UNALIGNED_LOAD64(old_compose_key.data() + old_compose_key.size() - kIndexValueSize);
            old_values[idx] = old_value;
            nfound += old_value != NullIndexValue;
            // TODO: find a way to modify iterator directly, currently just erase then re-insert
            _set.erase(it);
            _set.emplace_with_hash(hash, composite_key);
        }
    }
    *num_found = nfound;
    return Status::OK();
}

Status SliceMutableIndex::replace(const Slice* keys, const IndexValue* values, const std::vector<size_t>& idxes) {
    for (const auto idx : idxes) {
        std::string composite_key;
        const auto& skey = keys[idx];
        const auto value = values[idx];
        composite_key.reserve(skey.size + kIndexValueSize);
        composite_key.append(skey.data, skey.size);
        put_fixed64_le(&composite_key, value.get_value());
        uint64_t hash = StringHasher2()(composite_key);
        if (auto [it, inserted] = _set.emplace_with_hash(hash, composite_key); inserted) {
            _total_kv_pairs_usage += composite_key.size();
        } else {
            // TODO: find a way to modify iterator directly, currently just erase then re-insert
            _set.erase(it);
            _set.emplace_with_hash(hash, composite_key);
        }
    }
    return Status::OK();
}

Status SliceMutableIndex::append_wal(const Slice* keys, const IndexValue* values, const std::vector<size_t>& idxes,
                                     std::unique_ptr<WritableFile>& index_file, uint64_t* page_size,
                                     uint32_t* checksum) {
    faststring fixed_buf;
    size_t keys_size = 0;
    auto n = idxes.size();
    for (const auto idx : idxes) {
        keys_size += keys[idx].size;
    }
    fixed_buf.reserve(keys_size + n * (kWALKVSize + kIndexValueSize));
    put_fixed32_le(&fixed_buf, kKeySizeMagicNum);
    put_fixed32_le(&fixed_buf, idxes.size());
    for (const auto idx : idxes) {
        const auto& key = keys[idx];
        const auto value = (values != nullptr) ? values[idx] : IndexValue(NullIndexValue);
        WALKVSizeType kv_size = key.size + kIndexValueSize;
        put_fixed32_le(&fixed_buf, kv_size);
        fixed_buf.append(key.data, key.size);
        put_fixed64_le(&fixed_buf, value.get_value());
    }
    RETURN_IF_ERROR(index_file->append(fixed_buf));
    *page_size += fixed_buf.size();
    // incremental calc crc32
    *checksum = crc32c::Extend(*checksum, (const char*)fixed_buf.data(), fixed_buf.size());
    return Status::OK();
}

Status SliceMutableIndex::load_wals(size_t n, const Slice* keys, const IndexValue* values) {
    for (size_t i = 0; i < n; i++) {
        std::string composite_key;
        const auto& skey = keys[i];
        const auto value = values[i];
        composite_key.reserve(skey.size + kIndexValueSize);
        composite_key.append(skey.data, skey.size);
        put_fixed64_le(&composite_key, value.get_value());
        uint64_t hash = StringHasher2()(composite_key);
        if (auto [it, inserted] = _set.emplace_with_hash(hash, composite_key); inserted) {
            _total_kv_pairs_usage += composite_key.size();
        } else {
            // TODO: find a way to modify iterator directly, currently just erase then re-insert
            _set.erase(it);
            _set.emplace_with_hash(hash, composite_key);
        }
    }
    return Status::OK();
}

bool SliceMutableIndex::dump(phmap::BinaryOutputArchive& ar) {
    if (!ar.dump(size())) {
        LOG(ERROR) << "Failed to dump size";
        return false;
    }
    if (size() == 0) {
        return true;
    }
    for (const auto& composite_key : _set) {
        if (!ar.dump(static_cast<size_t>(composite_key.size()))) {
            LOG(ERROR) << "Failed to dump compose_key_size";
            return false;
        }
        if (composite_key.size() == 0) {
            continue;
        }
        if (!ar.dump(composite_key.data(), composite_key.size())) {
            LOG(ERROR) << "Failed to dump composite_key";
            return false;
        }
    }
    return true;

    // TODO: construct a large buffer and write instead of one by one.
    // TODO: dive in phmap internal detail and implement dump of std::string type inside, use ctrl_&slot_ directly to improve performance
    // return _set.dump(ar);
}

Status SliceMutableIndex::pk_dump(PrimaryKeyDump* dump, PrimaryIndexDumpPB* dump_pb) {
    for (const auto& composite_key : _set) {
        auto value = UNALIGNED_LOAD64(composite_key.data() + composite_key.size() - kIndexValueSize);
        RETURN_IF_ERROR(dump->add_pindex_kvs(
                std::string_view(composite_key.data(), composite_key.size() - kIndexValueSize), value, dump_pb));
    }
    return dump->finish_pindex_kvs(dump_pb);
}

bool SliceMutableIndex::load_snapshot(phmap::BinaryInputArchive& ar) {
    size_t size = 0;
    if (!ar.load(&size)) {
        LOG(ERROR) << "Failed to load size";
        return false;
    }
    if (size == 0) {
        return true;
    }
    reserve(size);
    for (auto i = 0; i < size; ++i) {
        size_t compose_key_size = 0;
        if (!ar.load(&compose_key_size)) {
            LOG(ERROR) << "Failed to load compose_key_size";
            return false;
        }
        if (compose_key_size == 0) {
            continue;
        }
        std::string composite_key;
        raw::stl_string_resize_uninitialized(&composite_key, compose_key_size);
        if (!ar.load(composite_key.data(), composite_key.size())) {
            LOG(ERROR) << "Failed to load composite_key";
            return false;
        }
        auto [it, inserted] = _set.emplace(composite_key);
        if (inserted) {
            _total_kv_pairs_usage += composite_key.size();
        } else {
            _set.erase(it);
            _set.emplace(composite_key);
        }
    }
    return true;

    // TODO: read a large buffer and parse instead of one by one.
    // TODO: dive in phmap internal detail and implement load of std::string type inside, use ctrl_&slot_ directly to improve performance
    // return _set.load(ar);
}

// TODO: read data in less batch, not one by one.
Status SliceMutableIndex::load(size_t& offset, std::unique_ptr<RandomAccessFile>& file) {
    const auto kv_header_size = 8;
    std::string buff;
    raw::stl_string_resize_uninitialized(&buff, kv_header_size);
    RETURN_IF_ERROR(file->read_at_fully(offset, buff.data(), buff.size()));
    offset += kv_header_size;
    const auto key_size = UNALIGNED_LOAD32(buff.data());
    DCHECK(key_size == kKeySizeMagicNum);
    auto nums = UNALIGNED_LOAD32(buff.data() + kv_header_size - 4);
    while (nums > 0) {
        size_t batch_num = (nums > 4096) ? 4096 : nums;
        Slice keys[batch_num];
        std::vector<IndexValue> values;
        values.reserve(batch_num);
        std::vector<std::string> kv_buffs(batch_num);
        for (size_t i = 0; i < batch_num; ++i) {
            raw::stl_string_resize_uninitialized(&buff, sizeof(uint32_t));
            RETURN_IF_ERROR(file->read_at_fully(offset, buff.data(), buff.size()));
            offset += sizeof(uint32_t);
            const auto kv_pair_size = UNALIGNED_LOAD32(buff.data());
            raw::stl_string_resize_uninitialized(&kv_buffs[i], kv_pair_size);
            RETURN_IF_ERROR(file->read_at_fully(offset, kv_buffs[i].data(), kv_buffs[i].size()));
            keys[i] = Slice(kv_buffs[i].data(), kv_pair_size - kIndexValueSize);
            const auto value = UNALIGNED_LOAD64(kv_buffs[i].data() + kv_pair_size - kIndexValueSize);
            values.emplace_back(value);
            offset += kv_pair_size;
        }
        RETURN_IF_ERROR(load_wals(batch_num, keys, values.data()));
        nums -= batch_num;
    }
    return Status::OK();
}

std::vector<std::vector<KVRef>> SliceMutableIndex::get_kv_refs_by_shard(size_t nshard, size_t num_entry,
                                                                        bool with_null) const {
    std::vector<std::vector<KVRef>> ret(nshard);
    uint32_t shard_bits = log2(nshard);
    for (auto i = 0; i < nshard; ++i) {
        ret[i].reserve(num_entry / nshard * 100 / 85);
    }
    for (const auto& composite_key : _set) {
        const auto value = UNALIGNED_LOAD64(composite_key.data() + composite_key.size() - kIndexValueSize);
        IndexHash h(StringHasher2()(composite_key));
        if (!with_null && value == NullIndexValue) {
            continue;
        }
        ret[h.shard(shard_bits)].emplace_back((uint8_t*)(composite_key.data()), h.hash, composite_key.size());
    }
    return ret;
}

Status SliceMutableIndex::flush_to_immutable_index(std::unique_ptr<ImmutableIndexWriter>& writer, size_t nshard,
                                                   size_t npage_hint, size_t nbucket, bool with_null) const {
    if (nshard > 0) {
        const auto& kv_ref_by_shard = get_kv_refs_by_shard(nshard, size(), with_null);
        for (const auto& kvs : kv_ref_by_shard) {
            RETURN_IF_ERROR(writer->write_shard(kKeySizeMagicNum, npage_hint, nbucket, kvs));
        }
    }
    return Status::OK();
}

StatusOr<std::unique_ptr<MutableIndex>> MutableIndex::create(size_t key_size) {
#define CASE_SIZE(s) \
    case s:          \
        return std::make_unique<FixedMutableIndex<s>>();
#define CASE_SIZE_8(s) \
    CASE_SIZE(s)       \
    CASE_SIZE(s + 1)   \
    CASE_SIZE(s + 2)   \
    CASE_SIZE(s + 3)   \
    CASE_SIZE(s + 4)   \
    CASE_SIZE(s + 5)   \
    CASE_SIZE(s + 6)   \
    CASE_SIZE(s + 7)
    switch (key_size) {
    case 0:
        return std::make_unique<SliceMutableIndex>();
        CASE_SIZE_8(1)
        CASE_SIZE_8(9)
        CASE_SIZE_8(17)
        CASE_SIZE_8(25)
        CASE_SIZE_8(33)
        CASE_SIZE_8(41)
        CASE_SIZE_8(49)
        CASE_SIZE_8(57)
        CASE_SIZE_8(65)
        CASE_SIZE_8(73)
        CASE_SIZE_8(81)
        CASE_SIZE_8(89)
        CASE_SIZE_8(97)
        CASE_SIZE_8(105)
        CASE_SIZE_8(113)
        CASE_SIZE_8(121)
#undef CASE_SIZE_8
#undef CASE_SIZE
    default:
        return Status::NotSupported("FixedMutableIndex not support key size large than 128");
    }
}

template <>
void ShardByLengthMutableIndex::_init_loop_helper<0>() {
    _shards.push_back(std::make_unique<SliceMutableIndex>());
    _shard_info_by_key_size[0] = std::make_pair(0, 1);
}

template <int N>
void ShardByLengthMutableIndex::_init_loop_helper() {
    _init_loop_helper<N - 1>();
    _shards.push_back(std::make_unique<FixedMutableIndex<N>>());
    _shard_info_by_key_size[N] = std::make_pair(N, 1);
}

Status ShardByLengthMutableIndex::init() {
    if (_fixed_key_size > 0) {
        auto st = MutableIndex::create(_fixed_key_size);
        if (!st.ok()) {
            return st.status();
        }
        _shards.push_back(std::move(st).value());
        _shard_info_by_key_size[_fixed_key_size] = std::make_pair(0, 1);
    } else if (_fixed_key_size == 0) {
        _shards.reserve(kSliceMaxFixLength + 1);
        _init_loop_helper<kSliceMaxFixLength>();
        return Status::OK();
    }
    return Status::OK();
}

StatusOr<std::unique_ptr<ShardByLengthMutableIndex>> ShardByLengthMutableIndex::create(size_t key_size,
                                                                                       const std::string& path) {
    auto mutable_index = std::make_unique<ShardByLengthMutableIndex>(key_size, path);
    RETURN_IF_ERROR(mutable_index->init());
    return mutable_index;
}

std::vector<std::vector<size_t>> ShardByLengthMutableIndex::split_keys_by_shard(size_t nshard, const Slice* keys,
                                                                                size_t idx_begin, size_t idx_end) {
    uint32_t shard_bits = log2(nshard);
    std::vector<std::vector<size_t>> idxes_by_shard(nshard);
    if (_fixed_key_size > 0) {
#define CASE_SIZE(s)                                                                        \
    case s: {                                                                               \
        auto hash_func = FixedKeyHash<s>();                                                 \
        for (auto i = idx_begin; i < idx_end; i++) {                                        \
            IndexHash hash(hash_func(*reinterpret_cast<const FixedKey<s>*>(keys[i].data))); \
            idxes_by_shard[hash.shard(shard_bits)].push_back(i);                            \
        }                                                                                   \
    } break;

#define CASE_SIZE_8(s) \
    CASE_SIZE(s)       \
    CASE_SIZE(s + 1)   \
    CASE_SIZE(s + 2)   \
    CASE_SIZE(s + 3)   \
    CASE_SIZE(s + 4)   \
    CASE_SIZE(s + 5)   \
    CASE_SIZE(s + 6)   \
    CASE_SIZE(s + 7)

        switch (_fixed_key_size) {
            CASE_SIZE_8(1)
            CASE_SIZE_8(9)
            CASE_SIZE_8(17)
            CASE_SIZE_8(25)
            CASE_SIZE_8(33)
            CASE_SIZE_8(41)
            CASE_SIZE_8(49)
            CASE_SIZE_8(57)
            CASE_SIZE_8(65)
            CASE_SIZE_8(73)
            CASE_SIZE_8(81)
            CASE_SIZE_8(89)
            CASE_SIZE_8(97)
            CASE_SIZE_8(105)
            CASE_SIZE_8(113)
            CASE_SIZE_8(121)
#undef CASE_SIZE_8
#undef CASE_SIZE
        }
    } else if (_fixed_key_size == 0) {
        const auto* fkeys = reinterpret_cast<const Slice*>(keys);
        for (size_t i = idx_begin; i < idx_end; i++) {
            const auto& key = fkeys[i];
            IndexHash hash(key_index_hash(key.data, key.size));
            idxes_by_shard[hash.shard(shard_bits)].push_back(i);
        }
    }
    return idxes_by_shard;
}

std::vector<std::vector<size_t>> ShardByLengthMutableIndex::split_keys_by_shard(size_t nshard, const Slice* keys,
                                                                                const std::vector<size_t>& idxes) {
    uint32_t shard_bits = log2(nshard);
    std::vector<std::vector<size_t>> idxes_by_shard(nshard);
    if (_fixed_key_size > 0) {
#define CASE_SIZE(s)                                                                          \
    case s: {                                                                                 \
        auto hash_func = FixedKeyHash<s>();                                                   \
        for (const auto idx : idxes) {                                                        \
            IndexHash hash(hash_func(*reinterpret_cast<const FixedKey<s>*>(keys[idx].data))); \
            idxes_by_shard[hash.shard(shard_bits)].emplace_back(idx);                         \
        }                                                                                     \
    } break;

#define CASE_SIZE_8(s) \
    CASE_SIZE(s)       \
    CASE_SIZE(s + 1)   \
    CASE_SIZE(s + 2)   \
    CASE_SIZE(s + 3)   \
    CASE_SIZE(s + 4)   \
    CASE_SIZE(s + 5)   \
    CASE_SIZE(s + 6)   \
    CASE_SIZE(s + 7)

        switch (_fixed_key_size) {
            CASE_SIZE_8(1)
            CASE_SIZE_8(9)
            CASE_SIZE_8(17)
            CASE_SIZE_8(25)
            CASE_SIZE_8(33)
            CASE_SIZE_8(41)
            CASE_SIZE_8(49)
            CASE_SIZE_8(57)
            CASE_SIZE_8(65)
            CASE_SIZE_8(73)
            CASE_SIZE_8(81)
            CASE_SIZE_8(89)
            CASE_SIZE_8(97)
            CASE_SIZE_8(105)
            CASE_SIZE_8(113)
            CASE_SIZE_8(121)
#undef CASE_SIZE_8
#undef CASE_SIZE
        }
    } else if (_fixed_key_size == 0) {
        const auto* fkeys = reinterpret_cast<const Slice*>(keys);
        for (const auto idx : idxes) {
            const auto& key = fkeys[idx];
            IndexHash hash(key_index_hash(key.data, key.size));
            idxes_by_shard[hash.shard(shard_bits)].emplace_back(idx);
        }
    }
    return idxes_by_shard;
}

Status ShardByLengthMutableIndex::get(size_t n, const Slice* keys, IndexValue* values, size_t* num_found,
                                      std::map<size_t, KeysInfo>& not_founds_by_key_size) {
    DCHECK(_fixed_key_size != -1);
    if (_fixed_key_size > 0) {
        const auto [shard_offset, shard_size] = _shard_info_by_key_size[_fixed_key_size];
        const auto idxes_by_shard = split_keys_by_shard(shard_size, keys, 0, n);
        auto& not_found = not_founds_by_key_size[_fixed_key_size];
        for (size_t i = 0; i < shard_size; ++i) {
            RETURN_IF_ERROR(_shards[shard_offset + i]->get(keys, values, &not_found, num_found, idxes_by_shard[i]));
        }
    } else {
        DCHECK(_fixed_key_size == 0);
        const auto* fkeys = reinterpret_cast<const Slice*>(keys);
        std::map<size_t, std::vector<size_t>> idxes_by_key_size;
        for (size_t i = 0; i < n; ++i) {
            auto key_size = fkeys[i].size;
            if (key_size > kSliceMaxFixLength) {
                key_size = 0;
            }
            idxes_by_key_size[key_size].push_back(i);
        }
        for (const auto& [key_size, idxes] : idxes_by_key_size) {
            const auto [shard_offset, shard_size] = _shard_info_by_key_size[key_size];
            const auto idxes_by_shard = split_keys_by_shard(shard_size, keys, idxes);
            auto& not_found = not_founds_by_key_size[key_size];
            for (size_t i = 0; i < shard_size; ++i) {
                RETURN_IF_ERROR(_shards[shard_offset + i]->get(keys, values, &not_found, num_found, idxes_by_shard[i]));
            }
        }
    }
    return Status::OK();
}

Status ShardByLengthMutableIndex::upsert(size_t n, const Slice* keys, const IndexValue* values, IndexValue* old_values,
                                         size_t* num_found, std::map<size_t, KeysInfo>& not_founds_by_key_size) {
    DCHECK(_fixed_key_size != -1);
    if (_fixed_key_size > 0) {
        const auto [shard_offset, shard_size] = _shard_info_by_key_size[_fixed_key_size];
        const auto idxes_by_shard = split_keys_by_shard(shard_size, keys, 0, n);
        auto& keys_info = not_founds_by_key_size[_fixed_key_size];
        for (auto i = 0; i < shard_size; ++i) {
            RETURN_IF_ERROR(_shards[shard_offset + i]->upsert(keys, values, old_values, &keys_info, num_found,
                                                              idxes_by_shard[i]));
        }
    } else {
        DCHECK(_fixed_key_size == 0);
        const auto* fkeys = reinterpret_cast<const Slice*>(keys);
        std::map<size_t, std::vector<size_t>> idxes_by_key_size;
        for (size_t i = 0; i < n; ++i) {
            auto key_size = fkeys[i].size;
            if (key_size > kSliceMaxFixLength) {
                key_size = 0;
            }
            idxes_by_key_size[key_size].push_back(i);
        }
        for (const auto& [key_size, idxes] : idxes_by_key_size) {
            const auto [shard_offset, shard_size] = _shard_info_by_key_size[key_size];
            const auto idxes_by_shard = split_keys_by_shard(shard_size, keys, idxes);
            auto& not_found = not_founds_by_key_size[key_size];
            for (auto i = 0; i < shard_size; ++i) {
                RETURN_IF_ERROR(_shards[shard_offset + i]->upsert(keys, values, old_values, &not_found, num_found,
                                                                  idxes_by_shard[i]));
            }
        }
    }
    return Status::OK();
}

Status ShardByLengthMutableIndex::upsert(size_t n, const Slice* keys, const IndexValue* values, size_t* num_found,
                                         std::map<size_t, KeysInfo>& not_founds_by_key_size) {
    DCHECK(_fixed_key_size != -1);
    if (_fixed_key_size > 0) {
        const auto [shard_offset, shard_size] = _shard_info_by_key_size[_fixed_key_size];
        const auto idxes_by_shard = split_keys_by_shard(shard_size, keys, 0, n);
        auto& keys_info = not_founds_by_key_size[_fixed_key_size];
        for (size_t i = 0; i < shard_size; ++i) {
            RETURN_IF_ERROR(_shards[shard_offset + i]->upsert(keys, values, &keys_info, num_found, idxes_by_shard[i]));
        }
    } else {
        DCHECK(_fixed_key_size == 0);
        const auto* fkeys = reinterpret_cast<const Slice*>(keys);
        std::map<size_t, std::vector<size_t>> idxes_by_key_size;
        for (size_t i = 0; i < n; ++i) {
            auto key_size = fkeys[i].size;
            if (key_size > kSliceMaxFixLength) {
                key_size = 0;
            }
            idxes_by_key_size[key_size].push_back(i);
        }
        for (const auto& [key_size, idxes] : idxes_by_key_size) {
            const auto [shard_offset, shard_size] = _shard_info_by_key_size[key_size];
            const auto idxes_by_shard = split_keys_by_shard(shard_size, keys, idxes);
            auto& not_found = not_founds_by_key_size[key_size];
            for (size_t i = 0; i < shard_size; ++i) {
                RETURN_IF_ERROR(
                        _shards[shard_offset + i]->upsert(keys, values, &not_found, num_found, idxes_by_shard[i]));
            }
        }
    }
    return Status::OK();
}

Status ShardByLengthMutableIndex::insert(size_t n, const Slice* keys, const IndexValue* values,
                                         std::set<size_t>& check_l1_key_sizes) {
    DCHECK(_fixed_key_size != -1);
    if (_fixed_key_size > 0) {
        const auto [shard_offset, shard_size] = _shard_info_by_key_size[_fixed_key_size];
        const auto idxes_by_shard = split_keys_by_shard(shard_size, keys, 0, n);
        for (size_t i = 0; i < shard_size; ++i) {
            RETURN_IF_ERROR(_shards[shard_offset + i]->insert(keys, values, idxes_by_shard[i]));
        }
        check_l1_key_sizes.insert(shard_offset);
    } else {
        DCHECK(_fixed_key_size == 0);
        const auto* fkeys = reinterpret_cast<const Slice*>(keys);
        std::map<size_t, std::vector<size_t>> idxes_by_key_size;
        for (size_t i = 0; i < n; ++i) {
            auto key_size = fkeys[i].size;
            if (key_size > kSliceMaxFixLength) {
                key_size = 0;
            }
            idxes_by_key_size[key_size].push_back(i);
        }
        for (const auto& [key_size, idxes] : idxes_by_key_size) {
            const auto [shard_offset, shard_size] = _shard_info_by_key_size[key_size];
            const auto idxes_by_shard = split_keys_by_shard(shard_size, keys, idxes);
            for (size_t i = 0; i < shard_size; ++i) {
                RETURN_IF_ERROR(_shards[shard_offset + i]->insert(keys, values, idxes_by_shard[i]));
            }
        }
    }
    return Status::OK();
}

Status ShardByLengthMutableIndex::replace(const Slice* keys, const IndexValue* values,
                                          const std::vector<size_t>& idxes) {
    DCHECK(_fixed_key_size != -1);
    if (_fixed_key_size > 0) {
        const auto [shard_offset, shard_size] = _shard_info_by_key_size[_fixed_key_size];
        const auto idxes_by_shard = split_keys_by_shard(shard_size, keys, idxes);
        for (size_t i = 0; i < shard_size; ++i) {
            RETURN_IF_ERROR(_shards[shard_offset + i]->replace(keys, values, idxes_by_shard[i]));
        }
    } else {
        DCHECK(_fixed_key_size == 0);
        const auto* fkeys = reinterpret_cast<const Slice*>(keys);
        std::map<size_t, std::vector<size_t>> idxes_by_key_size;
        for (const auto idx : idxes) {
            auto key_size = fkeys[idx].size;
            if (key_size > kSliceMaxFixLength) {
                key_size = 0;
            }
            idxes_by_key_size[key_size].push_back(idx);
        }
        for (const auto& [key_size, idxes] : idxes_by_key_size) {
            const auto [shard_offset, shard_size] = _shard_info_by_key_size[key_size];
            const auto idxes_by_shard = split_keys_by_shard(shard_size, keys, idxes);
            for (size_t i = 0; i < shard_size; ++i) {
                RETURN_IF_ERROR(_shards[shard_offset + i]->replace(keys, values, idxes_by_shard[i]));
            }
        }
    }
    return Status::OK();
}

Status ShardByLengthMutableIndex::erase(size_t n, const Slice* keys, IndexValue* old_values, size_t* num_found,
                                        std::map<size_t, KeysInfo>& not_founds_by_key_size) {
    DCHECK(_fixed_key_size != -1);
    if (_fixed_key_size > 0) {
        const auto [shard_offset, shard_size] = _shard_info_by_key_size[_fixed_key_size];
        const auto idxes_by_shard = split_keys_by_shard(shard_size, keys, 0, n);
        auto& keys_info = not_founds_by_key_size[_fixed_key_size];
        for (size_t i = 0; i < shard_size; ++i) {
            RETURN_IF_ERROR(
                    _shards[shard_offset + i]->erase(keys, old_values, &keys_info, num_found, idxes_by_shard[i]));
        }
    } else {
        DCHECK(_fixed_key_size == 0);
        const auto* fkeys = reinterpret_cast<const Slice*>(keys);
        std::map<size_t, std::vector<size_t>> idxes_by_key_size;
        for (size_t i = 0; i < n; ++i) {
            auto key_size = fkeys[i].size;
            if (key_size > kSliceMaxFixLength) {
                key_size = 0;
            }
            idxes_by_key_size[key_size].push_back(i);
        }
        for (const auto& [key_size, idxes] : idxes_by_key_size) {
            const auto [shard_offset, shard_size] = _shard_info_by_key_size[key_size];
            const auto idxes_by_shard = split_keys_by_shard(shard_size, keys, idxes);
            auto& not_found = not_founds_by_key_size[key_size];
            for (size_t i = 0; i < shard_size; ++i) {
                RETURN_IF_ERROR(
                        _shards[shard_offset + i]->erase(keys, old_values, &not_found, num_found, idxes_by_shard[i]));
            }
        }
    }
    return Status::OK();
}

Status ShardByLengthMutableIndex::append_wal(size_t n, const Slice* keys, const IndexValue* values) {
    DCHECK(_fixed_key_size != -1);
    if (_fixed_key_size > 0) {
        const auto [shard_offset, shard_size] = _shard_info_by_key_size[_fixed_key_size];
        const auto idxes_by_shard = split_keys_by_shard(shard_size, keys, 0, n);
        for (size_t i = 0; i < shard_size; ++i) {
            RETURN_IF_ERROR(_shards[shard_offset + i]->append_wal(keys, values, idxes_by_shard[i], _index_file,
                                                                  &_page_size, &_checksum));
        }
    } else {
        DCHECK(_fixed_key_size == 0);
        const auto* fkeys = reinterpret_cast<const Slice*>(keys);
        std::map<size_t, std::vector<size_t>> idxes_by_key_size;
        for (size_t i = 0; i < n; ++i) {
            auto key_size = fkeys[i].size;
            if (key_size > kSliceMaxFixLength) {
                key_size = 0;
            }
            idxes_by_key_size[key_size].push_back(i);
        }
        for (const auto& [key_size, idxes] : idxes_by_key_size) {
            const auto [shard_offset, shard_size] = _shard_info_by_key_size[key_size];
            const auto idxes_by_shard = split_keys_by_shard(shard_size, keys, idxes);
            for (size_t i = 0; i < shard_size; ++i) {
                RETURN_IF_ERROR(_shards[shard_offset + i]->append_wal(keys, values, idxes_by_shard[i], _index_file,
                                                                      &_page_size, &_checksum));
            }
        }
    }
    return Status::OK();
}

Status ShardByLengthMutableIndex::append_wal(const Slice* keys, const IndexValue* values,
                                             const std::vector<size_t>& idxes) {
    DCHECK(_fixed_key_size != -1);
    if (_fixed_key_size > 0) {
        const auto [shard_offset, shard_size] = _shard_info_by_key_size[_fixed_key_size];
        const auto idxes_by_shard = split_keys_by_shard(shard_size, keys, idxes);
        for (size_t i = 0; i < shard_size; ++i) {
            RETURN_IF_ERROR(_shards[shard_offset + i]->append_wal(keys, values, idxes_by_shard[i], _index_file,
                                                                  &_page_size, &_checksum));
        }
    } else {
        DCHECK(_fixed_key_size == 0);
        const auto* fkeys = reinterpret_cast<const Slice*>(keys);
        std::map<size_t, std::vector<size_t>> idxes_by_key_size;
        for (const auto idx : idxes) {
            auto key_size = fkeys[idx].size;
            if (key_size > kSliceMaxFixLength) {
                key_size = 0;
            }
            idxes_by_key_size[key_size].push_back(idx);
        }
        for (const auto& [key_size, idxes] : idxes_by_key_size) {
            const auto [shard_offset, shard_size] = _shard_info_by_key_size[key_size];
            const auto idxes_by_shard = split_keys_by_shard(shard_size, keys, idxes);
            for (size_t i = 0; i < shard_size; ++i) {
                RETURN_IF_ERROR(_shards[shard_offset + i]->append_wal(keys, values, idxes_by_shard[i], _index_file,
                                                                      &_page_size, &_checksum));
            }
        }
    }
    return Status::OK();
}

bool ShardByLengthMutableIndex::load_snapshot(phmap::BinaryInputArchive& ar, const std::set<uint32_t>& idxes) {
    for (const auto idx : idxes) {
        if (!_shards[idx]->load_snapshot(ar)) {
            return false;
        }
    }
    return true;
    // notice: accumulate will keep iterate the container, not return early.
    // return std::accumulate(idxes.begin(), idxes.end(), true, [](bool prev, size_t idx) { return _shards[idx]->load_snapshot(ar_in) && prev; });
}

size_t ShardByLengthMutableIndex::dump_bound() {
    return std::accumulate(_shards.begin(), _shards.end(), 0UL,
                           [](size_t s, const auto& e) { return e->size() > 0 ? s + e->dump_bound() : s; });
}

bool ShardByLengthMutableIndex::dump(phmap::BinaryOutputArchive& ar_out, std::set<uint32_t>& dumped_shard_idxes) {
    for (uint32_t i = 0; i < _shards.size(); ++i) {
        const auto& shard = _shards[i];
        if (shard->size() > 0) {
            if (!shard->dump(ar_out)) {
                return false;
            }
            dumped_shard_idxes.insert(i);
        }
    }
    return true;
}

Status ShardByLengthMutableIndex::pk_dump(PrimaryKeyDump* dump, PrimaryIndexDumpPB* dump_pb) {
    for (uint32_t i = 0; i < _shards.size(); ++i) {
        const auto& shard = _shards[i];
        RETURN_IF_ERROR(shard->pk_dump(dump, dump_pb));
    }
    return Status::OK();
}

static Status checksum_of_file(RandomAccessFile* file, uint64_t offset, uint32_t size, uint32* checksum) {
    std::string buff;
    raw::stl_string_resize_uninitialized(&buff, size);
    RETURN_IF_ERROR(file->read_at_fully(offset, buff.data(), buff.size()));
    *checksum = crc32c::Value(buff.data(), buff.size());
    return Status::OK();
}

Status ShardByLengthMutableIndex::commit(MutableIndexMetaPB* meta, const EditVersion& version, const CommitType& type) {
    std::shared_ptr<FileSystem> fs;
    ASSIGN_OR_RETURN(fs, FileSystem::CreateSharedFromString(_path));
    switch (type) {
    case kFlush: {
        // create a new empty _l0 file because all data in _l0 has write into _l1 files
        std::string file_name = get_l0_index_file_name(_path, version);
        WritableFileOptions wblock_opts;
        wblock_opts.mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE;
        ASSIGN_OR_RETURN(auto wfile, fs->new_writable_file(wblock_opts, file_name));
        DeferOp close_block([&wfile] {
            if (wfile) {
                WARN_IF_ERROR(wfile->close(), fmt::format("failed to close writable_file: {}", wfile->filename()));
            }
        });
        meta->clear_wals();
        IndexSnapshotMetaPB* snapshot = meta->mutable_snapshot();
        snapshot->clear_dumped_shard_idxes();
        version.to_pb(snapshot->mutable_version());
        PagePointerPB* data = snapshot->mutable_data();
        // create a new empty _l0 file, set _offset to 0
        data->set_offset(0);
        data->set_size(0);
        meta->set_format_version(PERSISTENT_INDEX_VERSION_4);
        _offset = 0;
        _page_size = 0;
        _checksum = 0;
        break;
    }
    case kSnapshot: {
        std::string file_name = get_l0_index_file_name(_path, version);
        // be maybe crash after create index file during last commit
        // so we delete expired index file first to make sure no garbage left
        (void)FileSystem::Default()->delete_file(file_name);
        std::set<uint32_t> dumped_shard_idxes;
        {
            // File is closed when archive object is destroyed and file size will be updated after file is
            // closed. So the archive object needed to be destroyed before reopen the file and assigned it
            // to _index_file. Otherwise some data of file maybe overwrite in future append.
            phmap::BinaryOutputArchive ar_out(file_name.data());
            if (!dump(ar_out, dumped_shard_idxes)) {
                std::string err_msg = strings::Substitute("failed to dump snapshot to file $0", file_name);
                LOG(WARNING) << err_msg;
                return Status::InternalError(err_msg);
            }
        }
        // dump snapshot success, set _index_file to new snapshot file
        WritableFileOptions wblock_opts;
        wblock_opts.mode = FileSystem::MUST_EXIST;
        ASSIGN_OR_RETURN(_index_file, fs->new_writable_file(wblock_opts, file_name));
        // open l0 to calc checksum
        std::unique_ptr<RandomAccessFile> l0_rfile;
        ASSIGN_OR_RETURN(l0_rfile, fs->new_random_access_file(file_name));
        MonotonicStopWatch watch;
        watch.start();
        size_t snapshot_size = _index_file->size();
        // special case, snapshot file was written by phmap::BinaryOutputArchive which does not use system profiled API
        // so add write stats manually
        IOProfiler::add_write(snapshot_size, watch.elapsed_time());
        meta->clear_wals();
        IndexSnapshotMetaPB* snapshot = meta->mutable_snapshot();
        version.to_pb(snapshot->mutable_version());
        PagePointerPB* data = snapshot->mutable_data();
        data->set_offset(0);
        data->set_size(snapshot_size);
        snapshot->clear_dumped_shard_idxes();
        snapshot->mutable_dumped_shard_idxes()->Add(dumped_shard_idxes.begin(), dumped_shard_idxes.end());
        RETURN_IF_ERROR(checksum_of_file(l0_rfile.get(), 0, snapshot_size, &_checksum));
        snapshot->set_checksum(_checksum);
        meta->set_format_version(PERSISTENT_INDEX_VERSION_4);
        _offset = snapshot_size;
        _page_size = 0;
        _checksum = 0;
        break;
    }
    case kAppendWAL: {
        IndexWalMetaPB* wal_pb = meta->add_wals();
        version.to_pb(wal_pb->mutable_version());
        PagePointerPB* data = wal_pb->mutable_data();
        data->set_offset(_offset);
        data->set_size(_page_size);
        wal_pb->set_checksum(_checksum);
        meta->set_format_version(PERSISTENT_INDEX_VERSION_4);
        _offset += _page_size;
        _page_size = 0;
        _checksum = 0;
        break;
    }
    default: {
        return Status::InternalError("Unknown commit type");
    }
    }
    return Status::OK();
}

Status ShardByLengthMutableIndex::load(const MutableIndexMetaPB& meta) {
    auto format_version = meta.format_version();
    if (format_version != PERSISTENT_INDEX_VERSION_2 && format_version != PERSISTENT_INDEX_VERSION_3 &&
        format_version != PERSISTENT_INDEX_VERSION_4) {
        std::string msg = strings::Substitute("different l0 format, should rebuid index. actual:$0, expect:$1",
                                              format_version, PERSISTENT_INDEX_VERSION_4);
        LOG(WARNING) << msg;
        return Status::InternalError(msg);
    }
    const IndexSnapshotMetaPB& snapshot_meta = meta.snapshot();
    const EditVersion& start_version = snapshot_meta.version();
    const PagePointerPB& page_pb = snapshot_meta.data();
    const auto snapshot_off = page_pb.offset();
    const auto snapshot_size = page_pb.size();
    std::set<uint32_t> dumped_shard_idxes;
    for (auto i = 0; i < snapshot_meta.dumped_shard_idxes_size(); ++i) {
        auto [_, insert] = dumped_shard_idxes.insert(snapshot_meta.dumped_shard_idxes(i));
        if (!insert) {
            LOG(WARNING) << "duplicate shard idx: " << snapshot_meta.dumped_shard_idxes(i);
            return Status::InternalError("duplicate shard idx");
        }
    }
    std::string index_file_name = get_l0_index_file_name(_path, start_version);
    std::shared_ptr<FileSystem> fs;
    ASSIGN_OR_RETURN(fs, FileSystem::CreateSharedFromString(_path));
    ASSIGN_OR_RETURN(auto read_file, fs->new_random_access_file(index_file_name));
    phmap::BinaryInputArchive ar(index_file_name.data());
    if (snapshot_size > 0) {
        // check snapshot's crc32 checksum
        const uint32_t expected_checksum = snapshot_meta.checksum();
        // If expected crc32 is 0, which means no crc32 here, skip check.
        // This may happen when upgrade from old version.
        if (expected_checksum > 0) {
            uint32_t current_checksum = 0;
            RETURN_IF_ERROR(checksum_of_file(read_file.get(), snapshot_off, snapshot_size, &current_checksum));
            if (current_checksum != expected_checksum) {
                std::string error_msg = fmt::format(
                        "persistent index l0 crc checksum fail. filename: {} offset: {} cur_crc: {} expect_crc: {}",
                        index_file_name, snapshot_off, current_checksum, expected_checksum);
                LOG(ERROR) << error_msg;
                return Status::Corruption(error_msg);
            }
        }
        MonotonicStopWatch watch;
        watch.start();
        // do load snapshot
        if (!load_snapshot(ar, dumped_shard_idxes)) {
            std::string err_msg = strings::Substitute("failed load snapshot from file $0", index_file_name);
            LOG(WARNING) << err_msg;
            return Status::InternalError(err_msg);
        }
        // special case, snapshot file was written by phmap::BinaryOutputArchive which does not use system profiled API
        // so add read stats manually
        IOProfiler::add_read(snapshot_size, watch.elapsed_time());
    }
    // if mutable index is empty, set _offset as 0, otherwise set _offset as snapshot size
    _offset = snapshot_off + snapshot_size;
    const int n = meta.wals_size();
    // read wals and build hash map
    for (int i = 0; i < n; i++) {
        const auto& page_pointer_pb = meta.wals(i).data();
        auto offset = page_pointer_pb.offset();
        const auto end = offset + page_pointer_pb.size();
        std::string buff;
        raw::stl_string_resize_uninitialized(&buff, 4);
        // check crc32
        const uint32_t expected_checksum = meta.wals(i).checksum();
        if (expected_checksum > 0) {
            uint32_t current_checksum = 0;
            RETURN_IF_ERROR(checksum_of_file(read_file.get(), offset, page_pointer_pb.size(), &current_checksum));
            if (current_checksum != expected_checksum) {
                std::string error_msg = fmt::format(
                        "persistent index l0 crc checksum fail. filename: {} offset: {} cur_crc: {} expect_crc: {}",
                        index_file_name, page_pointer_pb.offset(), current_checksum, expected_checksum);
                LOG(ERROR) << error_msg;
                return Status::Corruption(error_msg);
            }
        }
        while (offset < end) {
            RETURN_IF_ERROR(read_file->read_at_fully(offset, buff.data(), buff.size()));
            const auto key_size = UNALIGNED_LOAD32(buff.data());
            const auto [shard_offset, shard_size] = _shard_info_by_key_size[key_size];
            for (auto i = 0; i < shard_size; ++i) {
                RETURN_IF_ERROR(_shards[shard_offset + i]->load(offset, read_file));
            }
        }
        _offset += page_pointer_pb.size();
    }
    RETURN_IF_ERROR(FileSystemUtil::resize_file(index_file_name, _offset));
    WritableFileOptions wblock_opts;
    wblock_opts.mode = FileSystem::MUST_EXIST;
    ASSIGN_OR_RETURN(_index_file, fs->new_writable_file(wblock_opts, index_file_name));
    return Status::OK();
}

Status ShardByLengthMutableIndex::flush_to_immutable_index(const std::string& path, const EditVersion& version,
                                                           bool write_tmp_l1, bool keep_delete) {
    auto writer = std::make_unique<ImmutableIndexWriter>();
    std::string idx_file_path;
    if (!write_tmp_l1) {
        idx_file_path = strings::Substitute("$0/index.l1.$1.$2", path, version.major_number(), version.minor_number());
    } else {
        idx_file_path = path;
    }
    RETURN_IF_ERROR(writer->init(idx_file_path, version, !write_tmp_l1));
    DCHECK(_fixed_key_size != -1);
    for (const auto& [key_size, shard_info] : _shard_info_by_key_size) {
        const auto [shard_offset, shard_size] = shard_info;
        const auto size = std::accumulate(std::next(_shards.begin(), shard_offset),
                                          std::next(_shards.begin(), shard_offset + shard_size), (size_t)0,
                                          [](size_t s, const auto& e) { return s + e->size(); });
        if (size != 0) {
            size_t total_kv_pairs_usage = 0;
            if (key_size == 0) {
                total_kv_pairs_usage = dynamic_cast<SliceMutableIndex*>(_shards[0].get())->_total_kv_pairs_usage;
            } else {
                total_kv_pairs_usage = (key_size + kIndexValueSize) * size;
            }
            const auto [nshard, npage_hint] = MutableIndex::estimate_nshard_and_npage(total_kv_pairs_usage);
            const auto nbucket = MutableIndex::estimate_nbucket(key_size, size, nshard, npage_hint);
            const auto expand_exponent = nshard / shard_size;
            for (auto i = 0; i < shard_size; ++i) {
                // if keep_delete == true, flush immutable index with Delete Flag
                RETURN_IF_ERROR(_shards[shard_offset + i]->flush_to_immutable_index(writer, expand_exponent, npage_hint,
                                                                                    nbucket, keep_delete));
            }
        }
    }
    RETURN_IF_ERROR(writer->finish());
    return Status::OK();
}

size_t ShardByLengthMutableIndex::size() {
    return std::accumulate(_shards.begin(), _shards.end(), (size_t)0,
                           [](size_t s, const auto& e) { return s + e->size(); });
}

size_t ShardByLengthMutableIndex::capacity() {
    return std::accumulate(_shards.begin(), _shards.end(), (size_t)0,
                           [](size_t s, const auto& e) { return s + e->capacity(); });
}

size_t ShardByLengthMutableIndex::memory_usage() {
    return std::accumulate(_shards.begin(), _shards.end(), 0UL,
                           [](size_t s, const auto& e) { return s + e->memory_usage(); });
}

void ShardByLengthMutableIndex::clear() {
    for (const auto& shard : _shards) {
        shard->clear();
    }
}

Status ShardByLengthMutableIndex::create_index_file(std::string& path) {
    if (_index_file != nullptr) {
        std::string msg = strings::Substitute("l0 index file already exist: $0", _index_file->filename());
        return Status::InternalError(msg);
    }
    ASSIGN_OR_RETURN(_fs, FileSystem::CreateSharedFromString(_path));
    WritableFileOptions wblock_opts{.sync_on_close = true, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
    ASSIGN_OR_RETURN(_index_file, _fs->new_writable_file(wblock_opts, path));
    return Status::OK();
}

} // namespace starrocks