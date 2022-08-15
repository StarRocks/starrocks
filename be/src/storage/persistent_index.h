// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <memory>
#include <tuple>

#include "common/statusor.h"
#include "fs/fs.h"
#include "gen_cpp/persistent_index.pb.h"
#include "storage/edit_version.h"
#include "storage/rowset/rowset.h"
#include "util/phmap/phmap.h"
#include "util/phmap/phmap_dump.h"

namespace starrocks {

class Tablet;
namespace vectorized {
class Schema;
class Column;
} // namespace vectorized

// Add version for persistent index file to support future upgrade compatibility
// There is only one version for now
enum PersistentIndexFileVersion {
    UNKNOWN = 0,
    PERSISTENT_INDEX_VERSION_1,
    PERSISTENT_INDEX_VERSION_2,
};

static constexpr uint64_t NullIndexValue = -1;

enum CommitType {
    kFlush = 0,
    kSnapshot = 1,
    kAppendWAL = 2,
};

// Use `uint8_t[8]` to store the value of a `uint64_t` to reduce memory cost in phmap
struct IndexValue {
    uint8_t v[8];
    IndexValue() = default;
    explicit IndexValue(const uint64_t val) { UNALIGNED_STORE64(v, val); }

    uint64_t get_value() const { return UNALIGNED_LOAD64(v); }
    bool operator==(const IndexValue& rhs) const { return memcmp(v, rhs.v, 8) == 0; }
    void operator=(uint64_t rhs) { return UNALIGNED_STORE64(v, rhs); }
};

static constexpr size_t kIndexValueSize = 8;
static_assert(sizeof(IndexValue) == kIndexValueSize);

class ImmutableIndexShard;

uint64_t key_index_hash(const void* data, size_t len);

struct KeysInfo {
    std::vector<uint32_t> key_idxes;
    std::vector<uint64_t> hashes;
    size_t size() const { return key_idxes.size(); }
};

struct KVRef {
    const uint8_t* kv_pos;
    uint64_t hash;
    uint16_t size;
    KVRef() {}
    KVRef(const uint8_t* kv_pos, uint64_t hash, uint16_t size) : kv_pos(kv_pos), hash(hash), size(size) {}
};

class PersistentIndex;
class ImmutableIndex;

class MutableIndex {
public:
    MutableIndex();
    virtual ~MutableIndex();

    // get the number of entries in the index (including NullIndexValue)
    virtual size_t size() const = 0;

    // batch get
    // |n|: size of key/value array
    // |keys|: key array as raw buffer
    // |values|: value array for return values
    // |not_found|: information of keys not found, which need to be further checked in next level
    // |num_found|: add the number of keys found to this argument
    virtual Status get(size_t n, const Slice* keys, IndexValue* values, KeysInfo* not_found,
                       size_t* num_found) const = 0;

    // batch upsert and get old value
    // |n|: size of key/value array
    // |keys|: key array as raw buffer
    // |values|: value array
    // |old_values|: return old values for updates, or set to NullValue for inserts
    // |not_found|: information of keys not found, which need to be further checked in next level
    // |num_found|: add the number of keys found to this argument
    virtual Status upsert(size_t n, const Slice* keys, const IndexValue* values, IndexValue* old_values,
                          KeysInfo* not_found, size_t* num_found) = 0;

    // batch upsert
    // |n|: size of key/value array
    // |keys|: key array as raw buffer
    // |values|: value array
    // |not_found|: information of keys not found, which need to be further checked in next level
    // |num_found|: add the number of keys found(or already exist) to this argument
    virtual Status upsert(size_t n, const Slice* keys, const IndexValue* values, KeysInfo* not_found,
                          size_t* num_found) = 0;

    // load wals
    // |n|: size of key/value array
    // |keys|: key array as raw buffer
    // |values|: value array
    virtual Status load_wals(size_t n, const Slice* keys, const IndexValue* values) = 0;

    // load snapshot
    virtual bool load_snapshot(phmap::BinaryInputArchive& ar_in) = 0;

    // load according meta
    virtual Status load(const MutableIndexMetaPB& meta) = 0;

    // batch insert
    // |n|: size of key/value array
    // |keys|: key array as raw buffer
    // |values|: value array
    virtual Status insert(size_t n, const Slice* keys, const IndexValue* values) = 0;

    // batch erase(delete)
    // |n|: size of key/value array
    // |keys|: key array as raw buffer
    // |old_values|: return old values for updates, or set to NullValue if not exists
    // |not_found|: information of keys not found, which need to be further checked in next level
    // |num_found|: add the number of keys found to this argument
    virtual Status erase(size_t n, const Slice* keys, IndexValue* old_values, KeysInfo* not_found,
                         size_t* num_found) = 0;

    // batch replace
    // |keys|: key array as raw buffer
    // |values|: new value array
    // |replace_idxes|: the idx array of the kv needed to be replaced
    virtual Status replace(const Slice* keys, const IndexValue* values, const std::vector<size_t>& replace_idxes) = 0;

    virtual Status commit(MutableIndexMetaPB* meta, const EditVersion& version, const CommitType& type) = 0;

    virtual Status append_wal(size_t n, const Slice* keys, const IndexValue* values) = 0;

    virtual Status append_wal(size_t n, const Slice* keys, const IndexValue* values,
                              const std::vector<size_t>& idxes) = 0;

    // get dump size of hashmap
    virtual size_t dump_bound() = 0;

    virtual bool dump(phmap::BinaryOutputArchive& ar_out) = 0;

    // [not thread-safe]
    virtual size_t capacity() = 0;

    virtual void reserve(size_t size) = 0;

    virtual size_t memory_usage() = 0;

    // get all key-values pair references by shard, the result will remain valid until next modification
    // |nshard|: number of shard
    // |num_entry|: number of entries expected, it should be:
    //                 the num of KV entries if without_null == false
    //                 the num of KV entries excluding nulls if without_null == true
    // |without_null|: whether to include null entries
    // [not thread-safe]
    virtual std::vector<std::vector<KVRef>> get_kv_refs_by_shard(size_t nshard, size_t num_entry,
                                                                 bool without_null) const = 0;

    // flush to immutable index
    // [not thread-safe]
    virtual Status flush_to_immutable_index(const std::string& dir, const EditVersion& version) const = 0;

    static StatusOr<std::unique_ptr<MutableIndex>> create(size_t key_size, const std::string& path);
};

class ImmutableIndex {
public:
    // batch get
    // |n|: size of key/value array
    // |keys|: key array as slice array
    // |not_found|: information of keys not found in upper level, which needs to be checked in this level
    // |values|: value array for return values
    // |num_found|: add the number of keys found in L1 to this argument
    // |key_size|: the key size of keys array
    Status get(size_t n, const Slice* keys, const KeysInfo& keys_info, IndexValue* values, size_t* num_found,
               size_t key_size) const;

    // batch check key existence
    Status check_not_exist(size_t n, const Slice* keys, size_t key_size);

    // get Immutable index file size;
    void file_size(uint64_t* file_size) {
        if (_file != nullptr) {
            auto res = _file->get_size();
            CHECK(res.ok()) << res.status(); // FIXME: no abort
            *file_size = *res;
        }
    }

    void clear() {
        if (_file != nullptr) {
            _file.reset();
        }
    }

    static StatusOr<std::unique_ptr<ImmutableIndex>> load(std::unique_ptr<RandomAccessFile>&& rb);

private:
    friend class PersistentIndex;

    Status _get_fixlen_kvs_for_shard(std::vector<std::vector<KVRef>>& kvs_by_shard, size_t shard_idx,
                                     uint32_t shard_bits, std::unique_ptr<ImmutableIndexShard>* shard) const;

    Status _get_varlen_kvs_for_shard(std::vector<std::vector<KVRef>>& kvs_by_shard, size_t shard_idx,
                                     uint32_t shard_bits, std::unique_ptr<ImmutableIndexShard>* shard) const;

    // get all the kv refs of a single shard by `shard_idx`, and add them to `kvs_by_shard`, the shard number of
    // kvs_by_shard may be different from this object's own shard number
    // NOTE: used by PersistentIndex only
    Status _get_kvs_for_shard(std::vector<std::vector<KVRef>>& kvs_by_shard, size_t shard_idx, uint32_t shard_bits,
                              std::unique_ptr<ImmutableIndexShard>* shard) const;

    Status _get_in_fixlen_shard(size_t shard_idx, size_t n, const Slice* keys, const KeysInfo& keys_info,
                                IndexValue* values, size_t* num_found,
                                std::unique_ptr<ImmutableIndexShard>* shard) const;

    Status _get_in_varlen_shard(size_t shard_idx, size_t n, const Slice* keys, const KeysInfo& keys_info,
                                IndexValue* values, size_t* num_found,
                                std::unique_ptr<ImmutableIndexShard>* shard) const;

    Status _get_in_shard(size_t shard_idx, size_t n, const Slice* keys, const KeysInfo& keys_info, IndexValue* values,
                         size_t* num_found) const;

    Status _check_not_exist_in_fixlen_shard(size_t shard_idx, size_t n, const Slice* keys, const KeysInfo& keys_info,
                                            std::unique_ptr<ImmutableIndexShard>* shard) const;

    Status _check_not_exist_in_varlen_shard(size_t shard_idx, size_t n, const Slice* keys, const KeysInfo& keys_info,
                                            std::unique_ptr<ImmutableIndexShard>* shard) const;

    Status _check_not_exist_in_shard(size_t shard_idx, size_t n, const Slice* keys, const KeysInfo& keys_info) const;

    std::unique_ptr<RandomAccessFile> _file;
    EditVersion _version;
    size_t _size = 0;
    size_t _fixed_key_size = 0;
    size_t _fixed_value_size = 0;

    struct ShardInfo {
        uint64_t offset;
        uint64_t bytes;
        uint32_t npage;
        uint32_t size;
        uint32_t key_size;
        uint32_t value_size;
        uint32_t nbucket;
    };

    std::vector<ShardInfo> _shards;
    std::map<size_t, std::pair<size_t, size_t>> _shard_info_by_length;
};

// A persistent primary index contains an in-memory L0 and an on-SSD/NVMe L1,
// this saves memory usage comparing to the orig all-in-memory implementation.
// This is a internal class and is intended to be used by PrimaryIndex internally.
// TODO: code skeleton currently, implementation in future PRs
//
// Currently primary index is only modified in TabletUpdates::apply process, it's
// typical use pattern in apply:
//   pi.prepare(version)
//   if (pi.upsert(upsert_keys, values, old_values))
//   pi.erase(delete_keys, old_values)
//   pi.commit()
//   pi.on_commited()
// If any error occurred between prepare and on_commited, abort should be called, the
// index maybe corrupted, currently for simplicity, the whole index is cleared and rebuilt.
class PersistentIndex {
public:
    // |path|: directory that contains index files
    PersistentIndex(const std::string& path);
    ~PersistentIndex();

    // if index is loaded
    bool loaded() const { return (bool)_l0; }

    std::string path() const { return _path; }

    size_t key_size() const { return _key_size; }
    size_t kv_pair_size() const { return _kv_pair_size; }

    size_t size() const { return _size; }
    size_t capacity() const { return _l0 ? _l0->capacity() : 0; }
    size_t memory_usage() const { return _l0 ? _l0->memory_usage() : 0; }

    EditVersion version() const { return _version; }

    // create new empty index
    Status create(size_t key_size, const EditVersion& version);

    // load required states from underlying file
    Status load(const PersistentIndexMetaPB& index_meta);

    // build PersistentIndex from pre-existing tablet data
    Status load_from_tablet(Tablet* tablet);

    // start modification with intended version
    Status prepare(const EditVersion& version);

    // abort modification
    Status abort();

    // commit modification
    Status commit(PersistentIndexMetaPB* index_meta);

    // apply modification
    Status on_commited();

    // batch index operations

    // batch get
    // |n|: size of key/value array
    // |keys|: key array as raw buffer
    // |values|: value array for return values
    Status get(size_t n, const Slice* keys, IndexValue* values);

    // batch upsert
    // |n|: size of key/value array
    // |keys|: key array as raw buffer
    // |values|: value array
    // |old_values|: return old values for updates, or set to NullValue for inserts
    Status upsert(size_t n, const Slice* keys, const IndexValue* values, IndexValue* old_values);

    // batch insert, return error if key already exists
    // |n|: size of key/value array
    // |keys|: key array as raw buffer
    // |values|: value array
    // |check_l1|: also check l1 for insertion consistency(key must not exist previously), may imply heavy IO costs
    Status insert(size_t n, const Slice* keys, const IndexValue* values, bool check_l1);

    // batch erase
    // |n|: size of key/value array
    // |keys|: key array as raw buffer
    // |old_values|: return old values if key exist, or set to NullValue if not
    Status erase(size_t n, const Slice* keys, IndexValue* old_values);

    // TODO(qzc): maybe unused, remove it or refactor it with the methods in use by template after a period of time
    // batch replace
    // |n|: size of key/value array
    // |keys|: key array as raw buffer
    // |values|: value array
    // |src_rssid|: rssid array
    // |failed|: return not match rowid
    [[maybe_unused]] Status try_replace(size_t n, const Slice* keys, const IndexValue* values,
                                        const std::vector<uint32_t>& src_rssid, std::vector<uint32_t>* failed);

    // batch replace
    // |n|: size of key/value array
    // |keys|: key array as raw buffer
    // |values|: value array
    // |max_src_rssid|: maximum of rssid array
    // |failed|: return not match rowid
    Status try_replace(size_t n, const Slice* keys, const IndexValue* values, const uint32_t max_src_rssid,
                       std::vector<uint32_t>* failed);

    std::vector<int8_t> test_get_move_buckets(size_t target, const uint8_t* bucket_packs_in_page);

    Status test_flush_varlen_to_immutable_index(const std::string& dir, const EditVersion& version, size_t num_entry,
                                                const Slice* keys, const IndexValue* values);

private:
    size_t _dump_bound();

    // check _l0 should dump as snapshot or not
    bool _can_dump_directly();

    Status _delete_expired_index_file(const EditVersion& l0_version, const EditVersion& l1_version);

    // batch append wal
    // |n|: size of key/value array
    // |keys|: key array as raw buffer
    // |values|: value array, if operation is erase, |values| is nullptr
    Status _append_wal(size_t n, const Slice* key, const IndexValue* values);

    Status _check_and_flush_l0();

    Status _flush_l0();

    // merge l0 and l1 into new l1, then clear l0
    Status _merge_compaction();

    Status _load(const PersistentIndexMetaPB& index_meta);
    Status _reload(const PersistentIndexMetaPB& index_meta);

    // commit index meta
    Status _build_commit(Tablet* tablet, PersistentIndexMetaPB& index_meta);

    // insert rowset data into persistent index
    Status _insert_rowsets(Tablet* tablet, std::vector<RowsetSharedPtr>& rowsets, const vectorized::Schema& pkey_schema,
                           int64_t apply_version, std::unique_ptr<vectorized::Column> pk_column);

    // index storage directory
    std::string _path;
    size_t _key_size = 0;
    size_t _kv_pair_size = 0;
    size_t _size = 0;
    EditVersion _version;
    // _l1_version is used to get l1 file name, update in on_committed
    EditVersion _l1_version;
    std::unique_ptr<MutableIndex> _l0;
    std::unique_ptr<ImmutableIndex> _l1;
    std::shared_ptr<FileSystem> _fs;

    bool _dump_snapshot = false;
    bool _flushed = false;
};

} // namespace starrocks
