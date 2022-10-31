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
enum PersistentIndexFileVersion { UNKNOWN = 0, PERSISTENT_INDEX_VERSION_1, PERSISTENT_INDEX_VERSION_2 };

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
class ImmutableIndexWriter;

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
                              std::unique_ptr<WritableFile>& index_file, uint64_t* page_size) = 0;

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
    //                 the num of KV entries if without_null == false
    //                 the num of KV entries excluding nulls if without_null == true
    // |without_null|: whether to include null entries
    // [not thread-safe]
    virtual std::vector<std::vector<KVRef>> get_kv_refs_by_shard(size_t nshard, size_t num_entry,
                                                                 bool without_null) const = 0;

    virtual Status flush_to_immutable_index(std::unique_ptr<ImmutableIndexWriter>& writer, size_t nshard,
                                            size_t npage_hint, size_t nbucket) const = 0;

    // get the number of entries in the index (including NullIndexValue)
    virtual size_t size() const = 0;

    virtual size_t capacity() = 0;

    virtual void reserve(size_t size) = 0;

    virtual size_t memory_usage() = 0;

    virtual void update_overlap_info(size_t overlap_size, size_t overlap_usage) = 0;

    virtual size_t overlap_size() = 0;

    static StatusOr<std::unique_ptr<MutableIndex>> create(size_t key_size);

    static std::tuple<size_t, size_t> estimate_nshard_and_npage(const size_t total_kv_pairs_usage);

    static size_t estimate_nbucket(size_t key_size, size_t size, size_t nshard, size_t npage);
};

class ShardByLengthMutableIndex {
public:
    ShardByLengthMutableIndex() {}

    ShardByLengthMutableIndex(const size_t key_size, const std::string& path)
            : _fixed_key_size(key_size), _path(path) {}

    ~ShardByLengthMutableIndex() {
        if (_index_file) {
            _index_file->close();
        }
    }

    Status init();

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
    //                 the num of KV entries if without_null == false
    //                 the num of KV entries excluding nulls if without_null == true
    // |without_null|: whether to include null entries
    std::vector<std::pair<uint32_t, std::vector<std::vector<KVRef>>>> get_kv_refs_by_shard(size_t num_entry,
                                                                                           bool without_null);

    std::vector<std::vector<size_t>> split_keys_by_shard(size_t nshard, const Slice* keys, size_t idx_begin,
                                                         size_t idx_end);
    std::vector<std::vector<size_t>> split_keys_by_shard(size_t nshard, const Slice* keys,
                                                         const std::vector<size_t>& idxes);

    Status flush_to_immutable_index(const std::string& dir, const EditVersion& version);

    // get the number of entries in the index (including NullIndexValue)
    size_t size();

    size_t capacity();

    size_t memory_usage();

    Status update_overlap_info(size_t key_size, size_t num_overlap, const Slice* keys, const IndexValue* values,
                               const KeysInfo& keys_info, bool erase);

    static StatusOr<std::unique_ptr<ShardByLengthMutableIndex>> create(size_t key_size, const std::string& path);

private:
    friend class PersistentIndex;

    template <int N>
    void _init_loop_helper();

private:
    constexpr static size_t kSliceMaxFixLength = 64;
    uint32_t _fixed_key_size = -1;
    uint64_t _offset = 0;
    uint64_t _page_size = 0;
    std::string _path;
    std::unique_ptr<WritableFile> _index_file;
    std::shared_ptr<FileSystem> _fs;
    std::vector<std::unique_ptr<MutableIndex>> _shards;
    // TODO: confirm whether can be just one shard in a offset, which means shard size always be 1, it can simplify the manager of various shards.
    // <key size, <shard offset, shard size>>
    std::map<uint32_t, std::pair<uint32_t, uint32_t>> _shard_info_by_key_size;
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
        uint64_t data_size;
    };

    std::vector<ShardInfo> _shards;
    std::map<size_t, std::pair<size_t, size_t>> _shard_info_by_length;
};

class ImmutableIndexWriter {
public:
    ~ImmutableIndexWriter();

    Status init(const string& dir, const EditVersion& version);

    // write_shard() must be called serially in the order of key_size and it is caller's duty to guarantee this.
    Status write_shard(size_t key_size, size_t npage_hint, size_t nbucket, const std::vector<KVRef>& kvs);

    Status write_shard_as_rawbuff(const ImmutableIndex::ShardInfo& old_shard_info, ImmutableIndex* immutable_index);

    Status finish();

private:
    EditVersion _version;
    string _idx_file_path_tmp;
    string _idx_file_path;
    std::shared_ptr<FileSystem> _fs;
    std::unique_ptr<WritableFile> _wb;
    std::map<size_t, std::pair<size_t, size_t>> _shard_info_by_length;
    size_t _nshard = 0;
    size_t _cur_key_size = -1;
    size_t _cur_value_size = 0;
    size_t _total = 0;
    size_t _total_moved = 0;
    size_t _total_kv_size = 0;
    size_t _total_bytes = 0;
    ImmutableIndexMetaPB _meta;
};

// A persistent primary index contains an in-memory L0 and an on-SSD/NVMe L1,
// this saves memory usage comparing to the orig all-in-memory implementation.
// This is a internal class and is intended to be used by PrimaryIndex internally.
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
    PersistentIndex(std::string  path);
    ~PersistentIndex();

    bool loaded() const { return (bool)_l0; }

    std::string path() const { return _path; }

    size_t key_size() const { return _key_size; }

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
    size_t _size = 0;
    EditVersion _version;
    // _l1_version is used to get l1 file name, update in on_committed
    EditVersion _l1_version;
    std::unique_ptr<ShardByLengthMutableIndex> _l0;
    std::unique_ptr<ImmutableIndex> _l1;
    std::shared_ptr<FileSystem> _fs;

    bool _dump_snapshot = false;
    bool _flushed = false;
};

} // namespace starrocks
