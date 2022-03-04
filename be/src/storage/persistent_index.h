// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <memory>
#include <tuple>

#include "common/statusor.h"
#include "gen_cpp/persistent_index.pb.h"
#include "storage/edit_version.h"
#include "storage/fs/block_manager.h"
#include "util/phmap/phmap.h"
#include "util/phmap/phmap_dump.h"

namespace starrocks {

using IndexValue = uint64_t;
static constexpr IndexValue NullIndexValue = -1;

struct KeysInfo {
    std::vector<uint32_t> key_idxes;
    std::vector<uint64_t> hashes;
};

class MutableIndex {
public:
    MutableIndex();
    virtual ~MutableIndex();

    // get the number of entries in the index (including NullIndexValue)
    virtual size_t size() const = 0;

    // flush mutable index into immutable index
    // |num_entry|: num of valid entries in this index(excluding NullIndexValue)
    // |wb|: file block written to
    virtual Status flush_to_immutable_index(size_t num_entry, const EditVersion& version,
                                            fs::WritableBlock& wb) const = 0;

    // batch get
    // |n|: size of key/value array
    // |keys|: key array as raw buffer
    // |values|: value array for return values
    // |not_found|: information of keys not found, which need to be further checked in next level
    // |num_found|: add the number of keys found to this argument
    virtual Status get(size_t n, const void* keys, IndexValue* values, KeysInfo* not_found,
                       size_t* num_found) const = 0;

    // batch upsert and get old value
    // |n|: size of key/value array
    // |keys|: key array as raw buffer
    // |values|: value array
    // |old_values|: return old values for updates, or set to NullValue for inserts
    // |not_found|: information of keys not found, which need to be further checked in next level
    // |num_found|: add the number of keys found to this argument
    virtual Status upsert(size_t n, const void* keys, const IndexValue* values, IndexValue* old_values,
                          KeysInfo* not_found, size_t* num_found) = 0;

    // batch upsert
    // |n|: size of key/value array
    // |keys|: key array as raw buffer
    // |values|: value array
    // |not_found|: information of keys not found, which need to be further checked in next level
    // |num_found|: add the number of keys found(or already exist) to this argument
    virtual Status upsert(size_t n, const void* keys, const IndexValue* values, KeysInfo* not_found,
                          size_t* num_found) = 0;

    // load wals
    // |n|: size of key/value array
    // |keys|: key array as raw buffer
    // |values|: value array
    virtual Status load_wals(size_t n, const void* keys, const IndexValue* values) = 0;

    // batch insert
    // |n|: size of key/value array
    // |keys|: key array as raw buffer
    // |values|: value array
    virtual Status insert(size_t n, const void* keys, const IndexValue* values) = 0;

    // batch erase(delete)
    // |n|: size of key/value array
    // |keys|: key array as raw buffer
    // |old_values|: return old values for updates, or set to NullValue if not exists
    // |not_found|: information of keys not found, which need to be further checked in next level
    // |num_found|: add the number of keys found to this argument
    virtual Status erase(size_t n, const void* keys, IndexValue* old_values, KeysInfo* not_found,
                         size_t* num_found) = 0;

    // get dump size of hashmap
    virtual size_t dump_bound() = 0;

    virtual bool dump(phmap::BinaryOutputArchive& ar_out) = 0;

    virtual bool load_snapshot(phmap::BinaryInputArchive& ar_in) = 0;

    // [not thread-safe]
    virtual size_t size() = 0;

    // [not thread-safe]
    virtual size_t capacity() = 0;

    static StatusOr<std::unique_ptr<MutableIndex>> create(size_t key_size);
};

class ImmutableIndex {
public:
    // batch get
    // |n|: size of key/value array
    // |keys|: key array as raw buffer
    // |not_found|: information of keys not found in upper level, which needs to be checked in this level
    // |values|: value array for return values
    // |num_found|: add the number of keys found in L1 to this argument
    Status get(size_t n, const void* keys, const KeysInfo& keys_info, IndexValue* values, size_t* num_found) const;

    // batch check key existence
    Status check_not_exist(size_t n, const void* keys);
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

    size_t size() const { return _size; }
    size_t kv_size = key_size() + sizeof(IndexValue);

    EditVersion version() const { return _version; }

    // create new empty index
    Status create(size_t key_size, const EditVersion& version);

    // load required states from underlying file
    Status load(const PersistentIndexMetaPB& index_meta);

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
    Status get(size_t n, const void* keys, IndexValue* values);

    // batch upsert
    // |n|: size of key/value array
    // |keys|: key array as raw buffer
    // |values|: value array
    // |old_values|: return old values for updates, or set to NullValue for inserts
    Status upsert(size_t n, const void* keys, const IndexValue* values, IndexValue* old_values);

    // batch insert, return error if key already exists
    // |n|: size of key/value array
    // |keys|: key array as raw buffer
    // |values|: value array
    // |check_l1|: also check l1 for insertion consistency(key must not exist previously), may imply heavy IO costs
    Status insert(size_t n, const void* keys, const IndexValue* values, bool check_l1);

    // batch erase
    // |n|: size of key/value array
    // |keys|: key array as raw buffer
    // |old_values|: return old values if key exist, or set to NullValue if not
    Status erase(size_t n, const void* keys, IndexValue* old_values);

    size_t mutable_index_size();

    size_t mutable_index_capacity();

private:
    std::string _get_l0_index_file_name(std::string& dir, const EditVersion& version);

    size_t _dump_bound();

    bool _dump(phmap::BinaryOutputArchive& ar_out);

    // check _l0 should dump as snapshot or not
    bool _can_dump_directly();

    bool _load_snapshot(phmap::BinaryInputArchive& ar_in);

    Status _delete_expired_index_file(const EditVersion& version);

    // batch append wal
    // |n|: size of key/value array
    // |keys|: key array as raw buffer
    // |values|: value array, if operation is erase, |values| is nullptr
    Status _append_wal(size_t n, const void* key, const IndexValue* values);

    Status _flush_l0();

    // index storage directory
    std::string _path;
    size_t _key_size = 0;
    size_t _size = 0;
    EditVersion _version;
    std::unique_ptr<MutableIndex> _l0;
    std::unique_ptr<ImmutableIndex> _l1;
    // |_offset|: the start offset of last wal in index file
    // |_page_size|: the size of last wal in index file
    uint64_t _offset = 0;
    uint32_t _page_size = 0;
    std::unique_ptr<fs::WritableBlock> _index_block;

    bool _dump_snapshot = false;
};

} // namespace starrocks
