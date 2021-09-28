// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "storage/primary_index.h"

#include <mutex>

#include "storage/primary_key_encoder.h"
#include "storage/rowset/beta_rowset.h"
#include "storage/rowset/rowset.h"
#include "storage/rowset/vectorized/rowset_options.h"
#include "storage/tablet.h"
#include "storage/tablet_updates.h"
#include "storage/vectorized/chunk_helper.h"
#include "storage/vectorized/reader.h"
#include "util/starrocks_metrics.h"

namespace starrocks {

using vectorized::Column;
using vectorized::ColumnPtr;
using vectorized::ChunkHelper;
using vectorized::UInt64Column;
using vectorized::StdHashWithSeed;
using vectorized::PhmapSeed1;

using tablet_rowid_t = uint64_t;

class HashIndex {
public:
    using DeletesMap = PrimaryIndex::DeletesMap;

    HashIndex() = default;
    virtual ~HashIndex() = default;

    virtual size_t size() const = 0;

    virtual size_t capacity() const = 0;

    virtual void reserve(size_t size) = 0;

    virtual Status insert(uint32_t rssid, uint32_t rowid_start, const vectorized::Column& pks) = 0;
    virtual Status insert(uint32_t rssid, const vector<uint32_t>& rowids, const vectorized::Column& pks) = 0;
    virtual void upsert(uint32_t rssid, uint32_t rowid_start, const vectorized::Column& pks, DeletesMap* deletes) = 0;
    virtual void upsert(uint32_t rssid, const vector<uint32_t>& rowids, const vectorized::Column& pks,
                        DeletesMap* deletes) = 0;
    virtual void try_replace(uint32_t rssid, uint32_t rowid_start, const vectorized::Column& pks,
                             const vector<uint32_t>& src_rssid, vector<uint32_t>* failed) = 0;
    virtual void try_replace(uint32_t rssid, const vector<uint32_t>& rowids, const vectorized::Column& pks,
                             const vector<uint32_t>& src_rssid, vector<uint32_t>* failed) = 0;
    virtual void erase(const vectorized::Column& pks, DeletesMap* deletes) = 0;

    // just an estimate value for now.
    virtual std::size_t memory_usage() const = 0;

    virtual std::string memory_info() const = 0;
};

#pragma pack(push)
#pragma pack(4)
struct RowIdPack4 {
    uint64_t value;
    RowIdPack4() = default;
    RowIdPack4(uint64_t v) : value(v) {}
};
#pragma pack(pop)

static const size_t large_mem_alloc_threhold = 2 * 1024 * 1024 * 1024UL;
static const size_t phmap_hash_table_shard = 16;

template <class T>
class TraceAlloc {
public:
    using value_type = T;

    TraceAlloc() noexcept = default;
    template <class U>
    TraceAlloc(TraceAlloc<U> const&) noexcept {}

    value_type* allocate(std::size_t n) {
        if (n >= large_mem_alloc_threhold / phmap_hash_table_shard / sizeof(value_type)) {
            LOG(INFO) << "primary_index large alloc " << n << "*" << sizeof(value_type) << "="
                      << n * sizeof(value_type);
        }
        return static_cast<value_type*>(::operator new(n * sizeof(value_type)));
    }

    void deallocate(value_type* p, std::size_t) noexcept { ::operator delete(p); }
};

const uint32_t PREFETCHN = 8;

template <typename Key>
class HashIndexImpl : public HashIndex {
private:
    phmap::parallel_flat_hash_map<Key, RowIdPack4, StdHashWithSeed<Key, PhmapSeed1>, phmap::priv::hash_default_eq<Key>,
                                  TraceAlloc<phmap::priv::Pair<const Key, RowIdPack4>>, 4, phmap::NullMutex, false>
            _map;

public:
    HashIndexImpl() = default;
    ~HashIndexImpl() override = default;

    size_t size() const override { return _map.size(); }

    size_t capacity() const override { return _map.capacity(); }

    void reserve(size_t size) override { _map.reserve(size); };

    Status insert(uint32_t rssid, uint32_t rowid_start, const vectorized::Column& pks) override {
        auto* keys = reinterpret_cast<const Key*>(pks.raw_data());
        auto size = pks.size();
        uint64_t base = (((uint64_t)rssid) << 32) + rowid_start;
        for (uint32_t i = 0; i < size; i++) {
            uint32_t prefetch_i = i + PREFETCHN;
            if (LIKELY(prefetch_i < size)) _map.prefetch(keys[prefetch_i]);
            RowIdPack4 v(base + i);
            auto p = _map.insert({keys[i], v});
            if (!p.second) {
                uint64_t old = p.first->second.value;
                std::string msg = strings::Substitute(
                        "insert found duplicate key new(rssid=$0 rowid=$1) old(rssid=$2 rowid=$3) "
                        "key=$4",
                        rssid, rowid_start + i, (uint32_t)(old >> 32), (uint32_t)(old & 0xffffffff), keys[i]);
                LOG(ERROR) << msg;
                return Status::InternalError(msg);
            }
        }
        return Status::OK();
    }

    Status insert(uint32_t rssid, const vector<uint32_t>& rowids, const vectorized::Column& pks) override {
        auto* keys = reinterpret_cast<const Key*>(pks.raw_data());
        auto size = pks.size();
        DCHECK(size == rowids.size());
        uint64_t base = (((uint64_t)rssid) << 32);
        for (auto i = 0; i < size; i++) {
            uint32_t prefetch_i = i + PREFETCHN;
            if (LIKELY(prefetch_i < size)) _map.prefetch(keys[prefetch_i]);
            RowIdPack4 v(base + rowids[i]);
            auto p = _map.insert({keys[i], v});
            if (!p.second) {
                uint64_t old = p.first->second.value;
                std::string msg = strings::Substitute(
                        "insert found duplicate key new(rssid=$0 rowid=$1) old(rssid=$2 rowid=$3) "
                        "key=$4",
                        rssid, rowids[i], (uint32_t)(old >> 32), (uint32_t)(old & 0xffffffff), keys[i]);
                LOG(ERROR) << msg;
                return Status::InternalError(msg);
            }
        }
        return Status::OK();
    }

    void upsert(uint32_t rssid, uint32_t rowid_start, const vectorized::Column& pks, DeletesMap* deletes) override {
        auto* keys = reinterpret_cast<const Key*>(pks.raw_data());
        auto size = pks.size();
        uint64_t base = (((uint64_t)rssid) << 32) + rowid_start;
        for (uint32_t i = 0; i < size; i++) {
            uint32_t prefetch_i = i + PREFETCHN;
            if (LIKELY(prefetch_i < size)) _map.prefetch(keys[prefetch_i]);
            RowIdPack4 v(base + i);
            auto p = _map.insert({keys[i], v});
            if (!p.second) {
                uint64_t old = p.first->second.value;
                if ((old >> 32) == rssid) {
                    LOG(ERROR) << "found duplicate in upsert data rssid:" << rssid << " key=" << keys[i] << " idx=" << i
                               << " rowid=" << rowid_start + i;
                }
                (*deletes)[(uint32_t)(old >> 32)].push_back((uint32_t)(old & 0xffffffff));
                p.first->second = v;
            }
        }
    }

    void upsert(uint32_t rssid, const vector<uint32_t>& rowids, const vectorized::Column& pks,
                DeletesMap* deletes) override {
        auto* keys = reinterpret_cast<const Key*>(pks.raw_data());
        auto size = pks.size();
        uint64_t base = (((uint64_t)rssid) << 32);
        for (auto i = 0; i < size; i++) {
            uint32_t prefetch_i = i + PREFETCHN;
            if (LIKELY(prefetch_i < size)) _map.prefetch(keys[prefetch_i]);
            RowIdPack4 v(base + rowids[i]);
            auto p = _map.insert({keys[i], v});
            if (!p.second) {
                uint64_t old = p.first->second.value;
                if ((old >> 32) == rssid) {
                    LOG(ERROR) << "found duplicate in upsert data rssid:" << rssid << " key=" << keys[i] << " idx=" << i
                               << " rowid=" << rowids[i];
                }
                (*deletes)[(uint32_t)(old >> 32)].push_back((uint32_t)(old & 0xffffffff));
                p.first->second = v;
            }
        }
    }

    void try_replace(uint32_t rssid, uint32_t rowid_start, const vectorized::Column& pks,
                     const vector<uint32_t>& src_rssid, vector<uint32_t>* failed) override {
        auto* keys = reinterpret_cast<const Key*>(pks.raw_data());
        auto size = pks.size();
        uint64_t base = (((uint64_t)rssid) << 32) + rowid_start;
        for (uint32_t i = 0; i < size; i++) {
            uint32_t prefetch_i = i + PREFETCHN;
            if (LIKELY(prefetch_i < size)) _map.prefetch(keys[prefetch_i]);
            auto p = _map.find(keys[i]);
            if (p != _map.end() && ((uint32_t)(p->second.value >> 32) == src_rssid[i])) {
                // matched, can replace
                p->second = RowIdPack4(base + i);
            } else {
                // not match, mark failed
                failed->push_back(rowid_start + i);
            }
        }
    }

    void try_replace(uint32_t rssid, const vector<uint32_t>& rowids, const vectorized::Column& pks,
                     const vector<uint32_t>& src_rssid, vector<uint32_t>* failed) override {
        auto* keys = reinterpret_cast<const Key*>(pks.raw_data());
        auto size = pks.size();
        uint64_t base = (((uint64_t)rssid) << 32);
        for (auto i = 0; i < size; i++) {
            uint32_t prefetch_i = i + PREFETCHN;
            if (LIKELY(prefetch_i < size)) _map.prefetch(keys[prefetch_i]);
            auto p = _map.find(keys[i]);
            if (p != _map.end() && ((uint32_t)(p->second.value >> 32) == src_rssid[i])) {
                // matched, can replace
                p->second = RowIdPack4(base + rowids[i]);
            } else {
                // not match, mark failed
                failed->push_back(rowids[i]);
            }
        }
    }

    void erase(const vectorized::Column& pks, DeletesMap* deletes) override {
        auto* keys = reinterpret_cast<const Key*>(pks.raw_data());
        auto size = pks.size();
        for (auto i = 0; i < size; i++) {
            uint32_t prefetch_i = i + PREFETCHN;
            if (LIKELY(prefetch_i < size)) _map.prefetch(keys[prefetch_i]);
            auto iter = _map.find(keys[i]);
            if (iter != _map.end()) {
                uint64_t old = iter->second.value;
                (*deletes)[(uint32_t)(old >> 32)].push_back((uint32_t)(old & 0xffffffff));
                _map.erase(iter);
            }
        }
    }

    std::size_t memory_usage() const final {
        return _map.capacity() * (1 + (sizeof(Key) + 3) / 4 * 4 + sizeof(RowIdPack4));
    }

    std::string memory_info() const override {
        auto caps = _map.capacities();
        string caps_str;
        for (auto e : caps) {
            StringAppendF(&caps_str, "%zu,", e);
        }
        return Substitute("$0M($1/$2 $3)", memory_usage() / (1024 * 1024), size(), capacity(), caps_str);
    }
};

template <size_t S>
struct FixSlice {
    uint32_t v[S];
    FixSlice() = default;
    explicit FixSlice(const Slice& s) { assign(s); }
    void clear() { memset(v, 0, sizeof(FixSlice)); }
    void assign(const Slice& s) {
        clear();
        DCHECK(s.size <= S * 4) << "slice size > FixSlice size";
        memcpy(v, s.data, s.size);
    }
    bool operator==(const FixSlice<S>& rhs) const { return memcmp(v, rhs.v, S * 4) == 0; }
};

template <size_t S>
struct FixSliceHash {
    size_t operator()(const FixSlice<S>& v) const { return vectorized::crc_hash_64(v.v, 4 * S, 0x811C9DC5); }
};

template <size_t S>
class FixSliceHashIndex : public HashIndex {
private:
    phmap::parallel_flat_hash_map<FixSlice<S>, RowIdPack4, FixSliceHash<S>, phmap::priv::hash_default_eq<FixSlice<S>>,
                                  TraceAlloc<phmap::priv::Pair<const FixSlice<S>, RowIdPack4>>, 4, phmap::NullMutex,
                                  false>
            _map;

public:
    FixSliceHashIndex() = default;
    ~FixSliceHashIndex() override = default;

    size_t size() const override { return _map.size(); }

    size_t capacity() const override { return _map.capacity(); }

    void reserve(size_t size) override { _map.reserve(size); };

    Status insert(uint32_t rssid, uint32_t rowid_start, const vectorized::Column& pks) override {
        auto* keys = reinterpret_cast<const Slice*>(pks.raw_data());
        uint32_t size = pks.size();
        uint64_t base = (((uint64_t)rssid) << 32) + rowid_start;
        FixSlice<S> prefetch_keys[PREFETCHN];
        size_t prefetch_hashs[PREFETCHN];
        for (uint32_t i = 0; i < std::min(size, PREFETCHN); i++) {
            prefetch_keys[i].assign(keys[i]);
            prefetch_hashs[i] = FixSliceHash<S>()(prefetch_keys[i]);
            _map.prefetch_hash(prefetch_hashs[i]);
        }
        for (uint32_t i = 0; i < size; i++) {
            uint64_t v = base + i;
            uint32_t pslot = i % PREFETCHN;
            auto p = _map.emplace_with_hash(prefetch_hashs[pslot], prefetch_keys[pslot], v);
            if (!p.second) {
                uint64_t old = p.first->second.value;
                std::string msg = strings::Substitute(
                        "insert found duplicate key new(rssid=$0 rowid=$1) old(rssid=$2 rowid=$3) "
                        "key=$4 [$5]",
                        rssid, rowid_start + i, (uint32_t)(old >> 32), (uint32_t)(old & 0xffffffff),
                        keys[i].to_string(), hexdump(keys[i].data, keys[i].size));
                LOG(ERROR) << msg;
                return Status::InternalError(msg);
            }
            uint32_t prefetch_i = i + PREFETCHN;
            if (LIKELY(prefetch_i < size)) {
                prefetch_keys[pslot].assign(keys[prefetch_i]);
                prefetch_hashs[pslot] = FixSliceHash<S>()(prefetch_keys[pslot]);
                _map.prefetch_hash(prefetch_hashs[pslot]);
            }
        }
        return Status::OK();
    }

    Status insert(uint32_t rssid, const vector<uint32_t>& rowids, const vectorized::Column& pks) override {
        auto* keys = reinterpret_cast<const Slice*>(pks.raw_data());
        uint32_t size = pks.size();
        DCHECK(size == rowids.size());
        uint64_t base = (((uint64_t)rssid) << 32);
        FixSlice<S> prefetch_keys[PREFETCHN];
        size_t prefetch_hashs[PREFETCHN];
        for (uint32_t i = 0; i < std::min(size, PREFETCHN); i++) {
            prefetch_keys[i].assign(keys[i]);
            prefetch_hashs[i] = FixSliceHash<S>()(prefetch_keys[i]);
            _map.prefetch_hash(prefetch_hashs[i]);
        }
        for (uint32_t i = 0; i < size; i++) {
            uint64_t v = base + rowids[i];
            uint32_t pslot = i % PREFETCHN;
            auto p = _map.emplace_with_hash(prefetch_hashs[pslot], prefetch_keys[pslot], v);
            if (!p.second) {
                uint64_t old = p.first->second.value;
                std::string msg = strings::Substitute(
                        "insert found duplicate key new(rssid=$0 rowid=$1) old(rssid=$2 rowid=$3) "
                        "key=$4 [$5]",
                        rssid, rowids[i], (uint32_t)(old >> 32), (uint32_t)(old & 0xffffffff), keys[i].to_string(),
                        hexdump(keys[i].data, keys[i].size));
                LOG(ERROR) << msg;
                return Status::InternalError(msg);
            }
            uint32_t prefetch_i = i + PREFETCHN;
            if (LIKELY(prefetch_i < size)) {
                prefetch_keys[pslot].assign(keys[prefetch_i]);
                prefetch_hashs[pslot] = FixSliceHash<S>()(prefetch_keys[pslot]);
                _map.prefetch_hash(prefetch_hashs[pslot]);
            }
        }
        return Status::OK();
    }

    void upsert(uint32_t rssid, uint32_t rowid_start, const vectorized::Column& pks, DeletesMap* deletes) override {
        auto* keys = reinterpret_cast<const Slice*>(pks.raw_data());
        uint32_t size = (uint32_t)pks.size();
        uint64_t base = (((uint64_t)rssid) << 32) + rowid_start;
        FixSlice<S> prefetch_keys[PREFETCHN];
        size_t prefetch_hashs[PREFETCHN];
        for (uint32_t i = 0; i < std::min(size, PREFETCHN); i++) {
            prefetch_keys[i].assign(keys[i]);
            prefetch_hashs[i] = FixSliceHash<S>()(prefetch_keys[i]);
            _map.prefetch_hash(prefetch_hashs[i]);
        }
        for (uint32_t i = 0; i < size; i++) {
            uint64_t v = base + i;
            uint32_t pslot = i % PREFETCHN;
            auto p = _map.emplace_with_hash(prefetch_hashs[pslot], prefetch_keys[pslot], v);
            if (!p.second) {
                uint64_t old = p.first->second.value;
                if ((old >> 32) == rssid) {
                    LOG(ERROR) << "found duplicate in upsert data rssid:" << rssid << " key=" << keys[i].to_string()
                               << " [" << hexdump(keys[i].data, keys[i].size) << "]";
                }
                (*deletes)[(uint32_t)(old >> 32)].push_back((uint32_t)(old & 0xffffffff));
                p.first->second = v;
            }
            uint32_t prefetch_i = i + PREFETCHN;
            if (LIKELY(prefetch_i < size)) {
                prefetch_keys[pslot].assign(keys[prefetch_i]);
                prefetch_hashs[pslot] = FixSliceHash<S>()(prefetch_keys[pslot]);
                _map.prefetch_hash(prefetch_hashs[pslot]);
            }
        }
    }

    void upsert(uint32_t rssid, const vector<uint32_t>& rowids, const vectorized::Column& pks,
                DeletesMap* deletes) override {
        auto* keys = reinterpret_cast<const Slice*>(pks.raw_data());
        uint32_t size = (uint32_t)pks.size();
        uint64_t base = (((uint64_t)rssid) << 32);
        FixSlice<S> prefetch_keys[PREFETCHN];
        size_t prefetch_hashs[PREFETCHN];
        for (uint32_t i = 0; i < std::min(size, PREFETCHN); i++) {
            prefetch_keys[i].assign(keys[i]);
            prefetch_hashs[i] = FixSliceHash<S>()(prefetch_keys[i]);
            _map.prefetch_hash(prefetch_hashs[i]);
        }
        for (auto i = 0; i < size; i++) {
            uint64_t v = base + rowids[i];
            uint32_t pslot = i % PREFETCHN;
            auto p = _map.emplace_with_hash(prefetch_hashs[pslot], prefetch_keys[pslot], v);
            if (!p.second) {
                uint64_t old = p.first->second.value;
                if ((old >> 32) == rssid) {
                    LOG(ERROR) << "found duplicate in upsert data rssid:" << rssid << " key=" << keys[i].to_string()
                               << " [" << hexdump(keys[i].data, keys[i].size) << "]";
                }
                (*deletes)[(uint32_t)(old >> 32)].push_back((uint32_t)(old & 0xffffffff));
                p.first->second = v;
            }
            uint32_t prefetch_i = i + PREFETCHN;
            if (LIKELY(prefetch_i < size)) {
                prefetch_keys[pslot].assign(keys[prefetch_i]);
                prefetch_hashs[pslot] = FixSliceHash<S>()(prefetch_keys[pslot]);
                _map.prefetch_hash(prefetch_hashs[pslot]);
            }
        }
    }

    void try_replace(uint32_t rssid, uint32_t rowid_start, const vectorized::Column& pks,
                     const vector<uint32_t>& src_rssid, vector<uint32_t>* failed) override {
        auto* keys = reinterpret_cast<const Slice*>(pks.raw_data());
        uint32_t size = pks.size();
        uint64_t base = (((uint64_t)rssid) << 32) + rowid_start;
        FixSlice<S> prefetch_keys[PREFETCHN];
        size_t prefetch_hashs[PREFETCHN];
        for (uint32_t i = 0; i < std::min(size, PREFETCHN); i++) {
            prefetch_keys[i].assign(keys[i]);
            prefetch_hashs[i] = FixSliceHash<S>()(prefetch_keys[i]);
            _map.prefetch_hash(prefetch_hashs[i]);
        }
        for (uint32_t i = 0; i < size; i++) {
            uint32_t pslot = i % PREFETCHN;
            auto p = _map.find(prefetch_keys[pslot], prefetch_hashs[pslot]);
            if (p != _map.end() && ((uint32_t)(p->second.value >> 32) == src_rssid[i])) {
                // matched, can replace
                p->second.value = base + i;
            } else {
                // not match, mark failed
                failed->push_back(rowid_start + i);
            }
            uint32_t prefetch_i = i + PREFETCHN;
            if (LIKELY(prefetch_i < size)) {
                prefetch_keys[pslot].assign(keys[prefetch_i]);
                prefetch_hashs[pslot] = FixSliceHash<S>()(prefetch_keys[pslot]);
                _map.prefetch_hash(prefetch_hashs[pslot]);
            }
        }
    }

    void try_replace(uint32_t rssid, const vector<uint32_t>& rowids, const vectorized::Column& pks,
                     const vector<uint32_t>& src_rssid, vector<uint32_t>* failed) override {
        auto* keys = reinterpret_cast<const Slice*>(pks.raw_data());
        uint32_t size = pks.size();
        uint64_t base = (((uint64_t)rssid) << 32);
        FixSlice<S> prefetch_keys[PREFETCHN];
        size_t prefetch_hashs[PREFETCHN];
        for (uint32_t i = 0; i < std::min(size, PREFETCHN); i++) {
            prefetch_keys[i].assign(keys[i]);
            prefetch_hashs[i] = FixSliceHash<S>()(prefetch_keys[i]);
            _map.prefetch_hash(prefetch_hashs[i]);
        }
        for (uint32_t i = 0; i < size; i++) {
            uint32_t pslot = i % PREFETCHN;
            auto p = _map.find(prefetch_keys[pslot], prefetch_hashs[pslot]);
            if (p != _map.end() && ((uint32_t)(p->second.value >> 32) == src_rssid[i])) {
                // matched, can replace
                p->second.value = base + rowids[i];
            } else {
                // not match, mark failed
                failed->push_back(rowids[i]);
            }
            uint32_t prefetch_i = i + PREFETCHN;
            if (LIKELY(prefetch_i < size)) {
                prefetch_keys[pslot].assign(keys[prefetch_i]);
                prefetch_hashs[pslot] = FixSliceHash<S>()(prefetch_keys[pslot]);
                _map.prefetch_hash(prefetch_hashs[pslot]);
            }
        }
    }

    void erase(const vectorized::Column& pks, DeletesMap* deletes) override {
        auto* keys = reinterpret_cast<const Slice*>(pks.raw_data());
        uint32_t size = pks.size();
        FixSlice<S> prefetch_keys[PREFETCHN];
        size_t prefetch_hashs[PREFETCHN];
        for (uint32_t i = 0; i < std::min(size, PREFETCHN); i++) {
            prefetch_keys[i].assign(keys[i]);
            prefetch_hashs[i] = FixSliceHash<S>()(prefetch_keys[i]);
            _map.prefetch_hash(prefetch_hashs[i]);
        }
        for (auto i = 0; i < size; i++) {
            uint32_t pslot = i % PREFETCHN;
            auto iter = _map.find(prefetch_keys[pslot], prefetch_hashs[pslot]);
            if (iter != _map.end()) {
                uint64_t old = iter->second.value;
                (*deletes)[(uint32_t)(old >> 32)].push_back((uint32_t)(old & 0xffffffff));
                _map.erase(iter);
            }
            uint32_t prefetch_i = i + PREFETCHN;
            if (LIKELY(prefetch_i < size)) {
                prefetch_keys[pslot].assign(keys[prefetch_i]);
                prefetch_hashs[pslot] = FixSliceHash<S>()(prefetch_keys[pslot]);
                _map.prefetch_hash(prefetch_hashs[pslot]);
            }
        }
    }

    std::size_t memory_usage() const final { return _map.capacity() * (1 + S * 4 + sizeof(RowIdPack4)); }

    std::string memory_info() const override {
        auto caps = _map.capacities();
        string caps_str;
        for (auto e : caps) {
            StringAppendF(&caps_str, "%zu,", e);
        }
        return Substitute("$0M($1/$2 $3)", memory_usage() / (1024 * 1024), size(), capacity(), caps_str);
    }
};

struct StringHash {
    size_t operator()(const string& v) const { return vectorized::crc_hash_64(v.data(), v.length(), 0x811C9DC5); }
};

template <>
class HashIndexImpl<Slice> : public HashIndex {
private:
    using StringMap =
            phmap::parallel_flat_hash_map<string, tablet_rowid_t, StringHash, phmap::priv::hash_default_eq<string>,
                                          TraceAlloc<phmap::priv::Pair<const string, tablet_rowid_t>>, 4,
                                          phmap::NullMutex, false>;

    StringMap _map;
    size_t _total_length = 0;

public:
    HashIndexImpl() = default;
    ~HashIndexImpl() override = default;

    size_t size() const override { return _map.size(); }

    size_t capacity() const override { return _map.capacity(); }

    void reserve(size_t size) override { _map.reserve(size); }

    Status insert(uint32_t rssid, uint32_t rowid_start, const vectorized::Column& pks) override {
        auto* keys = reinterpret_cast<const Slice*>(pks.raw_data());
        uint32_t size = pks.size();
        uint64_t base = (((uint64_t)rssid) << 32) + rowid_start;
        for (uint32_t i = 0; i < size; i++) {
            uint32_t prefetch_i = i + PREFETCHN;
            if (LIKELY(prefetch_i < size)) {
                size_t hv = vectorized::crc_hash_64(keys[prefetch_i].data, keys[prefetch_i].size, 0x811C9DC5);
                _map.prefetch_hash(hv);
            }
            uint64_t v = base + i;
            auto p = _map.insert({keys[i].to_string(), v});
            if (!p.second) {
                uint64_t old = p.first->second;
                std::string msg = strings::Substitute(
                        "insert found duplicate key new(rssid=$0 rowid=$1) old(rssid=$2 rowid=$3) "
                        "key=$4 [$5]",
                        rssid, rowid_start + i, (uint32_t)(old >> 32), (uint32_t)(old & 0xffffffff),
                        keys[i].to_string(), hexdump(keys[i].data, keys[i].size));
                LOG(ERROR) << msg;
                return Status::InternalError(msg);
            }
            _total_length += keys[i].size;
        }
        return Status::OK();
    }

    Status insert(uint32_t rssid, const vector<uint32_t>& rowids, const vectorized::Column& pks) override {
        auto* keys = reinterpret_cast<const Slice*>(pks.raw_data());
        uint32_t size = pks.size();
        DCHECK(size == rowids.size());
        uint64_t base = (((uint64_t)rssid) << 32);
        for (uint32_t i = 0; i < size; i++) {
            uint32_t prefetch_i = i + PREFETCHN;
            if (LIKELY(prefetch_i < size)) {
                size_t hv = vectorized::crc_hash_64(keys[prefetch_i].data, keys[prefetch_i].size, 0x811C9DC5);
                _map.prefetch_hash(hv);
            }
            uint64_t v = base + rowids[i];
            auto p = _map.insert({keys[i].to_string(), v});
            if (!p.second) {
                uint64_t old = p.first->second;
                std::string msg = strings::Substitute(
                        "insert found duplicate key new(rssid=$0 rowid=$1) old(rssid=$2 rowid=$3) "
                        "key=$4 [$5]",
                        rssid, rowids[i], (uint32_t)(old >> 32), (uint32_t)(old & 0xffffffff), keys[i].to_string(),
                        hexdump(keys[i].data, keys[i].size));
                LOG(ERROR) << msg;
                return Status::InternalError(msg);
            }
            _total_length += keys[i].size;
        }
        return Status::OK();
    }

    void upsert(uint32_t rssid, uint32_t rowid_start, const vectorized::Column& pks, DeletesMap* deletes) override {
        auto* keys = reinterpret_cast<const Slice*>(pks.raw_data());
        uint32_t size = pks.size();
        uint64_t base = (((uint64_t)rssid) << 32) + rowid_start;
        for (uint32_t i = 0; i < size; i++) {
            uint32_t prefetch_i = i + PREFETCHN;
            if (LIKELY(prefetch_i < size)) {
                size_t hv = vectorized::crc_hash_64(keys[prefetch_i].data, keys[prefetch_i].size, 0x811C9DC5);
                _map.prefetch_hash(hv);
            }
            uint64_t v = base + i;
            auto p = _map.insert({keys[i].to_string(), v});
            if (!p.second) {
                uint64_t old = p.first->second;
                if ((old >> 32) == rssid) {
                    LOG(ERROR) << "found duplicate in upsert data rssid:" << rssid << " key=" << keys[i].to_string()
                               << " [" << hexdump(keys[i].data, keys[i].size) << "]";
                }
                (*deletes)[(uint32_t)(old >> 32)].push_back((uint32_t)(old & 0xffffffff));
                p.first->second = v;
            } else {
                _total_length += keys[i].size;
            }
        }
    }

    void upsert(uint32_t rssid, const vector<uint32_t>& rowids, const vectorized::Column& pks,
                DeletesMap* deletes) override {
        auto* keys = reinterpret_cast<const Slice*>(pks.raw_data());
        uint32_t size = pks.size();
        uint64_t base = (((uint64_t)rssid) << 32);
        for (uint32_t i = 0; i < size; i++) {
            uint32_t prefetch_i = i + PREFETCHN;
            if (LIKELY(prefetch_i < size)) {
                size_t hv = vectorized::crc_hash_64(keys[prefetch_i].data, keys[prefetch_i].size, 0x811C9DC5);
                _map.prefetch_hash(hv);
            }
            uint64_t v = base + rowids[i];
            auto p = _map.insert({keys[i].to_string(), v});
            if (!p.second) {
                uint64_t old = p.first->second;
                if ((old >> 32) == rssid) {
                    LOG(ERROR) << "found duplicate in upsert data rssid:" << rssid << " key=" << keys[i].to_string()
                               << " [" << hexdump(keys[i].data, keys[i].size) << "]";
                }
                (*deletes)[(uint32_t)(old >> 32)].push_back((uint32_t)(old & 0xffffffff));
                p.first->second = v;
            } else {
                _total_length += keys[i].size;
            }
        }
    }

    void try_replace(uint32_t rssid, uint32_t rowid_start, const vectorized::Column& pks,
                     const vector<uint32_t>& src_rssid, vector<uint32_t>* failed) override {
        auto* keys = reinterpret_cast<const Slice*>(pks.raw_data());
        uint32_t size = pks.size();
        uint64_t base = (((uint64_t)rssid) << 32) + rowid_start;
        for (uint32_t i = 0; i < size; i++) {
            uint32_t prefetch_i = i + PREFETCHN;
            if (LIKELY(prefetch_i < size)) {
                size_t hv = vectorized::crc_hash_64(keys[prefetch_i].data, keys[prefetch_i].size, 0x811C9DC5);
                _map.prefetch_hash(hv);
            }
            auto p = _map.find(keys[i].to_string());
            if (p != _map.end() && ((uint32_t)(p->second >> 32) == src_rssid[i])) {
                // matched, can replace
                p->second = base + i;
            } else {
                // not match, mark failed
                failed->push_back(rowid_start + i);
            }
        }
    }

    void try_replace(uint32_t rssid, const vector<uint32_t>& rowids, const vectorized::Column& pks,
                     const vector<uint32_t>& src_rssid, vector<uint32_t>* failed) override {
        auto* keys = reinterpret_cast<const Slice*>(pks.raw_data());
        uint32_t size = pks.size();
        uint64_t base = (((uint64_t)rssid) << 32);
        for (uint32_t i = 0; i < size; i++) {
            uint32_t prefetch_i = i + PREFETCHN;
            if (LIKELY(prefetch_i < size)) {
                size_t hv = vectorized::crc_hash_64(keys[prefetch_i].data, keys[prefetch_i].size, 0x811C9DC5);
                _map.prefetch_hash(hv);
            }
            auto p = _map.find(keys[i].to_string());
            if (p != _map.end() && ((uint32_t)(p->second >> 32) == src_rssid[i])) {
                // matched, can replace
                p->second = base + rowids[i];
            } else {
                // not match, mark failed
                failed->push_back(rowids[i]);
            }
        }
    }

    void erase(const vectorized::Column& pks, DeletesMap* deletes) override {
        auto* keys = reinterpret_cast<const Slice*>(pks.raw_data());
        uint32_t size = pks.size();
        for (uint32_t i = 0; i < size; i++) {
            uint32_t prefetch_i = i + PREFETCHN;
            if (LIKELY(prefetch_i < size)) {
                size_t hv = vectorized::crc_hash_64(keys[prefetch_i].data, keys[prefetch_i].size, 0x811C9DC5);
                _map.prefetch_hash(hv);
            }
            auto p = _map.find(keys[i].to_string());
            if (p != _map.end()) {
                uint64_t old = p->second;
                (*deletes)[(uint32_t)(old >> 32)].push_back((uint32_t)(old & 0xffffffff));
                _map.erase(p);
                _total_length -= keys[i].size;
            }
        }
    }

    std::size_t memory_usage() const final {
        // TODO(cbl): more accurate value
        size_t ret = _map.capacity() * (1 + 32 + sizeof(tablet_rowid_t));
        if (size() > 0 && _total_length / size() > 15) {
            // std::string with length > 15 will alloc new memory for storage
            ret += _total_length;
            // an malloc extra cost estimation
            ret += size() * 8;
        }
        return ret;
    }

    std::string memory_info() const override {
        auto caps = _map.capacities();
        string caps_str;
        for (auto e : caps) {
            StringAppendF(&caps_str, "%zu,", e);
        }
        return Substitute("$0M($1/$2 $3)", memory_usage() / (1024 * 1024), size(), capacity(), caps_str);
    }
};

static std::unique_ptr<HashIndex> create_hash_index(FieldType key_type, size_t fix_size) {
    if (key_type == OLAP_FIELD_TYPE_VARCHAR && fix_size > 0) {
        if (fix_size <= 8) {
            return std::make_unique<FixSliceHashIndex<2>>();
        } else if (fix_size <= 12) {
            return std::make_unique<FixSliceHashIndex<3>>();
        } else if (fix_size <= 16) {
            return std::make_unique<FixSliceHashIndex<4>>();
        } else if (fix_size <= 20) {
            return std::make_unique<FixSliceHashIndex<5>>();
        } else if (fix_size <= 24) {
            return std::make_unique<FixSliceHashIndex<6>>();
        } else if (fix_size <= 28) {
            return std::make_unique<FixSliceHashIndex<7>>();
        } else if (fix_size <= 32) {
            return std::make_unique<FixSliceHashIndex<8>>();
        } else if (fix_size <= 36) {
            return std::make_unique<FixSliceHashIndex<9>>();
        }
    }

#define CASE_TYPE(type) \
    case (type):        \
        return std::make_unique<HashIndexImpl<typename CppTypeTraits<type>::CppType>>()

    switch (key_type) {
        CASE_TYPE(OLAP_FIELD_TYPE_BOOL);
        CASE_TYPE(OLAP_FIELD_TYPE_TINYINT);
        CASE_TYPE(OLAP_FIELD_TYPE_SMALLINT);
        CASE_TYPE(OLAP_FIELD_TYPE_INT);
        CASE_TYPE(OLAP_FIELD_TYPE_BIGINT);
        CASE_TYPE(OLAP_FIELD_TYPE_LARGEINT);
        CASE_TYPE(OLAP_FIELD_TYPE_CHAR);
        CASE_TYPE(OLAP_FIELD_TYPE_VARCHAR);
    case (OLAP_FIELD_TYPE_DATE_V2):
        return std::make_unique<HashIndexImpl<int32_t>>();
    case (OLAP_FIELD_TYPE_TIMESTAMP):
        return std::make_unique<HashIndexImpl<int64_t>>();
    default:
        return nullptr;
    }
}

PrimaryIndex::PrimaryIndex() = default;

PrimaryIndex::~PrimaryIndex() {
    if (_tablet_id != 0) {
        LOG(INFO) << "primary index released tablet:" << _tablet_id << " memory: " << memory_usage();
    }
}

PrimaryIndex::PrimaryIndex(const vectorized::Schema& pk_schema) {
    _set_schema(pk_schema);
}

void PrimaryIndex::_set_schema(const vectorized::Schema& pk_schema) {
    _pk_schema = pk_schema;
    _enc_pk_type = PrimaryKeyEncoder::encoded_primary_key_type(_pk_schema);
    size_t fix_size = PrimaryKeyEncoder::get_encoded_fixed_size(_pk_schema);
    _pkey_to_rssid_rowid = std::move(create_hash_index(_enc_pk_type, fix_size));
}

Status PrimaryIndex::load(Tablet* tablet) {
    std::lock_guard<std::mutex> lg(_lock);
    if (_loaded) {
        return _status;
    }
    _status = _do_load(tablet);
    _loaded = true;
    return _status;
}

void PrimaryIndex::unload() {
    std::lock_guard<std::mutex> lg(_lock);
    if (!_loaded) {
        return;
    }
    LOG(INFO) << "unload primary index tablet:" << _tablet_id << " size:" << size() << "capacity:" << capacity()
              << " memory: " << memory_usage();
    if (_pkey_to_rssid_rowid) {
        _pkey_to_rssid_rowid.reset();
    }
    _status = Status::OK();
    _loaded = false;
}

static string int_list_to_string(const vector<uint32_t>& l) {
    string ret;
    for (size_t i = 0; i < l.size(); i++) {
        if (i > 0) {
            ret.append(",");
        }
        ret.append(std::to_string(l[i]));
    }
    return ret;
}

Status PrimaryIndex::_do_load(Tablet* tablet) {
    MonotonicStopWatch timer;
    timer.start();
    using vectorized::ChunkHelper;
    using TabletReader = vectorized::Reader;
    using ReaderParams = vectorized::ReaderParams;

    const TabletSchema& tablet_schema = tablet->tablet_schema();
    vector<ColumnId> pk_columns(tablet_schema.num_key_columns());
    for (auto i = 0; i < tablet_schema.num_key_columns(); i++) {
        pk_columns[i] = (ColumnId)i;
    }
    auto pkey_schema = ChunkHelper::convert_schema_to_format_v2(tablet_schema, pk_columns);
    _set_schema(pkey_schema);

    int64_t apply_version = 0;
    std::vector<RowsetSharedPtr> rowsets;
    std::vector<uint32_t> rowset_ids;
    RETURN_IF_ERROR(tablet->updates()->_get_apply_version_and_rowsets(&apply_version, &rowsets, &rowset_ids));

    size_t total_data_size = 0;
    size_t total_segments = 0;
    size_t total_rows = 0;
    for (auto& rowset : rowsets) {
        total_data_size += rowset->data_disk_size();
        total_segments += rowset->num_segments();
        total_rows += rowset->num_rows();
    }
    size_t total_rows2 = 0;
    size_t total_dels = 0;
    auto st = tablet->updates()->get_rowsets_total_stats(rowset_ids, &total_rows2, &total_dels);
    if (!st.ok() || total_rows2 != total_rows) {
        LOG(WARNING) << "load primary index get_rowsets_total_stats error " << st;
    }
    DCHECK(total_rows2 == total_rows);
    if (total_data_size > 4000000000 || total_rows > 10000000 || total_segments > 400) {
        LOG(INFO) << "load large primary index start tablet:" << tablet->tablet_id() << " version:" << apply_version
                  << " #rowset:" << rowsets.size() << " #segment:" << total_segments << " #row:" << total_rows << " -"
                  << total_dels << "=" << total_rows - total_dels << " bytes:" << total_data_size;
    }
    if (total_rows > total_dels) {
        _pkey_to_rssid_rowid->reserve(total_rows - total_dels);
    }

    OlapReaderStatistics stats;
    std::unique_ptr<vectorized::Column> pk_column;
    if (pk_columns.size() > 1) {
        if (!PrimaryKeyEncoder::create_column(pkey_schema, &pk_column).ok()) {
            CHECK(false) << "create column for primary key encoder failed";
        }
    }
    // only hold pkey, so can use larger chunk size
    vector<uint32_t> rowids;
    rowids.reserve(4096);
    auto chunk_shared_ptr = ChunkHelper::new_chunk(pkey_schema, 4096);
    auto chunk = chunk_shared_ptr.get();
    for (auto& rowset : rowsets) {
        RowsetReleaseGuard guard(rowset);
        auto beta_rowset = down_cast<BetaRowset*>(rowset.get());
        auto res =
                beta_rowset->get_segment_iterators2(pkey_schema, tablet->data_dir()->get_meta(), apply_version, &stats);
        if (!res.ok()) {
            return res.status();
        }
        auto& itrs = res.value();
        // TODO(cbl): auto close iterators on failure
        CHECK(itrs.size() == rowset->num_segments()) << "itrs.size != num_segments";
        for (size_t i = 0; i < itrs.size(); i++) {
            auto itr = itrs[i].get();
            if (itr == nullptr) {
                continue;
            }
            while (true) {
                chunk->reset();
                rowids.clear();
                auto st = itr->get_next(chunk, &rowids);
                if (st.is_end_of_file()) {
                    break;
                } else if (!st.ok()) {
                    return st;
                } else {
                    Column* pkc = nullptr;
                    if (pk_column) {
                        pk_column->reset_column();
                        PrimaryKeyEncoder::encode(pkey_schema, *chunk, 0, chunk->num_rows(), pk_column.get());
                        pkc = pk_column.get();
                    } else {
                        pkc = chunk->columns()[0].get();
                    }
                    auto st = insert(rowset->rowset_meta()->get_rowset_seg_id() + i, rowids, *pkc);
                    if (!st.ok()) {
                        LOG(ERROR) << "load index failed: tablet=" << tablet->tablet_id()
                                   << " rowsets:" << int_list_to_string(rowset_ids)
                                   << " rowset:" << rowset->rowset_meta()->get_rowset_seg_id() << " segment:" << i
                                   << " reason: " << st.to_string() << " current_size:" << size()
                                   << " updates: " << tablet->updates()->debug_string();
                        return st;
                    }
                }
            }
            itr->close();
        }
    }
    _tablet_id = tablet->tablet_id();
    if (size() != total_rows - total_dels) {
        LOG(WARNING) << Substitute("load primary index row count not match tablet:$0 index:$1 != stats:$2", _tablet_id,
                                   size(), total_rows - total_dels);
    }
    LOG(INFO) << "load primary index finish tablet:" << tablet->tablet_id() << " version:" << apply_version
              << " #rowset:" << rowsets.size() << " #segment:" << total_segments << " data_size:" << total_data_size
              << " rowsets:" << int_list_to_string(rowset_ids) << " size:" << size() << " capacity:" << capacity()
              << " memory:" << memory_usage() << " duration: " << timer.elapsed_time() / 1000000 << "ms";
    return Status::OK();
}

Status PrimaryIndex::insert(uint32_t rssid, uint32_t rowid_start, const vectorized::Column& pks) {
    DCHECK(_status.ok() && _pkey_to_rssid_rowid);
    return _pkey_to_rssid_rowid->insert(rssid, rowid_start, pks);
}

Status PrimaryIndex::insert(uint32_t rssid, const vector<uint32_t>& rowids, const vectorized::Column& pks) {
    DCHECK(_status.ok() && _pkey_to_rssid_rowid);
    return _pkey_to_rssid_rowid->insert(rssid, rowids, pks);
}

void PrimaryIndex::upsert(uint32_t rssid, uint32_t rowid_start, const vectorized::Column& pks, DeletesMap* deletes) {
    DCHECK(_status.ok() && _pkey_to_rssid_rowid);
    _pkey_to_rssid_rowid->upsert(rssid, rowid_start, pks, deletes);
}

void PrimaryIndex::upsert(uint32_t rssid, const vector<uint32_t>& rowids, const vectorized::Column& pks,
                          DeletesMap* deletes) {
    DCHECK(_status.ok() && _pkey_to_rssid_rowid);
    _pkey_to_rssid_rowid->upsert(rssid, rowids, pks, deletes);
}

void PrimaryIndex::try_replace(uint32_t rssid, uint32_t rowid_start, const vectorized::Column& pks,
                               const vector<uint32_t>& src_rssid, vector<uint32_t>* deletes) {
    DCHECK(_status.ok() && _pkey_to_rssid_rowid);
    _pkey_to_rssid_rowid->try_replace(rssid, rowid_start, pks, src_rssid, deletes);
}

void PrimaryIndex::try_replace(uint32_t rssid, const vector<uint32_t>& rowids, const vectorized::Column& pks,
                               const vector<uint32_t>& src_rssid, vector<uint32_t>* deletes) {
    DCHECK(_status.ok() && _pkey_to_rssid_rowid);
    _pkey_to_rssid_rowid->try_replace(rssid, rowids, pks, src_rssid, deletes);
}

void PrimaryIndex::erase(const Column& key_col, DeletesMap* deletes) {
    DCHECK(_status.ok() && _pkey_to_rssid_rowid);
    _pkey_to_rssid_rowid->erase(key_col, deletes);
}

std::size_t PrimaryIndex::memory_usage() const {
    return _pkey_to_rssid_rowid ? _pkey_to_rssid_rowid->memory_usage() : 0;
}

std::string PrimaryIndex::memory_info() const {
    return _pkey_to_rssid_rowid ? _pkey_to_rssid_rowid->memory_info() : "Null";
}

std::size_t PrimaryIndex::size() const {
    return _pkey_to_rssid_rowid ? _pkey_to_rssid_rowid->size() : 0;
}

std::size_t PrimaryIndex::capacity() const {
    return _pkey_to_rssid_rowid ? _pkey_to_rssid_rowid->capacity() : 0;
}

std::string PrimaryIndex::to_string() const {
    return Substitute("PrimaryIndex tablet:$0", _tablet_id);
}

std::unique_ptr<PrimaryIndex> TEST_create_primary_index(const vectorized::Schema& pk_schema) {
    return std::make_unique<PrimaryIndex>(pk_schema);
}

} // namespace starrocks
