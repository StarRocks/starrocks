// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "storage/primary_index.h"

#include <memory>
#include <mutex>

#include "common/tracer.h"
#include "gutil/strings/substitute.h"
#include "io/io_profiler.h"
#include "runtime/large_int_value.h"
#include "storage/chunk_helper.h"
#include "storage/primary_key_encoder.h"
#include "storage/rowset/rowset.h"
#include "storage/rowset/rowset_options.h"
#include "storage/tablet.h"
#include "storage/tablet_reader.h"
#include "storage/tablet_updates.h"
#include "util/stack_util.h"
#include "util/starrocks_metrics.h"
#include "util/xxh3.h"

namespace starrocks {

using tablet_rowid_t = PrimaryIndex::tablet_rowid_t;

class HashIndex {
public:
    using DeletesMap = PrimaryIndex::DeletesMap;

    HashIndex() = default;
    virtual ~HashIndex() = default;

    virtual size_t size() const = 0;

    virtual size_t capacity() const = 0;

    virtual void reserve(size_t size) = 0;

    // batch insert a range [idx_begin, idx_end) of keys
    virtual Status insert(uint32_t rssid, const vector<uint32_t>& rowids, const vectorized::Column& pks,
                          uint32_t idx_begin, uint32_t idx_end) = 0;
    // batch upsert a range [idx_begin, idx_end) of keys
    virtual void upsert(uint32_t rssid, uint32_t rowid_start, const vectorized::Column& pks, uint32_t idx_begin,
                        uint32_t idx_end, DeletesMap* deletes) = 0;
    // TODO(qzc): maybe unused, remove it or refactor it with the methods in use by template after a period of time
    // batch try_replace a range [idx_begin, idx_end) of keys
    [[maybe_unused]] virtual void try_replace(uint32_t rssid, uint32_t rowid_start, const vectorized::Column& pks,
                                              const vector<uint32_t>& src_rssid, uint32_t idx_begin, uint32_t idx_end,
                                              vector<uint32_t>* failed) = 0;

    virtual void try_replace(uint32_t rssid, uint32_t rowid_start, const vectorized::Column& pks,
                             const uint32_t max_src_rssid, uint32_t idx_begin, uint32_t idx_end,
                             vector<uint32_t>* failed) = 0;
    // batch erase a range [idx_begin, idx_end) of keys
    virtual void erase(const vectorized::Column& pks, uint32_t idx_begin, uint32_t idx_end, DeletesMap* deletes) = 0;

    virtual void get(const vectorized::Column& pks, uint32_t idx_begin, uint32_t idx_end,
                     std::vector<uint64_t>* rowids) = 0;

    // just an estimate value for now
    virtual std::size_t memory_usage() const = 0;
};

#pragma pack(push)
#pragma pack(4)
struct RowIdPack4 {
    uint64_t value;
    RowIdPack4() = default;
    RowIdPack4(uint64_t v) : value(v) {}
};
#pragma pack(pop)

template <class T>
class TraceAlloc {
public:
    using value_type = T;

    TraceAlloc() noexcept = default;
    template <class U>
    TraceAlloc(TraceAlloc<U> const&) noexcept {}

    value_type* allocate(std::size_t n) { return static_cast<value_type*>(::operator new(n * sizeof(value_type))); }

    void deallocate(value_type* p, std::size_t) noexcept { ::operator delete(p); }
};

const uint32_t PREFETCHN = 8;

template <typename Key>
class HashIndexImpl : public HashIndex {
private:
    phmap::parallel_flat_hash_map<Key, RowIdPack4, vectorized::StdHashWithSeed<Key, vectorized::PhmapSeed1>,
                                  phmap::priv::hash_default_eq<Key>,
                                  TraceAlloc<phmap::priv::Pair<const Key, RowIdPack4>>, 4, phmap::NullMutex, true>
            _map;

public:
    HashIndexImpl() = default;
    ~HashIndexImpl() override = default;

    size_t size() const override { return _map.size(); }

    size_t capacity() const override { return _map.capacity(); }

    void reserve(size_t size) override { _map.reserve(size); };

    Status insert(uint32_t rssid, const vector<uint32_t>& rowids, const vectorized::Column& pks, uint32_t idx_begin,
                  uint32_t idx_end) override {
        auto* keys = reinterpret_cast<const Key*>(pks.raw_data());
        DCHECK(idx_end <= rowids.size());
        uint64_t base = (((uint64_t)rssid) << 32);
        for (auto i = idx_begin; i < idx_end; i++) {
            uint32_t prefetch_i = i + PREFETCHN;
            if (LIKELY(prefetch_i < idx_end)) _map.prefetch(keys[prefetch_i]);
            RowIdPack4 v(base + rowids[i]);
            auto p = _map.insert({keys[i], v});
            if (!p.second) {
                uint64_t old = p.first->second.value;
                std::string msg = strings::Substitute(
                        "insert found duplicate key new(rssid=$0 rowid=$1) old(rssid=$2 rowid=$3) "
                        "key=$4",
                        rssid, rowids[i], (uint32_t)(old >> 32), (uint32_t)(old & ROWID_MASK), keys[i]);
                LOG(ERROR) << msg;
                return Status::InternalError(msg);
            }
        }
        return Status::OK();
    }

    void upsert(uint32_t rssid, uint32_t rowid_start, const vectorized::Column& pks, uint32_t idx_begin,
                uint32_t idx_end, DeletesMap* deletes) override {
        auto* keys = reinterpret_cast<const Key*>(pks.raw_data());
        uint64_t base = (((uint64_t)rssid) << 32) + rowid_start;
        for (uint32_t i = idx_begin; i < idx_end; i++) {
            uint32_t prefetch_i = i + PREFETCHN;
            if (LIKELY(prefetch_i < idx_end)) _map.prefetch(keys[prefetch_i]);
            RowIdPack4 v(base + i);
            auto p = _map.insert({keys[i], v});
            if (!p.second) {
                uint64_t old = p.first->second.value;
                if ((old >> 32) == rssid) {
                    LOG(ERROR) << "found duplicate in upsert data rssid:" << rssid << " key=" << keys[i] << " idx=" << i
                               << " rowid=" << rowid_start + i;
                }
                (*deletes)[(uint32_t)(old >> 32)].push_back((uint32_t)(old & ROWID_MASK));
                p.first->second = v;
            }
        }
    }

    [[maybe_unused]] void try_replace(uint32_t rssid, uint32_t rowid_start, const vectorized::Column& pks,
                                      const vector<uint32_t>& src_rssid, uint32_t idx_begin, uint32_t idx_end,
                                      vector<uint32_t>* failed) override {
        auto* keys = reinterpret_cast<const Key*>(pks.raw_data());
        uint64_t base = (((uint64_t)rssid) << 32) + rowid_start;
        for (uint32_t i = idx_begin; i < idx_end; i++) {
            uint32_t prefetch_i = i + PREFETCHN;
            if (LIKELY(prefetch_i < idx_end)) _map.prefetch(keys[prefetch_i]);
            auto p = _map.find(keys[i]);
            if (p != _map.end() && (uint32_t)(p->second.value >> 32) == src_rssid[i]) {
                // matched, can replace
                p->second = RowIdPack4(base + i);
            } else {
                // not match, mark failed
                failed->push_back(rowid_start + i);
            }
        }
    }

    void try_replace(uint32_t rssid, uint32_t rowid_start, const vectorized::Column& pks, const uint32_t max_src_rssid,
                     uint32_t idx_begin, uint32_t idx_end, vector<uint32_t>* failed) override {
        auto* keys = reinterpret_cast<const Key*>(pks.raw_data());
        uint64_t base = (((uint64_t)rssid) << 32) + rowid_start;
        for (uint32_t i = idx_begin; i < idx_end; i++) {
            uint32_t prefetch_i = i + PREFETCHN;
            if (LIKELY(prefetch_i < idx_end)) _map.prefetch(keys[prefetch_i]);
            auto p = _map.find(keys[i]);
            if (p != _map.end() && (uint32_t)(p->second.value >> 32) <= max_src_rssid) {
                p->second = RowIdPack4(base + i);
            } else {
                failed->push_back(rowid_start + i);
            }
        }
    }

    void erase(const vectorized::Column& pks, uint32_t idx_begin, uint32_t idx_end, DeletesMap* deletes) override {
        auto* keys = reinterpret_cast<const Key*>(pks.raw_data());
        for (auto i = idx_begin; i < idx_end; i++) {
            uint32_t prefetch_i = i + PREFETCHN;
            if (LIKELY(prefetch_i < idx_end)) _map.prefetch(keys[prefetch_i]);
            auto iter = _map.find(keys[i]);
            if (iter != _map.end()) {
                uint64_t old = iter->second.value;
                (*deletes)[(uint32_t)(old >> 32)].push_back((uint32_t)(old & ROWID_MASK));
                _map.erase(iter);
            }
        }
    }

    void get(const vectorized::Column& pks, uint32_t idx_begin, uint32_t idx_end,
             std::vector<uint64_t>* rowids) override {
        auto* keys = reinterpret_cast<const Key*>(pks.raw_data());
        for (auto i = idx_begin; i < idx_end; i++) {
            uint32_t prefetch_i = i + PREFETCHN;
            if (LIKELY(prefetch_i < idx_end)) _map.prefetch(keys[prefetch_i]);
            auto iter = _map.find(keys[i]);
            if (iter != _map.end()) {
                (*rowids)[i] = iter->second.value;
            } else {
                (*rowids)[i] = -1;
            }
        }
    }

    std::size_t memory_usage() const final {
        return _map.capacity() * (1 + (sizeof(Key) + 3) / 4 * 4 + sizeof(RowIdPack4));
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
    size_t operator()(const FixSlice<S>& v) const { return XXH3_64bits(v.v, 4 * S); }
};

template <size_t S>
class FixSliceHashIndex : public HashIndex {
private:
    phmap::parallel_flat_hash_map<FixSlice<S>, RowIdPack4, FixSliceHash<S>, phmap::priv::hash_default_eq<FixSlice<S>>,
                                  TraceAlloc<phmap::priv::Pair<const FixSlice<S>, RowIdPack4>>, 4, phmap::NullMutex,
                                  true>
            _map;

public:
    FixSliceHashIndex() = default;
    ~FixSliceHashIndex() override = default;

    size_t size() const override { return _map.size(); }

    size_t capacity() const override { return _map.capacity(); }

    void reserve(size_t size) override { _map.reserve(size); };

    Status insert(uint32_t rssid, const vector<uint32_t>& rowids, const vectorized::Column& pks, uint32_t idx_begin,
                  uint32_t idx_end) override {
        const auto* keys = reinterpret_cast<const Slice*>(pks.raw_data());
        DCHECK(idx_end <= rowids.size());
        uint64_t base = (((uint64_t)rssid) << 32);
        uint32_t n = idx_end - idx_begin;
        if (n >= PREFETCHN * 2) {
            FixSlice<S> prefetch_keys[PREFETCHN];
            size_t prefetch_hashes[PREFETCHN];
            for (uint32_t i = 0; i < PREFETCHN; i++) {
                prefetch_keys[i].assign(keys[idx_begin + i]);
                prefetch_hashes[i] = FixSliceHash<S>()(prefetch_keys[i]);
                _map.prefetch_hash(prefetch_hashes[i]);
            }
            for (uint32_t i = idx_begin; i < idx_end; i++) {
                uint64_t v = base + rowids[i];
                uint32_t pslot = (i - idx_begin) % PREFETCHN;
                auto p = _map.emplace_with_hash(prefetch_hashes[pslot], prefetch_keys[pslot], v);
                if (!p.second) {
                    uint64_t old = p.first->second.value;
                    std::string msg = strings::Substitute(
                            "insert found duplicate key new(rssid=$0 rowid=$1) old(rssid=$2 rowid=$3) "
                            "key=$4 [$5]",
                            rssid, rowids[i], (uint32_t)(old >> 32), (uint32_t)(old & ROWID_MASK), keys[i].to_string(),
                            hexdump(keys[i].data, keys[i].size));
                    LOG(ERROR) << msg;
                    return Status::InternalError(msg);
                }
                uint32_t prefetch_i = i + PREFETCHN;
                if (LIKELY(prefetch_i < idx_end)) {
                    prefetch_keys[pslot].assign(keys[prefetch_i]);
                    prefetch_hashes[pslot] = FixSliceHash<S>()(prefetch_keys[pslot]);
                    _map.prefetch_hash(prefetch_hashes[pslot]);
                }
            }
        } else {
            for (uint32_t i = idx_begin; i < idx_end; i++) {
                uint64_t v = base + rowids[i];
                auto p = _map.emplace(FixSlice<S>(keys[i]), v);
                if (!p.second) {
                    uint64_t old = p.first->second.value;
                    std::string msg = strings::Substitute(
                            "insert found duplicate key new(rssid=$0 rowid=$1) old(rssid=$2 rowid=$3) "
                            "key=$4 [$5]",
                            rssid, rowids[i], (uint32_t)(old >> 32), (uint32_t)(old & ROWID_MASK), keys[i].to_string(),
                            hexdump(keys[i].data, keys[i].size));
                    LOG(ERROR) << msg;
                    return Status::InternalError(msg);
                }
            }
        }
        return Status::OK();
    }

    void upsert(uint32_t rssid, uint32_t rowid_start, const vectorized::Column& pks, uint32_t idx_begin,
                uint32_t idx_end, DeletesMap* deletes) override {
        const auto* keys = reinterpret_cast<const Slice*>(pks.raw_data());
        uint64_t base = (((uint64_t)rssid) << 32) + rowid_start;
        uint32_t n = idx_end - idx_begin;
        if (n >= PREFETCHN * 2) {
            FixSlice<S> prefetch_keys[PREFETCHN];
            size_t prefetch_hashes[PREFETCHN];
            for (uint32_t i = 0; i < PREFETCHN; i++) {
                prefetch_keys[i].assign(keys[idx_begin + i]);
                prefetch_hashes[i] = FixSliceHash<S>()(prefetch_keys[i]);
                _map.prefetch_hash(prefetch_hashes[i]);
            }
            for (uint32_t i = idx_begin; i < idx_end; i++) {
                uint64_t v = base + i;
                uint32_t pslot = (i - idx_begin) % PREFETCHN;
                auto p = _map.emplace_with_hash(prefetch_hashes[pslot], prefetch_keys[pslot], v);
                if (!p.second) {
                    uint64_t old = p.first->second.value;
                    if ((old >> 32) == rssid) {
                        LOG(ERROR) << "found duplicate in upsert data rssid:" << rssid << " key=" << keys[i].to_string()
                                   << " [" << hexdump(keys[i].data, keys[i].size) << "]";
                    }
                    (*deletes)[(uint32_t)(old >> 32)].push_back((uint32_t)(old & ROWID_MASK));
                    p.first->second = v;
                }
                uint32_t prefetch_i = i + PREFETCHN;
                if (LIKELY(prefetch_i < idx_end)) {
                    prefetch_keys[pslot].assign(keys[prefetch_i]);
                    prefetch_hashes[pslot] = FixSliceHash<S>()(prefetch_keys[pslot]);
                    _map.prefetch_hash(prefetch_hashes[pslot]);
                }
            }
        } else {
            for (uint32_t i = idx_begin; i < idx_end; i++) {
                uint64_t v = base + i;
                auto p = _map.emplace(FixSlice<S>(keys[i]), v);
                if (!p.second) {
                    uint64_t old = p.first->second.value;
                    if ((old >> 32) == rssid) {
                        LOG(ERROR) << "found duplicate in upsert data rssid:" << rssid << " key=" << keys[i].to_string()
                                   << " [" << hexdump(keys[i].data, keys[i].size) << "]";
                    }
                    (*deletes)[(uint32_t)(old >> 32)].push_back((uint32_t)(old & ROWID_MASK));
                    p.first->second = v;
                }
            }
        }
    }
    [[maybe_unused]] void try_replace(uint32_t rssid, uint32_t rowid_start, const vectorized::Column& pks,
                                      const vector<uint32_t>& src_rssid, uint32_t idx_begin, uint32_t idx_end,
                                      vector<uint32_t>* failed) override {
        const auto* keys = reinterpret_cast<const Slice*>(pks.raw_data());
        uint64_t base = (((uint64_t)rssid) << 32) + rowid_start;
        uint32_t n = idx_end - idx_begin;
        if (n >= PREFETCHN * 2) {
            FixSlice<S> prefetch_keys[PREFETCHN];
            size_t prefetch_hashes[PREFETCHN];
            for (uint32_t i = 0; i < PREFETCHN; i++) {
                prefetch_keys[i].assign(keys[idx_begin + i]);
                prefetch_hashes[i] = FixSliceHash<S>()(prefetch_keys[i]);
                _map.prefetch_hash(prefetch_hashes[i]);
            }
            for (uint32_t i = idx_begin; i < idx_end; i++) {
                uint32_t pslot = (i - idx_begin) % PREFETCHN;
                auto p = _map.find(prefetch_keys[pslot], prefetch_hashes[pslot]);
                if (p != _map.end() && (uint32_t)(p->second.value >> 32) == src_rssid[i]) {
                    // matched, can replace
                    p->second.value = base + i;
                } else {
                    // not match, mark failed
                    failed->push_back(rowid_start + i);
                }
                uint32_t prefetch_i = i + PREFETCHN;
                if (LIKELY(prefetch_i < idx_end)) {
                    prefetch_keys[pslot].assign(keys[prefetch_i]);
                    prefetch_hashes[pslot] = FixSliceHash<S>()(prefetch_keys[pslot]);
                    _map.prefetch_hash(prefetch_hashes[pslot]);
                }
            }
        } else {
            for (uint32_t i = idx_begin; i < idx_end; i++) {
                auto p = _map.find(FixSlice<S>(keys[i]));
                if (p != _map.end() && (uint32_t)(p->second.value >> 32) == src_rssid[i]) {
                    // matched, can replace
                    p->second.value = base + i;
                } else {
                    // not match, mark failed
                    failed->push_back(rowid_start + i);
                }
            }
        }
    }

    void try_replace(uint32_t rssid, uint32_t rowid_start, const vectorized::Column& pks, const uint32_t max_src_rssid,
                     uint32_t idx_begin, uint32_t idx_end, vector<uint32_t>* failed) override {
        const auto* keys = reinterpret_cast<const Slice*>(pks.raw_data());
        uint64_t base = (((uint64_t)rssid) << 32) + rowid_start;
        uint32_t n = idx_end - idx_begin;
        if (n >= PREFETCHN * 2) {
            FixSlice<S> prefetch_keys[PREFETCHN];
            size_t prefetch_hashes[PREFETCHN];
            for (uint32_t i = 0; i < PREFETCHN; i++) {
                prefetch_keys[i].assign(keys[idx_begin + i]);
                prefetch_hashes[i] = FixSliceHash<S>()(prefetch_keys[i]);
                _map.prefetch_hash(prefetch_hashes[i]);
            }
            for (uint32_t i = idx_begin; i < idx_end; i++) {
                uint32_t pslot = (i - idx_begin) % PREFETCHN;
                auto p = _map.find(prefetch_keys[pslot], prefetch_hashes[pslot]);
                if (p != _map.end() && (uint32_t)(p->second.value >> 32) <= max_src_rssid) {
                    // matched, can replace
                    p->second.value = base + i;
                } else {
                    // not match, mark failed
                    failed->push_back(rowid_start + i);
                }
                uint32_t prefetch_i = i + PREFETCHN;
                if (LIKELY(prefetch_i < idx_end)) {
                    prefetch_keys[pslot].assign(keys[prefetch_i]);
                    prefetch_hashes[pslot] = FixSliceHash<S>()(prefetch_keys[pslot]);
                    _map.prefetch_hash(prefetch_hashes[pslot]);
                }
            }
        } else {
            for (uint32_t i = idx_begin; i < idx_end; i++) {
                auto p = _map.find(FixSlice<S>(keys[i]));
                if (p != _map.end() && (uint32_t)(p->second.value >> 32) <= max_src_rssid) {
                    // matched, can replace
                    p->second.value = base + i;
                } else {
                    // not match, mark failed
                    failed->push_back(rowid_start + i);
                }
            }
        }
    }

    void erase(const vectorized::Column& pks, uint32_t idx_begin, uint32_t idx_end, DeletesMap* deletes) override {
        const auto* keys = reinterpret_cast<const Slice*>(pks.raw_data());
        uint32_t n = idx_end - idx_begin;
        if (n >= PREFETCHN * 2) {
            FixSlice<S> prefetch_keys[PREFETCHN];
            size_t prefetch_hashes[PREFETCHN];
            for (uint32_t i = 0; i < PREFETCHN; i++) {
                prefetch_keys[i].assign(keys[idx_begin + i]);
                prefetch_hashes[i] = FixSliceHash<S>()(prefetch_keys[i]);
                _map.prefetch_hash(prefetch_hashes[i]);
            }
            for (auto i = idx_begin; i < idx_end; i++) {
                uint32_t pslot = (i - idx_begin) % PREFETCHN;
                auto iter = _map.find(prefetch_keys[pslot], prefetch_hashes[pslot]);
                if (iter != _map.end()) {
                    uint64_t old = iter->second.value;
                    (*deletes)[(uint32_t)(old >> 32)].push_back((uint32_t)(old & ROWID_MASK));
                    _map.erase(iter);
                }
                uint32_t prefetch_i = i + PREFETCHN;
                if (LIKELY(prefetch_i < idx_end)) {
                    prefetch_keys[pslot].assign(keys[prefetch_i]);
                    prefetch_hashes[pslot] = FixSliceHash<S>()(prefetch_keys[pslot]);
                    _map.prefetch_hash(prefetch_hashes[pslot]);
                }
            }
        } else {
            for (auto i = idx_begin; i < idx_end; i++) {
                auto iter = _map.find(FixSlice<S>(keys[i]));
                if (iter != _map.end()) {
                    uint64_t old = iter->second.value;
                    (*deletes)[(uint32_t)(old >> 32)].push_back((uint32_t)(old & ROWID_MASK));
                    _map.erase(iter);
                }
            }
        }
    }

    void get(const vectorized::Column& pks, uint32_t idx_begin, uint32_t idx_end,
             std::vector<uint64_t>* rowids) override {
        const auto* keys = reinterpret_cast<const Slice*>(pks.raw_data());
        uint32_t n = idx_end - idx_begin;
        if (n >= PREFETCHN * 2) {
            FixSlice<S> prefetch_keys[PREFETCHN];
            size_t prefetch_hashes[PREFETCHN];
            for (uint32_t i = 0; i < PREFETCHN; i++) {
                prefetch_keys[i].assign(keys[idx_begin + i]);
                prefetch_hashes[i] = FixSliceHash<S>()(prefetch_keys[i]);
                _map.prefetch_hash(prefetch_hashes[i]);
            }
            for (auto i = idx_begin; i < idx_end; i++) {
                uint32_t pslot = (i - idx_begin) % PREFETCHN;
                auto iter = _map.find(prefetch_keys[pslot], prefetch_hashes[pslot]);
                if (iter != _map.end()) {
                    (*rowids)[i] = iter->second.value;
                } else {
                    (*rowids)[i] = -1;
                }
                uint32_t prefetch_i = i + PREFETCHN;
                if (LIKELY(prefetch_i < idx_end)) {
                    prefetch_keys[pslot].assign(keys[prefetch_i]);
                    prefetch_hashes[pslot] = FixSliceHash<S>()(prefetch_keys[pslot]);
                    _map.prefetch_hash(prefetch_hashes[pslot]);
                }
            }
        } else {
            for (auto i = idx_begin; i < idx_end; i++) {
                auto iter = _map.find(FixSlice<S>(keys[i]));
                if (iter != _map.end()) {
                    (*rowids)[i] = iter->second.value;
                } else {
                    (*rowids)[i] = -1;
                }
            }
        }
    }

    std::size_t memory_usage() const final { return _map.capacity() * (1 + S * 4 + sizeof(RowIdPack4)); }
};

struct StringHasher1 {
    size_t operator()(const string& v) const { return XXH3_64bits(v.data(), v.length()); }
};

class SliceHashIndex : public HashIndex {
private:
    using StringMap =
            phmap::parallel_flat_hash_map<string, tablet_rowid_t, StringHasher1, phmap::priv::hash_default_eq<string>,
                                          TraceAlloc<phmap::priv::Pair<const string, tablet_rowid_t>>, 4,
                                          phmap::NullMutex, true>;
    StringMap _map;
    size_t _total_length = 0;

public:
    SliceHashIndex() = default;
    ~SliceHashIndex() override = default;

    size_t size() const override { return _map.size(); }

    size_t capacity() const override { return _map.capacity(); }

    void reserve(size_t size) override { _map.reserve(size); }
    Status insert(uint32_t rssid, const vector<uint32_t>& rowids, const vectorized::Column& pks, uint32_t idx_begin,
                  uint32_t idx_end) override {
        auto* keys = reinterpret_cast<const Slice*>(pks.raw_data());
        DCHECK(idx_end <= rowids.size());
        uint64_t base = (((uint64_t)rssid) << 32);
        for (uint32_t i = idx_begin; i < idx_end; i++) {
            uint64_t v = base + rowids[i];
            auto p = _map.insert({keys[i].to_string(), v});
            if (!p.second) {
                uint64_t old = p.first->second;
                std::string msg = strings::Substitute(
                        "insert found duplicate key new(rssid=$0 rowid=$1) old(rssid=$2 rowid=$3) "
                        "key=$4 [$5]",
                        rssid, rowids[i], (uint32_t)(old >> 32), (uint32_t)(old & ROWID_MASK), keys[i].to_string(),
                        hexdump(keys[i].data, keys[i].size));
                LOG(ERROR) << msg;
                return Status::InternalError(msg);
            }
            _total_length += keys[i].size;
        }
        return Status::OK();
    }

    void upsert(uint32_t rssid, uint32_t rowid_start, const vectorized::Column& pks, uint32_t idx_begin,
                uint32_t idx_end, DeletesMap* deletes) override {
        auto* keys = reinterpret_cast<const Slice*>(pks.raw_data());
        uint64_t base = (((uint64_t)rssid) << 32) + rowid_start;
        for (uint32_t i = idx_begin; i < idx_end; i++) {
            uint64_t v = base + i;
            auto p = _map.insert({keys[i].to_string(), v});
            if (!p.second) {
                uint64_t old = p.first->second;
                if ((old >> 32) == rssid) {
                    LOG(ERROR) << "found duplicate in upsert data rssid:" << rssid << " key=" << keys[i].to_string()
                               << " [" << hexdump(keys[i].data, keys[i].size) << "]";
                }
                (*deletes)[(uint32_t)(old >> 32)].push_back((uint32_t)(old & ROWID_MASK));
                p.first->second = v;
            } else {
                _total_length += keys[i].size;
            }
        }
    }

    [[maybe_unused]] void try_replace(uint32_t rssid, uint32_t rowid_start, const vectorized::Column& pks,
                                      const vector<uint32_t>& src_rssid, uint32_t idx_begin, uint32_t idx_end,
                                      vector<uint32_t>* failed) override {
        auto* keys = reinterpret_cast<const Slice*>(pks.raw_data());
        uint64_t base = (((uint64_t)rssid) << 32) + rowid_start;
        for (uint32_t i = idx_begin; i < idx_end; i++) {
            auto p = _map.find(keys[i].to_string());
            if (p != _map.end() && (uint32_t)(p->second >> 32) == src_rssid[i]) {
                // matched, can replace
                p->second = base + i;
            } else {
                // not match, mark failed
                failed->push_back(rowid_start + i);
            }
        }
    }

    void try_replace(uint32_t rssid, uint32_t rowid_start, const vectorized::Column& pks, const uint32_t max_src_rssid,
                     uint32_t idx_begin, uint32_t idx_end, vector<uint32_t>* failed) override {
        auto* keys = reinterpret_cast<const Slice*>(pks.raw_data());
        uint64_t base = (((uint64_t)rssid) << 32) + rowid_start;
        for (uint32_t i = idx_begin; i < idx_end; i++) {
            auto p = _map.find(keys[i].to_string());
            if (p != _map.end() && (uint32_t)(p->second >> 32) <= max_src_rssid) {
                // matched, can replace
                p->second = base + i;
            } else {
                // not match, mark failed
                failed->push_back(rowid_start + i);
            }
        }
    }

    void erase(const vectorized::Column& pks, uint32_t idx_begin, uint32_t idx_end, DeletesMap* deletes) override {
        auto* keys = reinterpret_cast<const Slice*>(pks.raw_data());
        for (uint32_t i = idx_begin; i < idx_end; i++) {
            auto p = _map.find(keys[i].to_string());
            if (p != _map.end()) {
                uint64_t old = p->second;
                (*deletes)[(uint32_t)(old >> 32)].push_back((uint32_t)(old & ROWID_MASK));
                _map.erase(p);
                _total_length -= keys[i].size;
            }
        }
    }

    void get(const vectorized::Column& pks, uint32_t idx_begin, uint32_t idx_end,
             std::vector<uint64_t>* rowids) override {
        auto* keys = reinterpret_cast<const Slice*>(pks.raw_data());
        for (uint32_t i = idx_begin; i < idx_end; i++) {
            auto p = _map.find(keys[i].to_string());
            if (p != _map.end()) {
                (*rowids)[i] = p->second;
            } else {
                (*rowids)[i] = -1;
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
};

class ShardByLengthSliceHashIndex : public HashIndex {
private:
    constexpr static size_t max_fix_length = 40;
    std::unique_ptr<HashIndex> _maps[max_fix_length];

    HashIndex* get_index_by_length(size_t len) {
#define CASE_LEN(L)                                        \
    case L: {                                              \
        auto& p = _maps[L];                                \
        if (!p) {                                          \
            p.reset(new FixSliceHashIndex<(L + 3) / 4>()); \
        }                                                  \
        return p.get();                                    \
    }
        switch (len) {
            CASE_LEN(1)
            CASE_LEN(2)
            CASE_LEN(3)
            CASE_LEN(4)
            CASE_LEN(5)
            CASE_LEN(6)
            CASE_LEN(7)
            CASE_LEN(8)
            CASE_LEN(9)
            CASE_LEN(10)
            CASE_LEN(11)
            CASE_LEN(12)
            CASE_LEN(13)
            CASE_LEN(14)
            CASE_LEN(15)
            CASE_LEN(16)
            CASE_LEN(17)
            CASE_LEN(18)
            CASE_LEN(19)
            CASE_LEN(20)
            CASE_LEN(21)
            CASE_LEN(22)
            CASE_LEN(23)
            CASE_LEN(24)
            CASE_LEN(25)
            CASE_LEN(26)
            CASE_LEN(27)
            CASE_LEN(28)
            CASE_LEN(29)
            CASE_LEN(30)
            CASE_LEN(31)
            CASE_LEN(32)
            CASE_LEN(33)
            CASE_LEN(34)
            CASE_LEN(35)
            CASE_LEN(36)
            CASE_LEN(37)
            CASE_LEN(38)
            CASE_LEN(39)
        default: {
            auto& p = _maps[0];
            if (!p) p = std::make_unique<SliceHashIndex>();
            return p.get();
        }
        }
#undef CASE_LEN
    }

public:
    ShardByLengthSliceHashIndex() = default;
    ~ShardByLengthSliceHashIndex() override = default;

    size_t size() const override {
        size_t ret = 0;
        for (const auto& _map : _maps) {
            if (_map) {
                ret += _map->size();
            }
        }
        return ret;
    }

    size_t capacity() const override {
        size_t ret = 0;
        for (const auto& _map : _maps) {
            if (_map) {
                ret += _map->capacity();
            }
        }
        return ret;
    }

    void reserve(size_t size) override {
        // cannot do reserve for sharding map
    }

    Status insert(uint32_t rssid, const vector<uint32_t>& rowids, const vectorized::Column& pks, uint32_t idx_begin,
                  uint32_t idx_end) override {
        if (idx_begin < idx_end) {
            auto* keys = reinterpret_cast<const Slice*>(pks.raw_data());
            for (uint32_t i = idx_begin + 1; i < idx_end; i++) {
                if (keys[i].size != keys[idx_begin].size) {
                    RETURN_IF_ERROR(
                            get_index_by_length(keys[idx_begin].size)->insert(rssid, rowids, pks, idx_begin, i));
                    idx_begin = i;
                }
            }
            RETURN_IF_ERROR(get_index_by_length(keys[idx_begin].size)->insert(rssid, rowids, pks, idx_begin, idx_end));
        }
        return Status::OK();
    }

    void upsert(uint32_t rssid, uint32_t rowid_start, const vectorized::Column& pks, uint32_t idx_begin,
                uint32_t idx_end, DeletesMap* deletes) override {
        if (idx_begin < idx_end) {
            auto* keys = reinterpret_cast<const Slice*>(pks.raw_data());
            for (uint32_t i = idx_begin + 1; i < idx_end; i++) {
                if (keys[i].size != keys[idx_begin].size) {
                    get_index_by_length(keys[idx_begin].size)->upsert(rssid, rowid_start, pks, idx_begin, i, deletes);
                    idx_begin = i;
                }
            }
            get_index_by_length(keys[idx_begin].size)->upsert(rssid, rowid_start, pks, idx_begin, idx_end, deletes);
        }
    }

    [[maybe_unused]] void try_replace(uint32_t rssid, uint32_t rowid_start, const vectorized::Column& pks,
                                      const vector<uint32_t>& src_rssid, uint32_t idx_begin, uint32_t idx_end,
                                      vector<uint32_t>* failed) override {
        if (idx_begin < idx_end) {
            auto* keys = reinterpret_cast<const Slice*>(pks.raw_data());
            for (uint32_t i = idx_begin + 1; i < idx_end; i++) {
                if (keys[i].size != keys[idx_begin].size) {
                    get_index_by_length(keys[idx_begin].size)
                            ->try_replace(rssid, rowid_start, pks, src_rssid, idx_begin, i, failed);
                    idx_begin = i;
                }
            }
            get_index_by_length(keys[idx_begin].size)
                    ->try_replace(rssid, rowid_start, pks, src_rssid, idx_begin, idx_end, failed);
        }
    }

    void try_replace(uint32_t rssid, uint32_t rowid_start, const vectorized::Column& pks, const uint32_t max_src_rssid,
                     uint32_t idx_begin, uint32_t idx_end, vector<uint32_t>* failed) override {
        if (idx_begin < idx_end) {
            auto* keys = reinterpret_cast<const Slice*>(pks.raw_data());
            for (uint32_t i = idx_begin + 1; i < idx_end; i++) {
                if (keys[i].size != keys[idx_begin].size) {
                    get_index_by_length(keys[idx_begin].size)
                            ->try_replace(rssid, rowid_start, pks, max_src_rssid, idx_begin, i, failed);
                    idx_begin = i;
                }
            }
            get_index_by_length(keys[idx_begin].size)
                    ->try_replace(rssid, rowid_start, pks, max_src_rssid, idx_begin, idx_end, failed);
        }
    }

    void erase(const vectorized::Column& pks, uint32_t idx_begin, uint32_t idx_end, DeletesMap* deletes) override {
        if (idx_begin < idx_end) {
            auto* keys = reinterpret_cast<const Slice*>(pks.raw_data());
            for (uint32_t i = idx_begin + 1; i < idx_end; i++) {
                if (keys[i].size != keys[idx_begin].size) {
                    get_index_by_length(keys[idx_begin].size)->erase(pks, idx_begin, i, deletes);
                    idx_begin = i;
                }
            }
            get_index_by_length(keys[idx_begin].size)->erase(pks, idx_begin, idx_end, deletes);
        }
    }

    void get(const vectorized::Column& pks, uint32_t idx_begin, uint32_t idx_end,
             std::vector<uint64_t>* rowids) override {
        if (idx_begin < idx_end) {
            auto* keys = reinterpret_cast<const Slice*>(pks.raw_data());
            for (uint32_t i = idx_begin + 1; i < idx_end; i++) {
                if (keys[i].size != keys[idx_begin].size) {
                    get_index_by_length(keys[idx_begin].size)->get(pks, idx_begin, i, rowids);
                    idx_begin = i;
                }
            }
            get_index_by_length(keys[idx_begin].size)->get(pks, idx_begin, idx_end, rowids);
        }
    }

    std::size_t memory_usage() const final {
        size_t ret = 0;
        for (const auto& _map : _maps) {
            if (_map) {
                ret += _map->memory_usage();
            }
        }
        return ret;
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
        } else if (fix_size <= 40) {
            return std::make_unique<FixSliceHashIndex<10>>();
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
    case OLAP_FIELD_TYPE_CHAR:
        return std::make_unique<ShardByLengthSliceHashIndex>();
    case OLAP_FIELD_TYPE_VARCHAR:
        return std::make_unique<ShardByLengthSliceHashIndex>();
    case OLAP_FIELD_TYPE_DATE_V2:
        return std::make_unique<HashIndexImpl<int32_t>>();
    case OLAP_FIELD_TYPE_TIMESTAMP:
        return std::make_unique<HashIndexImpl<int64_t>>();
    default:
        return nullptr;
    }
#undef CASE_TYPE
}

PrimaryIndex::PrimaryIndex() = default;

PrimaryIndex::~PrimaryIndex() {
    if (_tablet_id != 0) {
        if (!_status.ok()) {
            LOG(WARNING) << "bad primary index released table:" << _table_id << " tablet:" << _tablet_id
                         << " memory: " << memory_usage();
        } else {
            LOG(INFO) << "primary index released table:" << _table_id << " tablet:" << _tablet_id
                      << " memory: " << memory_usage();
        }
    }
}

PrimaryIndex::PrimaryIndex(const vectorized::Schema& pk_schema) {
    _set_schema(pk_schema);
}

void PrimaryIndex::_set_schema(const vectorized::Schema& pk_schema) {
    _pk_schema = pk_schema;
    std::vector<ColumnId> sort_key_idxes(pk_schema.num_fields());
    for (ColumnId i = 0; i < pk_schema.num_fields(); ++i) {
        sort_key_idxes[i] = i;
    }
    _enc_pk_type = PrimaryKeyEncoder::encoded_primary_key_type(_pk_schema, sort_key_idxes);
    _key_size = PrimaryKeyEncoder::get_encoded_fixed_size(_pk_schema);
    _pkey_to_rssid_rowid = create_hash_index(_enc_pk_type, _key_size);
}

Status PrimaryIndex::load(Tablet* tablet) {
    auto scope = IOProfiler::scope(IOProfiler::TAG_PKINDEX, tablet->tablet_id());
    std::lock_guard<std::mutex> lg(_lock);
    if (_loaded) {
        return _status;
    }
    _status = _do_load(tablet);
    _loaded = true;
    if (!_status.ok()) {
        LOG(WARNING) << "load PrimaryIndex error: " << _status << " tablet:" << _tablet_id << " stack:\n"
                     << get_stack_trace();
        if (_status.is_mem_limit_exceeded()) {
            LOG(WARNING) << CurrentThread::mem_tracker()->debug_string();
        }
    }
    return _status;
}

void PrimaryIndex::unload() {
    std::lock_guard<std::mutex> lg(_lock);
    if (!_loaded) {
        return;
    }
    LOG(INFO) << "unload primary index tablet:" << _tablet_id << " size:" << size() << " capacity:" << capacity()
              << " memory: " << memory_usage();
    if (_pkey_to_rssid_rowid) {
        _pkey_to_rssid_rowid.reset();
    }
    if (_persistent_index) {
        _persistent_index.reset();
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

Status PrimaryIndex::prepare(const EditVersion& version, size_t n) {
    auto scope = IOProfiler::scope(IOProfiler::TAG_PKINDEX, _tablet_id);
    if (_persistent_index != nullptr) {
        return _persistent_index->prepare(version, n);
    }
    return Status::OK();
}

Status PrimaryIndex::commit(PersistentIndexMetaPB* index_meta) {
    auto scope = IOProfiler::scope(IOProfiler::TAG_PKINDEX, _tablet_id);
    if (_persistent_index != nullptr) {
        return _persistent_index->commit(index_meta);
    }
    return Status::OK();
}

Status PrimaryIndex::on_commited() {
    auto scope = IOProfiler::scope(IOProfiler::TAG_PKINDEX, _tablet_id);
    if (_persistent_index != nullptr) {
        return _persistent_index->on_commited();
    }
    return Status::OK();
}

Status PrimaryIndex::abort() {
    auto scope = IOProfiler::scope(IOProfiler::TAG_PKINDEX, _tablet_id);
    if (_persistent_index != nullptr) {
        return _persistent_index->abort();
    }
    return Status::OK();
}

Status PrimaryIndex::_do_load(Tablet* tablet) {
    _table_id = tablet->belonged_table_id();
    _tablet_id = tablet->tablet_id();
    auto span = Tracer::Instance().start_trace_tablet("primary_index_load", tablet->tablet_id());
    auto scoped_span = trace::Scope(span);
    MonotonicStopWatch timer;
    timer.start();

    const TabletSchema& tablet_schema = tablet->tablet_schema();
    vector<ColumnId> pk_columns(tablet_schema.num_key_columns());
    for (auto i = 0; i < tablet_schema.num_key_columns(); i++) {
        pk_columns[i] = (ColumnId)i;
    }
    auto pkey_schema = ChunkHelper::convert_schema_to_format_v2(tablet_schema, pk_columns);
    _set_schema(pkey_schema);

    // load persistent index if enable persistent index meta
    size_t fix_size = PrimaryKeyEncoder::get_encoded_fixed_size(_pk_schema);

    if (tablet->get_enable_persistent_index() && (fix_size <= 128)) {
        // TODO
        // PersistentIndex and tablet data are currently stored in the same directory
        // We may need to support the separation of PersistentIndex and Tablet data
        DCHECK(_persistent_index == nullptr);
        _persistent_index = std::make_unique<PersistentIndex>(tablet->schema_hash_path());
        return _persistent_index->load_from_tablet(tablet);
    }

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
        LOG(WARNING) << "load primary index get_rowsets_total_stats error: " << st;
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
        auto res = rowset->get_segment_iterators2(pkey_schema, tablet->data_dir()->get_meta(), apply_version, &stats);
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
                    vectorized::Column* pkc = nullptr;
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
    if (size() != total_rows - total_dels) {
        LOG(WARNING) << Substitute("load primary index row count not match tablet:$0 index:$1 != stats:$2", _tablet_id,
                                   size(), total_rows - total_dels);
    }
    LOG(INFO) << "load primary index finish table:" << tablet->belonged_table_id() << " tablet:" << tablet->tablet_id()
              << " version:" << apply_version << " #rowset:" << rowsets.size() << " #segment:" << total_segments
              << " data_size:" << total_data_size << " rowsets:" << int_list_to_string(rowset_ids) << " size:" << size()
              << " capacity:" << capacity() << " memory:" << memory_usage()
              << " duration: " << timer.elapsed_time() / 1000000 << "ms";
    span->SetAttribute("memory", memory_usage());
    span->SetAttribute("size", size());
    return Status::OK();
}

Status PrimaryIndex::_build_persistent_values(uint32_t rssid, uint32_t rowid_start, uint32_t idx_begin,
                                              uint32_t idx_end, std::vector<uint64_t>* values) const {
    uint64_t base = (((uint64_t)rssid) << 32) + rowid_start;
    for (uint32_t i = idx_begin; i < idx_end; i++) {
        values->emplace_back(base + i);
    }
    return Status::OK();
}

Status PrimaryIndex::_build_persistent_values(uint32_t rssid, const vector<uint32_t>& rowids, uint32_t idx_begin,
                                              uint32_t idx_end, std::vector<uint64_t>* values) const {
    DCHECK(idx_end <= rowids.size());
    uint64_t base = ((uint64_t)rssid) << 32;
    for (uint32_t i = idx_begin; i < idx_end; i++) {
        values->emplace_back(base + rowids[i]);
    }
    return Status::OK();
}

const Slice* PrimaryIndex::_build_persistent_keys(const vectorized::Column& pks, uint32_t idx_begin, uint32_t idx_end,
                                                  std::vector<Slice>* key_slices) const {
    if (pks.is_binary() || pks.is_large_binary()) {
        const Slice* vkeys = reinterpret_cast<const Slice*>(pks.raw_data());
        return vkeys + idx_begin;
    } else {
        CHECK(_key_size > 0);
        const uint8_t* keys = pks.raw_data() + idx_begin * _key_size;
        for (size_t i = idx_begin; i < idx_end; i++) {
            key_slices->emplace_back(keys, _key_size);
            keys += _key_size;
        }
        return reinterpret_cast<const Slice*>(key_slices->data());
    }
}

Status PrimaryIndex::_insert_into_persistent_index(uint32_t rssid, const vector<uint32_t>& rowids,
                                                   const vectorized::Column& pks) {
    std::vector<Slice> keys;
    std::vector<uint64_t> values;
    values.reserve(pks.size());
    _build_persistent_values(rssid, rowids, 0, pks.size(), &values);
    const Slice* vkeys = _build_persistent_keys(pks, 0, pks.size(), &keys);
    RETURN_IF_ERROR(_persistent_index->insert(pks.size(), vkeys, reinterpret_cast<IndexValue*>(values.data()), true));
    return Status::OK();
}

Status PrimaryIndex::_upsert_into_persistent_index(uint32_t rssid, uint32_t rowid_start, const vectorized::Column& pks,
                                                   uint32_t idx_begin, uint32_t idx_end, DeletesMap* deletes) {
    auto scope = IOProfiler::scope(IOProfiler::TAG_PKINDEX, _tablet_id);
    Status st;
    uint32_t n = idx_end - idx_begin;
    std::vector<Slice> keys;
    std::vector<uint64_t> values;
    values.reserve(n);
    std::vector<uint64_t> old_values(n, NullIndexValue);
    const Slice* vkeys = _build_persistent_keys(pks, idx_begin, idx_end, &keys);
    RETURN_IF_ERROR(_build_persistent_values(rssid, rowid_start, idx_begin, idx_end, &values));
    RETURN_IF_ERROR(_persistent_index->upsert(n, vkeys, reinterpret_cast<IndexValue*>(values.data()),
                                              reinterpret_cast<IndexValue*>(old_values.data())));
    for (unsigned long old : old_values) {
        if ((old != NullIndexValue) && (old >> 32) == rssid) {
            LOG(ERROR) << "found duplicate in upsert data rssid:" << rssid;
            st = Status::InternalError("found duplicate in upsert data");
        }
        if (old != NullIndexValue) {
            (*deletes)[(uint32_t)(old >> 32)].push_back((uint32_t)(old & ROWID_MASK));
        }
    }
    return st;
}

Status PrimaryIndex::_erase_persistent_index(const vectorized::Column& key_col, DeletesMap* deletes) {
    Status st;
    std::vector<Slice> keys;
    std::vector<uint64_t> old_values(key_col.size(), NullIndexValue);
    const Slice* vkeys = _build_persistent_keys(key_col, 0, key_col.size(), &keys);
    st = _persistent_index->erase(key_col.size(), vkeys, reinterpret_cast<IndexValue*>(old_values.data()));
    if (!st.ok()) {
        LOG(WARNING) << "erase persistent index failed";
    }
    for (unsigned long old : old_values) {
        if (old != NullIndexValue) {
            (*deletes)[(uint32_t)(old >> 32)].push_back((uint32_t)(old & ROWID_MASK));
        }
    }
    return st;
}

Status PrimaryIndex::_get_from_persistent_index(const vectorized::Column& key_col,
                                                std::vector<uint64_t>* rowids) const {
    std::vector<Slice> keys;
    const Slice* vkeys = _build_persistent_keys(key_col, 0, key_col.size(), &keys);
    Status st = _persistent_index->get(key_col.size(), vkeys, reinterpret_cast<IndexValue*>(rowids->data()));
    if (!st.ok()) {
        LOG(WARNING) << "failed get value from persistent index";
    }
    return st;
}

[[maybe_unused]] Status PrimaryIndex::_replace_persistent_index(uint32_t rssid, uint32_t rowid_start,
                                                                const vectorized::Column& pks,
                                                                const vector<uint32_t>& src_rssid,
                                                                vector<uint32_t>* deletes) {
    auto scope = IOProfiler::scope(IOProfiler::TAG_PKINDEX, _tablet_id);
    std::vector<Slice> keys;
    std::vector<uint64_t> values;
    values.reserve(pks.size());
    RETURN_IF_ERROR(_build_persistent_values(rssid, rowid_start, 0, pks.size(), &values));
    Status st = _persistent_index->try_replace(pks.size(), _build_persistent_keys(pks, 0, pks.size(), &keys),
                                               reinterpret_cast<IndexValue*>(values.data()), src_rssid, deletes);
    if (!st.ok()) {
        LOG(WARNING) << "try replace persistent index failed";
    }
    return st;
}

Status PrimaryIndex::_replace_persistent_index(uint32_t rssid, uint32_t rowid_start, const vectorized::Column& pks,
                                               const uint32_t max_src_rssid, vector<uint32_t>* deletes) {
    std::vector<Slice> keys;
    std::vector<uint64_t> values;
    values.reserve(pks.size());
    RETURN_IF_ERROR(_build_persistent_values(rssid, rowid_start, 0, pks.size(), &values));
    Status st = _persistent_index->try_replace(pks.size(), _build_persistent_keys(pks, 0, pks.size(), &keys),
                                               reinterpret_cast<IndexValue*>(values.data()), max_src_rssid, deletes);
    if (!st.ok()) {
        LOG(WARNING) << "try replace persistent index failed";
    }
    return st;
}

Status PrimaryIndex::insert(uint32_t rssid, const vector<uint32_t>& rowids, const vectorized::Column& pks) {
    DCHECK(_status.ok() && (_pkey_to_rssid_rowid || _persistent_index));
    if (_persistent_index != nullptr) {
        auto scope = IOProfiler::scope(IOProfiler::TAG_PKINDEX, _tablet_id);
        return _insert_into_persistent_index(rssid, rowids, pks);
    } else {
        return _pkey_to_rssid_rowid->insert(rssid, rowids, pks, 0, pks.size());
    }
}

Status PrimaryIndex::insert(uint32_t rssid, uint32_t rowid_start, const vectorized::Column& pks) {
    vector<uint32_t> rids(pks.size());
    for (int i = 0; i < rids.size(); i++) {
        rids[i] = rowid_start + i;
    }
    return insert(rssid, rids, pks);
}

Status PrimaryIndex::upsert(uint32_t rssid, uint32_t rowid_start, const vectorized::Column& pks, DeletesMap* deletes) {
    DCHECK(_status.ok() && (_pkey_to_rssid_rowid || _persistent_index));
    Status st;
    if (_persistent_index != nullptr) {
        st = _upsert_into_persistent_index(rssid, rowid_start, pks, 0, pks.size(), deletes);
    } else {
        _pkey_to_rssid_rowid->upsert(rssid, rowid_start, pks, 0, pks.size(), deletes);
    }
    return st;
}

Status PrimaryIndex::upsert(uint32_t rssid, uint32_t rowid_start, const vectorized::Column& pks, uint32_t idx_begin,
                            uint32_t idx_end, DeletesMap* deletes) {
    DCHECK(_status.ok() && (_pkey_to_rssid_rowid || _persistent_index));
    Status st;
    if (_persistent_index != nullptr) {
        st = _upsert_into_persistent_index(rssid, rowid_start, pks, idx_begin, idx_end, deletes);
    } else {
        _pkey_to_rssid_rowid->upsert(rssid, rowid_start, pks, idx_begin, idx_end, deletes);
    }
    return st;
}

[[maybe_unused]] Status PrimaryIndex::try_replace(uint32_t rssid, uint32_t rowid_start, const vectorized::Column& pks,
                                                  const vector<uint32_t>& src_rssid, vector<uint32_t>* deletes) {
    DCHECK(_status.ok() && (_pkey_to_rssid_rowid || _persistent_index));
    Status st;
    if (_persistent_index != nullptr) {
        st = _replace_persistent_index(rssid, rowid_start, pks, src_rssid, deletes);
    } else {
        _pkey_to_rssid_rowid->try_replace(rssid, rowid_start, pks, src_rssid, 0, pks.size(), deletes);
    }
    return st;
}

Status PrimaryIndex::try_replace(uint32_t rssid, uint32_t rowid_start, const vectorized::Column& pks,
                                 const uint32_t max_src_rssid, vector<uint32_t>* deletes) {
    DCHECK(_status.ok() && (_pkey_to_rssid_rowid || _persistent_index));
    Status st;
    if (_persistent_index != nullptr) {
        st = _replace_persistent_index(rssid, rowid_start, pks, max_src_rssid, deletes);
    } else {
        _pkey_to_rssid_rowid->try_replace(rssid, rowid_start, pks, max_src_rssid, 0, pks.size(), deletes);
    }
    return st;
}

Status PrimaryIndex::erase(const vectorized::Column& key_col, DeletesMap* deletes) {
    DCHECK(_status.ok() && (_pkey_to_rssid_rowid || _persistent_index));
    Status st;
    if (_persistent_index != nullptr) {
        auto scope = IOProfiler::scope(IOProfiler::TAG_PKINDEX, _tablet_id);
        st = _erase_persistent_index(key_col, deletes);
    } else {
        _pkey_to_rssid_rowid->erase(key_col, 0, key_col.size(), deletes);
    }
    return st;
}

Status PrimaryIndex::get(const vectorized::Column& key_col, std::vector<uint64_t>* rowids) const {
    DCHECK(_status.ok() && (_pkey_to_rssid_rowid || _persistent_index));
    Status st;
    if (_persistent_index != nullptr) {
        auto scope = IOProfiler::scope(IOProfiler::TAG_PKINDEX, _tablet_id);
        st = _get_from_persistent_index(key_col, rowids);
    } else {
        _pkey_to_rssid_rowid->get(key_col, 0, key_col.size(), rowids);
    }
    return st;
}

std::size_t PrimaryIndex::memory_usage() const {
    if (_persistent_index) {
        return _persistent_index->memory_usage();
    }
    return _pkey_to_rssid_rowid ? _pkey_to_rssid_rowid->memory_usage() : 0;
}

std::size_t PrimaryIndex::size() const {
    if (_persistent_index) {
        return _persistent_index->size();
    }
    return _pkey_to_rssid_rowid ? _pkey_to_rssid_rowid->size() : 0;
}

std::size_t PrimaryIndex::capacity() const {
    if (_persistent_index) {
        return _persistent_index->capacity();
    }
    return _pkey_to_rssid_rowid ? _pkey_to_rssid_rowid->capacity() : 0;
}

void PrimaryIndex::reserve(size_t s) {
    if (_pkey_to_rssid_rowid) {
        _pkey_to_rssid_rowid->reserve(s);
    }
}

std::string PrimaryIndex::to_string() const {
    return Substitute("PrimaryIndex tablet:$0", _tablet_id);
}

std::unique_ptr<PrimaryIndex> TEST_create_primary_index(const vectorized::Schema& pk_schema) {
    return std::make_unique<PrimaryIndex>(pk_schema);
}

double PrimaryIndex::get_write_amp_score() {
    if (_persistent_index != nullptr) {
        return _persistent_index->get_write_amp_score();
    } else {
        return 0.0;
    }
}

Status PrimaryIndex::major_compaction(Tablet* tablet) {
    if (_persistent_index != nullptr) {
        return _persistent_index->major_compaction(tablet);
    } else {
        return Status::OK();
    }
}

Status PrimaryIndex::reset(Tablet* tablet, EditVersion version, PersistentIndexMetaPB* index_meta) {
    std::lock_guard<std::mutex> lg(_lock);
    _table_id = tablet->belonged_table_id();
    _tablet_id = tablet->tablet_id();
    const TabletSchema& tablet_schema = tablet->tablet_schema();
    vector<ColumnId> pk_columns(tablet_schema.num_key_columns());
    for (auto i = 0; i < tablet_schema.num_key_columns(); i++) {
        pk_columns[i] = (ColumnId)i;
    }
    auto pkey_schema = ChunkHelper::convert_schema_to_format_v2(tablet_schema, pk_columns);
    _set_schema(pkey_schema);

    size_t fix_size = PrimaryKeyEncoder::get_encoded_fixed_size(_pk_schema);

    if (tablet->get_enable_persistent_index() && (fix_size <= 128)) {
        if (_persistent_index != nullptr) {
            _persistent_index.reset();
        }
        _persistent_index = std::make_unique<PersistentIndex>(tablet->schema_hash_path());
        RETURN_IF_ERROR(_persistent_index->reset(tablet, version, index_meta));
    } else {
        if (_pkey_to_rssid_rowid != nullptr) {
            _pkey_to_rssid_rowid.reset();
        }
        _pkey_to_rssid_rowid = create_hash_index(_enc_pk_type, _key_size);
    }
    _loaded = true;

    return Status::OK();
}

void PrimaryIndex::reset_cancel_major_compaction() {
    if (_persistent_index != nullptr) {
        _persistent_index->reset_cancel_major_compaction();
    }
}

} // namespace starrocks
