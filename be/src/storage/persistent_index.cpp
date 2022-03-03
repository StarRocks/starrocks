// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "storage/persistent_index.h"

#include <cstring>
#include <numeric>

#include "gutil/strings/substitute.h"
#include "storage/fs/fs_util.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "util/debug_util.h"
#include "util/defer_op.h"
#include "util/faststring.h"
#include "util/filesystem_util.h"
#include "util/murmur_hash3.h"
#include "util/raw_container.h"

namespace starrocks {

constexpr size_t default_usage_percent = 85;
constexpr size_t page_size = 4096;
constexpr size_t page_header_size = 64;
constexpr size_t bucket_per_page = 16;
constexpr size_t shard_max = 1 << 16;
constexpr size_t pack_size = 16;
constexpr size_t page_pack_limit = (page_size - page_header_size) / pack_size;
constexpr uint64_t seed0 = 12980785309524476958ULL;
constexpr uint64_t seed1 = 9110941936030554525ULL;
const size_t kSnapshotSize = 10 * 1024 * 1024;

using KVPairPtr = const uint8_t*;

template <class T, class P>
T npad(T v, P p) {
    return (v + p - 1) / p;
}

template <class T, class P>
T pad(T v, P p) {
    return npad(v, p) * p;
}

struct IndexHash {
    IndexHash(uint64_t hash) : hash(hash) {}
    uint64_t shard() const { return hash >> 48; }
    uint64_t page() const { return (hash >> 16) & 0xffffffff; }
    uint64_t bucket() const { return (hash >> 8) & (bucket_per_page - 1); }
    uint64_t tag() const { return hash & 0xff; }

    uint64_t hash;
};

MutableIndex::MutableIndex() {}

MutableIndex::~MutableIndex() {}

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
    uint64_t operator()(const FixedKey<KeySize>& k) const {
        uint64_t ret;
        murmur_hash3_x64_64(k.data, KeySize, seed0, &ret);
        return ret;
    }
};

static std::tuple<size_t, size_t> estimate_nshard_and_npage(size_t kv_size, size_t size, size_t usage_percent) {
    size_t usage = size * kv_size;
    size_t cap = usage * 100 / usage_percent;
    size_t nshard = 1;
    while (nshard * 1024 * 1024 < cap) {
        nshard *= 2;
        if (nshard == shard_max) {
            break;
        }
    }
    size_t npage = npad(cap / nshard, page_size);
    return {nshard, npage};
}

// Page storage layout:
//   each page has 4096 / 16 = 256 packs, ie
//   |--------       4096 byte page             -------|
//   |16b pack0|16b pack0| ... |16b pack254|16b pack255|
//   | header  |       data for buckets                |
// Header layout
//   |BucketInfo0|BucketInfo1|...|BucketInfo14|BucketInfo15|
// Bucket data layout
//   | tags (16byte aligned) | kv0,kv1..,kvn (16 byte aligned) |

struct alignas(4) BucketInfo {
    uint16_t pageid;
    // bucket position as pack id
    uint8_t packid;
    uint8_t size;
};

struct alignas(page_header_size) PageHeader {
    BucketInfo buckets[bucket_per_page];
};

struct alignas(page_size) IndexPage {
    uint8_t data[page_size];
    PageHeader& header() { return *reinterpret_cast<PageHeader*>(data); }
    uint8_t* pack(uint8_t packid) { return &data[packid * pack_size]; }
};

struct ImmutableIndexShard {
    ImmutableIndexShard(size_t npage) : pages(npage) {}

    size_t npage() const { return pages.size(); }

    IndexPage& page(uint32_t pageid) { return pages[pageid]; }

    PageHeader& header(uint32_t pageid) { return pages[pageid].header(); }

    BucketInfo& bucket(uint32_t pageid, uint32_t bucketid) { return pages[pageid].header().buckets[bucketid]; }

    uint8_t* pack(uint32_t pageid, uint32_t bucketid) {
        auto& info = bucket(pageid, bucketid);
        return pages[info.pageid].pack(info.packid);
    }

    Status write(fs::WritableBlock& wb) const;

    static StatusOr<std::unique_ptr<ImmutableIndexShard>> create(size_t kv_size, const std::vector<IndexHash>& hashes,
                                                                 const std::vector<KVPairPtr>& kv_ptrs,
                                                                 size_t npage_hint);
    vector<IndexPage> pages;
    size_t num_entry_moved = 0;
};

Status ImmutableIndexShard::write(fs::WritableBlock& wb) const {
    return wb.append(Slice((uint8_t*)pages.data(), page_size * pages.size()));
}

inline size_t num_pack_for_bucket(size_t kv_size, size_t num_kv) {
    return npad(num_kv, pack_size) + npad(kv_size * num_kv, pack_size);
}

struct BucketToMove {
    uint32_t npack = 0;
    uint32_t pageid = 0;
    uint32_t bucketid = 0;
    BucketToMove(uint32_t npack, uint32_t pageid, uint32_t bucketid)
            : npack(npack), pageid(pageid), bucketid(bucketid) {}
    bool operator<(const BucketToMove& rhs) const { return npack < rhs.npack; }
};

struct MoveDest {
    uint32_t npack = 0;
    uint32_t pageid = 0;
    MoveDest(uint32_t npack, uint32_t pageid) : npack(npack), pageid(pageid) {}
    bool operator<(const MoveDest& rhs) const { return npack < rhs.npack; }
};

static Status find_buckets_to_move(uint32_t pageid, size_t min_pack_to_move, const uint8_t* bucket_packs_in_page,
                                   std::vector<BucketToMove>* buckets_to_move) {
    std::vector<int> bucketid_ordered_by_packs(bucket_per_page);
    for (int i = 0; i < bucket_per_page; i++) {
        bucketid_ordered_by_packs[i] = i;
    }
    std::sort(bucketid_ordered_by_packs.begin(), bucketid_ordered_by_packs.end(),
              [&](const int& l, const int& r) -> bool { return bucket_packs_in_page[l] < bucket_packs_in_page[r]; });
    // try find solution of moving 1 bucket
    for (int oi = 0; oi < bucket_per_page; oi++) {
        int i = bucketid_ordered_by_packs[oi];
        auto bucket_packs = bucket_packs_in_page[i];
        if (bucket_packs >= min_pack_to_move) {
            buckets_to_move->emplace_back(bucket_packs, pageid, i);
            return Status::OK();
        }
    }
    // try find solution of moving 2 bucket
    std::array<int, 3> move2_min = {INT_MAX, -1, -1};
    for (int oi = 0; oi < bucket_per_page; oi++) {
        int i = bucketid_ordered_by_packs[oi];
        if (bucket_packs_in_page[i] == 0) {
            continue;
        }
        for (int oj = oi + 1; oj < bucket_per_page; oj++) {
            int j = bucketid_ordered_by_packs[oj];
            auto bucket_packs = bucket_packs_in_page[i] + bucket_packs_in_page[j];
            if (bucket_packs == min_pack_to_move) {
                buckets_to_move->emplace_back(bucket_packs_in_page[i], pageid, i);
                buckets_to_move->emplace_back(bucket_packs_in_page[j], pageid, j);
                return Status::OK();
            } else if (bucket_packs > min_pack_to_move && bucket_packs < move2_min[0]) {
                move2_min = {bucket_packs, i, j};
                break;
            }
        }
    }
    if (move2_min[2] >= 0) {
        for (int i = 1; i < 3; i++) {
            auto bucketid = move2_min[i];
            buckets_to_move->emplace_back(bucket_packs_in_page[bucketid], pageid, bucketid);
        }
        return Status::OK();
    }
    // try find solution of moving 3 bucket
    std::array<int, 4> move3_min = {INT_MAX, -1, -1, -1};
    for (int oi = 0; oi < bucket_per_page; oi++) {
        int i = bucketid_ordered_by_packs[oi];
        if (bucket_packs_in_page[i] == 0) {
            continue;
        }
        for (int oj = oi + 1; oj < bucket_per_page; oj++) {
            int j = bucketid_ordered_by_packs[oj];
            for (int ok = oj + 1; ok < bucket_per_page; ok++) {
                int k = bucketid_ordered_by_packs[ok];
                auto bucket_packs = bucket_packs_in_page[i] + bucket_packs_in_page[j] + bucket_packs_in_page[k];
                if (bucket_packs == min_pack_to_move) {
                    buckets_to_move->emplace_back(bucket_packs_in_page[i], pageid, i);
                    buckets_to_move->emplace_back(bucket_packs_in_page[j], pageid, j);
                    buckets_to_move->emplace_back(bucket_packs_in_page[k], pageid, k);
                    return Status::OK();
                } else if (bucket_packs > min_pack_to_move && bucket_packs < move3_min[0]) {
                    move3_min = {bucket_packs, i, j, k};
                    break;
                }
            }
        }
    }
    if (move3_min[2] >= 0) {
        for (int i = 1; i < 4; i++) {
            auto bucketid = move3_min[i];
            buckets_to_move->emplace_back(bucket_packs_in_page[bucketid], pageid, bucketid);
        }
        return Status::OK();
    }
    // TODO: current algorithm is sub-optimal, find buckets to move using DP
    return Status::InternalError("find_buckets_to_move");
}

struct BucketMovement {
    uint32_t src_pageid;
    uint32_t src_bucketid;
    uint32_t dest_pageid;
    BucketMovement(uint32_t src_pageid, uint32_t src_bucketid, uint32_t dest_pageid)
            : src_pageid(src_pageid), src_bucketid(src_bucketid), dest_pageid(dest_pageid) {}
};

static void remove_packs_from_dests(std::vector<MoveDest>& dests, int idx, int npack) {
    auto& d = dests[idx];
    d.npack -= npack;
    if (d.npack == 0) {
        dests.erase(dests.begin() + idx);
    } else {
        auto mv_start = std::upper_bound(dests.begin(), dests.begin() + idx, dests[idx]) - dests.begin();
        if (mv_start < idx) {
            MoveDest tmp = dests[idx];
            for (long cur = idx; cur > mv_start; cur--) {
                dests[cur] = dests[cur - 1];
            }
            dests[mv_start] = tmp;
        }
    }
}

static StatusOr<std::vector<BucketMovement>> move_buckets(std::vector<BucketToMove>& buckets_to_move,
                                                          std::vector<MoveDest>& dests) {
    std::vector<BucketMovement> ret;
    std::sort(buckets_to_move.begin(), buckets_to_move.end());
    std::sort(dests.begin(), dests.end());
    // move largest bucket first
    for (ssize_t i = buckets_to_move.size() - 1; i >= 0; i--) {
        auto& src = buckets_to_move[i];
        auto pos = std::lower_bound(dests.begin(), dests.end(), src.npack,
                                    [](const MoveDest& lhs, const uint32_t& rhs) { return lhs.npack < rhs; });
        if (pos == dests.end()) {
            return Status::InternalError("move_buckets failed");
        }
        auto idx = pos - dests.begin();
        auto& dest = dests[idx];
        ret.emplace_back(src.pageid, src.bucketid, dest.pageid);
        remove_packs_from_dests(dests, idx, src.npack);
    }
    return std::move(ret);
}

static void copy_kv_to_page(size_t kv_size, size_t num_kv, const KVPairPtr* kv_ptrs, const uint8_t* tags,
                            uint8_t* dest_pack) {
    uint8_t* tags_dest = dest_pack;
    size_t tags_len = pad(num_kv, pack_size);
    memcpy(tags_dest, tags, num_kv);
    memset(tags_dest + num_kv, 0, tags_len - num_kv);
    uint8_t* kvs_dest = dest_pack + tags_len;
    for (size_t i = 0; i < num_kv; i++) {
        memcpy(kvs_dest, kv_ptrs[i], kv_size);
        kvs_dest += kv_size;
    }
}

StatusOr<std::unique_ptr<ImmutableIndexShard>> ImmutableIndexShard::create(size_t kv_size,
                                                                           const std::vector<IndexHash>& hashes,
                                                                           const std::vector<KVPairPtr>& kv_ptrs,
                                                                           size_t npage_hint) {
    size_t npage = npage_hint;
    size_t bucket_size_max = std::min(256UL, (page_size - page_header_size) / (kv_size + 1));
    size_t nbucket = npage * bucket_per_page;
    std::vector<uint8_t> bucket_sizes(nbucket);
    std::vector<std::pair<std::vector<KVPairPtr>, std::vector<uint8_t>>> bucket_kv_ptrs_tags(nbucket);
    size_t estimated_entry_per_bucket = npad(hashes.size() * 100 / 85, nbucket);
    for (auto& p : bucket_kv_ptrs_tags) {
        p.first.reserve(estimated_entry_per_bucket);
        p.second.reserve(estimated_entry_per_bucket);
    }
    DCHECK(hashes.size() == kv_ptrs.size());
    for (size_t i = 0; i < hashes.size(); i++) {
        auto& h = hashes[i];
        auto page = h.page() % npage;
        auto bucket = h.bucket();
        auto bid = page * bucket_per_page + bucket;
        auto& sz = bucket_sizes[bid];
        if (sz == bucket_size_max) {
            // TODO: increase npage and retry
            return Status::InternalError("bucket size limit exceeded");
        }
        sz++;
        bucket_kv_ptrs_tags[bid].first.emplace_back(kv_ptrs[i]);
        bucket_kv_ptrs_tags[bid].second.emplace_back(h.tag());
    }
    std::vector<uint8_t> bucket_packs(nbucket);
    for (size_t i = 0; i < nbucket; i++) {
        auto npack = num_pack_for_bucket(kv_size, bucket_sizes[i]);
        if (npack >= page_pack_limit) {
            return Status::InternalError("page page limit exceeded");
        }
        bucket_packs[i] = npack;
    }
    // check over-limit pages and reassign some buckets in those pages to under-limit pages
    std::vector<BucketToMove> buckets_to_move;
    std::vector<MoveDest> dests;
    std::vector<bool> page_has_move(npage, false);
    for (uint32_t pageid = 0; pageid < npage; pageid++) {
        const uint8_t* bucket_packs_in_page = &bucket_packs[pageid * bucket_per_page];
        int npack = std::accumulate(bucket_packs_in_page, bucket_packs_in_page + bucket_per_page, 0);
        if (npack < page_pack_limit) {
            dests.emplace_back(page_pack_limit - npack, pageid);
        } else if (npack > page_pack_limit) {
            page_has_move[pageid] = true;
            RETURN_IF_ERROR(
                    find_buckets_to_move(pageid, npack - page_pack_limit, bucket_packs_in_page, &buckets_to_move));
        }
    }
    auto move_rs = move_buckets(buckets_to_move, dests);
    if (!move_rs.ok()) {
        return std::move(move_rs).status();
    }
    auto& moves = move_rs.value();
    auto bucket_moved = [&](uint32_t pageid, uint32_t bucketid) -> bool {
        for (auto& move : moves) {
            if (move.src_pageid == pageid && move.src_bucketid == bucketid) {
                return true;
            }
        }
        return false;
    };
    // calculate bucket positions
    std::unique_ptr<ImmutableIndexShard> ret = std::make_unique<ImmutableIndexShard>(npage);
    for (auto& move : moves) {
        ret->num_entry_moved += bucket_sizes[move.src_pageid * bucket_per_page + move.src_bucketid];
    }
    for (uint32_t pageid = 0; pageid < npage; pageid++) {
        IndexPage& page = ret->page(pageid);
        PageHeader& header = ret->header(pageid);
        size_t cur_packid = page_header_size / pack_size;
        for (uint32_t bucketid = 0; bucketid < bucket_per_page; bucketid++) {
            if (page_has_move[pageid] && bucket_moved(pageid, bucketid)) {
                continue;
            }
            auto bid = pageid * bucket_per_page + bucketid;
            auto& bucket_info = header.buckets[bucketid];
            bucket_info.pageid = pageid;
            bucket_info.packid = cur_packid;
            bucket_info.size = bucket_sizes[bid];
            copy_kv_to_page(kv_size, bucket_info.size, bucket_kv_ptrs_tags[bid].first.data(),
                            bucket_kv_ptrs_tags[bid].second.data(), page.pack(cur_packid));
            cur_packid += bucket_packs[bid];
            DCHECK(cur_packid <= page_size / pack_size);
        }
        for (auto& move : moves) {
            if (move.dest_pageid == pageid) {
                auto bid = move.src_pageid * bucket_per_page + move.src_bucketid;
                auto& bucket_info = ret->bucket(move.src_pageid, move.src_bucketid);
                bucket_info.pageid = pageid;
                bucket_info.packid = cur_packid;
                bucket_info.size = bucket_sizes[bid];
                copy_kv_to_page(kv_size, bucket_info.size, bucket_kv_ptrs_tags[bid].first.data(),
                                bucket_kv_ptrs_tags[bid].second.data(), page.pack(cur_packid));
                cur_packid += bucket_packs[bid];
                DCHECK(cur_packid <= page_size / pack_size);
            }
        }
    }
    return std::move(ret);
}

Status write_immutable_index(size_t kv_size, const std::vector<std::vector<IndexHash>>& hashes_by_shard,
                             const std::vector<std::vector<KVPairPtr>>& kv_ptrs_by_shard, size_t npage_hint,
                             const EditVersion& version, fs::WritableBlock& wb) {
    ImmutableIndexMetaPB meta;
    size_t total = 0;
    size_t total_moved = 0;
    size_t total_bytes = 0;
    for (size_t i = 0; i < hashes_by_shard.size(); i++) {
        auto rs_create = ImmutableIndexShard::create(kv_size, hashes_by_shard[i], kv_ptrs_by_shard[i], npage_hint);
        if (!rs_create.ok()) {
            return std::move(rs_create).status();
        }
        auto& shard = rs_create.value();
        size_t pos_before = wb.bytes_appended();
        RETURN_IF_ERROR(shard->write(wb));
        size_t pos_after = wb.bytes_appended();
        auto shard_meta = meta.add_shards();
        shard_meta->set_size(hashes_by_shard[i].size());
        shard_meta->set_npage(shard->npage());
        auto ptr_meta = shard_meta->mutable_data();
        ptr_meta->set_offset(pos_before);
        ptr_meta->set_size(pos_after - pos_before);
        total += hashes_by_shard[i].size();
        total_moved += shard->num_entry_moved;
        total_bytes += pos_after - pos_before;
    }
    LOG(INFO) << strings::Substitute(
            "write immutable index kv_size:$0 shard:$1 npage_hint:$2 #kv:$3 #moved:$4($5) bytes:$6 usage:$7", kv_size,
            hashes_by_shard.size(), npage_hint, total, total_moved, total_moved * 1000 / total / 1000.0, total_bytes,
            kv_size * total * 1000 / total_bytes / 1000.0);
    version.to_pb(meta.mutable_version());
    meta.set_size(total);
    std::string footer;
    if (!meta.SerializeToString(&footer)) {
        return Status::InternalError("ImmutableIndexMetaPB::SerializeToString failed");
    }
    put_fixed32_le(&footer, static_cast<uint32_t>(footer.size()));
    uint32_t checksum = crc32c::Value(footer.data(), footer.size());
    put_fixed32_le(&footer, checksum);
    return wb.append(Slice(footer));
}

template <size_t KeySize>
class FixedMutableIndex : public MutableIndex {
private:
    phmap::flat_hash_map<FixedKey<KeySize>, IndexValue, FixedKeyHash<KeySize>> _map;

public:
    FixedMutableIndex() {}
    ~FixedMutableIndex() override {}

    size_t size() const override { return _map.size(); }

    Status flush_to_immutable_index(size_t num_entry, const EditVersion& version,
                                    fs::WritableBlock& wb) const override {
        size_t kv_size = KeySize + sizeof(IndexValue);
        auto [nshard, npage_hint] = estimate_nshard_and_npage(kv_size, num_entry, default_usage_percent);
        std::vector<std::vector<IndexHash>> hashes_by_shard(nshard);
        std::vector<std::vector<KVPairPtr>> kv_ptrs_by_shard(nshard);
        size_t estimated_per_shard_entry = num_entry / nshard * 100 / default_usage_percent;
        for (size_t i = 0; i < nshard; i++) {
            hashes_by_shard[i].reserve(estimated_per_shard_entry);
            kv_ptrs_by_shard[i].reserve(estimated_per_shard_entry);
        }
        auto hasher = FixedKeyHash<KeySize>();
        size_t shard_mask = nshard - 1;
        for (const auto& e : _map) {
            if (e.second == NullIndexValue) {
                continue;
            }
            const auto& k = e.first;
            IndexHash h(hasher(k));
            auto shard = h.shard() & shard_mask;
            hashes_by_shard[shard].emplace_back(h);
            kv_ptrs_by_shard[shard].emplace_back((const KVPairPtr)&k);
        }
        return write_immutable_index(kv_size, hashes_by_shard, kv_ptrs_by_shard, npage_hint, version, wb);
    }

    Status get(size_t n, const void* keys, IndexValue* values, KeysInfo* not_found, size_t* num_found) const override {
        const FixedKey<KeySize>* fkeys = reinterpret_cast<const FixedKey<KeySize>*>(keys);
        size_t nfound = 0;
        for (size_t i = 0; i < n; i++) {
            const auto& key = fkeys[i];
            uint64_t hash = FixedKeyHash<KeySize>()(key);
            auto iter = _map.find(key, hash);
            if (iter == _map.end()) {
                values[i] = NullIndexValue;
                not_found->key_idxes.emplace_back((uint32_t)i);
                not_found->hashes.emplace_back(hash);
            } else {
                values[i] = iter->second;
                nfound += (iter->second != NullIndexValue);
            }
        }
        *num_found = nfound;
        return Status::OK();
    }

    Status upsert(size_t n, const void* keys, const IndexValue* values, IndexValue* old_values, KeysInfo* not_found,
                  size_t* num_found) override {
        const FixedKey<KeySize>* fkeys = reinterpret_cast<const FixedKey<KeySize>*>(keys);
        size_t nfound = 0;
        for (size_t i = 0; i < n; i++) {
            const auto& key = fkeys[i];
            const auto v = values[i];
            uint64_t hash = FixedKeyHash<KeySize>()(key);
            auto p = _map.emplace_with_hash(hash, key, v);
            if (p.second) {
                not_found->key_idxes.emplace_back((uint32_t)i);
                not_found->hashes.emplace_back(hash);
            } else {
                auto old_value = p.first->second;
                old_values[i] = old_value;
                nfound += (old_value != NullIndexValue);
                p.first->second = v;
            }
        }
        *num_found = nfound;
        return Status::OK();
    }

    Status upsert(size_t n, const void* keys, const IndexValue* values, KeysInfo* not_found, size_t* num_found) {
        const FixedKey<KeySize>* fkeys = reinterpret_cast<const FixedKey<KeySize>*>(keys);
        size_t nfound = 0;
        for (size_t i = 0; i < n; i++) {
            const auto& key = fkeys[i];
            const auto v = values[i];
            uint64_t hash = FixedKeyHash<KeySize>()(key);
            auto p = _map.emplace_with_hash(hash, key, v);
            if (p.second) {
                // key not exist previously
                not_found->key_idxes.emplace_back((uint32_t)i);
                not_found->hashes.emplace_back(hash);
            } else {
                // key exist
                auto old_value = p.first->second;
                nfound += (old_value != NullIndexValue);
                p.first->second = v;
            }
        }
        *num_found = nfound;
        return Status::OK();
    }

    Status load_wals(size_t n, const void* keys, const IndexValue* values) {
        const FixedKey<KeySize>* fkeys = reinterpret_cast<const FixedKey<KeySize>*>(keys);
        for (size_t i = 0; i < n; i++) {
            const auto& key = fkeys[i];
            const auto v = values[i];
            uint64_t hash = FixedKeyHash<KeySize>()(key);
            auto p = _map.emplace_with_hash(hash, key, v);
            // key exist
            if (!p.second) {
                p.first->second = v;
            }
        }
        return Status::OK();
    }

    Status insert(size_t n, const void* keys, const IndexValue* values) override {
        const FixedKey<KeySize>* fkeys = reinterpret_cast<const FixedKey<KeySize>*>(keys);
        for (size_t i = 0; i < n; i++) {
            const auto& key = fkeys[i];
            const auto v = values[i];
            uint64_t hash = FixedKeyHash<KeySize>()(key);
            auto p = _map.emplace_with_hash(hash, key, v);
            if (!p.second) {
                std::string msg = strings::Substitute("FixedMutableIndex<$0> insert found duplicate key $1", KeySize,
                                                      hexdump((const char*)key.data, KeySize));
                LOG(WARNING) << msg;
                return Status::InternalError(msg);
            }
        }
        return Status::OK();
    }

    Status erase(size_t n, const void* keys, IndexValue* old_values, KeysInfo* not_found, size_t* num_found) override {
        const FixedKey<KeySize>* fkeys = reinterpret_cast<const FixedKey<KeySize>*>(keys);
        size_t nfound = 0;
        for (size_t i = 0; i < n; i++) {
            const auto& key = fkeys[i];
            uint64_t hash = FixedKeyHash<KeySize>()(key);
            auto p = _map.emplace_with_hash(hash, key, NullIndexValue);
            if (p.second) {
                // key not exist previously
                old_values[i] = NullIndexValue;
                not_found->key_idxes.emplace_back((uint32_t)i);
                not_found->hashes.emplace_back(hash);
            } else {
                // key exist
                old_values[i] = p.first->second;
                nfound += (p.first->second != NullIndexValue);
                p.first->second = NullIndexValue;
            }
        }
        *num_found = nfound;
        return Status::OK();
    }

    size_t dump_bound() { return _map.dump_bound(); }

    bool dump(phmap::BinaryOutputArchive& ar_out) { return _map.dump(ar_out); }

    bool load_snapshot(phmap::BinaryInputArchive& ar_in) { return _map.load(ar_in); }

    size_t size() { return _map.size(); }
    size_t capacity() { return _map.capacity(); }
};

StatusOr<std::unique_ptr<MutableIndex>> MutableIndex::create(size_t key_size) {
    if (key_size == 0) {
        return Status::NotSupported("varlen key size IndexL0 not supported");
    }

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
        CASE_SIZE_8(1)
        CASE_SIZE_8(9)
        CASE_SIZE_8(17)
        CASE_SIZE_8(25)
        CASE_SIZE_8(33)
        CASE_SIZE_8(41)
        CASE_SIZE_8(49)
        CASE_SIZE_8(57)
    default:
        return Status::NotSupported("large key size IndexL0 not supported");
    }

#undef CASE_SIZE_8
#undef CASE_SIZE
}

Status ImmutableIndex::get(size_t n, const void* keys, const KeysInfo& keys_info, IndexValue* values,
                           size_t* num_found) const {
    *num_found = 0;
    return Status::OK();
}

Status ImmutableIndex::check_not_exist(size_t n, const void* keys) {
    return Status::OK();
}

PersistentIndex::PersistentIndex(const std::string& path) : _path(path) {}

PersistentIndex::~PersistentIndex() {
    if (_index_block) {
        _index_block->close();
    }
}

std::string PersistentIndex::_get_l0_index_file_name(std::string& dir, const EditVersion& version) {
    return strings::Substitute("$0/index.l0.$1.$2", dir, version.major(), version.minor());
}

Status PersistentIndex::create(size_t key_size, const EditVersion& version) {
    if (loaded()) {
        return Status::InternalError("PersistentIndex already loaded");
    }

    PersistentIndexMetaPB meta;
    meta.set_key_size(key_size);
    version.to_pb(meta.mutable_version());
    meta.set_size(0);
    meta.mutable_l0_meta();
    // TODO: write to index file
    _key_size = key_size;
    _size = 0;
    _version = version;
    auto st = MutableIndex::create(key_size);
    if (!st.ok()) {
        return st.status();
    }
    _l0 = std::move(st).value();
    return Status::OK();
}

Status PersistentIndex::load(const PersistentIndexMetaPB& index_meta) {
    size_t key_size = index_meta.key_size();
    _size = index_meta.size();
    DCHECK_EQ(key_size, _key_size);
    if (!index_meta.has_l0_meta()) {
        return Status::InternalError("invalid PersistentIndexMetaPB");
    }
    MutableIndexMetaPB l0_meta = index_meta.l0_meta();
    IndexSnapshotMetaPB snapshot_meta = l0_meta.snapshot();
    EditVersion start_version = snapshot_meta.version();
    fs::BlockManager* block_mgr = fs::fs_util::block_manager();
    PagePointerPB page_pb = snapshot_meta.data();
    size_t snapshot_off = page_pb.offset();
    size_t snapshot_size = page_pb.size();
    std::unique_ptr<fs::ReadableBlock> rblock;
    DeferOp close_block([&rblock] {
        if (rblock) {
            rblock->close();
        }
    });
    std::string l0_index_file_name = _get_l0_index_file_name(_path, start_version);
    RETURN_IF_ERROR(block_mgr->open_block(l0_index_file_name, &rblock));
    // Assuming that the snapshot is always at the beginning of index file,
    // if not, we can't call phmap.load() directly because phmap.load() alaways
    // reads the contents of the file from the beginning
    phmap::BinaryInputArchive ar_in(l0_index_file_name.data());
    if (snapshot_size > 0 && !_load_snapshot(ar_in)) {
        std::string err_msg = strings::Substitute("failed load snapshot from file $0", l0_index_file_name);
        LOG(WARNING) << err_msg;
        return Status::InternalError(err_msg);
    }
    // if mutable index is empty, set _offset as 0, otherwise set _offset as snapshot size
    _offset = snapshot_off + snapshot_size;
    int n = l0_meta.wals_size();
    // read wals and build l0
    for (int i = 0; i < n; i++) {
        const auto& wal_pb = l0_meta.wals(i);
        const auto& page_pb = wal_pb.data();
        size_t offset = page_pb.offset();
        size_t size = page_pb.size();
        _offset = offset + size;
        size_t kv_size = key_size + sizeof(IndexValue);
        size_t nums = size / kv_size;
        std::string buff;
        while (nums > 0) {
            size_t batch_num = (nums > 4096) ? 4096 : nums;
            raw::stl_string_resize_uninitialized(&buff, batch_num * kv_size);
            RETURN_IF_ERROR(rblock->read(offset, buff));
            uint8_t keys[key_size * batch_num];
            std::vector<IndexValue> values;
            values.reserve(batch_num);
            size_t buf_offset = 0;
            for (size_t j = 0; j < batch_num; ++j) {
                memcpy(keys + j * key_size, buff.data() + buf_offset, key_size);
                IndexValue val = UNALIGNED_LOAD64(buff.data() + buf_offset + key_size);
                values.emplace_back(val);
                buf_offset += kv_size;
            }
            RETURN_IF_ERROR(_l0->load_wals(batch_num, keys, values.data()));
            offset += batch_num * kv_size;
            nums -= batch_num;
        }
    }
    // the data in the end maybe invalid
    // so we need to truncate file first
    RETURN_IF_ERROR(FileSystemUtil::resize_file(l0_index_file_name, _offset));
    fs::CreateBlockOptions wblock_opts({l0_index_file_name});
    wblock_opts.mode = Env::MUST_EXIST;
    RETURN_IF_ERROR(block_mgr->create_block(wblock_opts, &_index_block));
    RETURN_IF_ERROR(_delete_expired_index_file(start_version));
    return Status::OK();
}

Status PersistentIndex::prepare(const EditVersion& version) {
    _dump_snapshot = false;
    _version = version;
    return Status::OK();
}

Status PersistentIndex::abort() {
    _dump_snapshot = false;
    return Status::NotSupported("TODO");
}

Status PersistentIndex::commit(PersistentIndexMetaPB* index_meta) {
    // TODO: l0 may be need to flush
    if (_dump_snapshot) {
        // if _map size is small enough to dump directly, rewrite snapshot
        std::string file_name = _get_l0_index_file_name(_path, _version);
        // be maybe crash after create index file during last commit
        // so we delete expired index file first to make sure no garbage left
        Env::Default()->delete_file(file_name);
        size_t snapshot_size = _dump_bound();
        phmap::BinaryOutputArchive ar_out(file_name.data());
        if (!_dump(ar_out)) {
            std::string err_msg = strings::Substitute("faile to dump snapshot to file $0", file_name);
            LOG(WARNING) << err_msg;
            return Status::InternalError(err_msg);
        }
        // update PersistentIndexMetaPB
        MutableIndexMetaPB* l0_meta = index_meta->mutable_l0_meta();
        l0_meta->clear_wals();
        IndexSnapshotMetaPB* snapshot = l0_meta->mutable_snapshot();
        _version.to_pb(snapshot->mutable_version());
        PagePointerPB* data = snapshot->mutable_data();
        data->set_offset(0);
        data->set_size(snapshot_size);
        _offset += snapshot_size;
        _page_size = 0;
    } else {
        MutableIndexMetaPB* l0_meta = index_meta->mutable_l0_meta();
        IndexWalMetaPB* wal_pb = l0_meta->add_wals();
        _version.to_pb(wal_pb->mutable_version());

        PagePointerPB* data = wal_pb->mutable_data();
        data->set_offset(_offset);
        data->set_size(_page_size);

        _version.to_pb(index_meta->mutable_version());
        index_meta->set_size(_size);

        _offset += _page_size;
        _page_size = 0;
    }
    return Status::OK();
}

Status PersistentIndex::on_commited() {
    if (_dump_snapshot) {
        std::string expired_file_path = _index_block->path();
        std::string index_file_path = _get_l0_index_file_name(_path, _version);
        fs::BlockManager* block_mgr = fs::fs_util::block_manager();
        std::unique_ptr<fs::WritableBlock> wblock;
        fs::CreateBlockOptions wblock_opts({index_file_path});
        // new index file should be created in commit() phase
        wblock_opts.mode = Env::MUST_EXIST;
        RETURN_IF_ERROR(block_mgr->create_block(wblock_opts, &wblock));
        _index_block = std::move(wblock);
        VLOG(1) << "delete expired l0 index file: " << expired_file_path;
        Env::Default()->delete_file(expired_file_path);
    }
    _dump_snapshot = false;

    return Status::OK();
}

Status PersistentIndex::get(size_t n, const void* keys, IndexValue* values) {
    KeysInfo l1_checks;
    size_t num_found = 0;
    RETURN_IF_ERROR(_l0->get(n, keys, values, &l1_checks, &num_found));
    if (_l1) {
        RETURN_IF_ERROR(_l1->get(n, keys, l1_checks, values, &num_found));
    }
    return Status::OK();
}

Status PersistentIndex::upsert(size_t n, const void* keys, const IndexValue* values, IndexValue* old_values) {
    KeysInfo l1_checks;
    size_t num_found = 0;
    RETURN_IF_ERROR(_l0->upsert(n, keys, values, old_values, &l1_checks, &num_found));
    _dump_snapshot |= _can_dump_directly();
    if (_l1) {
        RETURN_IF_ERROR(_l1->get(n, keys, l1_checks, old_values, &num_found));
    }
    _size += (n - num_found);
    if (!_dump_snapshot) {
        RETURN_IF_ERROR(_append_wal(n, keys, values));
    }
    return Status::OK();
}

Status PersistentIndex::insert(size_t n, const void* keys, const IndexValue* values, bool check_l1) {
    RETURN_IF_ERROR(_l0->insert(n, keys, values));
    if (_l1 && check_l1) {
        RETURN_IF_ERROR(_l1->check_not_exist(n, keys));
    }
    _dump_snapshot |= _can_dump_directly();
    _size += n;
    if (!_dump_snapshot) {
        RETURN_IF_ERROR(_append_wal(n, keys, values));
    }
    return Status::OK();
}

Status PersistentIndex::erase(size_t n, const void* keys, IndexValue* old_values) {
    KeysInfo l1_checks;
    size_t num_erased = 0;
    RETURN_IF_ERROR(_l0->erase(n, keys, old_values, &l1_checks, &num_erased));
    _dump_snapshot |= _can_dump_directly();
    if (_l1) {
        RETURN_IF_ERROR(_l1->get(n, keys, l1_checks, old_values, &num_erased));
    }
    CHECK(_size >= num_erased) << strings::Substitute("_size($0) < num_erased($1)", _size, num_erased);
    _size -= num_erased;
    if (!_dump_snapshot) {
        RETURN_IF_ERROR(_append_wal(n, keys, nullptr));
    }
    return Status::OK();
}

Status PersistentIndex::_append_wal(size_t n, const void* keys, const IndexValue* values) {
    const uint8_t* fkeys = reinterpret_cast<const uint8_t*>(keys);
    faststring fixed_buf;
    fixed_buf.reserve(n * (_key_size + sizeof(IndexValue)));
    for (size_t i = 0; i < n; i++) {
        const auto v = (values != nullptr) ? values[i] : NullIndexValue;
        fixed_buf.append(fkeys + i * _key_size, _key_size);
        put_fixed64_le(&fixed_buf, v);
    }
    RETURN_IF_ERROR(_index_block->append(fixed_buf));
    _page_size += fixed_buf.size();
    return Status::OK();
}

Status PersistentIndex::_flush_l0() {
    auto idx_file_path = strings::Substitute("$0/index.l1.$1.$2", _path, _version.major(), _version.minor());
    auto idx_file_path_tmp = idx_file_path + ".tmp";
    fs::BlockManager* block_mgr = fs::fs_util::block_manager();
    std::unique_ptr<fs::WritableBlock> wblock;
    fs::CreateBlockOptions wblock_opts({idx_file_path_tmp});
    RETURN_IF_ERROR(block_mgr->create_block(wblock_opts, &wblock));
    DeferOp remove_tmp_file([&] { Env::Default()->delete_file(idx_file_path_tmp); });
    RETURN_IF_ERROR(_l0->flush_to_immutable_index(_size, _version, *wblock));
    RETURN_IF_ERROR(Env::Default()->rename_file(idx_file_path_tmp, idx_file_path));
    return Status::OK();
}

size_t PersistentIndex::mutable_index_size() {
    return (_l0 == nullptr) ? 0 : _l0->size();
}

size_t PersistentIndex::mutable_index_capacity() {
    return (_l0 == nullptr) ? 0 : _l0->capacity();
}

size_t PersistentIndex::_dump_bound() {
    return (_l0 == nullptr) ? 0 : _l0->dump_bound();
}

bool PersistentIndex::_dump(phmap::BinaryOutputArchive& ar_out) {
    return (_l0 == nullptr) ? false : _l0->dump(ar_out);
}

// TODO: maybe build snapshot is better than append wals when almost
// operations are upsert or erase
bool PersistentIndex::_can_dump_directly() {
    return _dump_bound() <= kSnapshotSize;
}

bool PersistentIndex::_load_snapshot(phmap::BinaryInputArchive& ar) {
    return (_l0 == nullptr) ? false : _l0->load_snapshot(ar);
}

Status PersistentIndex::_delete_expired_index_file(const EditVersion& version) {
    std::string file_name = strings::Substitute("index.l0.$0.$1", version.major(), version.minor());
    std::string prefix("index.l0");
    std::string dir = _path;
    auto cb = [&](const char* name) -> bool {
        std::string full(name);
        if (full.compare(0, prefix.length(), prefix) == 0 && full.compare(file_name) != 0) {
            std::string path = dir + "/" + name;
            VLOG(1) << "delete expired index file " << path;
            Status st = Env::Default()->delete_file(path);
            if (!st.ok()) {
                LOG(WARNING) << "delete exprired index file: " << path << ", failed, status is " << st.to_string();
                return false;
            }
        }
        return true;
    };
    return Env::Default()->iterate_dir(_path, cb);
}

} // namespace starrocks
