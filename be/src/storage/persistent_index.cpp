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
constexpr size_t bucket_size_max = 256;
constexpr uint64_t seed0 = 12980785309524476958ULL;
constexpr uint64_t seed1 = 9110941936030554525ULL;
// perform l0 snapshot if l0_memory exceeds this value
constexpr size_t l0_snapshot_size_max = 4 * 1024 * 1024;
// perform l0 flush to l1 if l0_memory exceeds this value and l1 is null
constexpr size_t l0_flush_size_min = 8 * 1024 * 1024;
// perform l0 l1 merge compaction if l1_file_size / l0_memory >= this value and l0_memory > l0_snapshot_size_max
constexpr size_t l0_l1_merge_ratio = 10;

const char* const index_file_magic = "IDX1";

using KVPairPtr = const uint8_t*;

template <class T, class P>
T npad(T v, P p) {
    return (v + p - 1) / p;
}

template <class T, class P>
T pad(T v, P p) {
    return npad(v, p) * p;
}

uint32_t log2(size_t n) {
    uint32_t x = 0;
    while (n > 1) {
        n >>= 1;
        x++;
    }
    return x;
}

struct IndexHash {
    IndexHash() {}
    IndexHash(uint64_t hash) : hash(hash) {}
    uint64_t shard(uint32_t n) const { return (n == 0) ? 0 : hash >> (64 - n); }
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

uint64_t key_index_hash(const void* data, size_t len) {
    uint64_t ret;
    murmur_hash3_x64_64(data, len, seed0, &ret);
    return ret;
}

static std::tuple<size_t, size_t> estimate_nshard_and_npage(size_t kv_size, size_t size, size_t usage_percent) {
    // if size == 0, will return { nshard:1, npage:0 }, meaning an empty shard
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

    static StatusOr<std::unique_ptr<ImmutableIndexShard>> create(size_t kv_size, const std::vector<KVRef>& kv_refs,
                                                                 size_t npage_hint);

    vector<IndexPage> pages;
    size_t num_entry_moved = 0;
};

Status ImmutableIndexShard::write(fs::WritableBlock& wb) const {
    if (pages.size() > 0) {
        return wb.append(Slice((uint8_t*)pages.data(), page_size * pages.size()));
    } else {
        return Status::OK();
    }
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
                                                                           const std::vector<KVRef>& kv_refs,
                                                                           size_t npage_hint) {
    if (kv_refs.size() == 0) {
        return std::make_unique<ImmutableIndexShard>(0);
    }
    size_t npage = npage_hint;
    size_t bucket_size_limit = std::min(bucket_size_max, (page_size - page_header_size) / (kv_size + 1));
    size_t nbucket = npage * bucket_per_page;
    std::vector<uint8_t> bucket_sizes(nbucket);
    std::vector<std::pair<std::vector<KVPairPtr>, std::vector<uint8_t>>> bucket_kv_ptrs_tags(nbucket);
    size_t estimated_entry_per_bucket = npad(kv_refs.size() * 100 / 85, nbucket);
    for (auto& p : bucket_kv_ptrs_tags) {
        p.first.reserve(estimated_entry_per_bucket);
        p.second.reserve(estimated_entry_per_bucket);
    }
    for (size_t i = 0; i < kv_refs.size(); i++) {
        auto h = IndexHash(kv_refs[i].hash);
        auto page = h.page() % npage;
        auto bucket = h.bucket();
        auto bid = page * bucket_per_page + bucket;
        auto& sz = bucket_sizes[bid];
        if (sz == bucket_size_limit) {
            // TODO: increase npage and retry
            return Status::InternalError("bucket size limit exceeded");
        }
        sz++;
        bucket_kv_ptrs_tags[bid].first.emplace_back(kv_refs[i].kv_pos);
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

class ImmutableIndexWriter {
public:
    ~ImmutableIndexWriter() {
        if (_wb) {
            Env::Default()->delete_file(_idx_file_path_tmp);
        }
    }

    Status init(const string& dir, const EditVersion& version) {
        _version = version;
        _idx_file_path = strings::Substitute("$0/index.l1.$1.$2", dir, version.major(), version.minor());
        _idx_file_path_tmp = _idx_file_path + ".tmp";
        fs::BlockManager* block_mgr = fs::fs_util::block_manager();
        fs::CreateBlockOptions wblock_opts({_idx_file_path_tmp, Env::OpenMode::CREATE_OR_OPEN_WITH_TRUNCATE});
        return block_mgr->create_block(wblock_opts, &_wb);
    }

    Status write_shard(size_t key_size, size_t value_size, size_t npage_hint, const std::vector<KVRef>& kvs) {
        // only fixed length shards supported currently
        if (_nshard == 0) {
            _fixed_key_size = key_size;
            _fixed_value_size = value_size;
        } else {
            CHECK(key_size == _fixed_key_size && value_size == _fixed_value_size) << "key/value sizes not the same";
        }
        size_t kv_size = key_size + value_size;
        auto rs_create = ImmutableIndexShard::create(kv_size, kvs, npage_hint);
        if (!rs_create.ok()) {
            return std::move(rs_create).status();
        }
        auto& shard = rs_create.value();
        size_t pos_before = _wb->bytes_appended();
        RETURN_IF_ERROR(shard->write(*_wb));
        size_t pos_after = _wb->bytes_appended();
        auto shard_meta = _meta.add_shards();
        shard_meta->set_size(kvs.size());
        shard_meta->set_npage(shard->npage());
        auto ptr_meta = shard_meta->mutable_data();
        ptr_meta->set_offset(pos_before);
        ptr_meta->set_size(pos_after - pos_before);
        _total += kvs.size();
        _total_moved += shard->num_entry_moved;
        _total_kv_size += kvs.size() * kv_size;
        _total_bytes += pos_after - pos_before;
        _nshard++;
        return Status::OK();
    }

    Status finish() {
        LOG(INFO) << strings::Substitute(
                "finish writing immutable index $0 #shard:$1 #kv:$2 #moved:$3($4) bytes:$5 usage:$6",
                _idx_file_path_tmp, _nshard, _total, _total_moved, _total_moved * 1000 / std::max(_total, 1UL) / 1000.0,
                _total_bytes, _total_kv_size * 1000 / std::max(_total_bytes, 1UL) / 1000.0);
        _version.to_pb(_meta.mutable_version());
        _meta.set_size(_total);
        _meta.set_fixed_key_size(_fixed_key_size);
        _meta.set_fixed_value_size(_fixed_value_size);
        std::string footer;
        if (!_meta.SerializeToString(&footer)) {
            return Status::InternalError("ImmutableIndexMetaPB::SerializeToString failed");
        }
        put_fixed32_le(&footer, static_cast<uint32_t>(footer.size()));
        uint32_t checksum = crc32c::Value(footer.data(), footer.size());
        put_fixed32_le(&footer, checksum);
        footer.append(index_file_magic, 4);
        RETURN_IF_ERROR(_wb->append(Slice(footer)));
        RETURN_IF_ERROR(_wb->finalize());
        RETURN_IF_ERROR(_wb->close());
        RETURN_IF_ERROR(Env::Default()->rename_file(_idx_file_path_tmp, _idx_file_path));
        _wb.reset();
        return Status::OK();
    }

private:
    EditVersion _version;
    string _idx_file_path_tmp;
    string _idx_file_path;
    std::unique_ptr<fs::WritableBlock> _wb;
    size_t _nshard = 0;
    size_t _fixed_key_size = 0;
    size_t _fixed_value_size = 0;
    size_t _total = 0;
    size_t _total_moved = 0;
    size_t _total_kv_size = 0;
    size_t _total_bytes = 0;
    ImmutableIndexMetaPB _meta;
};

template <size_t KeySize>
class FixedMutableIndex : public MutableIndex {
private:
    phmap::flat_hash_map<FixedKey<KeySize>, IndexValue, FixedKeyHash<KeySize>> _map;

public:
    FixedMutableIndex() {}
    ~FixedMutableIndex() override {}

    size_t size() const override { return _map.size(); }

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

    size_t capacity() { return _map.capacity(); }

    std::vector<std::vector<KVRef>> get_kv_refs_by_shard(size_t nshard, size_t num_entry,
                                                         bool without_null) const override {
        std::vector<std::vector<KVRef>> ret(nshard);
        uint32_t pow = log2(nshard);
        for (size_t i = 0; i < nshard; i++) {
            ret[i].reserve(num_entry / nshard * 100 / 85);
        }
        auto hasher = FixedKeyHash<KeySize>();
        for (const auto& e : _map) {
            if (without_null && e.second == NullIndexValue) {
                continue;
            }
            const auto& k = e.first;
            IndexHash h(hasher(k));
            auto shard = h.shard(pow);
            ret[shard].emplace_back((uint8_t*)&(e.first), h.hash);
        }
        return ret;
    }

    Status flush_to_immutable_index(const std::string& dir, const EditVersion& version) const override {
        size_t value_size = sizeof(IndexValue);
        size_t kv_size = KeySize + value_size;
        auto [nshard, npage_hint] = estimate_nshard_and_npage(kv_size, size(), default_usage_percent);
        ImmutableIndexWriter writer;
        RETURN_IF_ERROR(writer.init(dir, version));
        if (nshard > 0) {
            auto kv_ref_by_shard = get_kv_refs_by_shard(nshard, size(), true);
            for (auto& kvs : kv_ref_by_shard) {
                RETURN_IF_ERROR(writer.write_shard(KeySize, value_size, npage_hint, kvs));
            }
        }
        return writer.finish();
    }
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

#ifdef __SSE2__

#include <emmintrin.h>

size_t get_matched_tag_idxes(const uint8_t* tags, size_t ntag, uint8_t tag, uint8_t* matched_idxes) {
    size_t nmatched = 0;
    auto tests = _mm_set1_epi8(tag);
    for (size_t i = 0; i < ntag; i += 16) {
        auto tags16 = _mm_load_si128((__m128i*)(tags + i));
        auto eqs = _mm_cmpeq_epi8(tags16, tests);
        auto mask = _mm_movemask_epi8(eqs);
        while (mask != 0) {
            uint32_t match_pos = __builtin_ctz(mask);
            if (i + match_pos < ntag) {
                matched_idxes[nmatched++] = i + match_pos;
            }
            mask &= (mask - 1);
        }
    }
    return nmatched;
}

#else

size_t get_matched_tag_idxes(const uint8_t* tags, size_t ntag, uint8_t tag, uint8_t* matched_idxes) {
    size_t nmatched = 0;
    for (size_t i = 0; i < ntag; i++) {
        if (tags[i] == tag) {
            matched_idxes[nmatched++] = i;
        }
    }
    return nmatched;
}

#endif

Status ImmutableIndex::_get_kvs_for_shard(std::vector<std::vector<KVRef>>& kvs_by_shard, size_t shard_idx, uint32_t pow,
                                          std::unique_ptr<ImmutableIndexShard>* shard) const {
    const auto& shard_info = _shards[shard_idx];
    if (shard_info.size == 0) {
        return Status::OK();
    }
    *shard = std::move(std::make_unique<ImmutableIndexShard>(shard_info.npage));
    RETURN_IF_ERROR(_rb->read(shard_info.offset, Slice((uint8_t*)(*shard)->pages.data(), shard_info.bytes)));
    for (uint32_t pageid = 0; pageid < shard_info.npage; pageid++) {
        auto& header = (*shard)->header(pageid);
        for (uint32_t bucketid = 0; bucketid < bucket_per_page; bucketid++) {
            auto& info = header.buckets[bucketid];
            const uint8_t* bucket_pos = (*shard)->pages[info.pageid].pack(info.packid);
            size_t nele = info.size;
            const uint8_t* kvs = bucket_pos + pad(nele, pack_size);
            for (size_t i = 0; i < nele; i++) {
                const uint8_t* kv = kvs + (_fixed_key_size + _fixed_value_size) * i;
                IndexHash hash = IndexHash(key_index_hash(kv, _fixed_key_size));
                kvs_by_shard[hash.shard(pow)].emplace_back(kv, hash.hash);
            }
        }
    }
    return Status::OK();
}

Status ImmutableIndex::_get_in_shard(size_t shard_idx, size_t n, const void* keys, const KeysInfo& keys_info,
                                     IndexValue* values, size_t* num_found) const {
    const auto& shard_info = _shards[shard_idx];
    if (shard_info.size == 0 || shard_info.npage == 0 || keys_info.size() == 0) {
        return Status::OK();
    }
    size_t found = 0;
    std::unique_ptr<ImmutableIndexShard> shard = std::make_unique<ImmutableIndexShard>(shard_info.npage);
    CHECK(shard->pages.size() * page_size == shard_info.bytes) << "illegal shard size";
    RETURN_IF_ERROR(_rb->read(shard_info.offset, Slice((uint8_t*)shard->pages.data(), shard_info.bytes)));
    uint8_t candidate_idxes[bucket_size_max];
    for (size_t i = 0; i < keys_info.size(); i++) {
        IndexHash h(keys_info.hashes[i]);
        auto pageid = h.page() % shard_info.npage;
        auto bucketid = h.bucket();
        auto& bucket_info = shard->bucket(pageid, bucketid);
        uint8_t* bucket_pos = shard->pages[bucket_info.pageid].pack(bucket_info.packid);
        auto nele = bucket_info.size;
        auto ncandidates = get_matched_tag_idxes(bucket_pos, nele, h.tag(), candidate_idxes);
        auto key_idx = keys_info.key_idxes[i];
        const uint8_t* fixed_key_probe = (const uint8_t*)keys + _fixed_key_size * key_idx;
        auto kv_pos = bucket_pos + pad(nele, pack_size);
        values[key_idx] = NullIndexValue;
        for (size_t candidate_idx = 0; candidate_idx < ncandidates; candidate_idx++) {
            auto idx = candidate_idxes[candidate_idx];
            auto candidate_kv = kv_pos + (_fixed_key_size + _fixed_value_size) * idx;
            if (strings::memeq(candidate_kv, fixed_key_probe, _fixed_key_size)) {
                values[key_idx] = UNALIGNED_LOAD64(candidate_kv + _fixed_key_size);
                found++;
                break;
            }
        }
    }
    *num_found += found;
    return Status::OK();
}

Status ImmutableIndex::_check_not_exist_in_shard(size_t shard_idx, size_t n, const void* keys,
                                                 const KeysInfo& keys_info) const {
    const auto& shard_info = _shards[shard_idx];
    if (shard_info.size == 0 || keys_info.size() == 0) {
        return Status::OK();
    }
    std::unique_ptr<ImmutableIndexShard> shard = std::make_unique<ImmutableIndexShard>(shard_info.npage);
    CHECK(shard->pages.size() * page_size == shard_info.bytes) << "illegal shard size";
    RETURN_IF_ERROR(_rb->read(shard_info.offset, Slice((uint8_t*)shard->pages.data(), shard_info.bytes)));
    uint8_t candidate_idxes[bucket_size_max];
    for (size_t i = 0; i < keys_info.size(); i++) {
        IndexHash h(keys_info.hashes[i]);
        auto pageid = h.page() % shard_info.npage;
        auto bucketid = h.bucket();
        auto& bucket_info = shard->bucket(pageid, bucketid);
        uint8_t* bucket_pos = shard->pages[bucket_info.pageid].pack(bucket_info.packid);
        auto nele = bucket_info.size;
        auto key_idx = keys_info.key_idxes[i];
        auto ncandidates = get_matched_tag_idxes(bucket_pos, nele, h.tag(), candidate_idxes);
        const uint8_t* fixed_key_probe = (const uint8_t*)keys + _fixed_key_size * key_idx;
        auto kv_pos = bucket_pos + pad(nele, pack_size);
        for (size_t candidate_idx = 0; candidate_idx < ncandidates; candidate_idx++) {
            auto idx = candidate_idxes[candidate_idx];
            auto candidate_kv = kv_pos + (_fixed_key_size + _fixed_value_size) * idx;
            if (strings::memeq(candidate_kv, fixed_key_probe, _fixed_key_size)) {
                return Status::AlreadyExist("key already exists in immutable index");
            }
        }
    }
    return Status::OK();
}

static void split_keys_info_by_shard(const KeysInfo& keys_info, std::vector<KeysInfo>& keys_info_by_shards) {
    uint32_t pow = log2(keys_info_by_shards.size());
    for (size_t i = 0; i < keys_info.key_idxes.size(); i++) {
        auto& key_idx = keys_info.key_idxes[i];
        auto& hash = keys_info.hashes[i];
        size_t shard = IndexHash(hash).shard(pow);
        keys_info_by_shards[shard].key_idxes.emplace_back(key_idx);
        keys_info_by_shards[shard].hashes.emplace_back(hash);
    }
}

Status ImmutableIndex::get(size_t n, const void* keys, const KeysInfo& keys_info, IndexValue* values,
                           size_t* num_found) const {
    size_t found = 0;
    if (_shards.size() > 1) {
        std::vector<KeysInfo> keys_info_by_shard(_shards.size());
        split_keys_info_by_shard(keys_info, keys_info_by_shard);
        for (size_t i = 0; i < _shards.size(); i++) {
            RETURN_IF_ERROR(_get_in_shard(i, n, keys, keys_info_by_shard[i], values, &found));
        }
    } else {
        RETURN_IF_ERROR(_get_in_shard(0, n, keys, keys_info, values, &found));
    }
    *num_found += found;
    return Status::OK();
}

Status ImmutableIndex::check_not_exist(size_t n, const void* keys) {
    size_t nshard = _shards.size();
    uint32_t pow = log2(nshard);
    std::vector<KeysInfo> keys_info_by_shard(nshard);
    for (size_t i = 0; i < n; i++) {
        const uint8_t* key = (const uint8_t*)keys + _fixed_key_size * i;
        IndexHash h(key_index_hash(key, _fixed_key_size));
        auto shard = h.shard(pow);
        keys_info_by_shard[shard].key_idxes.emplace_back(i);
        keys_info_by_shard[shard].hashes.emplace_back(h.hash);
    }
    for (size_t i = 0; i < nshard; i++) {
        RETURN_IF_ERROR(_check_not_exist_in_shard(i, n, keys, keys_info_by_shard[i]));
    }
    return Status::OK();
}

StatusOr<std::unique_ptr<ImmutableIndex>> ImmutableIndex::load(std::unique_ptr<fs::ReadableBlock>&& rb) {
    uint64_t file_size;
    RETURN_IF_ERROR(rb->size(&file_size));
    if (file_size < 12) {
        return Status::Corruption(strings::Substitute("Bad segment file $0: file size $1 < 12", rb->path(), file_size));
    }
    size_t footer_read_size = std::min<size_t>(4096, file_size);
    std::string buff;
    raw::stl_string_resize_uninitialized(&buff, footer_read_size);
    RETURN_IF_ERROR(rb->read(file_size - footer_read_size, buff));
    uint32_t footer_length = UNALIGNED_LOAD32(buff.data() + footer_read_size - 12);
    uint32_t checksum = UNALIGNED_LOAD32(buff.data() + footer_read_size - 8);
    uint32_t magic = UNALIGNED_LOAD32(buff.data() + footer_read_size - 4);
    if (magic != UNALIGNED_LOAD32(index_file_magic)) {
        return Status::Corruption(strings::Substitute("load immutable index failed $0 illegal magic", rb->path()));
    }
    std::string_view meta_str;
    if (footer_length <= footer_read_size - 12) {
        meta_str = std::string_view(buff.data() + footer_read_size - 12 - footer_length, footer_length + 4);
    } else {
        raw::stl_string_resize_uninitialized(&buff, footer_length + 4);
        RETURN_IF_ERROR(rb->read(file_size - 12 - footer_length, buff));
        meta_str = std::string_view(buff.data(), footer_length + 4);
    }
    auto actual_checksum = crc32c::Value(meta_str.data(), meta_str.size());
    if (checksum != actual_checksum) {
        return Status::Corruption(strings::Substitute("load immutable index failed $0 checksum not match", rb->path()));
    }
    ImmutableIndexMetaPB meta;
    if (!meta.ParseFromArray(meta_str.data(), meta_str.size() - 4)) {
        return Status::Corruption(
                strings::Substitute("load immutable index failed $0 parse meta pb failed", rb->path()));
    }
    std::unique_ptr<ImmutableIndex> idx = std::make_unique<ImmutableIndex>();
    idx->_version = EditVersion(meta.version());
    idx->_size = meta.size();
    idx->_fixed_key_size = meta.fixed_key_size();
    idx->_fixed_value_size = meta.fixed_value_size();
    size_t nshard = meta.shards_size();
    idx->_shards.resize(nshard);
    for (size_t i = 0; i < nshard; i++) {
        const auto& src = meta.shards(i);
        auto& dest = idx->_shards[i];
        dest.size = src.size();
        dest.npage = src.npage();
        dest.offset = src.data().offset();
        dest.bytes = src.data().size();
    }
    idx->_rb.swap(rb);
    return std::move(idx);
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
    std::unique_ptr<fs::ReadableBlock> l1_rblock;
    DeferOp close_block([&rblock, &l1_rblock] {
        if (rblock) {
            rblock->close();
        }
        if (l1_rblock) {
            l1_rblock->close();
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

    if (index_meta.has_l1_version()) {
        _l1_version = index_meta.l1_version();
        auto l1_block_path = strings::Substitute("$0/index.l1.$1.$2", _path, _l1_version.major(), _l1_version.minor());
        LOG(INFO) << "l1_block_path is " << l1_block_path;
        RETURN_IF_ERROR(block_mgr->open_block(l1_block_path, &l1_rblock));
        auto l1_st = ImmutableIndex::load(std::move(l1_rblock));
        if (!l1_st.ok()) {
            return l1_st.status();
        }
        _l1 = std::move(l1_st).value();
    }

    RETURN_IF_ERROR(_delete_expired_index_file(start_version, _l1_version));
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
    RETURN_IF_ERROR(_check_and_flush_l0());
    // if _dump_snapshot is true, _l0 size is small enough to rewrite snapshot
    // if _l0->size() is 0, _l0 may be flush to _l1 or merge with _l1
    if (_dump_snapshot || _l0->size() == 0) {
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
        index_meta->set_size(_size);
        MutableIndexMetaPB* l0_meta = index_meta->mutable_l0_meta();
        l0_meta->clear_wals();
        IndexSnapshotMetaPB* snapshot = l0_meta->mutable_snapshot();
        _version.to_pb(snapshot->mutable_version());
        PagePointerPB* data = snapshot->mutable_data();
        data->set_offset(0);
        data->set_size(snapshot_size);
        _offset += snapshot_size;
        _page_size = 0;
        // if _l0 size is 0, _l0 maybe fulsh to _l1 or merge with _l1
        // both case will rewrite _l1 index file, update l1_version in meta
        if (_l0->size() == 0) {
            _version.to_pb(index_meta->mutable_l1_version());
        }

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
    if (_dump_snapshot || _l0->size() == 0) {
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

    if (_l0->size() == 0) {
        std::string l1_file = strings::Substitute("$0/index.l1.$0.$1", _path, _l1_version.major(), _l1_version.minor());
        VLOG(1) << "delete expired l1 index file: " << l1_file;
        Env::Default()->delete_file(l1_file);
        _l1_version = _version;
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
    size_t value_size = sizeof(IndexValue);
    size_t kv_size = _key_size + value_size;
    auto [nshard, npage_hint] = estimate_nshard_and_npage(kv_size, _size, default_usage_percent);
    auto kv_ref_by_shard = _l0->get_kv_refs_by_shard(nshard, _size, true);
    ImmutableIndexWriter writer;
    RETURN_IF_ERROR(writer.init(_path, _version));
    for (auto& kvs : kv_ref_by_shard) {
        RETURN_IF_ERROR(writer.write_shard(_key_size, value_size, npage_hint, kvs));
    }
    return writer.finish();
}

Status PersistentIndex::_rebuild() {
    _offset = 0;
    _page_size = 0;
    auto l0_st = MutableIndex::create(_key_size);
    if (!l0_st.ok()) {
        return l0_st.status();
    }
    _l0 = std::move(l0_st).value();

    auto idx_file_path = strings::Substitute("$0/index.l1.$1.$2", _path, _version.major(), _version.minor());
    fs::BlockManager* block_mgr = fs::fs_util::block_manager();
    std::unique_ptr<fs::ReadableBlock> rblock;
    RETURN_IF_ERROR(block_mgr->open_block(idx_file_path, &rblock));
    auto l1_st = ImmutableIndex::load(std::move(rblock));
    if (!l1_st.ok()) {
        return l1_st.status();
    }
    _l1 = std::move(l1_st).value();
    return Status::OK();
}

// check _l0 should be flush or not, if not, return
// if _l0 should be flush, there are two conditions:
//   1. _l1 is not exist, _flush_l0 and build _l1
//   2. _l1 is exist, merge _l0 and _l1
// rebuild _l0 and _l1
// In addition, there may be io waste because we write wal(dump snapshot) first and
// do _flush_l0 or merge compaction.
Status PersistentIndex::_check_and_flush_l0() {
    size_t kv_size = _key_size + sizeof(IndexValue);
    size_t l0_mem_size = kv_size * _l0->size();
    uint64_t l1_file_size = 0;
    if (_l1 != nullptr) {
        _l1->file_size(&l1_file_size);
    }
    if (l0_mem_size <= l0_flush_size_min &&
        ((l0_mem_size <= l0_snapshot_size_max) || (l1_file_size / l0_mem_size > 10))) {
        return Status::OK();
    }
    // flush _l0
    if (_l1 == nullptr) {
        RETURN_IF_ERROR(_flush_l0());
    } else {
        RETURN_IF_ERROR(_merge_compaction());
    }
    // rebuild _l0 and _l1
    RETURN_IF_ERROR(_rebuild());
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
    return _dump_bound() <= l0_snapshot_size_max;
}

bool PersistentIndex::_load_snapshot(phmap::BinaryInputArchive& ar) {
    return (_l0 == nullptr) ? false : _l0->load_snapshot(ar);
}

Status PersistentIndex::_delete_expired_index_file(const EditVersion& l0_version, const EditVersion& l1_version) {
    std::string l0_file_name = strings::Substitute("index.l0.$0.$1", l0_version.major(), l0_version.minor());
    std::string l1_file_name = strings::Substitute("index.l1.$0.$1", l1_version.major(), l1_version.minor());
    std::string l0_prefix("index.l0");
    std::string l1_prefix("index.l1");
    std::string dir = _path;
    auto cb = [&](std::string_view name) -> bool {
        std::string full(name);
        if ((full.compare(0, l0_prefix.length(), l0_prefix) == 0 && full.compare(l0_file_name) != 0) ||
            (full.compare(0, l1_prefix.length(), l1_prefix) == 0 && full.compare(l1_file_name) != 0)) {
            std::string path = dir + "/" + full;
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

template <size_t KeySize>
struct KVRefEq {
    bool operator()(const KVRef& lhs, const KVRef& rhs) const {
        return lhs.hash == rhs.hash && memcmp(lhs.kv_pos, rhs.kv_pos, KeySize) == 0;
    }
};

struct KVRefHash {
    uint64_t operator()(const KVRef& kv) const { return kv.hash; }
};

template <size_t KeySize>
Status merge_shard_kvs_fixed_len(std::vector<KVRef>& l0_kvs, std::vector<KVRef>& l1_kvs, size_t estimated_size,
                                 std::vector<KVRef>& ret) {
    phmap::flat_hash_set<KVRef, KVRefHash, KVRefEq<KeySize>> kvs_set;
    kvs_set.reserve(estimated_size);
    for (auto& kv : l1_kvs) {
        auto rs = kvs_set.emplace(kv);
        DCHECK(rs.second) << "duplicate key found when in l1 index";
        if (!rs.second) {
            // duplicate key found, illegal
            return Status::InternalError("duplicate key found in l1 index");
        }
    }
    for (auto& kv : l0_kvs) {
        IndexValue v = UNALIGNED_LOAD64(kv.kv_pos + KeySize);
        if (v == NullIndexValue) {
            // delete
            kvs_set.erase(kv);
        } else {
            auto rs = kvs_set.emplace(kv);
            if (!rs.second) {
                DCHECK(rs.first->hash == kv.hash) << "upsert kv in set, hash should be the same";
                // TODO: find a way to modify iterator directly, currently just erase then re-insert
                // rs.first->kv_pos = kv.kv_pos;
                kvs_set.erase(rs.first);
                kvs_set.emplace(kv);
            }
        }
    }
    ret.reserve(ret.size() + kvs_set.size());
    for (auto& e : kvs_set) {
        ret.emplace_back(e);
    }
    return Status::OK();
}

static Status merge_shard_kvs(size_t key_size, std::vector<KVRef>& l0_kvs, std::vector<KVRef>& l1_kvs,
                              size_t estimated_size, std::vector<KVRef>& ret) {
    if (key_size == 0) {
        return Status::NotSupported("merge_shard: varlen key size not supported");
    }
#define CASE_SIZE(s) \
    case s:          \
        return merge_shard_kvs_fixed_len<s>(l0_kvs, l1_kvs, estimated_size, ret);

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

Status PersistentIndex::_merge_compaction() {
    if (!_l1) {
        return Status::InternalError("cannot do merge_compaction without l1");
    }
    ImmutableIndexWriter writer;
    RETURN_IF_ERROR(writer.init(_path, _version));
    size_t value_size = sizeof(IndexValue);
    size_t kv_size = _key_size + value_size;
    auto [nshard, npage_hint] = estimate_nshard_and_npage(kv_size, _size, default_usage_percent);
    size_t estimated_size_per_shard = _size / nshard;
    std::vector<std::vector<KVRef>> l0_kvs_by_shard = _l0->get_kv_refs_by_shard(nshard, _l0->size(), false);
    std::vector<std::vector<KVRef>> l1_kvs_by_shard(nshard);
    size_t nshard_l1 = _l1->_shards.size();
    size_t cur_shard_idx = 0;
    // shard iteration example:
    //
    // nshard_l1(4) < nshard(8):
    //         l1_shard_idex: 0     1     2     3
    //    num_shard_finished: 2     4     6     8
    //         cur_shard_idx: 0 1   2 3   4 5   6 7
    //
    // nshard_l1(4) = nshard(4):
    //         l1_shard_idex: 0     1     2     3
    //    num_shard_finished: 1     2     3     4
    //         cur_shard_idx: 0     1     2     3
    //
    // nshard_l1(8) > nshard(4):
    //         l1_shard_idex: 0  1  2  3  4  5  6  7
    //    num_shard_finished: 0  1  1  2  2  3  3  4
    //         cur_shard_idx:    0     1     2     3
    std::vector<std::unique_ptr<ImmutableIndexShard>> index_shards((nshard_l1 / nshard) + 1);
    size_t index_shards_idx = 0;
    uint32_t pow = log2(nshard);
    for (size_t l1_shard_idx = 0; l1_shard_idx < nshard_l1; l1_shard_idx++) {
        RETURN_IF_ERROR(_l1->_get_kvs_for_shard(l1_kvs_by_shard, l1_shard_idx, pow, &index_shards[index_shards_idx++]));
        size_t num_shard_finished = (l1_shard_idx + 1) * nshard / nshard_l1;
        std::vector<KVRef> kvs;
        if (cur_shard_idx < num_shard_finished) {
            while (cur_shard_idx < num_shard_finished) {
                kvs.clear();
                RETURN_IF_ERROR(merge_shard_kvs(_key_size, l0_kvs_by_shard[cur_shard_idx],
                                                l1_kvs_by_shard[cur_shard_idx], estimated_size_per_shard, kvs));
                RETURN_IF_ERROR(writer.write_shard(_key_size, value_size, npage_hint, kvs));
                // clear to optimize memory usage
                l0_kvs_by_shard[cur_shard_idx].clear();
                l0_kvs_by_shard[cur_shard_idx].shrink_to_fit();
                l1_kvs_by_shard[cur_shard_idx].clear();
                l1_kvs_by_shard[cur_shard_idx].shrink_to_fit();
                cur_shard_idx++;
            }
            index_shards_idx = 0;
        }
    }
    return writer.finish();
}

} // namespace starrocks
