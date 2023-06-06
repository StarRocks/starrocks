// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "storage/persistent_index.h"

#include <cstring>
#include <numeric>

#include "fs/fs.h"
#include "gutil/strings/substitute.h"
#include "storage/chunk_helper.h"
#include "storage/primary_key_encoder.h"
#include "storage/rowset/beta_rowset.h"
#include "storage/tablet.h"
#include "storage/tablet_meta_manager.h"
#include "storage/tablet_updates.h"
#include "util/bit_util.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "util/debug_util.h"
#include "util/defer_op.h"
#include "util/faststring.h"
#include "util/filesystem_util.h"
#include "util/raw_container.h"
#include "util/xxh3.h"

namespace starrocks {

<<<<<<< HEAD
constexpr size_t default_usage_percent = 85;
constexpr size_t page_size = 4096;
constexpr size_t page_header_size = 64;
constexpr size_t bucket_per_page = 16;
constexpr size_t shard_max = 1 << 16;
constexpr uint64_t page_max = 1ULL << 32;
constexpr size_t pack_size = 16;
constexpr size_t page_pack_limit = (page_size - page_header_size) / pack_size;
constexpr size_t bucket_size_max = 256;
constexpr uint64_t seed0 = 12980785309524476958ULL;
constexpr uint64_t seed1 = 9110941936030554525ULL;
// perform l0 snapshot if l0_memory exceeds this value
constexpr size_t l0_snapshot_size_max = 16 * 1024 * 1024;
// perform l0 l1 merge compaction if l1_file_size / l0_memory >= this value and l0_memory > l0_snapshot_size_max
constexpr size_t l0_l1_merge_ratio = 10;
=======
constexpr size_t kDefaultUsagePercent = 85;
constexpr size_t kPageSize = 4096;
constexpr size_t kPageHeaderSize = 64;
constexpr size_t kBucketHeaderSize = 4;
constexpr size_t kBucketPerPage = 16;
constexpr size_t kRecordPerBucket = 8;
constexpr size_t kShardMax = 1 << 16;
constexpr uint64_t kPageMax = 1ULL << 32;
constexpr size_t kPackSize = 16;
constexpr size_t kPagePackLimit = (kPageSize - kPageHeaderSize) / kPackSize;
constexpr size_t kBucketSizeMax = 256;
constexpr size_t kMinEnableBFKVNum = 10000000;
constexpr size_t kLongKeySize = 64;
constexpr size_t kMaxKeyLength = 128; // we only support key length is less than or equal to 128 bytes for now
>>>>>>> bdd4f0b9c ([Enhancement] Make l0 snapshot size configurable (#24748))

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

struct IndexHash {
    IndexHash() {}
    IndexHash(uint64_t hash) : hash(hash) {}
    uint64_t shard(uint32_t n) const { return (hash >> (63 - n)) >> 1; }
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
    uint64_t operator()(const FixedKey<KeySize>& k) const { return XXH3_64bits(k.data, KeySize); }
};

uint64_t key_index_hash(const void* data, size_t len) {
    return XXH3_64bits(data, len);
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

    Status write(WritableFile& wb) const;

    static StatusOr<std::unique_ptr<ImmutableIndexShard>> try_create(size_t kv_size, const std::vector<KVRef>& kv_refs,
                                                                     size_t npage_hint);

    static StatusOr<std::unique_ptr<ImmutableIndexShard>> create(size_t kv_size, const std::vector<KVRef>& kv_refs,
                                                                 size_t npage_hint);

    vector<IndexPage> pages;
    size_t num_entry_moved = 0;
};

Status ImmutableIndexShard::write(WritableFile& wb) const {
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

static std::vector<int8_t> get_move_buckets(size_t target, const uint8_t* bucket_packs_in_page) {
    vector<int8_t> idxes;
    idxes.reserve(bucket_per_page);
    int32_t total_buckets = 0;
    for (int8_t i = 0; i < bucket_per_page; i++) {
        if (bucket_packs_in_page[i] > 0) {
            idxes.push_back(i);
        }
        total_buckets += bucket_packs_in_page[i];
    }
    std::sort(idxes.begin(), idxes.end(),
              [&](int8_t lhs, int8_t rhs) { return bucket_packs_in_page[lhs] < bucket_packs_in_page[rhs]; });
    // store idx if this sum value uses bucket_packs_in_page[idx], or -1
    std::vector<int8_t> dp(total_buckets + 1, -1);
    dp[0] = bucket_per_page;           // assign an id that will never be used but >= 0
    int32_t valid_sum = total_buckets; // total_buckets is already a valid solution
    auto get_list_from_dp = [&] {
        vector<int8_t> ret;
        ret.reserve(16);
        while (valid_sum > 0) {
            ret.emplace_back(dp[valid_sum]);
            valid_sum -= bucket_packs_in_page[dp[valid_sum]];
        }
        return ret;
    };
    int32_t max_sum = 0; // current max sum
    for (int8_t idx = 0; idx < idxes.size(); idx++) {
        int8_t i = idxes[idx];
        for (int32_t v = 0; v <= max_sum; v++) {
            if (dp[v] < 0 || dp[v] == i) {
                continue;
            }
            int32_t nv = v + bucket_packs_in_page[i];
            if (dp[nv] >= 0) {
                continue;
            }
            dp[nv] = i;
            if (nv > max_sum) {
                max_sum = nv;
            }
            if (nv >= target) {
                valid_sum = std::min(valid_sum, nv);
                if (valid_sum == target) {
                    return get_list_from_dp();
                }
            }
        }
    }
    return get_list_from_dp();
}

static Status find_buckets_to_move(uint32_t pageid, size_t min_pack_to_move, const uint8_t* bucket_packs_in_page,
                                   std::vector<BucketToMove>* buckets_to_move) {
    auto ret = get_move_buckets(min_pack_to_move, bucket_packs_in_page);

    size_t move_packs = 0;
    for (int32_t i = 0; i < ret.size(); ++i) {
        buckets_to_move->emplace_back(bucket_packs_in_page[ret[i]], pageid, ret[i]);
        move_packs += bucket_packs_in_page[ret[i]];
    }
    DCHECK(move_packs >= min_pack_to_move);

    return Status::OK();
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
    for (size_t npage = npage_hint; npage < page_max; npage++) {
        auto rs_create = ImmutableIndexShard::try_create(kv_size, kv_refs, npage);
        // increase npage and retry
        if (!rs_create.ok()) {
            continue;
        }
        return std::move(rs_create.value());
    }
    return Status::InternalError("failed to create immutable index shard");
}

StatusOr<std::unique_ptr<ImmutableIndexShard>> ImmutableIndexShard::try_create(size_t kv_size,
                                                                               const std::vector<KVRef>& kv_refs,
                                                                               size_t npage) {
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
            FileSystem::Default()->delete_file(_idx_file_path_tmp);
        }
    }

    Status init(const string& dir, const EditVersion& version) {
        _version = version;
        _idx_file_path = strings::Substitute("$0/index.l1.$1.$2", dir, version.major(), version.minor());
        _idx_file_path_tmp = _idx_file_path + ".tmp";
        ASSIGN_OR_RETURN(_fs, FileSystem::CreateSharedFromString(_idx_file_path_tmp));
        WritableFileOptions wblock_opts{.sync_on_close = true, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
        ASSIGN_OR_RETURN(_wb, _fs->new_writable_file(wblock_opts, _idx_file_path_tmp));
        return Status::OK();
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
        size_t pos_before = _wb->size();
        RETURN_IF_ERROR(shard->write(*_wb));
        size_t pos_after = _wb->size();
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
        _meta.set_format_version(PERSISTENT_INDEX_VERSION_1);
        std::string footer;
        if (!_meta.SerializeToString(&footer)) {
            return Status::InternalError("ImmutableIndexMetaPB::SerializeToString failed");
        }
        put_fixed32_le(&footer, static_cast<uint32_t>(footer.size()));
        uint32_t checksum = crc32c::Value(footer.data(), footer.size());
        put_fixed32_le(&footer, checksum);
        footer.append(index_file_magic, 4);
        RETURN_IF_ERROR(_wb->append(Slice(footer)));
        RETURN_IF_ERROR(_wb->close());
        RETURN_IF_ERROR(FileSystem::Default()->rename_file(_idx_file_path_tmp, _idx_file_path));
        _wb.reset();
        return Status::OK();
    }

private:
    EditVersion _version;
    string _idx_file_path_tmp;
    string _idx_file_path;
    std::shared_ptr<FileSystem> _fs;
    std::unique_ptr<WritableFile> _wb;
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
                nfound += (iter->second.get_value() != NullIndexValue);
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
                nfound += (old_value.get_value() != NullIndexValue);
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
                nfound += (old_value.get_value() != NullIndexValue);
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
            auto p = _map.emplace_with_hash(hash, key, IndexValue(NullIndexValue));
            if (p.second) {
                // key not exist previously
                old_values[i] = NullIndexValue;
                not_found->key_idxes.emplace_back((uint32_t)i);
                not_found->hashes.emplace_back(hash);
            } else {
                // key exist
                old_values[i] = p.first->second;
                nfound += (p.first->second.get_value() != NullIndexValue);
                p.first->second = NullIndexValue;
            }
        }
        *num_found = nfound;
        return Status::OK();
    }

    Status replace(const void* keys, const IndexValue* values, const std::vector<size_t>& replace_idxes) {
        const FixedKey<KeySize>* fkeys = reinterpret_cast<const FixedKey<KeySize>*>(keys);
        for (size_t i = 0; i < replace_idxes.size(); ++i) {
            const auto& key = fkeys[replace_idxes[i]];
            const auto v = values[replace_idxes[i]];
            uint64_t hash = FixedKeyHash<KeySize>()(key);
            auto p = _map.emplace_with_hash(hash, key, v);
            if (!p.second) {
                p.first->second = v;
            }
        }
        return Status::OK();
    }

    // return the dump file size if dump _map into a new file
    // If _map is empty, _map.dump_bound() will  set empty hash set serialize_size larger
    // than sizeof(uint64_t) in order to improve count distinct streaming aggregate performance.
    // Howevevr, the real snapshot file will only wite a size_(type is size_t) into file. So we
    // will use `sizeof(size_t)` as return value.
    size_t dump_bound() { return _map.empty() ? sizeof(size_t) : _map.dump_bound(); }

    bool dump(phmap::BinaryOutputArchive& ar_out) { return _map.dump(ar_out); }

    bool load_snapshot(phmap::BinaryInputArchive& ar_in) { return _map.load(ar_in); }

    size_t capacity() { return _map.capacity(); }

    void reserve(size_t size) { _map.reserve(size); }

    size_t memory_usage() { return _map.capacity() * (1 + (KeySize + 3) / 4 * 4 + sizeof(IndexValue)); }

    std::vector<std::vector<KVRef>> get_kv_refs_by_shard(size_t nshard, size_t num_entry,
                                                         bool without_null) const override {
        std::vector<std::vector<KVRef>> ret(nshard);
        uint32_t shard_bits = log2(nshard);
        for (size_t i = 0; i < nshard; i++) {
            ret[i].reserve(num_entry / nshard * 100 / 85);
        }
        auto hasher = FixedKeyHash<KeySize>();
        for (const auto& e : _map) {
            if (without_null && e.second.get_value() == NullIndexValue) {
                continue;
            }
            const auto& k = e.first;
            IndexHash h(hasher(k));
            auto shard = h.shard(shard_bits);
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

Status ImmutableIndex::_get_kvs_for_shard(std::vector<std::vector<KVRef>>& kvs_by_shard, size_t shard_idx,
                                          uint32_t shard_bits, std::unique_ptr<ImmutableIndexShard>* shard) const {
    const auto& shard_info = _shards[shard_idx];
    if (shard_info.size == 0) {
        return Status::OK();
    }
    *shard = std::move(std::make_unique<ImmutableIndexShard>(shard_info.npage));
    RETURN_IF_ERROR(_file->read_at_fully(shard_info.offset, (*shard)->pages.data(), shard_info.bytes));
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
                kvs_by_shard[hash.shard(shard_bits)].emplace_back(kv, hash.hash);
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
    RETURN_IF_ERROR(_file->read_at_fully(shard_info.offset, shard->pages.data(), shard_info.bytes));
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
    RETURN_IF_ERROR(_file->read_at_fully(shard_info.offset, shard->pages.data(), shard_info.bytes));
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
    uint32_t shard_bits = log2(keys_info_by_shards.size());
    for (size_t i = 0; i < keys_info.key_idxes.size(); i++) {
        auto& key_idx = keys_info.key_idxes[i];
        auto& hash = keys_info.hashes[i];
        size_t shard = IndexHash(hash).shard(shard_bits);
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
    uint32_t shard_bits = log2(nshard);
    std::vector<KeysInfo> keys_info_by_shard(nshard);
    for (size_t i = 0; i < n; i++) {
        const uint8_t* key = (const uint8_t*)keys + _fixed_key_size * i;
        IndexHash h(key_index_hash(key, _fixed_key_size));
        auto shard = h.shard(shard_bits);
        keys_info_by_shard[shard].key_idxes.emplace_back(i);
        keys_info_by_shard[shard].hashes.emplace_back(h.hash);
    }
    for (size_t i = 0; i < nshard; i++) {
        RETURN_IF_ERROR(_check_not_exist_in_shard(i, n, keys, keys_info_by_shard[i]));
    }
    return Status::OK();
}

StatusOr<std::unique_ptr<ImmutableIndex>> ImmutableIndex::load(std::unique_ptr<RandomAccessFile>&& file) {
    ASSIGN_OR_RETURN(auto file_size, file->get_size());
    if (file_size < 12) {
        return Status::Corruption(
                strings::Substitute("Bad segment file $0: file size $1 < 12", file->filename(), file_size));
    }
    size_t footer_read_size = std::min<size_t>(4096, file_size);
    std::string buff;
    raw::stl_string_resize_uninitialized(&buff, footer_read_size);
    RETURN_IF_ERROR(file->read_at_fully(file_size - footer_read_size, buff.data(), buff.size()));
    uint32_t footer_length = UNALIGNED_LOAD32(buff.data() + footer_read_size - 12);
    uint32_t checksum = UNALIGNED_LOAD32(buff.data() + footer_read_size - 8);
    uint32_t magic = UNALIGNED_LOAD32(buff.data() + footer_read_size - 4);
    if (magic != UNALIGNED_LOAD32(index_file_magic)) {
        return Status::Corruption(
                strings::Substitute("load immutable index failed $0 illegal magic", file->filename()));
    }
    std::string_view meta_str;
    if (footer_length <= footer_read_size - 12) {
        meta_str = std::string_view(buff.data() + footer_read_size - 12 - footer_length, footer_length + 4);
    } else {
        raw::stl_string_resize_uninitialized(&buff, footer_length + 4);
        RETURN_IF_ERROR(file->read_at_fully(file_size - 12 - footer_length, buff.data(), buff.size()));
        meta_str = std::string_view(buff.data(), footer_length + 4);
    }
    auto actual_checksum = crc32c::Value(meta_str.data(), meta_str.size());
    if (checksum != actual_checksum) {
        return Status::Corruption(
                strings::Substitute("load immutable index failed $0 checksum not match", file->filename()));
    }
    ImmutableIndexMetaPB meta;
    if (!meta.ParseFromArray(meta_str.data(), meta_str.size() - 4)) {
        return Status::Corruption(
                strings::Substitute("load immutable index failed $0 parse meta pb failed", file->filename()));
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
    idx->_file.swap(file);
    return std::move(idx);
}

PersistentIndex::PersistentIndex(const std::string& path) : _path(path) {}

PersistentIndex::~PersistentIndex() {
    if (_index_file) {
        _index_file->close();
    }
    if (_l1) {
        _l1->clear();
    }
}

std::string PersistentIndex::_get_l0_index_file_name(std::string& dir, const EditVersion& version) {
    return strings::Substitute("$0/index.l0.$1.$2", dir, version.major(), version.minor());
}

// Create a new empty PersistentIndex
Status PersistentIndex::create(size_t key_size, const EditVersion& version) {
    if (loaded()) {
        return Status::InternalError("PersistentIndex already loaded");
    }

    _key_size = key_size;
    _size = 0;
    _version = version;
    _offset = 0;
    _page_size = 0;
    auto st = MutableIndex::create(key_size);
    if (!st.ok()) {
        return st.status();
    }
    _l0 = std::move(st).value();
    ASSIGN_OR_RETURN(_fs, FileSystem::CreateSharedFromString(_path));
    std::string file_name = _get_l0_index_file_name(_path, version);
    WritableFileOptions wblock_opts;
    wblock_opts.mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE;
    ASSIGN_OR_RETURN(_index_file, _fs->new_writable_file(wblock_opts, file_name));
    return Status::OK();
}

Status PersistentIndex::load(const PersistentIndexMetaPB& index_meta) {
    _key_size = index_meta.key_size();
    _size = 0;
    _version = index_meta.version();
    auto st = MutableIndex::create(_key_size);
    if (!st.ok()) {
        return st.status();
    }
    _l0 = std::move(st).value();
    ASSIGN_OR_RETURN(_fs, FileSystem::CreateSharedFromString(_path));

    RETURN_IF_ERROR(_load(index_meta));
    // delete expired _l0 file and _l1 file
    MutableIndexMetaPB l0_meta = index_meta.l0_meta();
    IndexSnapshotMetaPB snapshot_meta = l0_meta.snapshot();
    EditVersion l0_version = snapshot_meta.version();
    RETURN_IF_ERROR(_delete_expired_index_file(l0_version, _l1_version));
    return Status::OK();
}

Status PersistentIndex::_load(const PersistentIndexMetaPB& index_meta) {
    size_t key_size = index_meta.key_size();
    _size = index_meta.size();
    DCHECK_EQ(key_size, _key_size);
    if (!index_meta.has_l0_meta()) {
        return Status::InternalError("invalid PersistentIndexMetaPB");
    }
    MutableIndexMetaPB l0_meta = index_meta.l0_meta();
    IndexSnapshotMetaPB snapshot_meta = l0_meta.snapshot();
    EditVersion start_version = snapshot_meta.version();
    PagePointerPB page_pb = snapshot_meta.data();
    size_t snapshot_off = page_pb.offset();
    size_t snapshot_size = page_pb.size();
    std::unique_ptr<RandomAccessFile> l1_rfile;

    std::string l0_index_file_name = _get_l0_index_file_name(_path, start_version);
    ASSIGN_OR_RETURN(auto read_file, _fs->new_random_access_file(l0_index_file_name));
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
            RETURN_IF_ERROR(read_file->read_at_fully(offset, buff.data(), buff.size()));
            uint8_t keys[key_size * batch_num];
            std::vector<IndexValue> values;
            values.reserve(batch_num);
            size_t buf_offset = 0;
            for (size_t j = 0; j < batch_num; ++j) {
                memcpy(keys + j * key_size, buff.data() + buf_offset, key_size);
                uint64_t val = UNALIGNED_LOAD64(buff.data() + buf_offset + key_size);
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
    WritableFileOptions wblock_opts;
    wblock_opts.mode = FileSystem::MUST_EXIST;
    ASSIGN_OR_RETURN(_index_file, _fs->new_writable_file(wblock_opts, l0_index_file_name));

    if (index_meta.has_l1_version()) {
        _l1_version = index_meta.l1_version();
        auto l1_block_path = strings::Substitute("$0/index.l1.$1.$2", _path, _l1_version.major(), _l1_version.minor());
        ASSIGN_OR_RETURN(l1_rfile, _fs->new_random_access_file(l1_block_path));
        auto l1_st = ImmutableIndex::load(std::move(l1_rfile));
        if (!l1_st.ok()) {
            return l1_st.status();
        }
        _l1 = std::move(l1_st).value();
    }
    return Status::OK();
}

Status PersistentIndex::_build_commit(Tablet* tablet, PersistentIndexMetaPB& index_meta) {
    // commit: flush _l0 and build _l1
    // write PersistentIndexMetaPB in RocksDB
    Status status = commit(&index_meta);
    if (!status.ok()) {
        LOG(WARNING) << "build persistent index failed because commit failed: " << status.to_string();
        return status;
    }
    // write pesistent index meta
    status = TabletMetaManager::write_persistent_index_meta(tablet->data_dir(), tablet->tablet_id(), index_meta);
    if (!status.ok()) {
        LOG(WARNING) << "build persistent index failed because write persistent index meta failed: "
                     << status.to_string();
        return status;
    }

    // There are two conditions:
    // First is _flushed is true, we have flushed all _l0 data into _l1 and reload, we don't need
    // to do additional process
    // Second is _flused is false, we have write all _l0 data into new snapshot file, we need to
    // create new _index_file from the new snapshot file
    if (!_flushed) {
        std::string l0_index_file_path = _get_l0_index_file_name(_path, _version);
        WritableFileOptions wblock_opts;
        wblock_opts.mode = FileSystem::MUST_EXIST;
        ASSIGN_OR_RETURN(_index_file, _fs->new_writable_file(wblock_opts, l0_index_file_path));
    }

    RETURN_IF_ERROR(_delete_expired_index_file(_version, _l1_version));
    _dump_snapshot = false;
    _flushed = false;
    return status;
}

Status PersistentIndex::_insert_rowsets(Tablet* tablet, std::vector<RowsetSharedPtr>& rowsets,
                                        const vectorized::Schema& pkey_schema, int64_t apply_version,
                                        std::unique_ptr<vectorized::Column> pk_column) {
    OlapReaderStatistics stats;
    std::vector<uint32_t> rowids;
    rowids.reserve(4096);
    auto chunk_shared_ptr = vectorized::ChunkHelper::new_chunk(pkey_schema, 4096);
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
                    if (pk_column != nullptr) {
                        pk_column->reset_column();
                        PrimaryKeyEncoder::encode(pkey_schema, *chunk, 0, chunk->num_rows(), pk_column.get());
                        pkc = pk_column.get();
                    } else {
                        pkc = chunk->columns()[0].get();
                    }
                    uint32_t rssid = rowset->rowset_meta()->get_rowset_seg_id() + i;
                    uint64_t base = ((uint64_t)rssid) << 32;
                    std::vector<IndexValue> values;
                    values.reserve(pkc->size());
                    DCHECK(pkc->size() <= rowids.size());
                    for (uint32_t i = 0; i < pkc->size(); i++) {
                        values.emplace_back(base + rowids[i]);
                    }
                    auto st = insert(pkc->size(), pkc->continuous_data(), values.data(), false);

                    if (!st.ok()) {
                        LOG(ERROR) << "load index failed: tablet=" << tablet->tablet_id()
                                   << " rowsets num:" << rowsets.size()
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
    return Status::OK();
}

Status PersistentIndex::load_from_tablet(Tablet* tablet) {
    MonotonicStopWatch timer;
    timer.start();
    if (tablet->keys_type() != PRIMARY_KEYS) {
        LOG(WARNING) << "tablet: " << tablet->tablet_id() << " is not primary key tablet";
        return Status::NotSupported("Only PrimaryKey table is supported to use persistent index");
    }

    PersistentIndexMetaPB index_meta;
    Status status = TabletMetaManager::get_persistent_index_meta(tablet->data_dir(), tablet->tablet_id(), &index_meta);
    if (!status.ok() && !status.is_not_found()) {
        return Status::InternalError("get tablet persistent index meta failed");
    }

    // There are three conditions
    // First is we do not find PersistentIndexMetaPB in TabletMeta, it maybe the first time to
    // enable persistent index
    // Second is we find PersistentIndexMetaPB in TabletMeta, but it's version is behind applied_version
    // in TabletMeta. It could be happened as below:
    //    1. Enable persistent index and apply rowset, applied_version is 1-0
    //    2. Restart be and disable persistent index, applied_version is update to 2-0
    //    3. Restart be and enable persistent index
    // In this case, we don't have all rowset data in persistent index files, so we also need to rebuild it
    // The last is we find PersistentIndexMetaPB and it's version is equal to latest applied version. In this case,
    // we can load from index file directly
    EditVersion lastest_applied_version;
    RETURN_IF_ERROR(tablet->updates()->get_latest_applied_version(&lastest_applied_version));
    if (status.ok()) {
        // all applied rowsets has save in existing persistent index meta
        // so we can load persistent index according to PersistentIndexMetaPB
        EditVersion version = index_meta.version();
        if (version == lastest_applied_version) {
            MutableIndexMetaPB l0_meta = index_meta.l0_meta();
            if (l0_meta.format_version() != PERSISTENT_INDEX_VERSION_1) {
                LOG(WARNING) << "different format version, we need to rebuild persistent index";
                status = Status::InternalError("different format version");
            } else {
                status = load(index_meta);
            }
            if (status.ok()) {
                LOG(INFO) << "load persistent index tablet:" << tablet->tablet_id()
                          << " version:" << version.to_string() << " size: " << _size
                          << " l0_size: " << (_l0 ? _l0->size() : 0) << " l0_capacity:" << (_l0 ? _l0->capacity() : 0)
                          << " #shard: " << (_l1 ? _l1->_shards.size() : 0) << " l1_size:" << (_l1 ? _l1->_size : 0)
                          << " memory: " << memory_usage() << " status: " << status.to_string()
                          << " time:" << timer.elapsed_time() / 1000000 << "ms";
                return status;
            } else {
                LOG(WARNING) << "load persistent index failed, tablet: " << tablet->tablet_id()
                             << ", status: " << status;
                if (index_meta.has_l0_meta()) {
                    EditVersion l0_version = index_meta.l0_meta().snapshot().version();
                    std::string l0_file_name =
                            strings::Substitute("index.l0.$0.$1", l0_version.major(), l0_version.minor());
                    Status st = FileSystem::Default()->delete_file(l0_file_name);
                    LOG(WARNING) << "delete error l0 index file: " << l0_file_name << ", status: " << st;
                }
                if (index_meta.has_l1_version()) {
                    EditVersion l1_version = index_meta.l1_version();
                    std::string l1_file_name =
                            strings::Substitute("index.l1.$0.$1", l1_version.major(), l1_version.minor());
                    Status st = FileSystem::Default()->delete_file(l1_file_name);
                    LOG(WARNING) << "delete error l1 index file: " << l1_file_name << ", status: " << st;
                }
            }
        }
    }

    const TabletSchema& tablet_schema = tablet->tablet_schema();
    vector<ColumnId> pk_columns(tablet_schema.num_key_columns());
    for (auto i = 0; i < tablet_schema.num_key_columns(); i++) {
        pk_columns[i] = (ColumnId)i;
    }
    auto pkey_schema = vectorized::ChunkHelper::convert_schema_to_format_v2(tablet_schema, pk_columns);
    size_t fix_size = PrimaryKeyEncoder::get_encoded_fixed_size(pkey_schema);
    if (fix_size == 0) {
        LOG(WARNING) << "Build persistent index failed because get key cloumn size failed";
        return Status::InternalError("get key column size failed");
    }

    // Init PersistentIndex
    _key_size = fix_size;
    _size = 0;
    _version = lastest_applied_version;
    auto st = MutableIndex::create(_key_size);
    if (!st.ok()) {
        LOG(WARNING) << "Build persistent index failed because initialization failed: " << st.status().to_string();
        return st.status();
    }
    _l0 = std::move(st).value();
    ASSIGN_OR_RETURN(_fs, FileSystem::CreateSharedFromString(_path));
    // set _dump_snapshot to true
    // In this case, only do flush or dump snapshot, set _dump_snapshot to avoid append wal
    _dump_snapshot = true;

    // Init PersistentIndexMetaPB
    //   1. reset |version| |key_size|
    //   2. delete WALs because maybe PersistentIndexMetaPB has expired wals
    //   3. reset SnapshotMeta
    //   4. write all data into new tmp _l0 index file (tmp file will be delete in _build_commit())
    index_meta.set_key_size(_key_size);
    lastest_applied_version.to_pb(index_meta.mutable_version());
    MutableIndexMetaPB* l0_meta = index_meta.mutable_l0_meta();
    l0_meta->clear_wals();
    IndexSnapshotMetaPB* snapshot = l0_meta->mutable_snapshot();
    lastest_applied_version.to_pb(snapshot->mutable_version());
    PagePointerPB* data = snapshot->mutable_data();
    data->set_offset(0);
    data->set_size(0);

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
    status = tablet->updates()->get_rowsets_total_stats(rowset_ids, &total_rows2, &total_dels);
    if (!status.ok() || total_rows2 != total_rows) {
        LOG(WARNING) << "load primary index get_rowsets_total_stats error: " << status;
    }
    DCHECK(total_rows2 == total_rows);
    if (total_data_size > 4000000000 || total_rows > 10000000 || total_segments > 400) {
        LOG(INFO) << "load large primary index start tablet:" << tablet->tablet_id() << " version:" << apply_version
                  << " #rowset:" << rowsets.size() << " #segment:" << total_segments << " #row:" << total_rows << " -"
                  << total_dels << "=" << total_rows - total_dels << " bytes:" << total_data_size;
    }
    if (total_rows > total_dels) {
        _l0->reserve(total_rows - total_dels);
    }

    OlapReaderStatistics stats;
    std::unique_ptr<vectorized::Column> pk_column;
    if (pk_columns.size() > 1) {
        if (!PrimaryKeyEncoder::create_column(pkey_schema, &pk_column).ok()) {
            CHECK(false) << "create column for primary key encoder failed";
        }
    }
    RETURN_IF_ERROR(_insert_rowsets(tablet, rowsets, pkey_schema, apply_version, std::move(pk_column)));
    if (size() != total_rows - total_dels) {
        LOG(WARNING) << strings::Substitute("load primary index row count not match tablet:$0 index:$1 != stats:$2",
                                            tablet->tablet_id(), size(), total_rows - total_dels);
    }
    RETURN_IF_ERROR(_build_commit(tablet, index_meta));
    LOG(INFO) << "build persistent index finish tablet: " << tablet->tablet_id() << " version:" << apply_version
              << " #rowset:" << rowsets.size() << " #segment:" << total_segments << " data_size:" << total_data_size
              << " size: " << _size << " l0_size: " << _l0->size() << " l0_capacity:" << _l0->capacity()
              << " #shard: " << (_l1 ? _l1->_shards.size() : 0) << " l1_size:" << (_l1 ? _l1->_size : 0)
              << " memory: " << memory_usage() << " time: " << timer.elapsed_time() / 1000000 << "ms";
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

// There are four cases as below in commit
//   1. _flush_l0
//   2. _merge_compaction
//   3. _dump_snapshot
//   4. _append_wal
// both case1 and case2 will create a new l1 file and a new empty l0 file
// case3 will write a new snapshot l0
// case4 will append wals into l0 file
Status PersistentIndex::commit(PersistentIndexMetaPB* index_meta) {
    DCHECK_EQ(index_meta->key_size(), _key_size);
    // check if _l0 need be flush, there are two conditions:
    //   1. _l1 is not exist, _flush_l0 and build _l1
    //   2. _l1 is exist, merge _l0 and _l1
    // rebuild _l0 and _l1
    // In addition, there may be I/O waste because we append wals firstly and do _flush_l0 or _merge_compaction.
    const auto l0_mem_size = _l0->memory_usage();
    uint64_t l1_file_size = _l1 ? _l1->file_size() : 0;
    // if l1 is not empty,
<<<<<<< HEAD
    if (l1_file_size != 0) {
        // and l0 memory usage is large enough,
        if (l0_mem_size * l0_l1_merge_ratio > l1_file_size) {
            // do l0 l1 merge compaction
=======
    if (_flushed) {
        RETURN_IF_ERROR(_merge_compaction());
    } else {
        if (l1_file_size != 0) {
            // and l0 memory usage is large enough,
            if (l0_mem_size * config::l0_l1_merge_ratio > l1_file_size) {
                // do l0 l1 merge compaction
                _flushed = true;
                RETURN_IF_ERROR(_merge_compaction());
            }
            // if l1 is empty, and l0 memory usage is large enough
        } else if (l0_mem_size > config::l0_snapshot_size) {
            // do flush l0
>>>>>>> bdd4f0b9c ([Enhancement] Make l0 snapshot size configurable (#24748))
            _flushed = true;
            RETURN_IF_ERROR(_merge_compaction());
        }
        // if l1 is empty, and l0 memory usage is large enough
    } else if (l0_mem_size > l0_snapshot_size_max) {
        // do flush l0
        _flushed = true;
        RETURN_IF_ERROR(_flush_l0());
    }
    _dump_snapshot |= !_flushed && l0_file_size() > config::l0_max_file_size;
    // for case1 and case2
    if (_flushed) {
        // create a new empty _l0 file because all data in _l0 has write into _l1 files
        std::string file_name = _get_l0_index_file_name(_path, _version);
        WritableFileOptions wblock_opts;
        wblock_opts.mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE;
        ASSIGN_OR_RETURN(auto wfile, _fs->new_writable_file(wblock_opts, file_name));
        DeferOp close_block([&wfile] {
            if (wfile) {
                wfile->close();
            }
        });
        // update PersistentIndexMetaPB
        VLOG(1) << "new l0 file path(flush) is " << file_name;
        index_meta->set_size(_size);
        _version.to_pb(index_meta->mutable_version());
        _version.to_pb(index_meta->mutable_l1_version());
        MutableIndexMetaPB* l0_meta = index_meta->mutable_l0_meta();
        l0_meta->clear_wals();
        IndexSnapshotMetaPB* snapshot = l0_meta->mutable_snapshot();
        _version.to_pb(snapshot->mutable_version());
        PagePointerPB* data = snapshot->mutable_data();
        data->set_offset(0);
        data->set_size(0);
        l0_meta->set_format_version(PERSISTENT_INDEX_VERSION_1);
        // if _flushed is true, we will create a new empty _l0 file, set _offset to 0
        _offset = 0;
        _page_size = 0;
        // clear _l0 and reload _l1
        RETURN_IF_ERROR(_reload(*index_meta));
    } else if (_dump_snapshot) {
        std::string file_name = _get_l0_index_file_name(_path, _version);
        // be maybe crash after create index file during last commit
        // so we delete expired index file first to make sure no garbage left
        FileSystem::Default()->delete_file(file_name);
        size_t snapshot_size = _dump_bound();
        phmap::BinaryOutputArchive ar_out(file_name.data());
        if (!_dump(ar_out)) {
            std::string err_msg = strings::Substitute("faile to dump snapshot to file $0", file_name);
            LOG(WARNING) << err_msg;
            return Status::InternalError(err_msg);
        }
        // update PersistentIndexMetaPB
        index_meta->set_size(_size);
        _version.to_pb(index_meta->mutable_version());
        MutableIndexMetaPB* l0_meta = index_meta->mutable_l0_meta();
        l0_meta->clear_wals();
        IndexSnapshotMetaPB* snapshot = l0_meta->mutable_snapshot();
        _version.to_pb(snapshot->mutable_version());
        PagePointerPB* data = snapshot->mutable_data();
        data->set_offset(0);
        data->set_size(snapshot_size);
        l0_meta->set_format_version(PERSISTENT_INDEX_VERSION_1);
        // if _dump_snapshot is true, we will dump a snapshot to new _l0 file, set _offset to snapshot_size
        _offset = snapshot_size;
        _page_size = 0;
    } else {
        MutableIndexMetaPB* l0_meta = index_meta->mutable_l0_meta();
        l0_meta->set_format_version(PERSISTENT_INDEX_VERSION_1);
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
    if (_flushed) {
        RETURN_IF_ERROR(_delete_expired_index_file(_version, _l1_version));
    } else if (_dump_snapshot) {
        std::string expired_l0_file_path = _index_file->filename();
        std::string index_file_path = _get_l0_index_file_name(_path, _version);
        if (_fs == nullptr) {
            ASSIGN_OR_RETURN(_fs, FileSystem::CreateSharedFromString(index_file_path));
        }
        std::unique_ptr<WritableFile> wfile;
        DeferOp close_block([&wfile] {
            if (wfile) {
                wfile->close();
            }
        });

        WritableFileOptions wblock_opts;
        // new index file should be created in commit() phase
        wblock_opts.mode = FileSystem::MUST_EXIST;
        ASSIGN_OR_RETURN(wfile, _fs->new_writable_file(wblock_opts, index_file_path));
        _index_file = std::move(wfile);
        VLOG(1) << "delete expired l0 index file: " << expired_l0_file_path;
        FileSystem::Default()->delete_file(expired_l0_file_path);
    }

    _dump_snapshot = false;
    _flushed = false;
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

[[maybe_unused]] Status PersistentIndex::try_replace(size_t n, const void* keys, const IndexValue* values,
                                                     const std::vector<uint32_t>& src_rssid,
                                                     std::vector<uint32_t>* failed) {
    std::vector<IndexValue> found_values;
    found_values.reserve(n);
    RETURN_IF_ERROR(get(n, keys, found_values.data()));
    std::vector<size_t> replace_idxes;
    for (size_t i = 0; i < n; ++i) {
        if (values[i].get_value() != NullIndexValue &&
            ((uint32_t)(found_values[i].get_value() >> 32)) == src_rssid[i]) {
            replace_idxes.emplace_back(i);
        } else {
            failed->emplace_back(values[i].get_value() & 0xFFFFFFFF);
        }
    }
    RETURN_IF_ERROR(_l0->replace(keys, values, replace_idxes));
    _dump_snapshot |= _can_dump_directly();
    if (!_dump_snapshot) {
        // write wal
        const uint8_t* fkeys = reinterpret_cast<const uint8_t*>(keys);
        faststring fixed_buf;
        fixed_buf.reserve(replace_idxes.size() * (_key_size + sizeof(IndexValue)));
        for (size_t i = 0; i < replace_idxes.size(); ++i) {
            fixed_buf.append(fkeys + replace_idxes[i] * _key_size, _key_size);
            put_fixed64_le(&fixed_buf, values[replace_idxes[i]].get_value());
        }
        RETURN_IF_ERROR(_index_file->append(fixed_buf));
        _page_size += fixed_buf.size();
    }
    return Status::OK();
}

Status PersistentIndex::try_replace(size_t n, const void* keys, const IndexValue* values, const uint32_t max_src_rssid,
                                    std::vector<uint32_t>* failed) {
    std::vector<IndexValue> found_values;
    found_values.reserve(n);
    RETURN_IF_ERROR(get(n, keys, found_values.data()));
    std::vector<size_t> replace_idxes;
    for (size_t i = 0; i < n; ++i) {
        if (values[i].get_value() != NullIndexValue &&
            ((uint32_t)(found_values[i].get_value() >> 32)) <= max_src_rssid) {
            replace_idxes.emplace_back(i);
        } else {
            failed->emplace_back(values[i].get_value() & 0xFFFFFFFF);
        }
    }
    RETURN_IF_ERROR(_l0->replace(keys, values, replace_idxes));
    _dump_snapshot |= _can_dump_directly();
    if (!_dump_snapshot) {
        // write wal
        const uint8_t* fkeys = reinterpret_cast<const uint8_t*>(keys);
        faststring fixed_buf;
        fixed_buf.reserve(replace_idxes.size() * (_key_size + sizeof(IndexValue)));
        for (size_t i = 0; i < replace_idxes.size(); ++i) {
            fixed_buf.append(fkeys + replace_idxes[i] * _key_size, _key_size);
            put_fixed64_le(&fixed_buf, values[replace_idxes[i]].get_value());
        }
        RETURN_IF_ERROR(_index_file->append(fixed_buf));
        _page_size += fixed_buf.size();
    }
    return Status::OK();
}

Status PersistentIndex::_append_wal(size_t n, const void* keys, const IndexValue* values) {
    const uint8_t* fkeys = reinterpret_cast<const uint8_t*>(keys);
    faststring fixed_buf;
    fixed_buf.reserve(n * (_key_size + sizeof(IndexValue)));
    for (size_t i = 0; i < n; i++) {
        const auto v = (values != nullptr) ? values[i] : IndexValue(NullIndexValue);
        fixed_buf.append(fkeys + i * _key_size, _key_size);
        put_fixed64_le(&fixed_buf, v.get_value());
    }
    RETURN_IF_ERROR(_index_file->append(fixed_buf));
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

Status PersistentIndex::_reload(const PersistentIndexMetaPB& index_meta) {
    _offset = 0;
    _page_size = 0;
    auto l0_st = MutableIndex::create(_key_size);
    if (!l0_st.ok()) {
        return l0_st.status();
    }
    _l0 = std::move(l0_st).value();

    Status st = _load(index_meta);
    if (!st.ok()) {
        LOG(WARNING) << "reload persistent index failed, status: " << st.to_string();
    }
    return st;
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
<<<<<<< HEAD
    return _dump_bound() <= l0_snapshot_size_max;
=======
    return _dump_bound() <= config::l0_snapshot_size;
>>>>>>> bdd4f0b9c ([Enhancement] Make l0 snapshot size configurable (#24748))
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
            Status st = FileSystem::Default()->delete_file(path);
            if (!st.ok()) {
                LOG(WARNING) << "delete exprired index file: " << path << ", failed, status is " << st.to_string();
                return false;
            }
        }
        return true;
    };
    return FileSystem::Default()->iterate_dir(_path, cb);
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
        uint64_t v = UNALIGNED_LOAD64(kv.kv_pos + KeySize);
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
    uint32_t shard_bits = log2(nshard);
    for (size_t l1_shard_idx = 0; l1_shard_idx < nshard_l1; l1_shard_idx++) {
        RETURN_IF_ERROR(
                _l1->_get_kvs_for_shard(l1_kvs_by_shard, l1_shard_idx, shard_bits, &index_shards[index_shards_idx++]));
        size_t num_shard_finished = (l1_shard_idx + 1) * nshard / nshard_l1;
        std::vector<KVRef> kvs;
        while (cur_shard_idx < num_shard_finished) {
            kvs.clear();
            RETURN_IF_ERROR(merge_shard_kvs(_key_size, l0_kvs_by_shard[cur_shard_idx], l1_kvs_by_shard[cur_shard_idx],
                                            estimated_size_per_shard, kvs));
            RETURN_IF_ERROR(writer.write_shard(_key_size, value_size, npage_hint, kvs));
            // clear to optimize memory usage
            l0_kvs_by_shard[cur_shard_idx].clear();
            l0_kvs_by_shard[cur_shard_idx].shrink_to_fit();
            l1_kvs_by_shard[cur_shard_idx].clear();
            l1_kvs_by_shard[cur_shard_idx].shrink_to_fit();
            cur_shard_idx++;
            index_shards_idx = 0;
        }
    }
    return writer.finish();
}

std::vector<int8_t> PersistentIndex::test_get_move_buckets(size_t target, const uint8_t* bucket_packs_in_page) {
    return get_move_buckets(target, bucket_packs_in_page);
}

} // namespace starrocks
