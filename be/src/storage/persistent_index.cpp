// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "storage/persistent_index.h"

#include <cstring>
#include <numeric>

#include "fs/fs.h"
#include "gutil/strings/substitute.h"
#include "storage/chunk_helper.h"
#include "storage/chunk_iterator.h"
#include "storage/primary_key_encoder.h"
#include "storage/rowset/rowset.h"
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
// if l0_mem_size exceeds this value, l0 need snapshot
#if BE_TEST
constexpr size_t kL0SnapshotSizeMax = 1 * 1024 * 1024;
#else
constexpr size_t kL0SnapshotSizeMax = 16 * 1024 * 1024;
#endif
constexpr size_t kLongKeySize = 64;

const char* const kIndexFileMagic = "IDX1";

using KVPairPtr = const uint8_t*;

template <class T, class P>
T npad(T v, P p) {
    return (v + p - 1) / p;
}

template <class T, class P>
T pad(T v, P p) {
    return npad(v, p) * p;
}

static std::string get_l0_index_file_name(std::string& dir, const EditVersion& version) {
    return strings::Substitute("$0/index.l0.$1.$2", dir, version.major(), version.minor());
}

struct IndexHash {
    IndexHash() {}
    IndexHash(uint64_t hash) : hash(hash) {}
    uint64_t shard(uint32_t n) const { return (hash >> (63 - n)) >> 1; }
    uint64_t page() const { return (hash >> 16) & 0xffffffff; }
    uint64_t bucket() const { return (hash >> 8) & (kBucketPerPage - 1); }
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

struct alignas(kPageHeaderSize) PageHeader {
    BucketInfo buckets[kBucketPerPage];
};

struct alignas(kPageSize) IndexPage {
    uint8_t data[kPageSize];
    PageHeader& header() { return *reinterpret_cast<PageHeader*>(data); }
    uint8_t* pack(uint8_t packid) { return &data[packid * kPackSize]; }
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

    static StatusOr<std::unique_ptr<ImmutableIndexShard>> try_create(size_t key_size, size_t npage, size_t nbucket,
                                                                     const std::vector<KVRef>& kv_refs);

    static StatusOr<std::unique_ptr<ImmutableIndexShard>> create(size_t key_size, size_t npage, size_t nbucket,
                                                                 const std::vector<KVRef>& kv_refs);

    std::vector<IndexPage> pages;
    size_t num_entry_moved = 0;
};

Status ImmutableIndexShard::write(WritableFile& wb) const {
    if (pages.size() > 0) {
        return wb.append(Slice((uint8_t*)pages.data(), kPageSize * pages.size()));
    } else {
        return Status::OK();
    }
}

inline size_t num_pack_for_bucket(size_t kv_size, size_t num_kv) {
    return npad(num_kv, kPackSize) + npad(kv_size * num_kv, kPackSize);
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

static std::vector<int8_t> get_move_buckets(size_t target, size_t nbucket, const uint8_t* bucket_packs_in_page) {
    vector<int8_t> idxes;
    idxes.reserve(nbucket);
    int32_t total_buckets = 0;
    for (int8_t i = 0; i < nbucket; i++) {
        if (bucket_packs_in_page[i] > 0) {
            idxes.push_back(i);
        }
        total_buckets += bucket_packs_in_page[i];
    }
    std::sort(idxes.begin(), idxes.end(),
              [&](int8_t lhs, int8_t rhs) { return bucket_packs_in_page[lhs] < bucket_packs_in_page[rhs]; });
    // store idx if this sum value uses bucket_packs_in_page[idx], or -1
    std::vector<int8_t> dp(total_buckets + 1, -1);
    dp[0] = nbucket;                   // assign an id that will never be used but >= 0
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

static Status find_buckets_to_move(uint32_t pageid, size_t nbucket, size_t min_pack_to_move,
                                   const uint8_t* bucket_packs_in_page, std::vector<BucketToMove>* buckets_to_move) {
    auto ret = get_move_buckets(min_pack_to_move, nbucket, bucket_packs_in_page);

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

static void copy_kv_to_page(size_t key_size, size_t num_kv, const KVPairPtr* kv_ptrs, const uint8_t* tags,
                            uint8_t* dest_pack, const uint16_t* kv_size) {
    uint8_t* tags_dest = dest_pack;
    size_t tags_len = pad(num_kv, kPackSize);
    memcpy(tags_dest, tags, num_kv);
    memset(tags_dest + num_kv, 0, tags_len - num_kv);
    uint8_t* kvs_dest = dest_pack + tags_len;
    uint16_t offset = tags_len + (num_kv + 1) * sizeof(uint16_t);
    if (key_size == 0) {
        for (size_t i = 0; i < num_kv; i++) {
            encode_fixed16_le(kvs_dest, offset);
            kvs_dest += sizeof(uint16_t);
            offset += kv_size[i];
        }
        encode_fixed16_le(kvs_dest, offset);
        kvs_dest += sizeof(uint16_t);
    }
    for (size_t i = 0; i < num_kv; i++) {
        memcpy(kvs_dest, kv_ptrs[i], kv_size[i]);
        kvs_dest += kv_size[i];
    }
}

StatusOr<std::unique_ptr<ImmutableIndexShard>> ImmutableIndexShard::create(size_t key_size, size_t npage_hint,
                                                                           size_t nbucket,
                                                                           const std::vector<KVRef>& kv_refs) {
    if (kv_refs.size() == 0) {
        return std::make_unique<ImmutableIndexShard>(0);
    }
    for (size_t npage = npage_hint; npage < kPageMax; npage++) {
        auto rs_create = ImmutableIndexShard::try_create(key_size, npage, nbucket, kv_refs);
        // increase npage and retry
        if (!rs_create.ok()) {
            continue;
        }
        return std::move(rs_create.value());
    }
    return Status::InternalError("failed to create immutable index shard");
}

StatusOr<std::unique_ptr<ImmutableIndexShard>> ImmutableIndexShard::try_create(size_t key_size, size_t npage,
                                                                               size_t nbucket,
                                                                               const std::vector<KVRef>& kv_refs) {
    const size_t total_bucket = npage * nbucket;
    std::vector<uint8_t> bucket_sizes(total_bucket);
    std::vector<std::pair<uint32_t, std::vector<uint16_t>>> bucket_data_size(total_bucket);
    std::vector<std::pair<std::vector<KVPairPtr>, std::vector<uint8_t>>> bucket_kv_ptrs_tags(total_bucket);
    size_t estimated_entry_per_bucket = npad(kv_refs.size() * 100 / 85, total_bucket);
    for (auto& [kv_ptrs, tags] : bucket_kv_ptrs_tags) {
        kv_ptrs.reserve(estimated_entry_per_bucket);
        tags.reserve(estimated_entry_per_bucket);
    }
    for (size_t i = 0; i < kv_refs.size(); i++) {
        auto h = IndexHash(kv_refs[i].hash);
        auto page = h.page() % npage;
        auto bucket = h.bucket() % nbucket;
        auto bid = page * nbucket + bucket;
        auto& sz = bucket_sizes[bid];
        if (sz >= kBucketSizeMax) {
            return Status::InternalError("bucket size exceed kBucketSizeMax");
        }
        sz++;
        auto& data_size = bucket_data_size[bid].first;
        data_size += kv_refs[i].size;
        if (pad(sz, kPackSize) + data_size > kPageSize) {
            return Status::InternalError("bucket size limit exceeded");
        }
        bucket_data_size[bid].second.emplace_back(kv_refs[i].size);
        bucket_kv_ptrs_tags[bid].first.emplace_back(kv_refs[i].kv_pos);
        bucket_kv_ptrs_tags[bid].second.emplace_back(h.tag());
    }
    std::vector<uint8_t> bucket_packs(total_bucket);
    for (size_t i = 0; i < total_bucket; i++) {
        auto npack = 0;
        if (key_size != 0) {
            npack = npad((size_t)bucket_sizes[i], kPackSize) + npad(bucket_data_size[i].first, kPackSize);
        } else {
            npack = npad((size_t)bucket_sizes[i], kPackSize) +
                    npad(bucket_data_size[i].first + sizeof(uint16_t) * ((size_t)bucket_sizes[i] + 1), kPackSize);
        }
        if (npack >= kPagePackLimit) {
            return Status::InternalError("page page limit exceeded");
        }
        bucket_packs[i] = npack;
    }
    // check over-limit pages and reassign some buckets in those pages to under-limit pages
    std::vector<BucketToMove> buckets_to_move;
    std::vector<MoveDest> dests;
    std::vector<bool> page_has_move(npage, false);
    for (uint32_t pageid = 0; pageid < npage; pageid++) {
        const uint8_t* bucket_packs_in_page = &bucket_packs[pageid * nbucket];
        int npack = std::accumulate(bucket_packs_in_page, bucket_packs_in_page + nbucket, 0);
        if (npack < kPagePackLimit) {
            dests.emplace_back(kPagePackLimit - npack, pageid);
        } else if (npack > kPagePackLimit) {
            page_has_move[pageid] = true;
            RETURN_IF_ERROR(find_buckets_to_move(pageid, nbucket, npack - kPagePackLimit, bucket_packs_in_page,
                                                 &buckets_to_move));
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
        ret->num_entry_moved += bucket_sizes[move.src_pageid * nbucket + move.src_bucketid];
    }
    for (uint32_t pageid = 0; pageid < npage; pageid++) {
        IndexPage& page = ret->page(pageid);
        PageHeader& header = ret->header(pageid);
        size_t cur_packid = npad(nbucket * kBucketHeaderSize, kPackSize);
        for (uint32_t bucketid = 0; bucketid < nbucket; bucketid++) {
            if (page_has_move[pageid] && bucket_moved(pageid, bucketid)) {
                continue;
            }
            auto bid = pageid * nbucket + bucketid;
            auto& bucket_info = header.buckets[bucketid];
            bucket_info.pageid = pageid;
            bucket_info.packid = cur_packid;
            bucket_info.size = bucket_sizes[bid];
            copy_kv_to_page(key_size, bucket_info.size, bucket_kv_ptrs_tags[bid].first.data(),
                            bucket_kv_ptrs_tags[bid].second.data(), page.pack(cur_packid),
                            bucket_data_size[bid].second.data());
            cur_packid += bucket_packs[bid];
            DCHECK(cur_packid <= kPageSize / kPackSize);
        }
        for (auto& move : moves) {
            if (move.dest_pageid == pageid) {
                auto bid = move.src_pageid * nbucket + move.src_bucketid;
                auto& bucket_info = ret->bucket(move.src_pageid, move.src_bucketid);
                bucket_info.pageid = pageid;
                bucket_info.packid = cur_packid;
                bucket_info.size = bucket_sizes[bid];
                copy_kv_to_page(key_size, bucket_info.size, bucket_kv_ptrs_tags[bid].first.data(),
                                bucket_kv_ptrs_tags[bid].second.data(), page.pack(cur_packid),
                                bucket_data_size[bid].second.data());
                cur_packid += bucket_packs[bid];
                DCHECK(cur_packid <= kPageSize / kPackSize);
            }
        }
    }
    return std::move(ret);
}

ImmutableIndexWriter::~ImmutableIndexWriter() {
    if (_wb) {
        FileSystem::Default()->delete_file(_idx_file_path_tmp);
    }
}

Status ImmutableIndexWriter::init(const string& dir, const EditVersion& version) {
    _version = version;
    _idx_file_path = strings::Substitute("$0/index.l1.$1.$2", dir, version.major(), version.minor());
    _idx_file_path_tmp = _idx_file_path + ".tmp";
    ASSIGN_OR_RETURN(_fs, FileSystem::CreateSharedFromString(_idx_file_path_tmp));
    WritableFileOptions wblock_opts{.sync_on_close = true, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
    ASSIGN_OR_RETURN(_wb, _fs->new_writable_file(wblock_opts, _idx_file_path_tmp));
    return Status::OK();
}

// write_shard() must be called serially in the order of key_size and it is caller's duty to guarantee this.
Status ImmutableIndexWriter::write_shard(size_t key_size, size_t npage_hint, size_t nbucket,
                                         const std::vector<KVRef>& kvs) {
    const bool new_key_length = _nshard == 0 || _cur_key_size != key_size;
    if (_nshard == 0) {
        _cur_key_size = key_size;
        _cur_value_size = kIndexValueSize;
    } else {
        if (new_key_length) {
            CHECK(key_size > _cur_key_size) << "key size is smaller than before";
        }
        _cur_key_size = key_size;
    }
    auto rs_create = ImmutableIndexShard::create(key_size, npage_hint, nbucket, kvs);
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
    shard_meta->set_key_size(key_size);
    shard_meta->set_value_size(kIndexValueSize);
    shard_meta->set_nbucket(nbucket);
    auto ptr_meta = shard_meta->mutable_data();
    ptr_meta->set_offset(pos_before);
    ptr_meta->set_size(pos_after - pos_before);
    _total += kvs.size();
    _total_moved += shard->num_entry_moved;
    size_t shard_kv_size = 0;
    if (key_size != 0) {
        shard_kv_size = (key_size + kIndexValueSize) * kvs.size();
        _total_kv_size += shard_kv_size;
    } else {
        shard_kv_size =
                std::accumulate(kvs.begin(), kvs.end(), (size_t)0, [](size_t s, const auto& e) { return s + e.size; });
        _total_kv_size += shard_kv_size;
    }
    shard_meta->set_data_size(shard_kv_size);
    _total_bytes += pos_after - pos_before;
    auto iter = _shard_info_by_length.find(_cur_key_size);
    if (iter == _shard_info_by_length.end()) {
        if (auto [it, inserted] = _shard_info_by_length.insert({_cur_key_size, {_nshard, 1}}); !inserted) {
            LOG(WARNING) << "insert shard info failed, key_size: " << _cur_key_size;
            return Status::InternalError("insert shard info failed");
        }
    } else {
        iter->second.second++;
    }
    _nshard++;
    return Status::OK();
}

Status ImmutableIndexWriter::write_shard_as_rawbuff(const ImmutableIndex::ShardInfo& old_shard_info,
                                                    ImmutableIndex* immutable_index) {
    const auto key_size = old_shard_info.key_size;
    const bool new_key_length = _nshard == 0 || _cur_key_size != key_size;
    if (_nshard == 0) {
        _cur_key_size = key_size;
        _cur_value_size = kIndexValueSize;
    } else {
        if (new_key_length) {
            CHECK(key_size > _cur_key_size) << "key size is smaller than before";
        }
        _cur_key_size = key_size;
    }
    std::string buff;
    raw::stl_string_resize_uninitialized(&buff, old_shard_info.bytes);
    RETURN_IF_ERROR(immutable_index->_file->read_at_fully(old_shard_info.offset, buff.data(), buff.size()));
    size_t pos_before = _wb->size();
    _wb->append(Slice(buff.data(), buff.size()));
    size_t pos_after = _wb->size();
    auto shard_info = _meta.add_shards();
    shard_info->set_size(old_shard_info.size);
    shard_info->set_npage(old_shard_info.npage);
    shard_info->set_key_size(old_shard_info.key_size);
    shard_info->set_value_size(old_shard_info.value_size);
    shard_info->set_nbucket(old_shard_info.nbucket);
    shard_info->set_data_size(old_shard_info.data_size);
    auto page_pointer = shard_info->mutable_data();
    page_pointer->set_offset(pos_before);
    page_pointer->set_size(pos_after - pos_before);
    _total += old_shard_info.size;
    _total_bytes += pos_after - pos_before;
    // not accurate, but not important as well
    _total_kv_size += old_shard_info.bytes;
    auto iter = _shard_info_by_length.find(_cur_key_size);
    if (iter == _shard_info_by_length.end()) {
        if (auto [it, inserted] = _shard_info_by_length.insert({_cur_key_size, {_nshard, 1}}); !inserted) {
            LOG(WARNING) << "insert shard info failed, key_size: " << _cur_key_size;
            return Status::InternalError("insert shard info failed");
        }
    } else {
        iter->second.second++;
    }
    _nshard++;
    return Status::OK();
}

Status ImmutableIndexWriter::finish() {
    LOG(INFO) << strings::Substitute(
            "finish writing immutable index $0 #shard:$1 #kv:$2 #moved:$3($4) bytes:$5 usage:$6", _idx_file_path_tmp,
            _nshard, _total, _total_moved, _total_moved * 1000 / std::max(_total, 1UL) / 1000.0, _total_bytes,
            _total_kv_size * 1000 / std::max(_total_bytes, 1UL) / 1000.0);
    _version.to_pb(_meta.mutable_version());
    _meta.set_size(_total);
    _meta.set_format_version(PERSISTENT_INDEX_VERSION_2);
    for (const auto& [key_size, shard_info] : _shard_info_by_length) {
        const auto [shard_offset, shard_num] = shard_info;
        auto info = _meta.add_shard_info();
        info->set_key_size(key_size);
        info->set_shard_off(shard_offset);
        info->set_shard_num(shard_num);
    }
    std::string footer;
    if (!_meta.SerializeToString(&footer)) {
        return Status::InternalError("ImmutableIndexMetaPB::SerializeToString failed");
    }
    put_fixed32_le(&footer, static_cast<uint32_t>(footer.size()));
    uint32_t checksum = crc32c::Value(footer.data(), footer.size());
    put_fixed32_le(&footer, checksum);
    footer.append(kIndexFileMagic, 4);
    RETURN_IF_ERROR(_wb->append(Slice(footer)));
    RETURN_IF_ERROR(_wb->close());
    RETURN_IF_ERROR(FileSystem::Default()->rename_file(_idx_file_path_tmp, _idx_file_path));
    _wb.reset();
    return Status::OK();
}

template <size_t KeySize>
class FixedMutableIndex : public MutableIndex {
public:
    using KeyType = FixedKey<KeySize>;
    FixedMutableIndex() {}
    ~FixedMutableIndex() override {}

    Status get(const Slice* keys, IndexValue* values, KeysInfo* not_found, size_t* num_found,
               const std::vector<size_t>& idxes) const override {
        size_t nfound = 0;
        for (const auto idx : idxes) {
            const auto& key = *reinterpret_cast<const KeyType*>(keys[idx].data);
            uint64_t hash = FixedKeyHash<KeySize>()(key);
            auto iter = _map.find(key, hash);
            if (iter == _map.end()) {
                values[idx] = NullIndexValue;
                not_found->key_idxes.emplace_back((uint32_t)idx);
                not_found->hashes.emplace_back(hash);
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
                not_found->key_idxes.emplace_back((uint32_t)idx);
                not_found->hashes.emplace_back(hash);
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
                  const std::vector<size_t>& idxes) {
        size_t nfound = 0;
        for (const auto idx : idxes) {
            const auto& key = *reinterpret_cast<const KeyType*>(keys[idx].data);
            const auto value = values[idx];
            uint64_t hash = FixedKeyHash<KeySize>()(key);
            if (auto [it, inserted] = _map.emplace_with_hash(hash, key, value); inserted) {
                not_found->key_idxes.emplace_back((uint32_t)idx);
                not_found->hashes.emplace_back(hash);
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
                std::string msg = strings::Substitute("FixedMutableIndex<$0> insert found duplicate key $1", KeySize,
                                                      hexdump((const char*)key.data, KeySize));
                LOG(WARNING) << msg;
                return Status::InternalError(msg);
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
                not_found->key_idxes.emplace_back((uint32_t)idx);
                not_found->hashes.emplace_back(hash);
            } else {
                old_values[idx] = it->second;
                nfound += it->second.get_value() != NullIndexValue;
                it->second = NullIndexValue;
            }
        }
        *num_found = nfound;
        return Status::OK();
    }

    Status replace(const Slice* keys, const IndexValue* values, const std::vector<size_t>& replace_idxes) {
        for (size_t i = 0; i < replace_idxes.size(); ++i) {
            const auto& key = *reinterpret_cast<const KeyType*>(keys[replace_idxes[i]].data);
            const auto value = values[replace_idxes[i]];
            uint64_t hash = FixedKeyHash<KeySize>()(key);
            if (auto [it, inserted] = _map.emplace_with_hash(hash, key, value); !inserted) {
                it->second = value;
            } else {
                _overlap_size += 1;
            }
        }
        return Status::OK();
    }

    Status append_wal(const Slice* keys, const IndexValue* values, const std::vector<size_t>& idxes,
                      std::unique_ptr<WritableFile>& index_file, uint64_t* page_size) {
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
        return Status::OK();
    }

    Status load_wals(size_t n, const Slice* keys, const IndexValue* values) {
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

    bool load_snapshot(phmap::BinaryInputArchive& ar) { return _map.load(ar); }

    Status load(size_t& offset, std::unique_ptr<RandomAccessFile>& file) {
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
    size_t dump_bound() { return _map.empty() ? sizeof(size_t) : _map.dump_bound(); }

    bool dump(phmap::BinaryOutputArchive& ar) { return _map.dump(ar); }

    std::vector<std::vector<KVRef>> get_kv_refs_by_shard(size_t nshard, size_t num_entry,
                                                         bool without_null) const override {
        std::vector<std::vector<KVRef>> ret(nshard);
        uint32_t shard_bits = log2(nshard);
        for (auto i = 0; i < nshard; ++i) {
            ret[i].reserve(num_entry / nshard * 100 / 85);
        }
        auto hasher = FixedKeyHash<KeySize>();
        for (const auto& [key, value] : _map) {
            if (without_null && value.get_value() == NullIndexValue) {
                continue;
            }
            IndexHash h(hasher(key));
            ret[h.shard(shard_bits)].emplace_back((uint8_t*)&key, h.hash, KeySize + kIndexValueSize);
        }
        return ret;
    }

    Status flush_to_immutable_index(std::unique_ptr<ImmutableIndexWriter>& writer, size_t nshard, size_t npage_hint,
                                    size_t nbucket) const override {
        if (nshard > 0) {
            const auto& kv_ref_by_shard = get_kv_refs_by_shard(nshard, size(), true);
            for (const auto& kvs : kv_ref_by_shard) {
                RETURN_IF_ERROR(writer->write_shard(KeySize, npage_hint, nbucket, kvs));
            }
        }
        return Status::OK();
    }

    size_t size() const override { return _map.size(); }

    size_t capacity() { return _map.capacity(); }

    void reserve(size_t size) { _map.reserve(size); }

    size_t memory_usage() { return _map.capacity() * (1 + (KeySize + 3) / 4 * 4 + kIndexValueSize); }

    void update_overlap_info(size_t overlap_size, size_t overlap_usage) override { _overlap_size += overlap_size; }
    size_t overlap_size() { return _overlap_size; }

private:
    size_t _overlap_size = 0;
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

struct StringHasher2 {
    uint64_t operator()(const std::string& s) const { return key_index_hash(s.data(), s.length() - kIndexValueSize); }
};

class EqualOnStringWithHash {
public:
    bool operator()(const std::string& lhs, const std::string& rhs) const {
        return memequal(lhs.data(), lhs.size() - kIndexValueSize, rhs.data(), rhs.size() - kIndexValueSize);
    }
};

class SliceMutableIndex : public MutableIndex {
public:
    using KeyType = std::string;

    using WALKVSizeType = uint32_t;
    static constexpr size_t kWALKVSize = 4;
    static_assert(sizeof(WALKVSizeType) == kWALKVSize);
    static constexpr size_t kKeySizeMagicNum = 0;

    SliceMutableIndex() {}
    ~SliceMutableIndex() override {}

    Status get(const Slice* keys, IndexValue* values, KeysInfo* not_found, size_t* num_found,
               const std::vector<size_t>& idxes) const override {
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
                not_found->key_idxes.emplace_back((uint32_t)idx);
                not_found->hashes.emplace_back(hash);
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

    Status upsert(const Slice* keys, const IndexValue* values, IndexValue* old_values, KeysInfo* not_found,
                  size_t* num_found, const std::vector<size_t>& idxes) override {
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
                not_found->key_idxes.emplace_back((uint32_t)idx);
                not_found->hashes.emplace_back(hash);
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

    Status upsert(const Slice* keys, const IndexValue* values, KeysInfo* not_found, size_t* num_found,
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
                not_found->key_idxes.emplace_back((uint32_t)idx);
                not_found->hashes.emplace_back(hash);
                _total_kv_pairs_usage += composite_key.size();
            } else {
                const auto& old_compose_key = *it;
                const auto old_value =
                        UNALIGNED_LOAD64(old_compose_key.data() + old_compose_key.size() - kIndexValueSize);
                nfound += old_value != NullIndexValue;
                // TODO: find a way to modify iterator directly, currently just erase then re-insert
                _set.erase(it);
                _set.emplace_with_hash(hash, composite_key);
            }
        }
        *num_found = nfound;
        return Status::OK();
    }

    Status insert(const Slice* keys, const IndexValue* values, const std::vector<size_t>& idxes) override {
        for (const auto idx : idxes) {
            std::string composite_key;
            const auto& skey = keys[idx];
            const auto value = values[idx];
            composite_key.reserve(skey.size + kIndexValueSize);
            composite_key.append(skey.data, skey.size);
            put_fixed64_le(&composite_key, value.get_value());
            uint64_t hash = StringHasher2()(composite_key);
            if (auto [_, inserted] = _set.emplace_with_hash(hash, composite_key); inserted) {
                _total_kv_pairs_usage += composite_key.size();
            } else {
                std::string msg = strings::Substitute("SliceMutableIndex key_size=$0 insert found duplicate key $1",
                                                      skey.size, hexdump((const char*)skey.data, skey.size));
                LOG(WARNING) << msg;
                return Status::InternalError(msg);
            }
        }
        return Status::OK();
    }

    Status erase(const Slice* keys, IndexValue* old_values, KeysInfo* not_found, size_t* num_found,
                 const std::vector<size_t>& idxes) override {
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
                not_found->key_idxes.emplace_back((uint32_t)idx);
                not_found->hashes.emplace_back(hash);
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

    Status replace(const Slice* keys, const IndexValue* values, const std::vector<size_t>& idxes) {
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
                _overlap_kv_pairs_usage += composite_key.size();
                _overlap_size += 1;
            } else {
                // TODO: find a way to modify iterator directly, currently just erase then re-insert
                _set.erase(it);
                _set.emplace_with_hash(hash, composite_key);
            }
        }
        return Status::OK();
    }

    Status append_wal(const Slice* keys, const IndexValue* values, const std::vector<size_t>& idxes,
                      std::unique_ptr<WritableFile>& index_file, uint64_t* page_size) {
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
        return Status::OK();
    }

    Status load_wals(size_t n, const Slice* keys, const IndexValue* values) {
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

    // return the dump file size if dump _set into a new file
    //  ｜--------    snapshot file      --------｜
    //  |  size_t ||   size_t  ||  char[]  | ... |   size_t  ||  char[]  |
    //  |total num|| data size ||  data    | ... | data size ||  data    |
    size_t dump_bound() { return sizeof(size_t) * (1 + size()) + _total_kv_pairs_usage; }

    bool dump(phmap::BinaryOutputArchive& ar) {
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

    bool load_snapshot(phmap::BinaryInputArchive& ar) {
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
    Status load(size_t& offset, std::unique_ptr<RandomAccessFile>& file) {
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

    std::vector<std::vector<KVRef>> get_kv_refs_by_shard(size_t nshard, size_t num_entry,
                                                         bool without_null) const override {
        std::vector<std::vector<KVRef>> ret(nshard);
        uint32_t shard_bits = log2(nshard);
        for (auto i = 0; i < nshard; ++i) {
            ret[i].reserve(num_entry / nshard * 100 / 85);
        }
        for (const auto& composite_key : _set) {
            const auto value = UNALIGNED_LOAD64(composite_key.data() + composite_key.size() - kIndexValueSize);
            IndexHash h(StringHasher2()(composite_key));
            if (without_null && value == NullIndexValue) {
                continue;
            }
            ret[h.shard(shard_bits)].emplace_back((uint8_t*)(composite_key.data()), h.hash, composite_key.size());
        }
        return ret;
    }

    Status flush_to_immutable_index(std::unique_ptr<ImmutableIndexWriter>& writer, size_t nshard, size_t npage_hint,
                                    size_t nbucket) const override {
        if (nshard > 0) {
            const auto& kv_ref_by_shard = get_kv_refs_by_shard(nshard, size(), true);
            for (const auto& kvs : kv_ref_by_shard) {
                RETURN_IF_ERROR(writer->write_shard(kKeySizeMagicNum, npage_hint, nbucket, kvs));
            }
        }
        return Status::OK();
    }

    size_t size() const override { return _set.size(); }

    size_t capacity() { return _set.capacity(); }

    void reserve(size_t size) { _set.reserve(size); }

    // TODO: more accurate estimation for phmap::flat_hash_set<std::string, ...
    size_t memory_usage() {
        auto ret = capacity() * (1 + 32);
        if (size() > 0 && _total_kv_pairs_usage / size() > 15) {
            // std::string with size > 15 will alloc new memory for storage
            ret += _total_kv_pairs_usage;
            // an malloc extra cost estimation
            ret += size() * 8;
        }
        return ret;
    }

    void update_overlap_info(size_t overlap_size, size_t overlap_usage) override {
        _overlap_kv_pairs_usage += overlap_usage;
        _overlap_size += overlap_size;
    }

    size_t overlap_size() { return _overlap_size; }

private:
    friend ShardByLengthMutableIndex;
    friend PersistentIndex;
    phmap::flat_hash_set<KeyType, StringHasher2, EqualOnStringWithHash> _set;
    size_t _total_kv_pairs_usage = 0;
    // _overlap_num and _overlap_kv_pairs_usage will lost after be restart,
    // but it is not very important because it will be fixed in later _merge_compaction.
    size_t _overlap_size = 0;
    size_t _overlap_kv_pairs_usage = 0;
};

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

Status ShardByLengthMutableIndex::update_overlap_info(size_t key_size, size_t num_overlap, const Slice* keys,
                                                      const IndexValue* values, const KeysInfo& keys_info, bool erase) {
    DCHECK(_fixed_key_size != -1);
    const auto [shard_offset, shard_size] = _shard_info_by_key_size[key_size];
    DCHECK(shard_size == 1);
    if (key_size > 0) {
        for (size_t i = 0; i < shard_size; ++i) {
            _shards[shard_offset + i]->update_overlap_info(num_overlap, 0);
        }
    } else {
        DCHECK(key_size == 0);
        DCHECK(_fixed_key_size == 0);
        size_t overlap_size = 0;
        for (size_t i = 0; i < keys_info.size(); i++) {
            auto key_idx = keys_info.key_idxes[i];
            if (values[key_idx].get_value() != NullIndexValue) {
                overlap_size += keys[key_idx].get_size() + kIndexValueSize;
            }
        }
        if (erase) {
            overlap_size *= 2;
        }
        for (size_t i = 0; i < shard_size; ++i) {
            _shards[shard_offset + i]->update_overlap_info(num_overlap, overlap_size);
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
            RETURN_IF_ERROR(
                    _shards[shard_offset + i]->append_wal(keys, values, idxes_by_shard[i], _index_file, &_page_size));
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
                                                                      &_page_size));
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
            RETURN_IF_ERROR(
                    _shards[shard_offset + i]->append_wal(keys, values, idxes_by_shard[i], _index_file, &_page_size));
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
                                                                      &_page_size));
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
                wfile->close();
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
        meta->set_format_version(PERSISTENT_INDEX_VERSION_2);
        _offset = 0;
        _page_size = 0;
        break;
    }
    case kSnapshot: {
        std::string file_name = get_l0_index_file_name(_path, version);
        // be maybe crash after create index file during last commit
        // so we delete expired index file first to make sure no garbage left
        FileSystem::Default()->delete_file(file_name);
        phmap::BinaryOutputArchive ar_out(file_name.data());
        std::set<uint32_t> dumped_shard_idxes;
        if (!dump(ar_out, dumped_shard_idxes)) {
            std::string err_msg = strings::Substitute("failed to dump snapshot to file $0", file_name);
            LOG(WARNING) << err_msg;
            return Status::InternalError(err_msg);
        }
        // dump snapshot success, set _index_file to new snapshot file
        WritableFileOptions wblock_opts;
        wblock_opts.mode = FileSystem::MUST_EXIST;
        ASSIGN_OR_RETURN(_index_file, fs->new_writable_file(wblock_opts, file_name));
        size_t snapshot_size = dump_bound();
        meta->clear_wals();
        IndexSnapshotMetaPB* snapshot = meta->mutable_snapshot();
        version.to_pb(snapshot->mutable_version());
        PagePointerPB* data = snapshot->mutable_data();
        data->set_offset(0);
        data->set_size(snapshot_size);
        snapshot->clear_dumped_shard_idxes();
        snapshot->mutable_dumped_shard_idxes()->Add(dumped_shard_idxes.begin(), dumped_shard_idxes.end());
        meta->set_format_version(PERSISTENT_INDEX_VERSION_2);
        _offset = snapshot_size;
        _page_size = 0;
        break;
    }
    case kAppendWAL: {
        IndexWalMetaPB* wal_pb = meta->add_wals();
        version.to_pb(wal_pb->mutable_version());
        PagePointerPB* data = wal_pb->mutable_data();
        data->set_offset(_offset);
        data->set_size(_page_size);
        meta->set_format_version(PERSISTENT_INDEX_VERSION_2);
        _offset += _page_size;
        _page_size = 0;
        break;
    }
    default: {
        return Status::InternalError("Unknown commit type");
    }
    }
    return Status::OK();
}

Status ShardByLengthMutableIndex::load(const MutableIndexMetaPB& meta) {
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
    phmap::BinaryInputArchive ar(index_file_name.data());
    if (snapshot_size > 0) {
        if (!load_snapshot(ar, dumped_shard_idxes)) {
            std::string err_msg = strings::Substitute("failed load snapshot from file $0", index_file_name);
            LOG(WARNING) << err_msg;
            return Status::InternalError(err_msg);
        }
    }
    ASSIGN_OR_RETURN(auto read_file, fs->new_random_access_file(index_file_name));
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

Status ShardByLengthMutableIndex::flush_to_immutable_index(const std::string& path, const EditVersion& version) {
    auto writer = std::make_unique<ImmutableIndexWriter>();
    RETURN_IF_ERROR(writer->init(path, version));
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
                RETURN_IF_ERROR(_shards[shard_offset + i]->flush_to_immutable_index(writer, expand_exponent, npage_hint,
                                                                                    nbucket));
            }
        }
    }
    return writer->finish();
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

Status ImmutableIndex::_get_fixlen_kvs_for_shard(std::vector<std::vector<KVRef>>& kvs_by_shard, size_t shard_idx,
                                                 uint32_t shard_bits,
                                                 std::unique_ptr<ImmutableIndexShard>* shard) const {
    const auto& shard_info = _shards[shard_idx];
    for (uint32_t pageid = 0; pageid < shard_info.npage; pageid++) {
        auto& header = (*shard)->header(pageid);
        for (uint32_t bucketid = 0; bucketid < shard_info.nbucket; bucketid++) {
            auto& info = header.buckets[bucketid];
            const uint8_t* bucket_pos = (*shard)->pages[info.pageid].pack(info.packid);
            size_t nele = info.size;
            const uint8_t* kvs = bucket_pos + pad(nele, kPackSize);
            for (size_t i = 0; i < nele; i++) {
                const uint8_t* kv = kvs + (shard_info.key_size + shard_info.value_size) * i;
                IndexHash hash = IndexHash(key_index_hash(kv, shard_info.key_size));
                kvs_by_shard[hash.shard(shard_bits)].emplace_back(kv, hash.hash,
                                                                  shard_info.key_size + shard_info.value_size);
            }
        }
    }
    return Status::OK();
}

Status ImmutableIndex::_get_varlen_kvs_for_shard(std::vector<std::vector<KVRef>>& kvs_by_shard, size_t shard_idx,
                                                 uint32_t shard_bits,
                                                 std::unique_ptr<ImmutableIndexShard>* shard) const {
    const auto& shard_info = _shards[shard_idx];
    for (uint32_t pageid = 0; pageid < shard_info.npage; pageid++) {
        auto& header = (*shard)->header(pageid);
        for (uint32_t bucketid = 0; bucketid < shard_info.nbucket; bucketid++) {
            auto& info = header.buckets[bucketid];
            const uint8_t* bucket_pos = (*shard)->pages[info.pageid].pack(info.packid);
            size_t nele = info.size;
            const uint8_t* offsets = bucket_pos + pad(nele, kPackSize);
            for (size_t i = 0; i < nele; i++) {
                auto kv_offset = UNALIGNED_LOAD16(offsets + sizeof(uint16_t) * i);
                auto kv_size = UNALIGNED_LOAD16(offsets + sizeof(uint16_t) * (i + 1)) - kv_offset;
                const uint8_t* kv = bucket_pos + kv_offset;
                IndexHash hash = IndexHash(key_index_hash(kv, kv_size - shard_info.value_size));
                kvs_by_shard[hash.shard(shard_bits)].emplace_back(kv, hash.hash, kv_size);
            }
        }
    }
    return Status::OK();
}

Status ImmutableIndex::_get_kvs_for_shard(std::vector<std::vector<KVRef>>& kvs_by_shard, size_t shard_idx,
                                          uint32_t shard_bits, std::unique_ptr<ImmutableIndexShard>* shard) const {
    const auto& shard_info = _shards[shard_idx];
    if (shard_info.size == 0) {
        return Status::OK();
    }
    *shard = std::move(std::make_unique<ImmutableIndexShard>(shard_info.npage));
    RETURN_IF_ERROR(_file->read_at_fully(shard_info.offset, (*shard)->pages.data(), shard_info.bytes));
    if (shard_info.key_size != 0) {
        return _get_fixlen_kvs_for_shard(kvs_by_shard, shard_idx, shard_bits, shard);
    } else {
        return _get_varlen_kvs_for_shard(kvs_by_shard, shard_idx, shard_bits, shard);
    }
}

Status ImmutableIndex::_get_in_fixlen_shard(size_t shard_idx, size_t n, const Slice* keys, const KeysInfo& keys_info,
                                            IndexValue* values, size_t* num_found,
                                            std::unique_ptr<ImmutableIndexShard>* shard) const {
    const auto& shard_info = _shards[shard_idx];
    size_t found = 0;
    uint8_t candidate_idxes[kBucketSizeMax];
    for (size_t i = 0; i < keys_info.size(); i++) {
        IndexHash h(keys_info.hashes[i]);
        auto pageid = h.page() % shard_info.npage;
        auto bucketid = h.bucket() % shard_info.nbucket;
        auto& bucket_info = (*shard)->bucket(pageid, bucketid);
        uint8_t* bucket_pos = (*shard)->pages[bucket_info.pageid].pack(bucket_info.packid);
        auto nele = bucket_info.size;
        auto ncandidates = get_matched_tag_idxes(bucket_pos, nele, h.tag(), candidate_idxes);
        auto key_idx = keys_info.key_idxes[i];
        const uint8_t* fixed_key_probe = (const uint8_t*)keys[key_idx].data;
        auto kv_pos = bucket_pos + pad(nele, kPackSize);
        values[key_idx] = NullIndexValue;
        for (size_t candidate_idx = 0; candidate_idx < ncandidates; candidate_idx++) {
            auto idx = candidate_idxes[candidate_idx];
            auto candidate_kv = kv_pos + (shard_info.key_size + shard_info.value_size) * idx;
            if (strings::memeq(candidate_kv, fixed_key_probe, shard_info.key_size)) {
                values[key_idx] = UNALIGNED_LOAD64(candidate_kv + shard_info.key_size);
                found++;
                break;
            }
        }
    }
    *num_found += found;
    return Status::OK();
}

Status ImmutableIndex::_get_in_varlen_shard(size_t shard_idx, size_t n, const Slice* keys, const KeysInfo& keys_info,
                                            IndexValue* values, size_t* num_found,
                                            std::unique_ptr<ImmutableIndexShard>* shard) const {
    const auto& shard_info = _shards[shard_idx];
    size_t found = 0;
    uint8_t candidate_idxes[kBucketSizeMax];
    for (size_t i = 0; i < keys_info.size(); i++) {
        IndexHash h(keys_info.hashes[i]);
        auto pageid = h.page() % shard_info.npage;
        auto bucketid = h.bucket() % shard_info.nbucket;
        auto& bucket_info = (*shard)->bucket(pageid, bucketid);
        uint8_t* bucket_pos = (*shard)->pages[bucket_info.pageid].pack(bucket_info.packid);
        auto nele = bucket_info.size;
        auto ncandidates = get_matched_tag_idxes(bucket_pos, nele, h.tag(), candidate_idxes);
        auto key_idx = keys_info.key_idxes[i];
        const uint8_t* key_probe = reinterpret_cast<const uint8_t*>(keys[key_idx].data);
        auto offset_pos = bucket_pos + pad(nele, kPackSize);
        values[key_idx] = NullIndexValue;
        for (size_t candidate_idx = 0; candidate_idx < ncandidates; candidate_idx++) {
            auto idx = candidate_idxes[candidate_idx];
            auto kv_offset = UNALIGNED_LOAD16(offset_pos + sizeof(uint16_t) * idx);
            auto kv_size = UNALIGNED_LOAD16(offset_pos + sizeof(uint16_t) * (idx + 1)) - kv_offset;
            auto candidate_kv = bucket_pos + kv_offset;
            if (keys[key_idx].size == kv_size - shard_info.value_size &&
                strings::memeq(candidate_kv, key_probe, kv_size - shard_info.value_size)) {
                values[key_idx] = UNALIGNED_LOAD64(candidate_kv + kv_size - shard_info.value_size);
                found++;
                break;
            }
        }
    }
    *num_found += found;
    return Status::OK();
}

Status ImmutableIndex::_get_in_shard(size_t shard_idx, size_t n, const Slice* keys, const KeysInfo& keys_info,
                                     IndexValue* values, size_t* num_found) const {
    const auto& shard_info = _shards[shard_idx];
    if (shard_info.size == 0 || shard_info.npage == 0 || keys_info.size() == 0) {
        return Status::OK();
    }
    std::unique_ptr<ImmutableIndexShard> shard = std::make_unique<ImmutableIndexShard>(shard_info.npage);
    CHECK(shard->pages.size() * kPageSize == shard_info.bytes) << "illegal shard size";
    RETURN_IF_ERROR(_file->read_at_fully(shard_info.offset, shard->pages.data(), shard_info.bytes));
    if (shard_info.key_size != 0) {
        return _get_in_fixlen_shard(shard_idx, n, keys, keys_info, values, num_found, &shard);
    } else {
        return _get_in_varlen_shard(shard_idx, n, keys, keys_info, values, num_found, &shard);
    }
}

Status ImmutableIndex::_check_not_exist_in_fixlen_shard(size_t shard_idx, size_t n, const Slice* keys,
                                                        const KeysInfo& keys_info,
                                                        std::unique_ptr<ImmutableIndexShard>* shard) const {
    const auto& shard_info = _shards[shard_idx];
    uint8_t candidate_idxes[kBucketSizeMax];
    for (size_t i = 0; i < keys_info.size(); i++) {
        IndexHash h(keys_info.hashes[i]);
        auto pageid = h.page() % shard_info.npage;
        auto bucketid = h.bucket() % shard_info.nbucket;
        auto& bucket_info = (*shard)->bucket(pageid, bucketid);
        uint8_t* bucket_pos = (*shard)->pages[bucket_info.pageid].pack(bucket_info.packid);
        auto nele = bucket_info.size;
        auto key_idx = keys_info.key_idxes[i];
        auto ncandidates = get_matched_tag_idxes(bucket_pos, nele, h.tag(), candidate_idxes);
        const uint8_t* fixed_key_probe = (const uint8_t*)keys[key_idx].data;
        auto kv_pos = bucket_pos + pad(nele, kPackSize);
        for (size_t candidate_idx = 0; candidate_idx < ncandidates; candidate_idx++) {
            auto idx = candidate_idxes[candidate_idx];
            auto candidate_kv = kv_pos + (shard_info.key_size + shard_info.value_size) * idx;
            if (strings::memeq(candidate_kv, fixed_key_probe, shard_info.key_size)) {
                return Status::AlreadyExist("key already exists in immutable index");
            }
        }
    }
    return Status::OK();
}

Status ImmutableIndex::_check_not_exist_in_varlen_shard(size_t shard_idx, size_t n, const Slice* keys,
                                                        const KeysInfo& keys_info,
                                                        std::unique_ptr<ImmutableIndexShard>* shard) const {
    const auto& shard_info = _shards[shard_idx];
    DCHECK(shard_info.key_size == 0);
    uint8_t candidate_idxes[kBucketSizeMax];
    for (size_t i = 0; i < keys_info.size(); i++) {
        IndexHash h(keys_info.hashes[i]);
        auto pageid = h.page() % shard_info.npage;
        auto bucketid = h.bucket() % shard_info.nbucket;
        auto& bucket_info = (*shard)->bucket(pageid, bucketid);
        uint8_t* bucket_pos = (*shard)->pages[bucket_info.pageid].pack(bucket_info.packid);
        auto nele = bucket_info.size;
        auto key_idx = keys_info.key_idxes[i];
        auto ncandidates = get_matched_tag_idxes(bucket_pos, nele, h.tag(), candidate_idxes);
        const uint8_t* key_probe = reinterpret_cast<const uint8_t*>(keys[key_idx].data);
        auto offset_pos = bucket_pos + pad(nele, kPackSize);
        for (size_t candidate_idx = 0; candidate_idx < ncandidates; candidate_idx++) {
            auto idx = candidate_idxes[candidate_idx];
            auto kv_offset = UNALIGNED_LOAD16(offset_pos + sizeof(uint16_t) * idx);
            auto kv_size = UNALIGNED_LOAD16(offset_pos + sizeof(uint16_t) * (idx + 1)) - kv_offset;
            auto candidate_kv = bucket_pos + kv_offset;
            if (keys[key_idx].size == kv_size - shard_info.value_size &&
                strings::memeq(candidate_kv, key_probe, kv_size - shard_info.value_size)) {
                return Status::AlreadyExist("key already exists in immutable index");
            }
        }
    }
    return Status::OK();
}

Status ImmutableIndex::_check_not_exist_in_shard(size_t shard_idx, size_t n, const Slice* keys,
                                                 const KeysInfo& keys_info) const {
    const auto& shard_info = _shards[shard_idx];
    if (shard_info.size == 0 || keys_info.size() == 0) {
        return Status::OK();
    }
    std::unique_ptr<ImmutableIndexShard> shard = std::make_unique<ImmutableIndexShard>(shard_info.npage);
    CHECK(shard->pages.size() * kPageSize == shard_info.bytes) << "illegal shard size";
    RETURN_IF_ERROR(_file->read_at_fully(shard_info.offset, shard->pages.data(), shard_info.bytes));
    if (shard_info.key_size != 0) {
        return _check_not_exist_in_fixlen_shard(shard_idx, n, keys, keys_info, &shard);
    } else {
        return _check_not_exist_in_varlen_shard(shard_idx, n, keys, keys_info, &shard);
    }
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

Status ImmutableIndex::get(size_t n, const Slice* keys, const KeysInfo& keys_info, IndexValue* values,
                           size_t* num_found, size_t key_size) const {
    auto iter = _shard_info_by_length.find(key_size);
    if (iter == _shard_info_by_length.end()) {
        return Status::OK();
    }
    size_t found = 0;
    const auto [shard_off, nshard] = iter->second;
    if (nshard > 1) {
        std::vector<KeysInfo> keys_info_by_shard(nshard);
        split_keys_info_by_shard(keys_info, keys_info_by_shard);
        for (size_t i = 0; i < nshard; i++) {
            RETURN_IF_ERROR(_get_in_shard(shard_off + i, n, keys, keys_info_by_shard[i], values, &found));
        }
    } else {
        RETURN_IF_ERROR(_get_in_shard(shard_off, n, keys, keys_info, values, &found));
    }
    *num_found += found;
    return Status::OK();
}

Status ImmutableIndex::check_not_exist(size_t n, const Slice* keys, size_t key_size) {
    auto iter = _shard_info_by_length.find(key_size);
    if (iter == _shard_info_by_length.end()) {
        return Status::OK();
    }
    const auto [shard_off, nshard] = iter->second;
    uint32_t shard_bits = log2(nshard);
    std::vector<KeysInfo> keys_info_by_shard(nshard);
    for (size_t i = 0; i < n; i++) {
        IndexHash h(key_index_hash(keys[i].data, keys[i].size));
        auto shard = h.shard(shard_bits);
        keys_info_by_shard[shard].key_idxes.emplace_back(i);
        keys_info_by_shard[shard].hashes.emplace_back(h.hash);
    }
    for (size_t i = 0; i < nshard; i++) {
        RETURN_IF_ERROR(_check_not_exist_in_shard(shard_off + i, n, keys, keys_info_by_shard[i]));
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
    if (magic != UNALIGNED_LOAD32(kIndexFileMagic)) {
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
    size_t nshard = meta.shards_size();
    idx->_shards.resize(nshard);
    for (size_t i = 0; i < nshard; i++) {
        const auto& src = meta.shards(i);
        auto& dest = idx->_shards[i];
        dest.size = src.size();
        dest.npage = src.npage();
        dest.offset = src.data().offset();
        dest.bytes = src.data().size();
        dest.key_size = src.key_size();
        dest.value_size = src.value_size();
        dest.nbucket = src.nbucket();
        // This is for compatibility, we don't add data_size in shard_info in the rc version
        // And data_size is added to reslove some bug(https://github.com/StarRocks/starrocks/issues/11868)
        // However, if we upgrade from rc version, the data_size will be used as default value(0) which will cause
        // some error in the subsequent logic
        // So we will use file size as data_size which will cause some of disk space to be wasted, but it is a acceptable
        // problem. And the wasted disk space will be reclaimed in the subsequent compaction, so it is acceptable
        if (src.size() != 0 && src.data_size() == 0) {
            dest.data_size = src.data().size();
        } else {
            dest.data_size = src.data_size();
        }
    }
    size_t nlength = meta.shard_info_size();
    for (size_t i = 0; i < nlength; i++) {
        const auto& src = meta.shard_info(i);
        if (auto [_, inserted] =
                    idx->_shard_info_by_length.insert({src.key_size(), {src.shard_off(), src.shard_num()}});
            !inserted) {
            LOG(WARNING) << "load failed because insert shard info failed, maybe duplicate, key size: "
                         << src.key_size();
            return Status::InternalError("load failed because of insert failed");
        }
    }
    idx->_file.swap(file);
    return std::move(idx);
}

PersistentIndex::PersistentIndex(const std::string& path) : _path(path) {}

PersistentIndex::~PersistentIndex() {
    if (_l1) {
        _l1->clear();
    }
}

// Create a new empty PersistentIndex
Status PersistentIndex::create(size_t key_size, const EditVersion& version) {
    if (loaded()) {
        return Status::InternalError("PersistentIndex already loaded");
    }
    _key_size = key_size;
    _size = 0;
    _version = version;
    auto st = ShardByLengthMutableIndex::create(_key_size, _path);
    if (!st.ok()) {
        return st.status();
    }
    _l0 = std::move(st).value();
    ASSIGN_OR_RETURN(_fs, FileSystem::CreateSharedFromString(_path));
    return Status::OK();
}

Status PersistentIndex::load(const PersistentIndexMetaPB& index_meta) {
    _key_size = index_meta.key_size();
    _size = 0;
    _version = index_meta.version();
    auto st = ShardByLengthMutableIndex::create(_key_size, _path);
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
    DCHECK(_l0 != nullptr);
    RETURN_IF_ERROR(_l0->load(l0_meta));
    std::unique_ptr<RandomAccessFile> l1_rfile;
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
    auto chunk_shared_ptr = ChunkHelper::new_chunk(pkey_schema, 4096);
    auto chunk = chunk_shared_ptr.get();
    for (auto& rowset : rowsets) {
        RowsetReleaseGuard guard(rowset);
        auto res = rowset->get_segment_iterators2(pkey_schema, tablet->data_dir()->get_meta(), apply_version, &stats);
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
                    Status st;
                    if (pkc->is_binary()) {
                        st = insert(pkc->size(), reinterpret_cast<const Slice*>(pkc->raw_data()), values.data(), false);
                    } else {
                        std::vector<Slice> keys;
                        keys.reserve(pkc->size());
                        const auto* fkeys = pkc->continuous_data();
                        for (size_t i = 0; i < pkc->size(); ++i) {
                            keys.emplace_back(fkeys, _key_size);
                            fkeys += _key_size;
                        }
                        st = insert(pkc->size(), reinterpret_cast<const Slice*>(keys.data()), values.data(), false);
                    }
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
            // If format version is not equal to PERSISTENT_INDEX_VERSION_2, this maybe upgrade from
            // PERSISTENT_INDEX_VERSION_2.
            // We need to rebuild persistent index because the meta structure is changed
            if (index_meta.format_version() != PERSISTENT_INDEX_VERSION_2) {
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
    auto pkey_schema = ChunkHelper::convert_schema_to_format_v2(tablet_schema, pk_columns);
    size_t fix_size = PrimaryKeyEncoder::get_encoded_fixed_size(pkey_schema);

    // Init PersistentIndex
    _key_size = fix_size;
    _size = 0;
    _version = lastest_applied_version;
    auto st = ShardByLengthMutableIndex::create(_key_size, _path);
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
    index_meta.clear_l0_meta();
    index_meta.clear_l1_version();
    index_meta.set_key_size(_key_size);
    index_meta.set_format_version(PERSISTENT_INDEX_VERSION_2);
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
//   4. append_wal
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
    if (l1_file_size != 0) {
        // and l0 memory usage is large enough,
        if (l0_mem_size * config::l0_l1_merge_ratio > l1_file_size) {
            // do l0 l1 merge compaction
            _flushed = true;
            RETURN_IF_ERROR(_merge_compaction());
        }
        // if l1 is empty, and l0 memory usage is large enough
    } else if (l0_mem_size > kL0SnapshotSizeMax) {
        // do flush l0
        _flushed = true;
        RETURN_IF_ERROR(_flush_l0());
    }
    _dump_snapshot |= !_flushed && _l0->file_size() > config::l0_max_file_size;
    // for case1 and case2
    if (_flushed) {
        // update PersistentIndexMetaPB
        index_meta->set_size(_size);
        index_meta->set_format_version(PERSISTENT_INDEX_VERSION_2);
        _version.to_pb(index_meta->mutable_version());
        _version.to_pb(index_meta->mutable_l1_version());
        MutableIndexMetaPB* l0_meta = index_meta->mutable_l0_meta();
        RETURN_IF_ERROR(_l0->commit(l0_meta, _version, kFlush));
        // clear _l0 and reload _l1
        RETURN_IF_ERROR(_reload(*index_meta));
    } else if (_dump_snapshot) {
        index_meta->set_size(_size);
        index_meta->set_format_version(PERSISTENT_INDEX_VERSION_2);
        _version.to_pb(index_meta->mutable_version());
        MutableIndexMetaPB* l0_meta = index_meta->mutable_l0_meta();
        RETURN_IF_ERROR(_l0->commit(l0_meta, _version, kSnapshot));
    } else {
        index_meta->set_size(_size);
        index_meta->set_format_version(PERSISTENT_INDEX_VERSION_2);
        _version.to_pb(index_meta->mutable_version());
        MutableIndexMetaPB* l0_meta = index_meta->mutable_l0_meta();
        RETURN_IF_ERROR(_l0->commit(l0_meta, _version, kAppendWAL));
    }
    return Status::OK();
}

Status PersistentIndex::on_commited() {
    if (_flushed || _dump_snapshot) {
        RETURN_IF_ERROR(_delete_expired_index_file(_version, _l1_version));
    }
    _dump_snapshot = false;
    _flushed = false;
    return Status::OK();
}

Status PersistentIndex::get(size_t n, const Slice* keys, IndexValue* values) {
    std::map<size_t, KeysInfo> not_founds_by_key_size;
    size_t num_found = 0;
    RETURN_IF_ERROR(_l0->get(n, keys, values, &num_found, not_founds_by_key_size));
    if (_l1) {
        for (const auto& [key_size, keys_info] : not_founds_by_key_size) {
            RETURN_IF_ERROR(_l1->get(n, keys, keys_info, values, &num_found, key_size));
        }
    }
    return Status::OK();
}

Status PersistentIndex::upsert(size_t n, const Slice* keys, const IndexValue* values, IndexValue* old_values) {
    std::map<size_t, KeysInfo> not_founds_by_key_size;
    size_t num_found = 0;
    RETURN_IF_ERROR(_l0->upsert(n, keys, values, old_values, &num_found, not_founds_by_key_size));
    _dump_snapshot |= _can_dump_directly();
    if (_l1) {
        size_t num_found_before = num_found;
        for (const auto& [key_size, keys_info] : not_founds_by_key_size) {
            RETURN_IF_ERROR(_l1->get(n, keys, keys_info, old_values, &num_found, key_size));
            _l0->update_overlap_info(key_size, num_found - num_found_before, keys, old_values, keys_info, false);
            num_found_before = num_found;
        }
    }
    _size += n - num_found;
    if (!_dump_snapshot) {
        RETURN_IF_ERROR(_l0->append_wal(n, keys, values));
    }
    return Status::OK();
}

Status PersistentIndex::insert(size_t n, const Slice* keys, const IndexValue* values, bool check_l1) {
    std::set<size_t> check_l1_key_sizes;
    RETURN_IF_ERROR(_l0->insert(n, keys, values, check_l1_key_sizes));
    if (_l1 && check_l1) {
        for (const auto check_l1_key_size : check_l1_key_sizes) {
            RETURN_IF_ERROR(_l1->check_not_exist(n, keys, check_l1_key_size));
        }
    }
    _dump_snapshot |= _can_dump_directly();
    _size += n;
    if (!_dump_snapshot) {
        RETURN_IF_ERROR(_l0->append_wal(n, keys, values));
    }
    return Status::OK();
}

Status PersistentIndex::erase(size_t n, const Slice* keys, IndexValue* old_values) {
    std::map<size_t, KeysInfo> not_founds_by_key_size;
    size_t num_erased = 0;
    RETURN_IF_ERROR(_l0->erase(n, keys, old_values, &num_erased, not_founds_by_key_size));
    _dump_snapshot |= _can_dump_directly();
    if (_l1) {
        size_t num_erased_before = num_erased;
        for (const auto& [key_size, keys_info] : not_founds_by_key_size) {
            RETURN_IF_ERROR(_l1->get(n, keys, keys_info, old_values, &num_erased, key_size));
            _l0->update_overlap_info(key_size, num_erased - num_erased_before, keys, old_values, keys_info, true);
            num_erased_before = num_erased;
        }
    }
    CHECK(_size >= num_erased) << strings::Substitute("_size($0) < num_erased($1)", _size, num_erased);
    _size -= num_erased;
    if (!_dump_snapshot) {
        RETURN_IF_ERROR(_l0->append_wal(n, keys, nullptr));
    }
    return Status::OK();
}

[[maybe_unused]] Status PersistentIndex::try_replace(size_t n, const Slice* keys, const IndexValue* values,
                                                     const std::vector<uint32_t>& src_rssid,
                                                     std::vector<uint32_t>* failed) {
    std::vector<IndexValue> found_values;
    found_values.resize(n);
    RETURN_IF_ERROR(get(n, keys, found_values.data()));
    std::vector<size_t> replace_idxes;
    for (size_t i = 0; i < n; ++i) {
        if (found_values[i].get_value() != NullIndexValue &&
            ((uint32_t)(found_values[i].get_value() >> 32)) == src_rssid[i]) {
            replace_idxes.emplace_back(i);
        } else {
            failed->emplace_back(values[i].get_value() & 0xFFFFFFFF);
        }
    }
    RETURN_IF_ERROR(_l0->replace(keys, values, replace_idxes));
    _dump_snapshot |= _can_dump_directly();
    if (!_dump_snapshot) {
        RETURN_IF_ERROR(_l0->append_wal(keys, values, replace_idxes));
    }
    return Status::OK();
}

Status PersistentIndex::try_replace(size_t n, const Slice* keys, const IndexValue* values, const uint32_t max_src_rssid,
                                    std::vector<uint32_t>* failed) {
    std::vector<IndexValue> found_values;
    found_values.resize(n);
    RETURN_IF_ERROR(get(n, keys, found_values.data()));
    std::vector<size_t> replace_idxes;
    size_t num_not_found = 0;
    for (size_t i = 0; i < n; ++i) {
        if (found_values[i].get_value() != NullIndexValue &&
            ((uint32_t)(found_values[i].get_value() >> 32)) <= max_src_rssid) {
            replace_idxes.emplace_back(i);
        } else {
            if (found_values[i].get_value() == NullIndexValue) {
                num_not_found++;
            }
            failed->emplace_back(values[i].get_value() & 0xFFFFFFFF);
        }
    }
    RETURN_IF_ERROR(_l0->replace(keys, values, replace_idxes));
    _dump_snapshot |= _can_dump_directly();
    if (!_dump_snapshot) {
        RETURN_IF_ERROR(_l0->append_wal(keys, values, replace_idxes));
    }
    return Status::OK();
}

Status PersistentIndex::_flush_l0() {
    return _l0->flush_to_immutable_index(_path, _version);
}

Status PersistentIndex::_reload(const PersistentIndexMetaPB& index_meta) {
    auto l0_st = ShardByLengthMutableIndex::create(_key_size, _path);
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

size_t PersistentIndex::_dump_bound() {
    return (_l0 == nullptr) ? 0 : _l0->dump_bound();
}

// TODO: maybe build snapshot is better than append wals when almost
// operations are upsert or erase
bool PersistentIndex::_can_dump_directly() {
    return _dump_bound() <= kL0SnapshotSizeMax;
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

template <>
struct KVRefEq<0> {
    bool operator()(const KVRef& lhs, const KVRef& rhs) const {
        return lhs.hash == rhs.hash && lhs.size == rhs.size &&
               memcmp(lhs.kv_pos, rhs.kv_pos, lhs.size - kIndexValueSize) == 0;
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
    for (const auto& kv : l1_kvs) {
        const auto [_, inserted] = kvs_set.emplace(kv);
        DCHECK(inserted) << "duplicate key found when in l1 index";
        if (!inserted) {
            // duplicate key found, illegal
            return Status::InternalError("duplicate key found in l1 index");
        }
    }
    for (const auto& kv : l0_kvs) {
        const auto v = UNALIGNED_LOAD64(kv.kv_pos + KeySize);
        if (v == NullIndexValue) {
            // delete
            kvs_set.erase(kv);
        } else {
            auto [it, inserted] = kvs_set.emplace(kv);
            if (!inserted) {
                DCHECK(it->hash == kv.hash) << "upsert kv in set, hash should be the same";
                // TODO: find a way to modify iterator directly, currently just erase then re-insert
                // it->kv_pos = kv.kv_pos;
                kvs_set.erase(it);
                kvs_set.emplace(kv);
            }
        }
    }
    ret.reserve(ret.size() + kvs_set.size());
    for (const auto& kv : kvs_set) {
        ret.emplace_back(kv);
    }
    return Status::OK();
}

Status merge_shard_kvs_var_len(std::vector<KVRef>& l0_kvs, std::vector<KVRef>& l1_kvs, size_t estimated_size,
                               std::vector<KVRef>& ret) {
    phmap::flat_hash_set<KVRef, KVRefHash, KVRefEq<0>> kvs_set;
    kvs_set.reserve(estimated_size);
    for (auto& kv : l1_kvs) {
        const auto [_, inserted] = kvs_set.emplace(kv);
        DCHECK(inserted) << "duplicate key found when in l1 index";
        if (!inserted) {
            // duplicate key found, illegal
            return Status::InternalError("duplicate key found in l1 index");
        }
    }
    for (auto& kv : l0_kvs) {
        const uint64_t v = UNALIGNED_LOAD64(kv.kv_pos + kv.size - kIndexValueSize);
        if (v == NullIndexValue) {
            // delete
            kvs_set.erase(kv);
        } else {
            if (auto [it, inserted] = kvs_set.emplace(kv); !inserted) {
                DCHECK(it->hash == kv.hash) << "upsert kv in set, hash should be the same";
                // TODO: find a way to modify iterator directly, currently just erase then re-insert
                // it->kv_pos = kv.kv_pos;
                kvs_set.erase(it);
                kvs_set.emplace(kv);
            }
        }
    }
    ret.reserve(ret.size() + kvs_set.size());
    for (const auto& kv : kvs_set) {
        ret.emplace_back(kv);
    }
    return Status::OK();
}

static Status merge_shard_kvs(size_t key_size, std::vector<KVRef>& l0_kvs, std::vector<KVRef>& l1_kvs,
                              size_t estimated_size, std::vector<KVRef>& ret) {
    if (key_size > 0) {
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
    } else if (key_size == 0) {
        return merge_shard_kvs_var_len(l0_kvs, l1_kvs, estimated_size, ret);
    }
    return Status::OK();
}

Status PersistentIndex::_merge_compaction() {
    if (!_l1) {
        return Status::InternalError("cannot do merge_compaction without l1");
    }
    auto writer = std::make_unique<ImmutableIndexWriter>();
    RETURN_IF_ERROR(writer->init(_path, _version));
    for (const auto [key_size, shard_info] : _l0->_shard_info_by_key_size) {
        auto [l0_shard_offset, l0_shard_size] = shard_info;
        const auto l0_kv_pairs_size = std::accumulate(std::next(_l0->_shards.begin(), l0_shard_offset),
                                                      std::next(_l0->_shards.begin(), l0_shard_offset + l0_shard_size),
                                                      0UL, [](size_t s, const auto& e) { return s + e->size(); });
        size_t l0_kv_pairs_usage = 0;
        if (key_size == 0) {
            l0_kv_pairs_usage = dynamic_cast<SliceMutableIndex*>(_l0->_shards[0].get())->_total_kv_pairs_usage;
        } else {
            l0_kv_pairs_usage = (key_size + kIndexValueSize) * l0_kv_pairs_size;
        }
        auto iter = _l1->_shard_info_by_length.find(key_size);
        if (iter == _l1->_shard_info_by_length.end()) {
            if (l0_kv_pairs_size != 0) {
                const auto [nshard, npage_hint] = MutableIndex::estimate_nshard_and_npage(l0_kv_pairs_usage);
                const auto nbucket = MutableIndex::estimate_nbucket(key_size, l0_kv_pairs_size, nshard, npage_hint);
                const int expand_exponent = nshard / l0_shard_size;
                for (auto i = 0; i < l0_shard_size; ++i) {
                    RETURN_IF_ERROR(_l0->_shards[l0_shard_offset + i]->flush_to_immutable_index(writer, expand_exponent,
                                                                                                npage_hint, nbucket));
                }
            }
            continue;
        }
        const auto [l1_shard_offset, l1_shard_size] = iter->second;
        const auto l1_kv_pairs_size = std::accumulate(std::next(_l1->_shards.begin(), l1_shard_offset),
                                                      std::next(_l1->_shards.begin(), l1_shard_offset + l1_shard_size),
                                                      (size_t)0, [](size_t s, const auto& e) { return s + e.size; });
        const auto l1_kv_pairs_usage = std::accumulate(std::next(_l1->_shards.begin(), l1_shard_offset),
                                                       std::next(_l1->_shards.begin(), l1_shard_offset + l1_shard_size),
                                                       0UL, [](size_t s, const auto& e) { return s + e.data_size; });
        if (l0_kv_pairs_size == 0 && l1_kv_pairs_size != 0) {
            for (auto i = 0; i < l1_shard_size; i++) {
                RETURN_IF_ERROR(writer->write_shard_as_rawbuff(_l1->_shards[l1_shard_offset + i], _l1.get()));
            }
            continue;
        }
        auto total_kv_pairs_size = l0_kv_pairs_size + l1_kv_pairs_size;
        auto total_kv_pairs_usage = l0_kv_pairs_usage + l1_kv_pairs_usage;
        if (key_size == 0) {
            size_t overlap_usage = dynamic_cast<SliceMutableIndex*>(_l0->_shards[0].get())->_overlap_kv_pairs_usage;
            size_t overlap_size = dynamic_cast<SliceMutableIndex*>(_l0->_shards[0].get())->_overlap_size;
            CHECK(total_kv_pairs_usage >= overlap_usage);
            CHECK(total_kv_pairs_size >= overlap_size);
            total_kv_pairs_size -= overlap_size;
            total_kv_pairs_usage -= overlap_usage;
        } else {
            size_t overlap_size = _l0->_shards[l0_shard_offset]->overlap_size();
            CHECK(total_kv_pairs_size >= overlap_size);
            total_kv_pairs_size -= overlap_size;
            total_kv_pairs_usage -= (key_size + kIndexValueSize) * overlap_size;
        }
        const auto [nshard, npage_hint] = MutableIndex::estimate_nshard_and_npage(total_kv_pairs_usage);
        const auto nbucket = MutableIndex::estimate_nbucket(key_size, total_kv_pairs_size, nshard, npage_hint);
        const auto estimated_size_per_shard = total_kv_pairs_size / nshard;
        if (_key_size > 0) {
            l0_shard_offset = 0;
        }
        auto l0_kvs_by_shard = _l0->_shards[l0_shard_offset]->get_kv_refs_by_shard(nshard, l0_kv_pairs_size, false);
        std::vector<std::vector<KVRef>> l1_kvs_by_shard(nshard);
        const auto nshard_l1 = l1_shard_size;
        // shard iteration example:
        //
        // nshard_l1(4) < nshard(8):
        //          l1_shard_idx: 0     1     2     3
        //    num_shard_finished: 2     4     6     8
        //         cur_shard_idx: 0 1   2 3   4 5   6 7
        //
        // nshard_l1(4) = nshard(4):
        //          l1_shard_idx: 0     1     2     3
        //    num_shard_finished: 1     2     3     4
        //         cur_shard_idx: 0     1     2     3
        //
        // nshard_l1(8) > nshard(4):
        //          l1_shard_idx: 0  1  2  3  4  5  6  7
        //    num_shard_finished: 0  1  1  2  2  3  3  4
        //         cur_shard_idx:    0     1     2     3
        size_t cur_shard_idx = 0;
        std::vector<std::unique_ptr<ImmutableIndexShard>> index_shards((nshard_l1 / nshard) + 1);
        auto index_shards_idx = 0;
        uint32_t shard_bits = log2(nshard);
        for (size_t l1_shard_idx = 0; l1_shard_idx < nshard_l1; l1_shard_idx++) {
            RETURN_IF_ERROR(_l1->_get_kvs_for_shard(l1_kvs_by_shard, l1_shard_offset + l1_shard_idx, shard_bits,
                                                    &index_shards[index_shards_idx++]));
            size_t num_shard_finished = (l1_shard_idx + 1) * nshard / nshard_l1;
            std::vector<KVRef> kvs;
            while (cur_shard_idx < num_shard_finished) {
                kvs.clear();
                RETURN_IF_ERROR(merge_shard_kvs(key_size, l0_kvs_by_shard[cur_shard_idx],
                                                l1_kvs_by_shard[cur_shard_idx], estimated_size_per_shard, kvs));
                RETURN_IF_ERROR(writer->write_shard(key_size, npage_hint, nbucket, kvs));
                // clear to optimize memory usage
                l0_kvs_by_shard[cur_shard_idx].clear();
                l0_kvs_by_shard[cur_shard_idx].shrink_to_fit();
                l1_kvs_by_shard[cur_shard_idx].clear();
                l1_kvs_by_shard[cur_shard_idx].shrink_to_fit();
                cur_shard_idx++;
                index_shards_idx = 0;
            }
        }
    }
    return writer->finish();
}

std::vector<int8_t> PersistentIndex::test_get_move_buckets(size_t target, const uint8_t* bucket_packs_in_page) {
    return get_move_buckets(target, kBucketPerPage, bucket_packs_in_page);
}

// This function is only used for unit test and the following code is temporary
// The following test case will be refactor after L0 support varlen keys
Status PersistentIndex::test_flush_varlen_to_immutable_index(const std::string& dir, const EditVersion& version,
                                                             const size_t num_entry, const Slice* keys,
                                                             const IndexValue* values) {
    const auto total_data_size = std::accumulate(keys, keys + num_entry, 0,
                                                 [](size_t s, const auto& e) { return s + e.size + kIndexValueSize; });
    const auto [nshard, npage_hint] = MutableIndex::estimate_nshard_and_npage(total_data_size);
    const auto nbucket =
            MutableIndex::estimate_nbucket(SliceMutableIndex::kKeySizeMagicNum, num_entry, nshard, npage_hint);
    ImmutableIndexWriter writer;
    RETURN_IF_ERROR(writer.init(dir, version));
    std::vector<std::vector<KVRef>> kv_ref_by_shard(nshard);
    const auto shard_bits = log2(nshard);
    for (size_t i = 0; i < nshard; i++) {
        kv_ref_by_shard[i].reserve(num_entry / nshard * 100 / 85);
    }
    std::string kv_buf;
    kv_buf.reserve(total_data_size);
    size_t kv_offset = 0;
    for (size_t i = 0; i < num_entry; i++) {
        uint64_t hash = key_index_hash(keys[i].data, keys[i].size);
        kv_buf.append(keys[i].to_string());
        put_fixed64_le(&kv_buf, values[i].get_value());
        kv_ref_by_shard[IndexHash(hash).shard(shard_bits)].emplace_back((uint8_t*)(kv_buf.data() + kv_offset), hash,
                                                                        keys[i].size + kIndexValueSize);
        kv_offset += keys[i].size + kIndexValueSize;
    }
    for (const auto& kvs : kv_ref_by_shard) {
        RETURN_IF_ERROR(writer.write_shard(SliceMutableIndex::kKeySizeMagicNum, npage_hint, nbucket, kvs));
    }
    return writer.finish();
}

} // namespace starrocks
