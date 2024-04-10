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

#include "storage/pindex_common.h"

#include "fs/fs.h"
#include "storage/update_manager.h"
#include "util/crc32c.h"

namespace starrocks {

bool write_pindex_bf = true;

using KVPairPtr = const uint8_t*;

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

std::vector<int8_t> get_move_buckets(size_t target, size_t nbucket, const uint8_t* bucket_packs_in_page) {
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
    for (signed char i : idxes) {
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
    for (signed char& i : ret) {
        buckets_to_move->emplace_back(bucket_packs_in_page[i], pageid, i);
        move_packs += bucket_packs_in_page[i];
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
    MonotonicStopWatch watch;
    watch.start();
    uint64_t retry_cnt = 0;
    for (size_t npage = npage_hint; npage < kPageMax;) {
        auto rs_create = ImmutableIndexShard::try_create(key_size, npage, nbucket, kv_refs);
        // increase npage and retry
        if (!rs_create.ok()) {
            // grows at 50%
            npage = npage + npage / 2 + 1;
            retry_cnt++;
            continue;
        }
        if (retry_cnt > 10) {
            LOG(INFO) << "ImmutableIndexShard create cost(ms): " << watch.elapsed_time() / 1000000;
        }
        return std::move(rs_create.value());
    }
    return Status::InternalError("failed to create immutable index shard");
}

StatusOr<std::unique_ptr<ImmutableIndexShard>> ImmutableIndexShard::try_create(size_t key_size, size_t npage,
                                                                               size_t nbucket,
                                                                               const std::vector<KVRef>& kv_refs) {
    if (!kv_refs.empty()) {
        // This scenario should not happen in theory, since the usage and size stats by key size is not exactly
        // accurate, so we add this code as a defense
        if (npage == 0) {
            LOG(ERROR) << "find a empty shard with kvs, key size: " << key_size << ", kv_num: " << kv_refs.size();
            npage = 1;
        }
    }
    const size_t total_bucket = npage * nbucket;
    std::vector<uint8_t> bucket_sizes(total_bucket);
    std::vector<std::pair<uint32_t, std::vector<uint16_t>>> bucket_data_size(total_bucket);
    std::vector<std::pair<std::vector<KVPairPtr>, std::vector<uint8_t>>> bucket_kv_ptrs_tags(total_bucket);
    size_t estimated_entry_per_bucket = npad(kv_refs.size() * 100 / 85, total_bucket);
    for (auto& [kv_ptrs, tags] : bucket_kv_ptrs_tags) {
        kv_ptrs.reserve(estimated_entry_per_bucket);
        tags.reserve(estimated_entry_per_bucket);
    }
    for (const auto& kv_ref : kv_refs) {
        auto h = IndexHash(kv_ref.hash);
        auto page = h.page() % npage;
        auto bucket = h.bucket() % nbucket;
        auto bid = page * nbucket + bucket;
        auto& sz = bucket_sizes[bid];
        sz++;
        auto& data_size = bucket_data_size[bid].first;
        data_size += kv_ref.size;
        if (pad(sz, kPackSize) + data_size > kPageSize) {
            return Status::InternalError("bucket size limit exceeded");
        }
        bucket_data_size[bid].second.emplace_back(kv_ref.size);
        bucket_kv_ptrs_tags[bid].first.emplace_back(kv_ref.kv_pos);
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

Status ImmutableIndexShard::write(WritableFile& wb) const {
    if (pages.size() > 0) {
        return wb.append(Slice((uint8_t*)pages.data(), kPageSize * pages.size()));
    } else {
        return Status::OK();
    }
}

Status ImmutableIndexShard::compress_and_write(const CompressionTypePB& compression_type, WritableFile& wb,
                                               size_t* uncompressed_size) const {
    if (compression_type == CompressionTypePB::NO_COMPRESSION) {
        return write(wb);
    }
    if (pages.size() > 0) {
        const BlockCompressionCodec* codec = nullptr;
        RETURN_IF_ERROR(get_block_compression_codec(compression_type, &codec));
        Slice input((uint8_t*)pages.data(), kPageSize * pages.size());
        *uncompressed_size = input.get_size();
        faststring compressed_body;
        compressed_body.resize(codec->max_compressed_len(*uncompressed_size));
        Slice compressed_slice(compressed_body);
        RETURN_IF_ERROR(codec->compress(input, &compressed_slice));
        return wb.append(compressed_slice);
    } else {
        return Status::OK();
    }
}

Status ImmutableIndexShard::decompress_pages(const CompressionTypePB& compression_type, uint32_t npage,
                                             size_t uncompressed_size, size_t compressed_size) {
    if (uncompressed_size == 0) {
        // No compression
        return Status::OK();
    }
    if (kPageSize * npage != uncompressed_size) {
        return Status::Corruption(
                fmt::format("invalid uncompressed shared size, {} / {}", kPageSize * npage, uncompressed_size));
    }
    const BlockCompressionCodec* codec = nullptr;
    RETURN_IF_ERROR(get_block_compression_codec(compression_type, &codec));
    Slice compressed_body((uint8_t*)pages.data(), compressed_size);
    std::vector<IndexPage> uncompressed_pages(npage);
    Slice decompressed_body((uint8_t*)uncompressed_pages.data(), uncompressed_size);
    RETURN_IF_ERROR(codec->decompress(compressed_body, &decompressed_body));
    pages.swap(uncompressed_pages);
    return Status::OK();
}

ImmutableIndexWriter::~ImmutableIndexWriter() {
    if (_idx_wb) {
        WARN_IF_ERROR(FileSystem::Default()->delete_file(_idx_file_path_tmp),
                      "Failed to delete file:" + _idx_file_path_tmp);
    }
    if (_bf_wb) {
        WARN_IF_ERROR(FileSystem::Default()->delete_file(_bf_file_path), "Failed to delete file:" + _bf_file_path);
    }
}

Status ImmutableIndexWriter::init(const string& idx_file_path, const EditVersion& version, bool sync_on_close) {
    _version = version;
    _idx_file_path = idx_file_path;
    _idx_file_path_tmp = _idx_file_path + ".tmp";
    ASSIGN_OR_RETURN(_fs, FileSystem::CreateSharedFromString(_idx_file_path_tmp));
    WritableFileOptions wblock_opts{.sync_on_close = sync_on_close, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
    ASSIGN_OR_RETURN(_idx_wb, _fs->new_writable_file(wblock_opts, _idx_file_path_tmp));

    _bf_file_path = _idx_file_path + BloomFilterSuffix;
    ASSIGN_OR_RETURN(_bf_wb, _fs->new_writable_file(wblock_opts, _bf_file_path));
    // The minimum unit of compression is shard now, and read on a page-by-page basis is disable after compression.
    if (config::enable_pindex_compression && !config::enable_pindex_read_by_page) {
        _meta.set_compression_type(CompressionTypePB::LZ4_FRAME);
    } else {
        _meta.set_compression_type(CompressionTypePB::NO_COMPRESSION);
    }
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
            RETURN_ERROR_IF_FALSE(key_size > _cur_key_size, "key size is smaller than before");
        }
        _cur_key_size = key_size;
    }
    if (write_pindex_bf) {
        std::unique_ptr<BloomFilter> bf;
        Status st = BloomFilter::create(BLOCK_BLOOM_FILTER, &bf);
        if (!st.ok()) {
            LOG(WARNING) << "failed to create bloom filter, status: " << st;
            return st;
        }
        st = bf->init(kvs.size(), 0.05, HASH_MURMUR3_X64_64);
        if (!st.ok()) {
            LOG(WARNING) << "init bloom filter failed, status: " << st;
            return st;
        }
        for (const auto& kv : kvs) {
            bf->add_hash(kv.hash);
        }
        _shard_bf_size.emplace_back(bf->size());
        // update memory usage is too high, flush bloom filter advance to avoid use too much memory
        if (!StorageEngine::instance()->update_manager()->keep_pindex_bf()) {
            for (auto& bf : _bf_vec) {
                RETURN_IF_ERROR(_bf_wb->append(Slice(bf->data(), bf->size())));
            }
            _bf_vec.clear();
            _bf_flushed = true;
        }
        _bf_vec.emplace_back(std::move(bf));
    }

    auto rs_create = ImmutableIndexShard::create(key_size, npage_hint, nbucket, kvs);
    if (!rs_create.ok()) {
        return std::move(rs_create).status();
    }
    auto& shard = rs_create.value();
    size_t pos_before = _idx_wb->size();
    size_t uncompressed_size = 0;
    RETURN_IF_ERROR(shard->compress_and_write(static_cast<CompressionTypePB>(_meta.compression_type()), *_idx_wb,
                                              &uncompressed_size));
    size_t pos_after = _idx_wb->size();
    auto shard_meta = _meta.add_shards();
    shard_meta->set_size(kvs.size());
    shard_meta->set_npage(shard->npage());
    shard_meta->set_key_size(key_size);
    shard_meta->set_value_size(kIndexValueSize);
    shard_meta->set_nbucket(nbucket);
    shard_meta->set_uncompressed_size(uncompressed_size);
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
    _total_kv_bytes += pos_after - pos_before;
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

Status ImmutableIndexWriter::write_bf() {
    size_t pos_before = _idx_wb->size();
    LOG(INFO) << "write kv size:" << pos_before << ", _bf_wb size: " << _bf_wb->size();
    if (_bf_wb->size() != 0) {
        VLOG(10) << "_bf_wb already write size: " << _bf_wb->size();
        DCHECK(_bf_flushed);
        uint64_t remaining = _bf_wb->size();
        uint64_t offset = 0;
        std::string read_buffer;
        raw::stl_string_resize_uninitialized(&read_buffer, 4096);
        std::unique_ptr<RandomAccessFile> rfile;
        ASSIGN_OR_RETURN(rfile, _fs->new_random_access_file(_bf_file_path));
        while (remaining > 0) {
            if (remaining < 4096) {
                raw::stl_string_resize_uninitialized(&read_buffer, remaining);
            }
            RETURN_IF_ERROR(rfile->read_at_fully(offset, read_buffer.data(), read_buffer.size()));
            RETURN_IF_ERROR(_idx_wb->append(Slice(read_buffer.data(), read_buffer.size())));
            offset += read_buffer.size();
            remaining -= read_buffer.size();
        }
    }
    for (auto& bf : _bf_vec) {
        RETURN_IF_ERROR(_idx_wb->append(Slice(bf->data(), bf->size())));
    }
    _meta.mutable_shard_bf_off()->Add(pos_before);
    for (auto bf_len : _shard_bf_size) {
        _meta.mutable_shard_bf_off()->Add(pos_before + bf_len);
        pos_before += bf_len;
        _total_bf_bytes += bf_len;
    }
    if (pos_before != _idx_wb->size()) {
        std::string err_msg =
                strings::Substitute("immmutable index file size inconsistent. file: $0, expect: $1, actual: $2",
                                    _idx_wb->filename(), pos_before, _idx_wb->size());
        LOG(ERROR) << err_msg;
        return Status::InternalError(err_msg);
    }
    if (_bf_flushed) {
        _bf_vec.clear();
    }
    return Status::OK();
}

Status ImmutableIndexWriter::finish() {
    if (write_pindex_bf) {
        RETURN_IF_ERROR(write_bf());
    }
    LOG(INFO) << strings::Substitute(
            "finish writing immutable index $0 #shard:$1 #kv:$2 #moved:$3($4) kv_bytes:$5 usage:$6 bf_bytes:$7 "
            "compression_type:$8",
            _idx_file_path_tmp, _nshard, _total, _total_moved, _total_moved * 1000 / std::max(_total, 1UL) / 1000.0,
            _total_kv_bytes, _total_kv_size * 1000 / std::max(_total_kv_bytes, 1UL) / 1000.0, _total_bf_bytes,
            _meta.compression_type());
    _version.to_pb(_meta.mutable_version());
    _meta.set_size(_total);
    _meta.set_format_version(PERSISTENT_INDEX_VERSION_4);
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
    RETURN_IF_ERROR(_idx_wb->append(Slice(footer)));
    RETURN_IF_ERROR(_idx_wb->close());
    RETURN_IF_ERROR(FileSystem::Default()->rename_file(_idx_file_path_tmp, _idx_file_path));
    _idx_wb.reset();
    RETURN_IF_ERROR(_bf_wb->close());
    (void)FileSystem::Default()->delete_file(_bf_file_path);
    _bf_wb.reset();
    return Status::OK();
}

} // namespace starrocks