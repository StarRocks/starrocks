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

#include "storage/pindex_immutable_index.h"

#include "io/io_profiler.h"
#include "storage/primary_key_dump.h"
#include "storage/update_manager.h"
#include "util/crc32c.h"
#include "util/faststring.h"
#include "util/filesystem_util.h"
#include "util/xxh3.h"

namespace starrocks {

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
                auto hash = IndexHash(key_index_hash(kv, shard_info.key_size));
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
                auto hash = IndexHash(key_index_hash(kv, kv_size - shard_info.value_size));
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
    *shard = std::make_unique<ImmutableIndexShard>(shard_info.npage);
    RETURN_IF_ERROR(_file->read_at_fully(shard_info.offset, (*shard)->pages.data(), shard_info.bytes));
    RETURN_IF_ERROR((*shard)->decompress_pages(_compression_type, shard_info.npage, shard_info.uncompressed_size,
                                               shard_info.bytes));
    if (shard_info.key_size != 0) {
        return _get_fixlen_kvs_for_shard(kvs_by_shard, shard_idx, shard_bits, shard);
    } else {
        return _get_varlen_kvs_for_shard(kvs_by_shard, shard_idx, shard_bits, shard);
    }
}

Status ImmutableIndex::_get_in_fixlen_shard(size_t shard_idx, size_t n, const Slice* keys,
                                            const std::vector<KeyInfo>& keys_info, IndexValue* values,
                                            KeysInfo* found_keys_info,
                                            std::unique_ptr<ImmutableIndexShard>* shard) const {
    const auto& shard_info = _shards[shard_idx];
    uint8_t candidate_idxes[kBucketSizeMax];
    for (const auto& key_info : keys_info) {
        IndexHash h(key_info.second);
        auto pageid = h.page() % shard_info.npage;
        auto bucketid = h.bucket() % shard_info.nbucket;
        auto& bucket_info = (*shard)->bucket(pageid, bucketid);
        uint8_t* bucket_pos = (*shard)->pages[bucket_info.pageid].pack(bucket_info.packid);
        auto nele = bucket_info.size;
        auto ncandidates = get_matched_tag_idxes(bucket_pos, nele, h.tag(), candidate_idxes);
        auto key_idx = key_info.first;
        const auto* fixed_key_probe = (const uint8_t*)keys[key_idx].data;
        auto kv_pos = bucket_pos + pad(nele, kPackSize);
        values[key_idx] = NullIndexValue;
        for (size_t candidate_idx = 0; candidate_idx < ncandidates; candidate_idx++) {
            auto idx = candidate_idxes[candidate_idx];
            auto candidate_kv = kv_pos + (shard_info.key_size + shard_info.value_size) * idx;
            if (strings::memeq(candidate_kv, fixed_key_probe, shard_info.key_size)) {
                values[key_idx] = UNALIGNED_LOAD64(candidate_kv + shard_info.key_size);
                found_keys_info->key_infos.emplace_back(key_idx, h.hash);
                break;
            }
        }
    }
    return Status::OK();
}

Status ImmutableIndex::_get_in_varlen_shard(size_t shard_idx, size_t n, const Slice* keys,
                                            std::vector<KeyInfo>& keys_info, IndexValue* values,
                                            KeysInfo* found_keys_info,
                                            std::unique_ptr<ImmutableIndexShard>* shard) const {
    const auto& shard_info = _shards[shard_idx];
    uint8_t candidate_idxes[kBucketSizeMax];
    for (const auto& key_info : keys_info) {
        IndexHash h(key_info.second);
        auto pageid = h.page() % shard_info.npage;
        auto bucketid = h.bucket() % shard_info.nbucket;
        auto& bucket_info = (*shard)->bucket(pageid, bucketid);
        uint8_t* bucket_pos = (*shard)->pages[bucket_info.pageid].pack(bucket_info.packid);
        auto nele = bucket_info.size;
        auto ncandidates = get_matched_tag_idxes(bucket_pos, nele, h.tag(), candidate_idxes);
        auto key_idx = key_info.first;
        const auto* key_probe = reinterpret_cast<const uint8_t*>(keys[key_idx].data);
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
                found_keys_info->key_infos.emplace_back(key_idx, h.hash);
                break;
            }
        }
    }
    return Status::OK();
}

bool ImmutableIndex::_filter(size_t shard_idx, std::vector<KeyInfo>& keys_info, std::vector<KeyInfo>* res) const {
    // add configure enable_pindex_filter, if there are some bug exists, set it to false
    if (!config::enable_pindex_filter || _bf_off.empty()) {
        return false;
    }
    if (!_bf_vec.empty() && _bf_vec.size() <= shard_idx) {
        LOG(ERROR) << "error shard idx:" << shard_idx << ", size:" << _bf_vec.size();
        return false;
    }

    if (!_bf_vec.empty() && _bf_vec[shard_idx] != nullptr) {
        for (size_t i = 0; i < keys_info.size(); i++) {
            auto key_idx = keys_info[i].first;
            auto hash = keys_info[i].second;
            if (_bf_vec[shard_idx]->test_hash(hash)) {
                res->emplace_back(std::make_pair(key_idx, hash));
            }
        }
        return true;
    }

    // read bloom filter for specified shard
    size_t off = _bf_off[shard_idx];
    size_t len = _bf_off[shard_idx + 1] - off;
    std::string bf_buff;
    raw::stl_string_resize_uninitialized(&bf_buff, len);
    Status st = _file->read_at_fully(off, bf_buff.data(), bf_buff.size());
    if (!st.ok()) {
        LOG(WARNING) << "shard_idx: " << shard_idx << "read bloom filter failed, " << st;
        return false;
    }
    std::unique_ptr<BloomFilter> bf;
    st = BloomFilter::create(BLOCK_BLOOM_FILTER, &bf);
    if (!st.ok()) {
        LOG(WARNING) << "shard_idx: " << shard_idx << "bloom filter init failed, " << st;
        return false;
    }
    st = bf->init(bf_buff.data(), len, HASH_MURMUR3_X64_64);
    if (!st.ok()) {
        LOG(WARNING) << "shard_idx: " << shard_idx << "bloom filter init failed, " << st;
        return false;
    }
    for (size_t i = 0; i < keys_info.size(); i++) {
        auto key_idx = keys_info[i].first;
        auto hash = keys_info[i].second;
        if (bf->test_hash(hash)) {
            res->emplace_back(std::make_pair(key_idx, hash));
        }
    }
    return true;
}

Status ImmutableIndex::_split_keys_info_by_page(size_t shard_idx, std::vector<KeyInfo>& keys_info,
                                                std::map<size_t, std::vector<KeyInfo>>& keys_info_by_page) const {
    const auto& shard_info = _shards[shard_idx];
    for (size_t i = 0; i < keys_info.size(); i++) {
        auto key_idx = keys_info[i].first;
        auto hash = keys_info[i].second;
        auto pageid = IndexHash(hash).page() % shard_info.npage;
        auto iter = keys_info_by_page.find(pageid);
        if (iter == keys_info_by_page.end()) {
            std::vector<KeyInfo> k;
            k.emplace_back(key_idx, hash);
            keys_info_by_page[pageid] = std::move(k);
        } else {
            iter->second.emplace_back(key_idx, hash);
        }
    }
    return Status::OK();
}

Status ImmutableIndex::_get_in_fixlen_shard_by_page(size_t shard_idx, size_t n, const Slice* keys, IndexValue* values,
                                                    KeysInfo* found_keys_info,
                                                    std::map<size_t, std::vector<KeyInfo>>& keys_info_by_page,
                                                    std::map<size_t, IndexPage>& pages) const {
    const auto& shard_info = _shards[shard_idx];
    uint8_t candidate_idxes[kBucketSizeMax];
    for (auto [_, keys_info] : keys_info_by_page) {
        for (size_t i = 0; i < keys_info.size(); i++) {
            IndexHash h(keys_info[i].second);
            auto pageid = h.page() % shard_info.npage;
            auto bucketid = h.bucket() % shard_info.nbucket;
            auto iter = pages.find(pageid);
            RETURN_ERROR_IF_FALSE(iter != pages.end());
            auto& bucket_info = iter->second.header().buckets[bucketid];
            uint8_t* bucket_pos;
            if (pageid == bucket_info.pageid) {
                bucket_pos = iter->second.pack(bucket_info.packid);
            } else {
                auto it = pages.find(bucket_info.pageid);
                if (it != pages.end()) {
                    bucket_pos = it->second.pack(bucket_info.packid);
                } else {
                    IndexPage page;
                    RETURN_IF_ERROR(_file->read_at_fully(shard_info.offset + kPageSize * bucket_info.pageid, page.data,
                                                         kPageSize));
                    pages[bucket_info.pageid] = std::move(page);
                    bucket_pos = pages[bucket_info.pageid].pack(bucket_info.packid);
                }
            }
            auto nele = bucket_info.size;
            auto ncandidates = get_matched_tag_idxes(bucket_pos, nele, h.tag(), candidate_idxes);
            auto key_idx = keys_info[i].first;
            const auto* fixed_key_probe = (const uint8_t*)keys[key_idx].data;
            auto kv_pos = bucket_pos + pad(nele, kPackSize);
            values[key_idx] = NullIndexValue;
            for (size_t candidate_idx = 0; candidate_idx < ncandidates; candidate_idx++) {
                auto idx = candidate_idxes[candidate_idx];
                auto candidate_kv = kv_pos + (shard_info.key_size + shard_info.value_size) * idx;
                if (strings::memeq(candidate_kv, fixed_key_probe, shard_info.key_size)) {
                    values[key_idx] = UNALIGNED_LOAD64(candidate_kv + shard_info.key_size);
                    found_keys_info->key_infos.emplace_back(key_idx, h.hash);
                    break;
                }
            }
        }
    }
    return Status::OK();
}

Status ImmutableIndex::_get_in_varlen_shard_by_page(size_t shard_idx, size_t n, const Slice* keys, IndexValue* values,
                                                    KeysInfo* found_keys_info,
                                                    std::map<size_t, std::vector<KeyInfo>>& keys_info_by_page,
                                                    std::map<size_t, IndexPage>& pages) const {
    const auto& shard_info = _shards[shard_idx];
    uint8_t candidate_idxes[kBucketSizeMax];
    for (auto [_, keys_info] : keys_info_by_page) {
        for (size_t i = 0; i < keys_info.size(); i++) {
            IndexHash h(keys_info[i].second);
            auto pageid = h.page() % shard_info.npage;
            auto bucketid = h.bucket() % shard_info.nbucket;
            auto iter = pages.find(pageid);
            RETURN_ERROR_IF_FALSE(iter != pages.end());
            auto& bucket_info = iter->second.header().buckets[bucketid];
            uint8_t* bucket_pos;
            if (pageid == bucket_info.pageid) {
                bucket_pos = iter->second.pack(bucket_info.packid);
            } else {
                auto it = pages.find(bucket_info.pageid);
                if (it != pages.end()) {
                    bucket_pos = it->second.pack(bucket_info.packid);
                } else {
                    IndexPage page;
                    RETURN_IF_ERROR(_file->read_at_fully(shard_info.offset + kPageSize * bucket_info.pageid, page.data,
                                                         kPageSize));
                    pages[bucket_info.pageid] = std::move(page);
                    bucket_pos = pages[bucket_info.pageid].pack(bucket_info.packid);
                }
            }
            auto nele = bucket_info.size;
            auto ncandidates = get_matched_tag_idxes(bucket_pos, nele, h.tag(), candidate_idxes);
            auto key_idx = keys_info[i].first;
            const auto* key_probe = reinterpret_cast<const uint8_t*>(keys[key_idx].data);
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
                    found_keys_info->key_infos.emplace_back(key_idx, h.hash);
                    break;
                }
            }
        }
    }
    return Status::OK();
}

Status ImmutableIndex::_get_in_shard_by_page(size_t shard_idx, size_t n, const Slice* keys, IndexValue* values,
                                             KeysInfo* found_keys_info,
                                             std::map<size_t, std::vector<KeyInfo>>& keys_info_by_page,
                                             IOStat* stat) const {
    const auto& shard_info = _shards[shard_idx];
    std::map<size_t, IndexPage> pages;
    for (auto [pageid, keys_info] : keys_info_by_page) {
        IndexPage page;
        RETURN_IF_ERROR(_file->read_at_fully(shard_info.offset + kPageSize * pageid, page.data, kPageSize));
        if (stat != nullptr) {
            stat->read_iops++;
            stat->read_io_bytes += kPageSize;
        }
        pages[pageid] = std::move(page);
    }
    if (shard_info.key_size != 0) {
        return _get_in_fixlen_shard_by_page(shard_idx, n, keys, values, found_keys_info, keys_info_by_page, pages);
    } else {
        return _get_in_varlen_shard_by_page(shard_idx, n, keys, values, found_keys_info, keys_info_by_page, pages);
    }
}

Status ImmutableIndex::pk_dump(PrimaryKeyDump* dump, PrimaryIndexDumpPB* dump_pb) {
    // put all kvs in one shard
    std::vector<std::vector<KVRef>> kvs_by_shard(1);
    for (size_t shard_idx = 0; shard_idx < _shards.size(); shard_idx++) {
        const auto& shard_info = _shards[shard_idx];
        if (shard_info.size == 0) {
            // skip empty shard
            continue;
        }
        auto shard = std::make_unique<ImmutableIndexShard>(shard_info.npage);
        RETURN_IF_ERROR(_file->read_at_fully(shard_info.offset, shard->pages.data(), shard_info.bytes));
        RETURN_IF_ERROR(shard->decompress_pages(_compression_type, shard_info.npage, shard_info.uncompressed_size,
                                                shard_info.bytes));
        if (shard_info.key_size != 0) {
            RETURN_IF_ERROR(_get_fixlen_kvs_for_shard(kvs_by_shard, shard_idx, 0, &shard));
        } else {
            RETURN_IF_ERROR(_get_varlen_kvs_for_shard(kvs_by_shard, shard_idx, 0, &shard));
        }
    }

    // read kv from KVRef
    for (const auto& each : kvs_by_shard) {
        for (const auto& each_kv : each) {
            auto value = UNALIGNED_LOAD64(each_kv.kv_pos + each_kv.size - kIndexValueSize);
            RETURN_IF_ERROR(dump->add_pindex_kvs(
                    std::string_view(reinterpret_cast<const char*>(each_kv.kv_pos), each_kv.size - kIndexValueSize),
                    value, dump_pb));
        }
    }
    return dump->finish_pindex_kvs(dump_pb);
}

Status ImmutableIndex::_get_in_shard(size_t shard_idx, size_t n, const Slice* keys, std::vector<KeyInfo>& keys_info,
                                     IndexValue* values, KeysInfo* found_keys_info, IOStat* stat) const {
    const auto& shard_info = _shards[shard_idx];
    if (shard_info.size == 0 || shard_info.npage == 0 || keys_info.size() == 0) {
        return Status::OK();
    }

    DCHECK(_bf_vec.empty() || _bf_vec.size() > shard_idx);
    std::vector<KeyInfo> check_keys_info;
    bool filter = _filter(shard_idx, keys_info, &check_keys_info);
    if (!filter) {
        check_keys_info.swap(keys_info);
    } else {
        if (stat != nullptr) {
            stat->filtered_kv_cnt += (keys_info.size() - check_keys_info.size());
        }
    }

    if (check_keys_info.empty()) {
        // All keys have been filtered by bloom filter.
        return Status::OK();
    }

    if (config::enable_pindex_read_by_page && shard_info.uncompressed_size == 0) {
        std::map<size_t, std::vector<KeyInfo>> keys_info_by_page;
        RETURN_IF_ERROR(_split_keys_info_by_page(shard_idx, check_keys_info, keys_info_by_page));
        return _get_in_shard_by_page(shard_idx, n, keys, values, found_keys_info, keys_info_by_page, stat);
    }

    std::unique_ptr<ImmutableIndexShard> shard = std::make_unique<ImmutableIndexShard>(shard_info.npage);
    if (shard_info.uncompressed_size == 0) {
        RETURN_ERROR_IF_FALSE(shard->pages.size() * kPageSize == shard_info.bytes, "illegal shard size");
    } else {
        RETURN_ERROR_IF_FALSE(shard->pages.size() * kPageSize == shard_info.uncompressed_size, "illegal shard size");
    }
    RETURN_IF_ERROR(_file->read_at_fully(shard_info.offset, shard->pages.data(), shard_info.bytes));
    RETURN_IF_ERROR(shard->decompress_pages(_compression_type, shard_info.npage, shard_info.uncompressed_size,
                                            shard_info.bytes));
    if (stat != nullptr) {
        stat->read_iops++;
        stat->read_io_bytes += shard_info.bytes;
    }
    if (shard_info.key_size != 0) {
        return _get_in_fixlen_shard(shard_idx, n, keys, check_keys_info, values, found_keys_info, &shard);
    } else {
        return _get_in_varlen_shard(shard_idx, n, keys, check_keys_info, values, found_keys_info, &shard);
    }
}

Status ImmutableIndex::_check_not_exist_in_fixlen_shard(size_t shard_idx, size_t n, const Slice* keys,
                                                        const KeysInfo& keys_info,
                                                        std::unique_ptr<ImmutableIndexShard>* shard) const {
    const auto& shard_info = _shards[shard_idx];
    uint8_t candidate_idxes[kBucketSizeMax];
    for (size_t i = 0; i < keys_info.size(); i++) {
        IndexHash h(keys_info.key_infos[i].second);
        auto pageid = h.page() % shard_info.npage;
        auto bucketid = h.bucket() % shard_info.nbucket;
        auto& bucket_info = (*shard)->bucket(pageid, bucketid);
        uint8_t* bucket_pos = (*shard)->pages[bucket_info.pageid].pack(bucket_info.packid);
        auto nele = bucket_info.size;
        auto key_idx = keys_info.key_infos[i].first;
        auto ncandidates = get_matched_tag_idxes(bucket_pos, nele, h.tag(), candidate_idxes);
        const auto* fixed_key_probe = (const uint8_t*)keys[key_idx].data;
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
        IndexHash h(keys_info.key_infos[i].second);
        auto pageid = h.page() % shard_info.npage;
        auto bucketid = h.bucket() % shard_info.nbucket;
        auto& bucket_info = (*shard)->bucket(pageid, bucketid);
        uint8_t* bucket_pos = (*shard)->pages[bucket_info.pageid].pack(bucket_info.packid);
        auto nele = bucket_info.size;
        auto key_idx = keys_info.key_infos[i].first;
        auto ncandidates = get_matched_tag_idxes(bucket_pos, nele, h.tag(), candidate_idxes);
        const auto* key_probe = reinterpret_cast<const uint8_t*>(keys[key_idx].data);
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
    if (shard_info.uncompressed_size == 0) {
        RETURN_ERROR_IF_FALSE(shard->pages.size() * kPageSize == shard_info.bytes, "illegal shard size");
    } else {
        RETURN_ERROR_IF_FALSE(shard->pages.size() * kPageSize == shard_info.uncompressed_size, "illegal shard size");
    }
    RETURN_IF_ERROR(_file->read_at_fully(shard_info.offset, shard->pages.data(), shard_info.bytes));
    RETURN_IF_ERROR(shard->decompress_pages(_compression_type, shard_info.npage, shard_info.uncompressed_size,
                                            shard_info.bytes));
    if (shard_info.key_size != 0) {
        return _check_not_exist_in_fixlen_shard(shard_idx, n, keys, keys_info, &shard);
    } else {
        return _check_not_exist_in_varlen_shard(shard_idx, n, keys, keys_info, &shard);
    }
}

static void split_keys_info_by_shard(std::vector<KeyInfo>& keys_info, std::vector<KeysInfo>& keys_info_by_shards) {
    uint32_t shard_bits = log2(keys_info_by_shards.size());
    for (const auto& key_info : keys_info) {
        auto key_idx = key_info.first;
        auto hash = key_info.second;
        size_t shard = IndexHash(hash).shard(shard_bits);
        keys_info_by_shards[shard].key_infos.emplace_back(key_idx, hash);
    }
}

bool ImmutableIndex::_need_bloom_filter(size_t idx_begin, size_t idx_end,
                                        std::vector<KeysInfo>& keys_info_by_shard) const {
    if (_bf_off.empty()) {
        return false;
    }

    if (!config::enable_pindex_filter || !StorageEngine::instance()->update_manager()->keep_pindex_bf()) {
        return false;
    }

    DCHECK(idx_end < _bf_off.size());
    size_t bf_bytes = _bf_off[idx_end] - _bf_off[idx_begin];
    size_t read_shard_bytes = 0;
    for (size_t i = 0; i < keys_info_by_shard.size(); i++) {
        if (!keys_info_by_shard[i].key_infos.empty()) {
            read_shard_bytes += _shards[i].bytes;
        }
    }
    return bf_bytes * config::max_bf_read_bytes_percent <= read_shard_bytes;
}

// There are several conditions
// 1. enable_pindex_filter is false, bloom filter is disable
// 2. _bf_off is empty which means there are no bloom filter exist in index file, this could be happened when we upgrade from
//    elder version
// 3. bloom filter already kept in memory
// 4. bloom filter is not in memory and memory usage is too high, skip the bloom filter to reduce memory usage
// 5. bloom filter is not in memory and memory usage is not high, we will read bloom filter from index file
Status ImmutableIndex::_prepare_bloom_filter(size_t idx_begin, size_t idx_end) const {
    if (!config::enable_pindex_filter || _bf_off.empty()) {
        return Status::OK();
    }
    if (_bf_vec.empty()) {
        _bf_vec.resize(_shards.size());
    }
    DCHECK(idx_begin < idx_end);
    DCHECK(_bf_vec.size() >= _shards.size() && _bf_vec.size() >= idx_end);
    if (_bf_vec.size() < _shards.size()) {
        return Status::OK();
    }
    // alread loaded in memory
    if (_bf_vec[idx_begin] != nullptr) {
        return Status::OK();
    }
    DCHECK(_bf_off.size() > idx_end);
    size_t batch_bytes = kBatchBloomFilterReadSize;
    size_t read_bytes = 0;
    size_t start_idx = idx_begin;
    size_t num = 0;
    for (size_t i = idx_begin; i < idx_end; i++) {
        if (read_bytes >= batch_bytes) {
            size_t offset = _bf_off[start_idx];
            size_t bytes = _bf_off[start_idx + num] - offset;
            std::string buff;
            raw::stl_string_resize_uninitialized(&buff, bytes);
            RETURN_IF_ERROR(_file->read_at_fully(offset, buff.data(), buff.size()));
            for (size_t i = 0; i < num; i++) {
                size_t buff_off = _bf_off[start_idx + i] - _bf_off[start_idx];
                size_t buff_size = _bf_off[start_idx + i + 1] - _bf_off[start_idx + i];
                std::unique_ptr<BloomFilter> bf;
                RETURN_IF_ERROR(BloomFilter::create(BLOCK_BLOOM_FILTER, &bf));
                RETURN_IF_ERROR(bf->init(buff.data() + buff_off, buff_size, HASH_MURMUR3_X64_64));
                _bf_vec[start_idx + i] = std::move(bf);
            }
            start_idx = i;
            read_bytes = _bf_off[i + 1] - _bf_off[i];
            num = 1;
        } else {
            num++;
            read_bytes += _bf_off[i + 1] - _bf_off[i];
        }
    }
    if (start_idx < idx_end) {
        size_t offset = _bf_off[start_idx];
        size_t bytes = _bf_off[start_idx + num] - offset;
        std::string buff;
        raw::stl_string_resize_uninitialized(&buff, bytes);
        RETURN_IF_ERROR(_file->read_at_fully(offset, buff.data(), buff.size()));
        for (size_t i = 0; i < num; i++) {
            size_t buff_off = _bf_off[start_idx + i] - _bf_off[start_idx];
            size_t buff_size = _bf_off[start_idx + i + 1] - _bf_off[start_idx + i];
            std::unique_ptr<BloomFilter> bf;
            RETURN_IF_ERROR(BloomFilter::create(BLOCK_BLOOM_FILTER, &bf));
            RETURN_IF_ERROR(bf->init(buff.data() + buff_off, buff_size, HASH_MURMUR3_X64_64));
            _bf_vec[start_idx + i] = std::move(bf);
        }
    }
    return Status::OK();
}

Status ImmutableIndex::get(size_t n, const Slice* keys, KeysInfo& keys_info, IndexValue* values,
                           KeysInfo* found_keys_info, size_t key_size, IOStat* stat) {
    auto iter = _shard_info_by_length.find(key_size);
    if (iter == _shard_info_by_length.end()) {
        return Status::OK();
    }

    const auto [shard_off, nshard] = iter->second;
    if (nshard > 1) {
        std::vector<KeysInfo> keys_info_by_shard(nshard);
        MonotonicStopWatch watch;
        watch.start();
        split_keys_info_by_shard(keys_info.key_infos, keys_info_by_shard);
        if (_need_bloom_filter(shard_off, shard_off + nshard, keys_info_by_shard)) {
            RETURN_IF_ERROR(_prepare_bloom_filter(shard_off, shard_off + nshard));
        }
        for (size_t i = 0; i < nshard; i++) {
            RETURN_IF_ERROR(_get_in_shard(shard_off + i, n, keys, keys_info_by_shard[i].key_infos, values,
                                          found_keys_info, stat));
        }
        if (stat != nullptr) {
            stat->get_in_shard_cost += watch.elapsed_time();
        }
    } else {
        MonotonicStopWatch watch;
        watch.start();
        KeysInfo infos;
        infos.key_infos.assign(keys_info.key_infos.begin(), keys_info.key_infos.end());
        if (config::enable_pindex_filter && StorageEngine::instance()->update_manager()->keep_pindex_bf()) {
            RETURN_IF_ERROR(_prepare_bloom_filter(shard_off, shard_off + nshard));
        }
        RETURN_IF_ERROR(_get_in_shard(shard_off, n, keys, infos.key_infos, values, found_keys_info, stat));
        if (stat != nullptr) {
            stat->get_in_shard_cost += watch.elapsed_time();
        }
    }
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
        keys_info_by_shard[shard].key_infos.emplace_back(i, h.hash);
    }
    for (size_t i = 0; i < nshard; i++) {
        RETURN_IF_ERROR(_check_not_exist_in_shard(shard_off + i, n, keys, keys_info_by_shard[i]));
    }
    return Status::OK();
}

StatusOr<std::unique_ptr<ImmutableIndex>> ImmutableIndex::load(std::unique_ptr<RandomAccessFile>&& file,
                                                               bool load_bf_data) {
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

    auto format_version = meta.format_version();
    if (format_version != PERSISTENT_INDEX_VERSION_2 && format_version != PERSISTENT_INDEX_VERSION_3 &&
        format_version != PERSISTENT_INDEX_VERSION_4) {
        std::string msg =
                strings::Substitute("different immutable index format, should rebuid index. actual:$0, expect:$1",
                                    format_version, PERSISTENT_INDEX_VERSION_4);
        LOG(WARNING) << msg;
        return Status::InternalError(msg);
    }

    std::unique_ptr<ImmutableIndex> idx = std::make_unique<ImmutableIndex>();
    idx->_version = EditVersion(meta.version());
    idx->_size = meta.size();
    if (meta.compression_type() > 0) {
        idx->_compression_type = static_cast<CompressionTypePB>(meta.compression_type());
    } else {
        idx->_compression_type = CompressionTypePB::NO_COMPRESSION;
    }
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
        dest.uncompressed_size = src.uncompressed_size();
        if (idx->_compression_type == CompressionTypePB::NO_COMPRESSION) {
            RETURN_ERROR_IF_FALSE(dest.uncompressed_size == 0,
                                  "compression type: " + std::to_string(idx->_compression_type) +
                                          " uncompressed_size: " + std::to_string(dest.uncompressed_size));
        }
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

    std::vector<std::unique_ptr<BloomFilter>> bf_vec(nshard);
    size_t nshard_bf = meta.shard_bf_off_size();
    DCHECK(nshard_bf == 0 || nshard_bf == nshard + 1);
    std::vector<size_t> bf_off;
    for (size_t i = 0; i < nshard_bf; i++) {
        bf_off.emplace_back(meta.shard_bf_off(i));
    }

    if (load_bf_data && nshard_bf != 0) {
        size_t batch_bytes = kBatchBloomFilterReadSize;
        size_t read_bytes = 0;
        size_t start_idx = 0;
        size_t num = 0;
        for (size_t i = 0; i < nshard; i++) {
            if (read_bytes >= batch_bytes) {
                size_t offset = bf_off[start_idx];
                size_t bytes = bf_off[start_idx + num] - offset;
                std::string buff;
                raw::stl_string_resize_uninitialized(&buff, bytes);
                RETURN_IF_ERROR(file->read_at_fully(offset, buff.data(), buff.size()));
                for (size_t i = 0; i < num; i++) {
                    size_t buff_off = bf_off[start_idx + i] - bf_off[start_idx];
                    size_t buff_size = bf_off[start_idx + i + 1] - bf_off[start_idx + i];
                    std::unique_ptr<BloomFilter> bf;
                    RETURN_IF_ERROR(BloomFilter::create(BLOCK_BLOOM_FILTER, &bf));
                    RETURN_IF_ERROR(bf->init(buff.data() + buff_off, buff_size, HASH_MURMUR3_X64_64));
                    bf_vec[start_idx + i] = std::move(bf);
                }
                start_idx = i;
                read_bytes = bf_off[i + 1] - bf_off[i];
                num = 1;
            } else {
                num++;
                read_bytes += bf_off[i + 1] - bf_off[i];
            }
        }
        if (start_idx < nshard) {
            size_t offset = bf_off[start_idx];
            size_t bytes = bf_off[start_idx + num] - offset;
            std::string buff;
            raw::stl_string_resize_uninitialized(&buff, bytes);
            RETURN_IF_ERROR(file->read_at_fully(offset, buff.data(), buff.size()));
            for (size_t i = 0; i < num; i++) {
                size_t buff_off = bf_off[start_idx + i] - bf_off[start_idx];
                size_t buff_size = bf_off[start_idx + i + 1] - bf_off[start_idx + i];
                std::unique_ptr<BloomFilter> bf;
                RETURN_IF_ERROR(BloomFilter::create(BLOCK_BLOOM_FILTER, &bf));
                RETURN_IF_ERROR(bf->init(buff.data() + buff_off, buff_size, HASH_MURMUR3_X64_64));
                bf_vec[start_idx + i] = std::move(bf);
            }
        }
        idx->_bf_vec.swap(bf_vec);
    }
    idx->_file.swap(file);
    idx->_bf_off.swap(bf_off);
    return std::move(idx);
}

} // namespace starrocks