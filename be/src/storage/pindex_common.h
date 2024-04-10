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

#pragma once

#include <memory>
#include <tuple>

#include "common/statusor.h"
#include "fs/fs.h"
#include "gen_cpp/persistent_index.pb.h"
#include "storage/edit_version.h"
#include "storage/rowset/bloom_filter.h"
#include "storage/rowset/rowset.h"
#include "storage/storage_engine.h"

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
constexpr size_t kLongKeySize = 64;
constexpr size_t kFixedMaxKeySize = 128;
constexpr size_t kBatchBloomFilterReadSize = 4ULL << 20;

// Add version for persistent index file to support future upgrade compatibility
// There is only one version for now
enum PersistentIndexFileVersion {
    PERSISTENT_INDEX_VERSION_UNKNOWN = 0,
    PERSISTENT_INDEX_VERSION_1,
    PERSISTENT_INDEX_VERSION_2,
    PERSISTENT_INDEX_VERSION_3,
    PERSISTENT_INDEX_VERSION_4
};

enum CommitType {
    kFlush = 0,
    kSnapshot = 1,
    kAppendWAL = 2,
};

static constexpr uint64_t NullIndexValue = -1;
static std::string MergeSuffix = ".merged";
static std::string BloomFilterSuffix = ".bf";

extern bool write_pindex_bf;

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
constexpr static size_t kSliceMaxFixLength = 64;

const char* const kIndexFileMagic = "IDX1";

struct IOStat {
    uint32_t read_iops = 0;
    uint32_t filtered_kv_cnt = 0;
    uint64_t get_in_shard_cost = 0;
    uint64_t read_io_bytes = 0;
    uint64_t l0_write_cost = 0;
    uint64_t l1_l2_read_cost = 0;
    uint64_t flush_or_wal_cost = 0;
    uint64_t compaction_cost = 0;
    uint64_t reload_meta_cost = 0;

    std::string print_str() {
        return fmt::format(
                "IOStat read_iops: {} filtered_kv_cnt: {} get_in_shard_cost: {} read_io_bytes: {} "
                "l0_write_cost: {} "
                "l1_l2_read_cost: {} flush_or_wal_cost: {} compaction_cost: {} reload_meta_cost: {}",
                read_iops, filtered_kv_cnt, get_in_shard_cost, read_io_bytes, l0_write_cost, l1_l2_read_cost,
                flush_or_wal_cost, compaction_cost, reload_meta_cost);
    }
};

struct KVRef {
    const uint8_t* kv_pos;
    uint64_t hash;
    uint16_t size;
    KVRef() = default;
    KVRef(const uint8_t* kv_pos, uint64_t hash, uint16_t size) : kv_pos(kv_pos), hash(hash), size(size) {}
};

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

struct IndexHash {
    IndexHash() = default;
    IndexHash(uint64_t hash) : hash(hash) {}
    uint64_t shard(uint32_t n) const { return (hash >> (63 - n)) >> 1; }
    uint64_t page() const { return (hash >> 16) & 0xffffffff; }
    uint64_t bucket() const { return (hash >> 8) & (kBucketPerPage - 1); }
    uint64_t tag() const { return hash & 0xff; }

    uint64_t hash;
};

using KeyInfo = std::pair<uint32_t, uint64_t>;
struct KeysInfo {
    std::vector<KeyInfo> key_infos;
    size_t size() const { return key_infos.size(); }

    void set_difference(KeysInfo& input) {
        std::vector<std::pair<uint32_t, uint64_t>> infos;
        std::set_difference(key_infos.begin(), key_infos.end(), input.key_infos.begin(), input.key_infos.end(),
                            std::back_inserter(infos), [](auto& a, auto& b) { return a.first < b.first; });
        key_infos.swap(infos);
    }
};

template <class T, class P>
T npad(T v, P p) {
    return (v + p - 1) / p;
}

template <class T, class P>
T pad(T v, P p) {
    return npad(v, p) * p;
}

std::vector<int8_t> get_move_buckets(size_t target, size_t nbucket, const uint8_t* bucket_packs_in_page);

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

    Status compress_and_write(const CompressionTypePB& compression_type, WritableFile& wb,
                              size_t* uncompressed_size) const;

    Status decompress_pages(const CompressionTypePB& compression_type, uint32_t npage, size_t uncompressed_size,
                            size_t compressed_size);

    static StatusOr<std::unique_ptr<ImmutableIndexShard>> try_create(size_t key_size, size_t npage, size_t nbucket,
                                                                     const std::vector<KVRef>& kv_refs);

    static StatusOr<std::unique_ptr<ImmutableIndexShard>> create(size_t key_size, size_t npage, size_t nbucket,
                                                                 const std::vector<KVRef>& kv_refs);

    std::vector<IndexPage> pages;
    size_t num_entry_moved = 0;
};

class ImmutableIndexWriter {
public:
    ~ImmutableIndexWriter();

    Status init(const string& idx_file_path, const EditVersion& version, bool sync_on_close);

    // write_shard() must be called serially in the order of key_size and it is caller's duty to guarantee this.
    Status write_shard(size_t key_size, size_t npage_hint, size_t nbucket, const std::vector<KVRef>& kvs);

    Status write_bf();

    Status finish();

    // return total kv count of this immutable index
    size_t total_kv_size() { return _total_kv_size; }

    size_t file_size() { return _total_kv_bytes + _total_bf_bytes; }

    bool bf_flushed() { return _bf_flushed; }

    size_t total_kv_num() { return _total; }

    std::string index_file() { return _idx_file_path; }

private:
    EditVersion _version;
    string _idx_file_path_tmp;
    string _idx_file_path;
    string _bf_file_path;
    std::shared_ptr<FileSystem> _fs;
    std::unique_ptr<WritableFile> _idx_wb;
    std::unique_ptr<WritableFile> _bf_wb;
    std::vector<size_t> _shard_bf_size;
    std::vector<std::unique_ptr<BloomFilter>> _bf_vec;
    std::map<size_t, std::pair<size_t, size_t>> _shard_info_by_length;
    size_t _nshard = 0;
    size_t _cur_key_size = -1;
    size_t _cur_value_size = 0;
    size_t _total = 0;
    size_t _total_moved = 0;
    size_t _total_kv_size = 0;
    size_t _total_kv_bytes = 0;
    size_t _total_bf_bytes = 0;
    ImmutableIndexMetaPB _meta;
    bool _bf_flushed = false;
};

} // namespace starrocks