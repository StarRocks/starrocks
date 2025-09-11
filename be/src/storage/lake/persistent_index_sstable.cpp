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

#include "storage/lake/persistent_index_sstable.h"

#include <butil/time.h> // NOLINT

#include "fs/fs.h"
#include "fs/key_cache.h"
#include "gen_cpp/types.pb.h"
#include "storage/lake/lake_delvec_loader.h"
#include "storage/lake/utils.h"
#include "storage/sstable/table_builder.h"
#include "util/trace.h"

namespace starrocks::lake {

Status PersistentIndexSstable::init(std::unique_ptr<RandomAccessFile> rf, const PersistentIndexSstablePB& sstable_pb,
                                    Cache* cache, TabletManager* tablet_mgr, int64_t tablet_id, bool need_filter) {
    sstable::Options options;
    if (need_filter) {
        _filter_policy.reset(const_cast<sstable::FilterPolicy*>(sstable::NewBloomFilterPolicy(10)));
        options.filter_policy = _filter_policy.get();
    }
    options.block_cache = cache;
    sstable::Table* table;
    RETURN_IF_ERROR(sstable::Table::Open(options, rf.get(), sstable_pb.filesize(), &table));
    _sst.reset(table);
    _rf = std::move(rf);
    _sstable_pb.CopyFrom(sstable_pb);
    // load delvec
    if (_sstable_pb.has_delvec()) {
        // load delvec
        SegmentReadOptions seg_options;
        auto delvec_loader = std::make_unique<LakeDelvecLoader>(tablet_mgr, nullptr, true /* fill cache */,
                                                                seg_options.lake_io_opts);
        RETURN_IF_ERROR(delvec_loader->load(TabletSegmentId(tablet_id, _sstable_pb.shared_rssid()),
                                            _sstable_pb.shared_version(), &_delvec));
    }
    return Status::OK();
}

Status PersistentIndexSstable::build_sstable(const phmap::btree_map<std::string, IndexValueWithVer, std::less<>>& map,
                                             WritableFile* wf, uint64_t* filesz) {
    std::unique_ptr<sstable::FilterPolicy> filter_policy;
    filter_policy.reset(const_cast<sstable::FilterPolicy*>(sstable::NewBloomFilterPolicy(10)));
    sstable::Options options;
    options.filter_policy = filter_policy.get();
    sstable::TableBuilder builder(options, wf);
    for (const auto& [k, v] : map) {
        IndexValuesWithVerPB index_value_pb;
        auto* value = index_value_pb.add_values();
        value->set_version(v.first);
        value->set_rssid(v.second.get_rssid());
        value->set_rowid(v.second.get_rowid());
        builder.Add(Slice(k), Slice(index_value_pb.SerializeAsString()));
    }
    RETURN_IF_ERROR(builder.Finish());
    *filesz = builder.FileSize();
    return Status::OK();
}

Status PersistentIndexSstable::multi_get(const Slice* keys, const KeyIndexSet& key_indexes, int64_t version,
                                         IndexValue* values, KeyIndexSet* found_key_indexes) const {
    std::vector<std::string> index_value_with_vers(key_indexes.size());
    sstable::ReadIOStat stat;
    sstable::ReadOptions options;
    options.stat = &stat;
    // Currently, there is no need to set predicate for MultiGet of persistent index sstable. Because predicate
    // only used for sstable compaction to filter out some keys for tablet split purpose and such keys can not
    // be read by the persistent index by designed. So even we provide a predicate, all keys read by multi_get
    // will always meet the condition.
    auto start_ts = butil::gettimeofday_us();
    RETURN_IF_ERROR(_sst->MultiGet(options, keys, key_indexes.begin(), key_indexes.end(), &index_value_with_vers));
    auto end_ts = butil::gettimeofday_us();
    TRACE_COUNTER_INCREMENT("multi_get_us", end_ts - start_ts);
    TRACE_COUNTER_INCREMENT("read_block_hit_cache_cnt", stat.block_cnt_from_cache);
    TRACE_COUNTER_INCREMENT("read_block_miss_cache_cnt", stat.block_cnt_from_file);
    size_t i = 0;
    for (auto& key_index : key_indexes) {
        // Index_value_with_vers is empty means key is not found in sst.
        // Value in sst can not be empty.
        if (index_value_with_vers[i].empty()) {
            ++i;
            continue;
        }
        IndexValuesWithVerPB index_value_with_ver_pb;
        if (!index_value_with_ver_pb.ParseFromString(index_value_with_vers[i])) {
            return Status::InternalError("parse index value info failed");
        }
        // Check if this rowid is already filtered by delvec
        if (_delvec) {
            if (_delvec->roaring()->contains(index_value_with_ver_pb.values(0).rowid())) {
                ++i;
                continue;
            }
        }
        // fill shared rssid & version if have
        if (_sstable_pb.has_shared_version() && _sstable_pb.shared_version() > 0) {
            DCHECK(_sstable_pb.has_shared_rssid());
            for (size_t j = 0; j < index_value_with_ver_pb.values_size(); ++j) {
                index_value_with_ver_pb.mutable_values(j)->set_rssid(_sstable_pb.shared_rssid());
                index_value_with_ver_pb.mutable_values(j)->set_version(_sstable_pb.shared_version());
            }
        }

        if (index_value_with_ver_pb.values_size() > 0) {
            if (version < 0) {
                values[key_index] = build_index_value(index_value_with_ver_pb.values(0));
                found_key_indexes->insert(key_index);
            } else {
                for (size_t j = 0; j < index_value_with_ver_pb.values_size(); ++j) {
                    if (index_value_with_ver_pb.values(j).version() == version) {
                        values[key_index] = build_index_value(index_value_with_ver_pb.values(j));
                        found_key_indexes->insert(key_index);
                        break;
                    }
                }
            }
        }
        ++i;
    }
    return Status::OK();
}

size_t PersistentIndexSstable::memory_usage() const {
    return (_sst != nullptr) ? _sst->memory_usage() : 0;
}

PersistentIndexSstableStreamBuilder::PersistentIndexSstableStreamBuilder(std::unique_ptr<WritableFile> wf,
                                                                         std::string encryption_meta)
        : _wf(std::move(wf)), _finished(false), _encryption_meta(std::move(encryption_meta)) {
    _filter_policy.reset(const_cast<sstable::FilterPolicy*>(sstable::NewBloomFilterPolicy(10)));
    sstable::Options options;
    options.filter_policy = _filter_policy.get();
    _table_builder = std::make_unique<sstable::TableBuilder>(options, _wf.get());
}

Status PersistentIndexSstableStreamBuilder::add(const Slice& key) {
    if (_finished) {
        return Status::InvalidArgument("Builder already finished");
    }

    if (!_status.ok()) {
        return _status;
    }

    IndexValuesWithVerPB index_value_pb;
    auto* val = index_value_pb.add_values();
    val->set_rowid(_sst_rowid++);

    _table_builder->Add(key, Slice(index_value_pb.SerializeAsString()));
    _status = _table_builder->status();
    return _status;
}

Status PersistentIndexSstableStreamBuilder::finish(uint64_t* file_size) {
    if (_finished) {
        return Status::InvalidArgument("Builder already finished");
    }

    if (!_status.ok()) {
        return _status;
    }

    _status = _table_builder->Finish();
    if (_status.ok()) {
        _finished = true;
        if (file_size != nullptr) {
            *file_size = _table_builder->FileSize();
        }
    }
    return _status;
}

uint64_t PersistentIndexSstableStreamBuilder::num_entries() const {
    return _table_builder ? _table_builder->NumEntries() : 0;
}

FileInfo PersistentIndexSstableStreamBuilder::file_info() const {
    FileInfo file_info;
    file_info.path = file_name(_wf->filename());
    file_info.size = _table_builder ? _table_builder->FileSize() : 0;
    file_info.encryption_meta = _encryption_meta;
    return file_info;
}

Status PersistentIndexSstableStreamBuilder::status() const {
    return _status;
}

} // namespace starrocks::lake
