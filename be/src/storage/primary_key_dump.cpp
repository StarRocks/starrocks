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

#include "storage/primary_key_dump.h"

#include <memory>

#include "fs/fs.h"
#include "fs/fs_util.h"
#include "storage/chunk_helper.h"
#include "storage/chunk_iterator.h"
#include "storage/del_vector.h"
#include "storage/delta_column_group.h"
#include "storage/primary_key_encoder.h"
#include "storage/rowset/rowset.h"
#include "storage/rowset/segment_iterator.h"
#include "storage/rowset/segment_options.h"
#include "storage/rowset/segment_writer.h"
#include "storage/tablet.h"
#include "storage/tablet_meta_manager.h"
#include "storage/tablet_updates.h"
#include "storage/type_traits.h"
#include "types/logical_type.h"

namespace starrocks {

PrimaryKeyDump::PrimaryKeyDump(Tablet* tablet) {
    _tablet = tablet;
    _dump_filepath = tablet->schema_hash_path() + "/" + std::to_string(_tablet->tablet_id()) + ".pkdump";
    _partial_pindex_kvs = std::make_unique<PartialKVsPB>();
}

// for UT only
PrimaryKeyDump::PrimaryKeyDump(const std::string& dump_filepath) {
    _dump_filepath = dump_filepath;
    _partial_pindex_kvs = std::make_unique<PartialKVsPB>();
}

Status PrimaryKeyDump::init_dump_file() {
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(_dump_filepath));
    WritableFileOptions wblock_opts{.sync_on_close = true, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
    ASSIGN_OR_RETURN(_dump_wfile, fs->new_writable_file(wblock_opts, _dump_filepath));
    return Status::OK();
}

Status PrimaryKeyDump::_dump_tablet_meta() {
    TabletMetaPB meta_pb;
    _tablet->tablet_meta()->to_meta_pb(&meta_pb);
    _dump_pb.mutable_tablet_meta()->CopyFrom(meta_pb);
    return Status::OK();
}

Status PrimaryKeyDump::_dump_rowset_meta() {
    auto rowset_map = _tablet->updates()->get_rowset_map();
    for (const auto& each : (*rowset_map)) {
        RowsetMetaIdPB rowset_meta_id;
        rowset_meta_id.set_rowset_id(each.first);
        rowset_meta_id.mutable_rowset_meta()->CopyFrom(each.second->rowset_meta()->get_meta_pb_without_schema());
        _dump_pb.add_rowset_metas()->CopyFrom(rowset_meta_id);
    }
    return Status::OK();
}

Status PrimaryKeyDump::add_pindex_kvs(const std::string_view& key, uint64_t value, PrimaryIndexDumpPB* dump_pb) {
    // Avoid protobuf exceed memory limit
    if (_partial_pindex_kvs_bytes + key.size() + sizeof(uint64_t) >= MAX_PROTOBUF_SIZE) {
        std::string serialized_data;
        if (_partial_pindex_kvs->SerializeToString(&serialized_data)) {
            PagePointerPB page;
            page.set_offset(_dump_wfile->size());
            RETURN_IF_ERROR(_dump_wfile->append(Slice(serialized_data)));
            page.set_size(_dump_wfile->size() - page.offset());
            dump_pb->add_kvs()->CopyFrom(page);
            _partial_pindex_kvs->Clear();
            _partial_pindex_kvs_bytes = 0;
        } else {
            return Status::InternalError("dump to file, serialize error");
        }
    }
    _partial_pindex_kvs->add_keys(key.data(), key.size());
    _partial_pindex_kvs->add_values(value);
    _partial_pindex_kvs_bytes += key.size() + sizeof(uint64_t);
    return Status::OK();
}

Status PrimaryKeyDump::finish_pindex_kvs(PrimaryIndexDumpPB* dump_pb) {
    std::string serialized_data;
    if (_partial_pindex_kvs_bytes > 0) {
        if (_partial_pindex_kvs->SerializeToString(&serialized_data)) {
            PagePointerPB page;
            page.set_offset(_dump_wfile->size());
            RETURN_IF_ERROR(_dump_wfile->append(Slice(serialized_data)));
            page.set_size(_dump_wfile->size() - page.offset());
            dump_pb->add_kvs()->CopyFrom(page);
            _partial_pindex_kvs->Clear();
            _partial_pindex_kvs_bytes = 0;
        } else {
            return Status::InternalError("dump to file, serialize error");
        }
    }
    return Status::OK();
}

Status PrimaryKeyDump::_dump_primary_index() {
    return _tablet->updates()->primary_index_dump(this, _dump_pb.mutable_primary_index());
}

Status PrimaryKeyDump::_dump_delvec() {
    RETURN_IF_ERROR(TabletMetaManager::del_vector_iterate(
            _tablet->data_dir()->get_meta(), _tablet->tablet_id(), 0, UINT32_MAX,
            [&](uint32_t segment_id, int64_t version, std::string_view value) -> bool {
                DeleteVectorStatPB delvec_stat;
                delvec_stat.set_segment_id(segment_id);
                delvec_stat.set_version(version);
                DelVectorPtr delvec_ptr = std::make_shared<DelVector>();
                if (!delvec_ptr->load(version, value.data(), value.size()).ok()) {
                    return false;
                }
                delvec_stat.set_cardinality(delvec_ptr->cardinality());
                _dump_pb.add_delvec_stats()->CopyFrom(delvec_stat);
                return true;
            }));
    return Status::OK();
}

Status PrimaryKeyDump::_dump_dcg() {
    std::map<uint32_t, DeltaColumnGroupList> dcgs;
    RETURN_IF_ERROR(TabletMetaManager::scan_tablet_delta_column_group_by_segment(_tablet->data_dir()->get_meta(),
                                                                                 _tablet->tablet_id(), &dcgs));
    for (const auto& each : dcgs) {
        DeltaColumnGroupListIdPB dcg_list;
        dcg_list.set_segment_id(each.first);
        (void)DeltaColumnGroupListSerializer::serialize_delta_column_group_list(each.second,
                                                                                dcg_list.mutable_dcg_list());
        _dump_pb.add_dcg_lists()->CopyFrom(dcg_list);
    }

    return Status::OK();
}

class PrimaryKeyChunkDumper {
public:
    PrimaryKeyChunkDumper(PrimaryKeyDump* dump, PrimaryKeyColumnPB* pk_column_pb)
            : _dump(dump), _pk_column_pb(pk_column_pb) {}
    ~PrimaryKeyChunkDumper() { (void)fs::delete_file(_tmp_file); }
    Status init(const TabletSchemaCSPtr& tablet_schema, const std::string& tablet_path) {
        _tmp_file = tablet_path + "/PrimaryKeyChunkDumper_" + std::to_string(static_cast<int64_t>(pthread_self()));
        (void)fs::delete_file(_tmp_file);
        ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(tablet_path));
        WritableFileOptions opts{.sync_on_close = true, .mode = FileSystem::OpenMode::CREATE_OR_OPEN_WITH_TRUNCATE};
        ASSIGN_OR_RETURN(auto wfile, fs->new_writable_file(opts, _tmp_file));
        SegmentWriterOptions writer_options;
        _writer = std::make_unique<SegmentWriter>(std::move(wfile), _pk_column_pb->segment_id(), tablet_schema,
                                                  writer_options);
        RETURN_IF_ERROR(_writer->init(false));
        return Status::OK();
    }
    Status dump_chunk(const Chunk& chunk) { return _writer->append_chunk(chunk); }
    Status finalize(WritableFile* wfile) {
        uint64_t segment_file_size = 0;
        uint64_t index_size = 0;
        uint64_t footer_position = 0;
        RETURN_IF_ERROR(_writer->finalize(&segment_file_size, &index_size, &footer_position));
        _page.set_offset(wfile->size());
        RETURN_IF_ERROR(fs::copy_append_file(_tmp_file, wfile));
        _page.set_size(wfile->size() - _page.offset());
        _pk_column_pb->mutable_page()->CopyFrom(_page);
        return Status::OK();
    }

private:
    PrimaryKeyDump* _dump;
    PrimaryKeyColumnPB* _pk_column_pb;
    std::unique_ptr<SegmentWriter> _writer;
    PagePointerPB _page;
    std::string _tmp_file;
};

class PrimaryKeyChunkReader {
public:
    PrimaryKeyChunkReader() = default;
    ~PrimaryKeyChunkReader() { (void)fs::delete_file(_tmp_file); }

    StatusOr<ChunkIteratorPtr> read(const std::string& dump_filepath, const Schema& schema,
                                    const TabletSchemaCSPtr& tablet_schema, const PrimaryKeyColumnPB& pk_column_pb) {
        RETURN_IF_ERROR(_copy_to_tmp_file(dump_filepath, pk_column_pb));
        ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(_tmp_file));
        SegmentReadOptions seg_options;
        seg_options.fs = fs;
        seg_options.stats = &_stats;
        seg_options.tablet_schema = tablet_schema;
        ASSIGN_OR_RETURN(auto seg_ptr,
                         Segment::open(fs, FileInfo{_tmp_file}, pk_column_pb.segment_id(), tablet_schema));
        return seg_ptr->new_iterator(schema, seg_options);
    }

private:
    Status _copy_to_tmp_file(const std::string& dump_filepath, const PrimaryKeyColumnPB& pk_column_pb) {
        _tmp_file = "./PrimaryKeyChunkReader_" + std::to_string(static_cast<int64_t>(pthread_self()));
        (void)fs::delete_file(_tmp_file);
        RETURN_IF_ERROR(fs::copy_file_by_range(dump_filepath, _tmp_file, pk_column_pb.page().offset(),
                                               pk_column_pb.page().size()));
        return Status::OK();
    }

private:
    std::string _tmp_file;
    OlapReaderStatistics _stats;
};

static std::pair<Schema, std::shared_ptr<TabletSchema>> build_pkey_schema(const TabletSchemaCSPtr& tablet_schema) {
    vector<uint32_t> pk_columns;
    vector<int32_t> pk_columns2;
    for (size_t i = 0; i < tablet_schema->num_key_columns(); i++) {
        pk_columns.push_back((uint32_t)i);
        pk_columns2.push_back((int32_t)i);
    }
    Schema pkey_schema = ChunkHelper::convert_schema(tablet_schema, pk_columns);
    auto pkey_tschema = TabletSchema::create(tablet_schema, pk_columns2);
    return {pkey_schema, pkey_tschema};
}

Status PrimaryKeyDump::_dump_segment_keys() {
    // 1. generate primary key schema
    auto tablet_schema = _tablet->tablet_schema();
    auto schema_pair = build_pkey_schema(tablet_schema);
    Schema& pkey_schema = schema_pair.first;
    auto pkey_tschema = schema_pair.second;
    // 2. scan all rowset
    auto rowset_map = _tablet->updates()->get_rowset_map();
    // only hold pkey, so can use larger chunk size
    OlapReaderStatistics stats;
    auto chunk_shared_ptr = ChunkHelper::new_chunk(pkey_schema, 4096);
    auto chunk = chunk_shared_ptr.get();
    for (auto& rowset : *rowset_map) {
        RowsetReleaseGuard guard(rowset.second);
        auto res = rowset.second->get_segment_iterators2(pkey_schema, tablet_schema, nullptr, 0, &stats);
        if (!res.ok()) {
            return res.status();
        }
        auto& itrs = res.value();
        CHECK(itrs.size() == rowset.second->num_segments()) << "itrs.size != num_segments";
        for (size_t i = 0; i < itrs.size(); i++) {
            auto itr = itrs[i].get();
            if (itr == nullptr) {
                continue;
            }
            PrimaryKeyColumnPB pk_column_pb;
            pk_column_pb.set_segment_id(rowset.second->rowset_meta()->get_rowset_seg_id() + i);
            PrimaryKeyChunkDumper dumper(this, &pk_column_pb);
            RETURN_IF_ERROR(dumper.init(pkey_tschema, _tablet->schema_hash_path()));
            while (true) {
                chunk->reset();
                auto st = itr->get_next(chunk);
                if (st.is_end_of_file()) {
                    break;
                } else if (!st.ok()) {
                    return st;
                } else {
                    RETURN_IF_ERROR(dumper.dump_chunk(*chunk));
                }
            }
            RETURN_IF_ERROR(dumper.finalize(_dump_wfile.get()));
            itr->close();
            _dump_pb.add_primary_key_column()->CopyFrom(pk_column_pb);
        }
    }
    return Status::OK();
}

Status PrimaryKeyDump::_dump_rowset_stat() {
    std::map<uint32_t, std::string> output_rowset_stats;
    RETURN_IF_ERROR(_tablet->updates()->get_rowset_stats(&output_rowset_stats));
    for (const auto& each : output_rowset_stats) {
        RowsetStatIdPB rowset_stat_id;
        rowset_stat_id.set_rowset_id(each.first);
        rowset_stat_id.set_rowset_stat(each.second);
        _dump_pb.add_rowset_stats()->CopyFrom(rowset_stat_id);
    }
    return Status::OK();
}

Status PrimaryKeyDump::_dump_to_file() {
    std::string serialized_data;
    if (_dump_pb.SerializeToString(&serialized_data)) {
        RETURN_IF_ERROR(_dump_wfile->append(Slice(serialized_data)));
        faststring tail_buf;
        // Record protobuf size
        put_fixed64_le(&tail_buf, serialized_data.size());
        RETURN_IF_ERROR(_dump_wfile->append(Slice(tail_buf)));
    } else {
        return Status::InternalError("dump to file, serialize error");
    }
    return Status::OK();
}

Status PrimaryKeyDump::dump() {
    RETURN_IF_ERROR(init_dump_file());
    RETURN_IF_ERROR(_dump_tablet_meta());
    RETURN_IF_ERROR(_dump_rowset_meta());
    RETURN_IF_ERROR(_dump_rowset_stat());
    RETURN_IF_ERROR(_dump_delvec());
    RETURN_IF_ERROR(_dump_dcg());
    RETURN_IF_ERROR(_dump_segment_keys());
    RETURN_IF_ERROR(_dump_primary_index());
    RETURN_IF_ERROR(_dump_to_file());
    return Status::OK();
}

Status PrimaryKeyDump::read_deserialize_from_file(const std::string& dump_filepath, PrimaryKeyDumpPB* dump_pb) {
    std::unique_ptr<RandomAccessFile> rfile;
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(dump_filepath));
    ASSIGN_OR_RETURN(rfile, fs->new_random_access_file(dump_filepath));
    ASSIGN_OR_RETURN(int64_t file_size, rfile->get_size());
    // 1. get protobuf size from tail
    std::unique_ptr<char[]> tail(new char[8]);
    Slice tail_slice(tail.get(), 8);
    RETURN_IF_ERROR(rfile->read_at(file_size - 8, tail_slice.data, 8));
    uint64_t protobuf_size = decode_fixed64_le((uint8_t*)tail_slice.data);
    // 2. read and deserialize protobuf
    std::string buff;
    raw::stl_string_resize_uninitialized(&buff, protobuf_size);
    RETURN_IF_ERROR(rfile->read_at_fully(file_size - 8 - protobuf_size, buff.data(), buff.size()));
    if (dump_pb->ParseFromString(buff)) {
        return Status::OK();
    } else {
        return Status::Corruption("read deserialize from file fail, " + dump_filepath);
    }
}

Status PrimaryKeyDump::deserialize_pkcol_pkindex_from_meta(
        const std::string& dump_filepath, const PrimaryKeyDumpPB& dump_pb,
        const std::function<void(const Chunk&)>& column_key_func,
        const std::function<void(const std::string&, const PartialKVsPB&)>& index_kvs_func) {
    std::unique_ptr<RandomAccessFile> rfile;
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(dump_filepath));
    ASSIGN_OR_RETURN(rfile, fs->new_random_access_file(dump_filepath));
    // 1. deserialize pk column
    for (const auto& primary_key_column : dump_pb.primary_key_column()) {
        TabletSchemaCSPtr tablet_schema = std::make_shared<TabletSchema>(dump_pb.tablet_meta().schema());
        auto schema_pair = build_pkey_schema(tablet_schema);
        Schema& pkey_schema = schema_pair.first;
        auto pkey_tschema = schema_pair.second;
        auto chunk_shared_ptr = ChunkHelper::new_chunk(pkey_schema, 4096);
        auto chunk = chunk_shared_ptr.get();
        PrimaryKeyChunkReader reader;
        ASSIGN_OR_RETURN(auto itr, reader.read(dump_filepath, pkey_schema, pkey_tschema, primary_key_column));
        while (true) {
            chunk->reset();
            auto st = itr->get_next(chunk);
            if (st.is_end_of_file()) {
                break;
            } else if (!st.ok()) {
                return st;
            } else {
                column_key_func(*chunk);
            }
        }
    }
    // 2. deserialize pk index
    for (const auto& level : dump_pb.primary_index().primary_index_levels()) {
        for (const auto& page : level.kvs()) {
            std::string buff;
            raw::stl_string_resize_uninitialized(&buff, page.size());
            RETURN_IF_ERROR(rfile->read_at_fully(page.offset(), buff.data(), buff.size()));
            PartialKVsPB partial_kvs_pb;
            if (partial_kvs_pb.ParseFromString(buff)) {
                index_kvs_func(level.filename(), partial_kvs_pb);
            } else {
                return Status::Corruption("deserialize kvs from meta fail, " + dump_filepath);
            }
        }
    }
    return Status::OK();
}

} // namespace starrocks