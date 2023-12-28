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

#include "storage/chunk_helper.h"
#include "storage/chunk_iterator.h"
#include "storage/del_vector.h"
#include "storage/delta_column_group.h"
#include "storage/primary_key_encoder.h"
#include "storage/rowset/rowset.h"
#include "storage/rowset/segment_iterator.h"
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
    _partial_pk_column_keys = std::make_unique<PartialKeysPB>();
    _partial_pindex_kvs = std::make_unique<PartialKVsPB>();
}

Status PrimaryKeyDump::_init_dump_file() {
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
        rowset_meta_id.set_rid(each.first);
        each.second->rowset_meta()->to_rowset_pb(rowset_meta_id.mutable_rowset_meta());
        rowset_meta_id.mutable_rowset_meta()->clear_tablet_schema();
        _dump_pb.add_rowset_metas()->CopyFrom(rowset_meta_id);
    }
    return Status::OK();
}

Status PrimaryKeyDump::add_pindex_kvs(const std::string& key, uint64_t value, PrimaryIndexDumpPB* dump_pb) {
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
    _partial_pindex_kvs->add_keys(key);
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

Status PrimaryKeyDump::add_pk_column_keys(const std::string& key, PrimaryKeyColumnPB* dump_pb) {
    // Avoid protobuf exceed memory limit
    if (_partial_pk_column_keys_bytes + key.size() >= MAX_PROTOBUF_SIZE) {
        std::string serialized_data;
        if (_partial_pk_column_keys->SerializeToString(&serialized_data)) {
            PagePointerPB page;
            page.set_offset(_dump_wfile->size());
            RETURN_IF_ERROR(_dump_wfile->append(Slice(serialized_data)));
            page.set_size(_dump_wfile->size() - page.offset());
            dump_pb->add_keys()->CopyFrom(page);
            _partial_pk_column_keys->Clear();
            _partial_pk_column_keys_bytes = 0;
        } else {
            return Status::InternalError("dump to file, serialize error");
        }
    }
    _partial_pk_column_keys->add_keys(key);
    _partial_pk_column_keys_bytes += key.size();
    return Status::OK();
}

Status PrimaryKeyDump::finish_pk_column_keys(PrimaryKeyColumnPB* dump_pb) {
    std::string serialized_data;
    if (_partial_pk_column_keys_bytes > 0) {
        if (_partial_pk_column_keys->SerializeToString(&serialized_data)) {
            PagePointerPB page;
            page.set_offset(_dump_wfile->size());
            RETURN_IF_ERROR(_dump_wfile->append(Slice(serialized_data)));
            page.set_size(_dump_wfile->size() - page.offset());
            dump_pb->add_keys()->CopyFrom(page);
            _partial_pk_column_keys->Clear();
            _partial_pk_column_keys_bytes = 0;
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
                delvec_stat.set_sid(segment_id);
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
        dcg_list.set_sid(each.first);
        (void)DeltaColumnGroupListSerializer::serialize_delta_column_group_list(each.second,
                                                                                dcg_list.mutable_dcg_list());
        _dump_pb.add_dcg_lists()->CopyFrom(dcg_list);
    }

    return Status::OK();
}

class PrimaryKeyColumnDumper {
public:
    virtual ~PrimaryKeyColumnDumper() = default;
    virtual Status dump_columns(PrimaryKeyDump* dump, Column* pkc, PrimaryKeyColumnPB* pk_column_pb) = 0;
};

class SlicePrimaryKeyColumnDumper : public PrimaryKeyColumnDumper {
public:
    Status dump_columns(PrimaryKeyDump* dump, Column* pkc, PrimaryKeyColumnPB* pk_column_pb) override {
        auto* keys = reinterpret_cast<const Slice*>(pkc->raw_data());
        for (int i = 0; i < pkc->size(); i++) {
            RETURN_IF_ERROR(dump->add_pk_column_keys(hexdump(keys[i].data, keys[i].size), pk_column_pb));
        }
        return dump->finish_pk_column_keys(pk_column_pb);
    }
};

template <typename T>
class FixedPrimaryKeyColumnDumper : public PrimaryKeyColumnDumper {
public:
    Status dump_columns(PrimaryKeyDump* dump, Column* pkc, PrimaryKeyColumnPB* pk_column_pb) override {
        auto* keys = reinterpret_cast<const T*>(pkc->raw_data());
        for (int i = 0; i < pkc->size(); i++) {
            RETURN_IF_ERROR(dump->add_pk_column_keys(hexdump(reinterpret_cast<const char*>(&(keys[i])), sizeof(T)),
                                                     pk_column_pb));
        }
        return dump->finish_pk_column_keys(pk_column_pb);
    }
};

static std::unique_ptr<PrimaryKeyColumnDumper> choose_dumper(LogicalType key_type, size_t fix_size) {
#define CASE_TYPE(type) \
    case (type):        \
        return std::make_unique<FixedPrimaryKeyColumnDumper<typename CppTypeTraits<type>::CppType>>()

    switch (key_type) {
        CASE_TYPE(TYPE_BOOLEAN);
        CASE_TYPE(TYPE_TINYINT);
        CASE_TYPE(TYPE_SMALLINT);
        CASE_TYPE(TYPE_INT);
        CASE_TYPE(TYPE_BIGINT);
        CASE_TYPE(TYPE_LARGEINT);
    case TYPE_CHAR:
        return std::make_unique<SlicePrimaryKeyColumnDumper>();
    case TYPE_VARCHAR:
        return std::make_unique<SlicePrimaryKeyColumnDumper>();
    case TYPE_DATE:
        return std::make_unique<FixedPrimaryKeyColumnDumper<int32_t>>();
    case TYPE_DATETIME:
        return std::make_unique<FixedPrimaryKeyColumnDumper<int64_t>>();
    default:
        return nullptr;
    }
#undef CASE_TYPE
}

Status PrimaryKeyDump::_dump_segment_keys() {
    // 1. generate primary key schema
    auto tablet_schema = _tablet->tablet_schema();
    vector<uint32_t> pk_columns;
    for (size_t i = 0; i < tablet_schema->num_key_columns(); i++) {
        pk_columns.push_back((uint32_t)i);
    }
    Schema pkey_schema = ChunkHelper::convert_schema(tablet_schema, pk_columns);
    std::unique_ptr<Column> pk_column;
    if (pk_columns.size() > 1) {
        if (!PrimaryKeyEncoder::create_column(pkey_schema, &pk_column).ok()) {
            CHECK(false) << "create column for primary key encoder failed";
        }
    }
    auto enc_pk_type = PrimaryKeyEncoder::encoded_primary_key_type(pkey_schema, pk_columns);
    auto key_size = PrimaryKeyEncoder::get_encoded_fixed_size(pkey_schema);
    auto dumper = choose_dumper(enc_pk_type, key_size);
    if (dumper == nullptr) {
        return Status::InternalError("invalid primary key");
    }
    // 2. scan all rowset
    int64_t apply_version = 0;
    std::vector<RowsetSharedPtr> rowsets;
    std::vector<uint32_t> rowset_ids;
    RETURN_IF_ERROR(_tablet->updates()->get_apply_version_and_rowsets(&apply_version, &rowsets, &rowset_ids));
    // only hold pkey, so can use larger chunk size
    OlapReaderStatistics stats;
    auto chunk_shared_ptr = ChunkHelper::new_chunk(pkey_schema, 4096);
    auto chunk = chunk_shared_ptr.get();
    for (auto& rowset : rowsets) {
        RowsetReleaseGuard guard(rowset);
        auto res = rowset->get_segment_iterators2(pkey_schema, tablet_schema, _tablet->data_dir()->get_meta(),
                                                  apply_version, &stats);
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
            PrimaryKeyColumnPB pk_column_pb;
            pk_column_pb.set_sid(rowset->rowset_meta()->get_rowset_seg_id() + i);
            while (true) {
                chunk->reset();
                auto st = itr->get_next(chunk);
                if (st.is_end_of_file()) {
                    break;
                } else if (!st.ok()) {
                    return st;
                } else {
                    Column* pkc = nullptr;
                    if (pk_column) {
                        pk_column->reset_column();
                        PrimaryKeyEncoder::encode(pkey_schema, *chunk, 0, chunk->num_rows(), pk_column.get());
                        pkc = pk_column.get();
                    } else {
                        pkc = chunk->columns()[0].get();
                    }
                    RETURN_IF_ERROR(dumper->dump_columns(this, pkc, &pk_column_pb));
                }
            }
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
        rowset_stat_id.set_rid(each.first);
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
    RETURN_IF_ERROR(_init_dump_file());
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

Status PrimaryKeyDump::deserialize_kvs_from_meta(
        const std::string& dump_filepath, const PrimaryKeyDumpPB& dump_pb,
        const std::function<void(const PartialKeysPB&)>& column_key_func,
        const std::function<void(const std::string&, const PartialKVsPB&)>& index_kvs_func) {
    std::unique_ptr<RandomAccessFile> rfile;
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(dump_filepath));
    ASSIGN_OR_RETURN(rfile, fs->new_random_access_file(dump_filepath));
    // 1. deserialize pk column
    for (const auto& primary_key_column : dump_pb.primary_key_column()) {
        for (const auto& page : primary_key_column.keys()) {
            std::string buff;
            raw::stl_string_resize_uninitialized(&buff, page.size());
            RETURN_IF_ERROR(rfile->read_at_fully(page.offset(), buff.data(), buff.size()));
            PartialKeysPB partial_keys_pb;
            if (partial_keys_pb.ParseFromString(buff)) {
                column_key_func(partial_keys_pb);
            } else {
                return Status::Corruption("deserialize kvs from meta fail, " + dump_filepath);
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