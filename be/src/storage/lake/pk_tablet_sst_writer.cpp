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

#include "storage/lake/pk_tablet_sst_writer.h"

#include <fmt/format.h>

#include "column/chunk.h"
#include "common/config.h"
#include "fs/bundle_file.h"
#include "fs/fs_util.h"
#include "fs/key_cache.h"
#include "runtime/current_thread.h"
#include "serde/column_array_serde.h"
#include "storage/chunk_helper.h"
#include "storage/lake/filenames.h"
#include "storage/lake/tablet_manager.h"
#include "storage/primary_index.h"
#include "storage/primary_key_encoder.h"
#include "storage/rowset/segment_writer.h"
#include "storage/sstable/table_builder.h"

namespace starrocks::lake {

Status PkTabletSSTWriter::append_sst_record(const Chunk& data) {
    if (_pk_sst_builder == nullptr) {
        return Status::InternalError("pk sst writer not initialized");
    }
    if (_pk_column == nullptr) {
        vector<uint32_t> pk_columns;
        for (size_t i = 0; i < _tablet_schema_ptr->num_key_columns(); i++) {
            pk_columns.push_back((uint32_t)i);
        }
        _pkey_schema = ChunkHelper::convert_schema(_tablet_schema_ptr, pk_columns);
        RETURN_IF_ERROR(PrimaryKeyEncoder::create_column(_pkey_schema, &_pk_column));
        _key_size = PrimaryKeyEncoder::get_encoded_fixed_size(_pkey_schema);
    }
    auto clone_pk_column = _pk_column->clone_empty();
    TRY_CATCH_BAD_ALLOC(PrimaryKeyEncoder::encode(_pkey_schema, data, 0, data.num_rows(), clone_pk_column.get()));
    std::vector<Slice> keys;
    const Slice* vkeys =
            PrimaryIndex::build_persistent_keys(*clone_pk_column, _key_size, 0, clone_pk_column->size(), &keys);
    for (size_t i = 0; i < clone_pk_column->size(); i++) {
        RETURN_IF_ERROR(_pk_sst_builder->add(vkeys[i]));
    }
    return Status::OK();
}

Status PkTabletSSTWriter::reset_sst_writer(const std::shared_ptr<LocationProvider>& location_provider,
                                           const std::shared_ptr<FileSystem>& fs) {
    WritableFileOptions wopts;
    std::string encryption_meta;
    if (config::enable_transparent_data_encryption) {
        ASSIGN_OR_RETURN(auto pair, KeyCache::instance().create_encryption_meta_pair_using_current_kek());
        wopts.encryption_info = pair.info;
        encryption_meta = std::move(pair.encryption_meta);
    }
    std::unique_ptr<WritableFile> sst_wf;
    if (location_provider && fs) {
        ASSIGN_OR_RETURN(sst_wf,
                         fs->new_writable_file(wopts, location_provider->sst_location(_tablet_id, gen_sst_filename())));
    } else {
        ASSIGN_OR_RETURN(sst_wf,
                         fs::new_writable_file(wopts, _tablet_mgr->sst_location(_tablet_id, gen_sst_filename())));
    }
    _pk_sst_builder = std::make_unique<PersistentIndexSstableStreamBuilder>(std::move(sst_wf), encryption_meta);
    return Status::OK();
}

StatusOr<FileInfo> PkTabletSSTWriter::flush_sst_writer() {
    if (_pk_sst_builder == nullptr) {
        return Status::InternalError("pk sst writer not initialized");
    }
    RETURN_IF_ERROR(_pk_sst_builder->finish());
    auto sst_file_info = _pk_sst_builder->file_info();
    _pk_sst_builder.reset();
    return sst_file_info;
}

} // namespace starrocks::lake