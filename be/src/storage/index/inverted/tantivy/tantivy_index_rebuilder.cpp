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

#include "storage/index/inverted/tantivy/tantivy_index_rebuilder.h"

#include <algorithm>
#include <filesystem>
#include <memory>
#include <vector>

#include "column/nullable_column.h"
#include "common/config.h"
#include "fs/fs.h"
#include "fs/key_cache.h"
#include "runtime/exec_env.h"
#include "storage/chunk_helper.h"
#include "storage/index/compound_index_file_writer.h"
#include "storage/index/index_descriptor.h"
#include "storage/index/inverted/inverted_index_option.h"
#include "storage/index/inverted/inverted_plugin_factory.h"
#include "storage/index/inverted/inverted_writer.h"
#include "storage/rowset/column_iterator.h"
#include "storage/rowset/segment.h"
#include "storage/types.h"
#include "util/defer_op.h"

namespace starrocks {

namespace {

constexpr size_t kReadBatchSize = 4096;

Status append_column_to_writer(const Column& column, InvertedWriter* writer) {
    const Column* data_column = &column;
    const uint8_t* null_flags = nullptr;
    bool has_null = false;
    if (column.is_nullable()) {
        const auto* nullable = down_cast<const NullableColumn*>(&column);
        data_column = nullable->data_column().get();
        null_flags = nullable->immutable_null_column_data().data();
        has_null = nullable->has_null();
    }

    const auto* values = reinterpret_cast<const Slice*>(data_column->raw_data());
    if (!has_null) {
        writer->add_values(values, column.size());
        return Status::OK();
    }

    size_t begin = 0;
    while (begin < column.size()) {
        const bool is_null = null_flags[begin] != 0;
        size_t end = begin + 1;
        while (end < column.size() && (null_flags[end] != 0) == is_null) {
            ++end;
        }
        if (is_null) {
            writer->add_nulls(static_cast<uint32_t>(end - begin));
        } else {
            writer->add_values(values + begin, end - begin);
        }
        begin = end;
    }
    return Status::OK();
}

} // namespace

Status TantivyIndexRebuilder::rebuild(const FileInfo& segment_file_info, uint32_t segment_id,
                                      const TabletSchemaCSPtr& tablet_schema) {
    TantivyIndexRebuilder rebuilder;
    return rebuilder._rebuild(segment_file_info, segment_id, tablet_schema);
}

Status TantivyIndexRebuilder::_rebuild(const FileInfo& segment_file_info, uint32_t segment_id,
                                       const TabletSchemaCSPtr& tablet_schema) {
    std::vector<TabletIndex> indexes;
    for (const auto& index : *tablet_schema->indexes()) {
        if (index.index_type() != GIN) {
            continue;
        }
        ASSIGN_OR_RETURN(auto imp_type, get_inverted_imp_type(index));
        if (imp_type == InvertedImplementType::TANTIVY) {
            indexes.push_back(index);
        }
    }
    if (indexes.empty()) {
        return Status::OK();
    }

    const auto& store_paths = ExecEnv::GetInstance()->store_paths();
    const auto tmp_root = store_paths.empty()
                                  ? std::filesystem::temp_directory_path() / config::tantivy_index_local_tmp_dir
                                  : std::filesystem::path(store_paths[0].path) / config::tantivy_index_local_tmp_dir;

    ASSIGN_OR_RETURN(auto segment_fs, FileSystem::CreateSharedFromString(segment_file_info.path));
    ASSIGN_OR_RETURN(auto segment, Segment::open(segment_fs, segment_file_info, segment_id, tablet_schema));

    RandomAccessFileOptions file_opts;
    if (!segment_file_info.encryption_meta.empty()) {
        ASSIGN_OR_RETURN(file_opts.encryption_info,
                         KeyCache::instance().unwrap_encryption_meta(segment_file_info.encryption_meta));
    }
    ASSIGN_OR_RETURN(auto read_file, segment_fs->new_random_access_file_with_bundling(file_opts, segment_file_info));

    std::vector<std::filesystem::path> build_dirs;
    DeferOp cleanup_build_dirs([&] {
        for (const auto& build_dir : build_dirs) {
            std::error_code ec;
            std::filesystem::remove_all(build_dir, ec);
        }
    });
    std::vector<CompoundIndexEntry> entries;

    for (auto& index : indexes) {
        if (index.col_unique_ids().size() != 1) {
            return Status::NotSupported("tantivy rebuild: only single-column GIN indexes are supported");
        }
        const int32_t column_index = tablet_schema->field_index(index.col_unique_ids()[0]);
        if (column_index < 0) {
            return Status::Corruption("tantivy rebuild: indexed column not found in tablet schema");
        }
        const auto& tablet_column = tablet_schema->column(column_index);

        ASSIGN_OR_RETURN(auto plugin, InvertedPluginFactory::get_plugin(InvertedImplementType::TANTIVY));
        const std::string build_dir = IndexDescriptor::lake_compound_index_build_dir(
                tmp_root.string(), 0, 0, segment_id, index.index_id(), reinterpret_cast<uintptr_t>(this));
        build_dirs.emplace_back(build_dir);
        std::unique_ptr<InvertedWriter> writer;
        RETURN_IF_ERROR(plugin->create_inverted_index_writer(
                get_type_info(tablet_column), std::string(tablet_column.name()), build_dir, &index, &writer));
        RETURN_IF_ERROR(writer->init());

        ASSIGN_OR_RETURN(auto column_iterator, segment->new_column_iterator(tablet_column, nullptr));
        OlapReaderStatistics stats;
        ColumnIteratorOptions iterator_options;
        iterator_options.read_file = read_file.get();
        iterator_options.stats = &stats;
        RETURN_IF_ERROR(column_iterator->init(iterator_options));
        RETURN_IF_ERROR(column_iterator->seek_to_first());

        auto field = ChunkHelper::convert_field(column_index, tablet_column);
        auto column = ChunkHelper::column_from_field(field);
        ordinal_t rows_read = 0;
        const ordinal_t total_rows = column_iterator->num_rows();
        while (rows_read < total_rows) {
            column->reset_column();
            size_t batch = std::min(kReadBatchSize, static_cast<size_t>(total_rows - rows_read));
            RETURN_IF_ERROR(column_iterator->next_batch(&batch, column.get()));
            if (batch == 0) {
                break;
            }
            RETURN_IF_ERROR(append_column_to_writer(*column, writer.get()));
            rows_read += batch;
        }
        if (rows_read != total_rows) {
            return Status::Corruption("tantivy rebuild: column iterator returned fewer rows than the segment");
        }
        ASSIGN_OR_RETURN(auto entry, writer->finish_compound(nullptr));
        entries.push_back(std::move(entry));
    }

    const std::string compound_path = IndexDescriptor::compound_index_file_path_from_segment(segment_file_info.path);
    bool compound_complete = false;
    DeferOp cleanup_compound([&] {
        if (!compound_complete) {
            (void)segment_fs->delete_file(compound_path);
        }
    });
    WritableFileOptions write_options{.sync_on_close = true, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
    ASSIGN_OR_RETURN(auto compound_file, segment_fs->new_writable_file(write_options, compound_path));
    auto pack_status = CompoundIndexFileWriter::pack(entries, compound_file.get());
    if (!pack_status.ok()) {
        (void)compound_file->close();
        return pack_status;
    }
    RETURN_IF_ERROR(compound_file->close());
    compound_complete = true;
    return Status::OK();
}

} // namespace starrocks
