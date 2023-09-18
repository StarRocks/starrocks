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

#include <exception>
#include <filesystem>

#include "common/status.h"
#include "formats/parquet/file_reader.h"
#include "fs/fs.h"
#include "runtime/descriptor_helper.h"

namespace starrocks::parquet {

class ParquetCLIReader {
public:
    ParquetCLIReader(const std::string& filepath)
            : _filepath(filepath),
              _file(_create_file(filepath)),
              _scanner_ctx(std::make_shared<HdfsScannerContext>()),
              _scan_stats(std::make_shared<HdfsScanStats>()) {
        _scanner_ctx->timezone = "Asia/Shanghai";
        _scanner_ctx->stats = _scan_stats.get();
        _scanner_ctx->lazy_column_coalesce_counter = _pool.add(new std::atomic<int32_t>(0));
    }
    ~ParquetCLIReader() = default;

    Status init() {
        if (_file == nullptr) {
            return Status::InternalError(fmt::format("File {} not found", _filepath));
        }
        std::shared_ptr<FileMetaData> file_metadata;
        {
            std::shared_ptr<FileReader> reader =
                    std::make_shared<FileReader>(4096, _file.get(), std::filesystem::file_size(_filepath));
            HdfsScannerContext ctx;
            HdfsScanStats stats;
            ctx.stats = &stats;
            ctx.lazy_column_coalesce_counter = _pool.add(new std::atomic<int32_t>(0));
            RETURN_IF_ERROR(reader->init(&ctx));
            file_metadata = reader->get_file_metadata();
        }

        std::vector<SlotDesc> slot_descs;
        std::vector<TypeDescriptor> column_types;
        _chunk = std::make_shared<Chunk>();
        for (const auto& field : file_metadata->schema().get_parquet_fields()) {
            try {
                ASSIGN_OR_RETURN(const TypeDescriptor& type, _build_type(field));
                column_types.emplace_back(type);
                slot_descs.emplace_back(SlotDesc{field.name, type});
                _chunk->append_column(ColumnHelper::create_column(type, true), _chunk->num_columns());
            } catch (const std::string& msg) {
                return Status::InvalidArgument(msg);
            }
        }
        slot_descs.emplace_back();

        _scanner_ctx->tuple_desc = _create_tuple_descriptor(nullptr, &_pool, slot_descs);
        _make_column_info_vector(_scanner_ctx->tuple_desc, &_scanner_ctx->materialized_columns);
        _scanner_ctx->scan_ranges.emplace_back(_create_scan_range(_filepath));

        {
            _file_reader = std::make_shared<FileReader>(4096, _file.get(), std::filesystem::file_size(_filepath));
            RETURN_IF_ERROR(_file_reader->init(_scanner_ctx.get()));
        }
        return Status::OK();
    }

    StatusOr<std::string> debug(size_t rows_to_read) {
        std::stringstream ss;
        size_t rows_read = 0;
        Status st;
        do {
            _chunk->reset();
            st = _file_reader->get_next(&_chunk);
            _chunk->check_or_die();
            if (!st.ok() && !st.is_end_of_file()) {
                return st;
            }
            for (size_t i = 0; i < _chunk->num_rows(); i++) {
                ss << _chunk->debug_row(i) << std::endl;
                rows_read++;
                if (rows_read >= rows_to_read) {
                    break;
                }
            }
        } while (!st.is_end_of_file());
        return ss.str();
    }

private:
    std::unique_ptr<RandomAccessFile> _create_file(const std::string& filepath) {
        return *FileSystem::Default()->new_random_access_file(filepath);
    }

    StatusOr<TypeDescriptor> _build_type(const ParquetField& field) {
        TypeDescriptor type;
        if (field.type.type == TYPE_STRUCT) {
            type.type = TYPE_STRUCT;
            for (const auto& i : field.children) {
                ASSIGN_OR_RETURN(auto child_type, _build_type(i));
                type.children.emplace_back(child_type);
                type.field_names.emplace_back(i.name);
            }
        } else if (field.type.type == TYPE_MAP) {
            type.type = TYPE_MAP;
            for (const auto& i : field.children) {
                ASSIGN_OR_RETURN(auto child_type, _build_type(i));
                type.children.emplace_back(child_type);
            }
        } else if (field.type.type == TYPE_ARRAY) {
            type.type = TYPE_ARRAY;
            ASSIGN_OR_RETURN(auto child_type, _build_type(field.children[0]));
            type.children.emplace_back(child_type);
        } else {
            ASSIGN_OR_RETURN(type, _build_primitive_type(field))
        }
        return type;
    }

    StatusOr<TypeDescriptor> _build_primitive_type(const ParquetField& field) {
        const auto& schema = field.schema_element;
        if (schema.__isset.converted_type) {
            if (schema.converted_type == tparquet::ConvertedType::UINT_8 ||
                schema.converted_type == tparquet::ConvertedType::UINT_16 ||
                schema.converted_type == tparquet::ConvertedType::UINT_32 ||
                schema.converted_type == tparquet::ConvertedType::INT_8 ||
                schema.converted_type == tparquet::ConvertedType::INT_16 ||
                schema.converted_type == tparquet::ConvertedType::INT_32) {
                return TypeDescriptor::from_logical_type(LogicalType::TYPE_INT);
            } else if (schema.converted_type == tparquet::ConvertedType::UINT_64 ||
                       schema.converted_type == tparquet::ConvertedType::INT_64) {
                return TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT);
            } else if (schema.converted_type == tparquet::ConvertedType::UTF8) {
                return TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR);
            } else if (schema.converted_type == tparquet::ConvertedType::DATE) {
                return TypeDescriptor::from_logical_type(LogicalType::TYPE_DATE);
            } else if (schema.converted_type == tparquet::ConvertedType::TIME_MILLIS ||
                       schema.converted_type == tparquet::ConvertedType::TIME_MICROS ||
                       schema.converted_type == tparquet::ConvertedType::INTERVAL) {
                return TypeDescriptor::from_logical_type(LogicalType::TYPE_TIME);
            } else if (schema.converted_type == tparquet::ConvertedType::TIMESTAMP_MILLIS ||
                       schema.converted_type == tparquet::ConvertedType::TIMESTAMP_MICROS) {
                return TypeDescriptor::from_logical_type(LogicalType::TYPE_DATETIME);
            } else if (schema.converted_type == tparquet::ConvertedType::DECIMAL) {
                if (schema.precision > TypeDescriptor::MAX_PRECISION || schema.scale > TypeDescriptor::MAX_SCALE) {
                    return Status::InternalError(fmt::format("Not supported decimal, precision: {}, scale: {}",
                                                             schema.precision, schema.scale));
                }
                return TypeDescriptor::from_logical_type(LogicalType::TYPE_DECIMAL128, -1, schema.precision,
                                                         schema.scale);
            } else if (schema.converted_type == tparquet::ConvertedType::ENUM) {
                return TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR);
            } else if (schema.converted_type == tparquet::ConvertedType::JSON) {
                return TypeDescriptor::from_logical_type(LogicalType::TYPE_JSON);
            } else if (schema.converted_type == tparquet::ConvertedType::BSON) {
                return TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR);
            }
        } else if (schema.__isset.type) {
            if (schema.type == tparquet::Type::BOOLEAN) {
                return TypeDescriptor::from_logical_type(LogicalType::TYPE_BOOLEAN);
            } else if (schema.type == tparquet::Type::FIXED_LEN_BYTE_ARRAY) {
                return TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR);
            } else if (schema.type == tparquet::Type::INT32) {
                return TypeDescriptor::from_logical_type(LogicalType::TYPE_INT);
            } else if (schema.type == tparquet::Type::INT64) {
                return TypeDescriptor::from_logical_type(LogicalType::TYPE_BIGINT);
            } else if (schema.type == tparquet::Type::INT96) {
                return TypeDescriptor::from_logical_type(LogicalType::TYPE_DATETIME);
            } else if (schema.type == tparquet::Type::BYTE_ARRAY) {
                return TypeDescriptor::from_logical_type(LogicalType::TYPE_VARCHAR);
            } else if (schema.type == tparquet::Type::DOUBLE) {
                return TypeDescriptor::from_logical_type(LogicalType::TYPE_DOUBLE);
            } else if (schema.type == tparquet::Type::FLOAT) {
                return TypeDescriptor::from_logical_type(LogicalType::TYPE_FLOAT);
            }
        }
        return Status::InternalError(fmt::format("Has unsupported type {}", schema.name));
    }

    struct SlotDesc {
        std::string name;
        TypeDescriptor type;
    };

    static TupleDescriptor* _create_tuple_descriptor(RuntimeState* state, ObjectPool* pool,
                                                     const std::vector<SlotDesc>& slot_descs) {
        TDescriptorTableBuilder table_desc_builder;
        TTupleDescriptorBuilder tuple_desc_builder;
        for (int i = 0;; i++) {
            if (slot_descs[i].name == "") {
                break;
            }
            TSlotDescriptorBuilder b2;
            b2.column_name(slot_descs[i].name).type(slot_descs[i].type).id(i).nullable(true);
            tuple_desc_builder.add_slot(b2.build());
        }
        tuple_desc_builder.build(&table_desc_builder);

        std::vector<TTupleId> row_tuples = std::vector<TTupleId>{0};
        std::vector<bool> nullable_tuples = std::vector<bool>{true};
        DescriptorTbl* tbl = nullptr;
        CHECK(DescriptorTbl::create(state, pool, table_desc_builder.desc_tbl(), &tbl, config::vector_chunk_size).ok());
        RowDescriptor* row_desc = pool->add(new RowDescriptor(*tbl, row_tuples, nullable_tuples));
        return row_desc->tuple_descriptors()[0];
    }

    static void _make_column_info_vector(const TupleDescriptor* tuple_desc,
                                         std::vector<HdfsScannerContext::ColumnInfo>* columns) {
        columns->clear();
        for (int i = 0; i < tuple_desc->slots().size(); i++) {
            SlotDescriptor* slot = tuple_desc->slots()[i];
            HdfsScannerContext::ColumnInfo c;
            c.col_name = slot->col_name();
            c.col_idx = i;
            c.slot_id = slot->id();
            c.col_type = slot->type();
            c.slot_desc = slot;
            columns->emplace_back(c);
        }
    }

    THdfsScanRange* _create_scan_range(const std::string& file_path, size_t scan_length = 0) {
        auto* scan_range = _pool.add(new THdfsScanRange());

        scan_range->relative_path = file_path;
        scan_range->file_length = std::filesystem::file_size(file_path);
        scan_range->offset = 4;
        scan_range->length = scan_length > 0 ? scan_length : scan_range->file_length;

        return scan_range;
    }

    const std::string _filepath;
    const std::unique_ptr<RandomAccessFile> _file;
    const std::shared_ptr<HdfsScannerContext> _scanner_ctx;
    const std::shared_ptr<HdfsScanStats> _scan_stats;

    std::shared_ptr<FileReader> _file_reader;
    std::shared_ptr<Chunk> _chunk;
    ObjectPool _pool;
};

} // namespace starrocks::parquet
