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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/runtime/descriptors.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "runtime/descriptors.h"

#include <boost/algorithm/string/join.hpp>
#include <ios>
#include <sstream>

#include "common/object_pool.h"
#include "common/status.h"
#include "exprs/base64.h"
#include "exprs/expr.h"
#include "gen_cpp/Descriptors_types.h"
#include "gen_cpp/PlanNodes_types.h"
#include "gen_cpp/descriptors.pb.h"
#include "util/compression/block_compression.h"
#include "util/thrift_util.h"

namespace starrocks {
using boost::algorithm::join;

const int RowDescriptor::INVALID_IDX = -1;
std::string NullIndicatorOffset::debug_string() const {
    std::stringstream out;
    out << "(offset=" << byte_offset << " mask=" << std::hex << static_cast<int>(bit_mask) << std::dec << ")";
    return out.str();
}

std::ostream& operator<<(std::ostream& os, const NullIndicatorOffset& null_indicator) {
    os << null_indicator.debug_string();
    return os;
}

SlotDescriptor::SlotDescriptor(SlotId id, std::string name, TypeDescriptor type)
        : _id(id),
          _type(std::move(type)),
          _parent(0),
          _null_indicator_offset(0, 0),
          _col_name(std::move(name)),
          _col_unique_id(-1),
          _slot_idx(0),
          _slot_size(_type.get_slot_size()),
          _is_materialized(false),
          _is_output_column(false),
          _is_nullable(true) {}

SlotDescriptor::SlotDescriptor(const TSlotDescriptor& tdesc)
        : _id(tdesc.id),
          _type(TypeDescriptor::from_thrift(tdesc.slotType)),
          _parent(tdesc.parent),
          _null_indicator_offset(tdesc.nullIndicatorByte, tdesc.nullIndicatorBit),
          _col_name(tdesc.colName),
          _col_unique_id(tdesc.col_unique_id),
          _slot_idx(tdesc.slotIdx),
          _slot_size(_type.get_slot_size()),
          _is_materialized(tdesc.isMaterialized),
          _is_output_column(tdesc.__isset.isOutputColumn ? tdesc.isOutputColumn : true),
          _is_nullable(tdesc.__isset.isNullable ? tdesc.isNullable : true) {}

SlotDescriptor::SlotDescriptor(const PSlotDescriptor& pdesc)
        : _id(pdesc.id()),
          _type(TypeDescriptor::from_protobuf(pdesc.slot_type())),
          _parent(pdesc.parent()),
          _null_indicator_offset(pdesc.null_indicator_byte(), pdesc.null_indicator_bit()),
          _col_name(pdesc.col_name()),
          _col_unique_id(-1),
          _slot_idx(pdesc.slot_idx()),
          _slot_size(_type.get_slot_size()),
          _is_materialized(pdesc.is_materialized()),
          _is_output_column(true),
          // keep same as is_nullable()
          _is_nullable(_null_indicator_offset.bit_mask != 0) {}

void SlotDescriptor::to_protobuf(PSlotDescriptor* pslot) const {
    pslot->set_id(_id);
    pslot->set_parent(_parent);
    *pslot->mutable_slot_type() = _type.to_protobuf();
    // NOTE: column_pos is not used anymore, use default value 0
    pslot->set_column_pos(0);
    // NOTE: _tuple_offset is not used anymore, use default value 0.
    pslot->set_byte_offset(0);
    pslot->set_null_indicator_byte(_null_indicator_offset.byte_offset);
    pslot->set_null_indicator_bit(_null_indicator_offset.bit_offset);
    pslot->set_col_name(_col_name);
    pslot->set_slot_idx(_slot_idx);
    pslot->set_is_materialized(_is_materialized);
}

std::string SlotDescriptor::debug_string() const {
    std::stringstream out;
    out << "Slot(id=" << _id << " type=" << _type << " name=" << _col_name
        << " null=" << _null_indicator_offset.debug_string() << ")";
    return out.str();
}

TableDescriptor::TableDescriptor(const TTableDescriptor& tdesc)
        : _name(tdesc.tableName), _database(tdesc.dbName), _id(tdesc.id) {}

std::string TableDescriptor::debug_string() const {
    std::stringstream out;
    out << "#name=" << _name;
    return out.str();
}

// ============== HDFS Table Descriptor ============

HdfsPartitionDescriptor::HdfsPartitionDescriptor(const THdfsTable& thrift_table, const THdfsPartition& thrift_partition)
        : _file_format(thrift_partition.file_format),
          _location(thrift_partition.location.suffix),
          _thrift_partition_key_exprs(thrift_partition.partition_key_exprs) {}

HdfsPartitionDescriptor::HdfsPartitionDescriptor(const THudiTable& thrift_table, const THdfsPartition& thrift_partition)
        : _file_format(thrift_partition.file_format),
          _location(thrift_partition.location.suffix),
          _thrift_partition_key_exprs(thrift_partition.partition_key_exprs) {}

HdfsPartitionDescriptor::HdfsPartitionDescriptor(const TDeltaLakeTable& thrift_table,
                                                 const THdfsPartition& thrift_partition)
        : _file_format(thrift_partition.file_format),
          _location(thrift_partition.location.suffix),
          _thrift_partition_key_exprs(thrift_partition.partition_key_exprs) {}

HdfsPartitionDescriptor::HdfsPartitionDescriptor(const TIcebergTable& thrift_table,
                                                 const THdfsPartition& thrift_partition)
        : _thrift_partition_key_exprs(thrift_partition.partition_key_exprs) {}

Status HdfsPartitionDescriptor::create_part_key_exprs(RuntimeState* state, ObjectPool* pool, int32_t chunk_size) {
    RETURN_IF_ERROR(Expr::create_expr_trees(pool, _thrift_partition_key_exprs, &_partition_key_value_evals, state));
    RETURN_IF_ERROR(Expr::prepare(_partition_key_value_evals, state));
    RETURN_IF_ERROR(Expr::open(_partition_key_value_evals, state));
    return Status::OK();
}

HdfsTableDescriptor::HdfsTableDescriptor(const TTableDescriptor& tdesc, ObjectPool* pool)
        : HiveTableDescriptor(tdesc, pool) {
    _hdfs_base_path = tdesc.hdfsTable.hdfs_base_dir;
    _columns = tdesc.hdfsTable.columns;
    _partition_columns = tdesc.hdfsTable.partition_columns;
    for (const auto& entry : tdesc.hdfsTable.partitions) {
        auto* partition = pool->add(new HdfsPartitionDescriptor(tdesc.hdfsTable, entry.second));
        _partition_id_to_desc_map[entry.first] = partition;
    }
    _hive_column_names = tdesc.hdfsTable.hive_column_names;
    _hive_column_types = tdesc.hdfsTable.hive_column_types;
    _input_format = tdesc.hdfsTable.input_format;
    _serde_lib = tdesc.hdfsTable.serde_lib;
}

const std::string& HdfsTableDescriptor::get_hive_column_names() const {
    return _hive_column_names;
}

const std::string& HdfsTableDescriptor::get_hive_column_types() const {
    return _hive_column_types;
}

const std::string& HdfsTableDescriptor::get_input_format() const {
    return _input_format;
}

const std::string& HdfsTableDescriptor::get_serde_lib() const {
    return _serde_lib;
}

FileTableDescriptor::FileTableDescriptor(const TTableDescriptor& tdesc, ObjectPool* pool)
        : HiveTableDescriptor(tdesc, pool) {
    _table_location = tdesc.fileTable.location;
    _columns = tdesc.fileTable.columns;
    _hive_column_names = tdesc.fileTable.hive_column_names;
    _hive_column_types = tdesc.fileTable.hive_column_types;
    _input_format = tdesc.fileTable.input_format;
    _serde_lib = tdesc.fileTable.serde_lib;
}

const std::string& FileTableDescriptor::get_hive_column_names() const {
    return _hive_column_names;
}

const std::string& FileTableDescriptor::get_hive_column_types() const {
    return _hive_column_types;
}

const std::string& FileTableDescriptor::get_input_format() const {
    return _input_format;
}

const std::string& FileTableDescriptor::get_serde_lib() const {
    return _serde_lib;
}

IcebergTableDescriptor::IcebergTableDescriptor(const TTableDescriptor& tdesc, ObjectPool* pool)
        : HiveTableDescriptor(tdesc, pool) {
    _table_location = tdesc.icebergTable.location;
    _columns = tdesc.icebergTable.columns;
    _t_iceberg_schema = tdesc.icebergTable.iceberg_schema;
    _partition_column_names = tdesc.icebergTable.partition_column_names;
}

std::vector<int32_t> IcebergTableDescriptor::partition_index_in_schema() {
    std::vector<int32_t> indexes;
    indexes.reserve(_partition_column_names.size());

    for (const auto& name : _partition_column_names) {
        for (int i = 0; i < _columns.size(); ++i) {
            if (_columns[i].column_name == name) {
                indexes.emplace_back(i);
                break;
            }
        }
    }

    return indexes;
}

const std::vector<std::string> IcebergTableDescriptor::full_column_names() {
    std::vector<std::string> full_column_names;
    for (const auto& column : _columns) {
        full_column_names.emplace_back(column.column_name);
    }

    return full_column_names;
}

Status IcebergTableDescriptor::set_partition_desc_map(const starrocks::TIcebergTable& thrift_table,
                                                      starrocks::ObjectPool* pool) {
    if (thrift_table.__isset.compressed_partitions) {
        ASSIGN_OR_RETURN(TPartitionMap * tPartitionMap,
                         deserialize_partition_map(thrift_table.compressed_partitions, pool));
        for (const auto& entry : tPartitionMap->partitions) {
            auto* partition = pool->add(new HdfsPartitionDescriptor(thrift_table, entry.second));
            _partition_id_to_desc_map[entry.first] = partition;
        }
    } else {
        for (const auto& entry : thrift_table.partitions) {
            auto* partition = pool->add(new HdfsPartitionDescriptor(thrift_table, entry.second));
            _partition_id_to_desc_map[entry.first] = partition;
        }
    }
    return Status::OK();
}

DeltaLakeTableDescriptor::DeltaLakeTableDescriptor(const TTableDescriptor& tdesc, ObjectPool* pool)
        : HiveTableDescriptor(tdesc, pool) {
    _table_location = tdesc.deltaLakeTable.location;
    _columns = tdesc.deltaLakeTable.columns;
    _partition_columns = tdesc.deltaLakeTable.partition_columns;
    for (const auto& entry : tdesc.deltaLakeTable.partitions) {
        auto* partition = pool->add(new HdfsPartitionDescriptor(tdesc.deltaLakeTable, entry.second));
        _partition_id_to_desc_map[entry.first] = partition;
    }
}

HudiTableDescriptor::HudiTableDescriptor(const TTableDescriptor& tdesc, ObjectPool* pool)
        : HiveTableDescriptor(tdesc, pool) {
    _table_location = tdesc.hudiTable.location;
    _columns = tdesc.hudiTable.columns;
    _partition_columns = tdesc.hudiTable.partition_columns;
    for (const auto& entry : tdesc.hudiTable.partitions) {
        auto* partition = pool->add(new HdfsPartitionDescriptor(tdesc.hudiTable, entry.second));
        _partition_id_to_desc_map[entry.first] = partition;
    }
    _hudi_instant_time = tdesc.hudiTable.instant_time;
    _hive_column_names = tdesc.hudiTable.hive_column_names;
    _hive_column_types = tdesc.hudiTable.hive_column_types;
    _input_format = tdesc.hudiTable.input_format;
    _serde_lib = tdesc.hudiTable.serde_lib;
}

const std::string& HudiTableDescriptor::get_instant_time() const {
    return _hudi_instant_time;
}

const std::string& HudiTableDescriptor::get_hive_column_names() const {
    return _hive_column_names;
}

const std::string& HudiTableDescriptor::get_hive_column_types() const {
    return _hive_column_types;
}

const std::string& HudiTableDescriptor::get_input_format() const {
    return _input_format;
}

const std::string& HudiTableDescriptor::get_serde_lib() const {
    return _serde_lib;
}

PaimonTableDescriptor::PaimonTableDescriptor(const TTableDescriptor& tdesc, ObjectPool* pool)
        : HiveTableDescriptor(tdesc, pool) {
    _catalog_type = tdesc.paimonTable.catalog_type;
    _metastore_uri = tdesc.paimonTable.metastore_uri;
    _warehouse_path = tdesc.paimonTable.warehouse_path;
    _database_name = tdesc.dbName;
    _table_name = tdesc.tableName;
}

const std::string& PaimonTableDescriptor::get_catalog_type() const {
    return _catalog_type;
}

const std::string& PaimonTableDescriptor::get_metastore_uri() const {
    return _metastore_uri;
}

const std::string& PaimonTableDescriptor::get_warehouse_path() const {
    return _warehouse_path;
}

const std::string& PaimonTableDescriptor::get_database_name() const {
    return _database_name;
}

const std::string& PaimonTableDescriptor::get_table_name() const {
    return _table_name;
}

HiveTableDescriptor::HiveTableDescriptor(const TTableDescriptor& tdesc, ObjectPool* pool) : TableDescriptor(tdesc) {}

bool HiveTableDescriptor::is_partition_col(const SlotDescriptor* slot) const {
    return get_partition_col_index(slot) >= 0;
}

HdfsPartitionDescriptor* HiveTableDescriptor::get_partition(int64_t partition_id) const {
    auto it = _partition_id_to_desc_map.find(partition_id);
    if (it == _partition_id_to_desc_map.end()) {
        return nullptr;
    }
    return it->second;
}

int HiveTableDescriptor::get_partition_col_index(const SlotDescriptor* slot) const {
    int idx = 0;
    for (const auto& partition_column : _partition_columns) {
        if (partition_column.column_name == slot->col_name()) {
            return idx;
        }
        ++idx;
    }
    return -1;
}

StatusOr<TPartitionMap*> HiveTableDescriptor::deserialize_partition_map(
        const TCompressedPartitionMap& compressed_partition_map, ObjectPool* pool) {
    const std::string& base64_partition_map = compressed_partition_map.compressed_serialized_partitions;
    std::string compressed_buf;
    compressed_buf.resize(base64_partition_map.size() + 3);
    base64_decode2(base64_partition_map.data(), base64_partition_map.size(), compressed_buf.data());
    compressed_buf.resize(compressed_partition_map.compressed_len);

    std::string uncompressed_buf;
    uncompressed_buf.resize(compressed_partition_map.original_len);
    Slice uncompress_output(uncompressed_buf);
    const BlockCompressionCodec* zlib_uncompress_codec = nullptr;
    RETURN_IF_ERROR(get_block_compression_codec(starrocks::CompressionTypePB::ZLIB, &zlib_uncompress_codec));
    RETURN_IF_ERROR(zlib_uncompress_codec->decompress(compressed_buf, &uncompress_output));

    TPartitionMap* tPartitionMap = pool->add(new TPartitionMap());
    RETURN_IF_ERROR(deserialize_thrift_msg(reinterpret_cast<uint8_t*>(uncompress_output.data),
                                           reinterpret_cast<uint32_t*>(&uncompress_output.size), TProtocolType::BINARY,
                                           tPartitionMap));

    return tPartitionMap;
}

// =============================================

OlapTableDescriptor::OlapTableDescriptor(const TTableDescriptor& tdesc) : TableDescriptor(tdesc) {}

std::string OlapTableDescriptor::debug_string() const {
    std::stringstream out;
    out << "OlapTable(" << TableDescriptor::debug_string() << ")";
    return out.str();
}

SchemaTableDescriptor::SchemaTableDescriptor(const TTableDescriptor& tdesc)
        : TableDescriptor(tdesc), _schema_table_type(tdesc.schemaTable.tableType) {}
SchemaTableDescriptor::~SchemaTableDescriptor() = default;

std::string SchemaTableDescriptor::debug_string() const {
    std::stringstream out;
    out << "SchemaTable(" << TableDescriptor::debug_string() << ")";
    return out.str();
}

BrokerTableDescriptor::BrokerTableDescriptor(const TTableDescriptor& tdesc) : TableDescriptor(tdesc) {}

BrokerTableDescriptor::~BrokerTableDescriptor() = default;

std::string BrokerTableDescriptor::debug_string() const {
    std::stringstream out;
    out << "BrokerTable(" << TableDescriptor::debug_string() << ")";
    return out.str();
}

EsTableDescriptor::EsTableDescriptor(const TTableDescriptor& tdesc) : TableDescriptor(tdesc) {}

EsTableDescriptor::~EsTableDescriptor() = default;

std::string EsTableDescriptor::debug_string() const {
    std::stringstream out;
    out << "EsTable(" << TableDescriptor::debug_string() << ")";
    return out.str();
}

MySQLTableDescriptor::MySQLTableDescriptor(const TTableDescriptor& tdesc)
        : TableDescriptor(tdesc),
          _mysql_db(tdesc.mysqlTable.db),
          _mysql_table(tdesc.mysqlTable.table),
          _host(tdesc.mysqlTable.host),
          _port(tdesc.mysqlTable.port),
          _user(tdesc.mysqlTable.user),
          _passwd(tdesc.mysqlTable.passwd) {}

std::string MySQLTableDescriptor::debug_string() const {
    std::stringstream out;
    out << "MySQLTable(" << TableDescriptor::debug_string() << " _db" << _mysql_db << " table=" << _mysql_table
        << " host=" << _host << " port=" << _port << " user=" << _user << " passwd=" << _passwd;
    return out.str();
}

JDBCTableDescriptor::JDBCTableDescriptor(const TTableDescriptor& tdesc)
        : TableDescriptor(tdesc),
          _jdbc_driver_name(tdesc.jdbcTable.jdbc_driver_name),
          _jdbc_driver_url(tdesc.jdbcTable.jdbc_driver_url),
          _jdbc_driver_checksum(tdesc.jdbcTable.jdbc_driver_checksum),
          _jdbc_driver_class(tdesc.jdbcTable.jdbc_driver_class),
          _jdbc_url(tdesc.jdbcTable.jdbc_url),
          _jdbc_table(tdesc.jdbcTable.jdbc_table),
          _jdbc_user(tdesc.jdbcTable.jdbc_user),
          _jdbc_passwd(tdesc.jdbcTable.jdbc_passwd) {}

std::string JDBCTableDescriptor::debug_string() const {
    std::stringstream out;
    out << "JDBCTable(" << TableDescriptor::debug_string() << " jdbc_driver_name=" << _jdbc_driver_name
        << " jdbc_driver_url=" << _jdbc_driver_url << " jdbc_driver_checksum=" << _jdbc_driver_checksum
        << " jdbc_driver_class=" << _jdbc_driver_class << " jdbc_url=" << _jdbc_url << " jdbc_table=" << _jdbc_table
        << " jdbc_user=" << _jdbc_user << " jdbc_passwd=" << _jdbc_passwd << "}";
    return out.str();
}

TupleDescriptor::TupleDescriptor(const TTupleDescriptor& tdesc)
        : _id(tdesc.id), _table_desc(nullptr), _byte_size(tdesc.byteSize) {}

TupleDescriptor::TupleDescriptor(const PTupleDescriptor& pdesc)
        : _id(pdesc.id()), _table_desc(nullptr), _byte_size(pdesc.byte_size()) {}

void TupleDescriptor::add_slot(SlotDescriptor* slot) {
    _slots.push_back(slot);
    _decoded_slots.push_back(slot);
}

void TupleDescriptor::to_protobuf(PTupleDescriptor* ptuple) const {
    ptuple->Clear();
    ptuple->set_id(_id);
    ptuple->set_byte_size(_byte_size);
    // NOTE: _num_null_bytes is not used, set a default value 1
    ptuple->set_num_null_bytes(1);
    ptuple->set_table_id(-1);
    // NOTE: _num_null_slots is not used, set a default value 1
    ptuple->set_num_null_slots(1);
}

std::string TupleDescriptor::debug_string() const {
    std::stringstream out;
    out << "Tuple(id=" << _id << " size=" << _byte_size;
    if (_table_desc != nullptr) {
        //out << " " << _table_desc->debug_string();
    }

    out << " slots=[";
    for (size_t i = 0; i < _slots.size(); ++i) {
        if (i > 0) {
            out << ", ";
        }
        out << _slots[i]->debug_string();
    }

    out << "]";
    out << ")";
    return out.str();
}

RowDescriptor::RowDescriptor(const DescriptorTbl& desc_tbl, const std::vector<TTupleId>& row_tuples,
                             const std::vector<bool>& nullable_tuples)
        : _tuple_idx_nullable_map(nullable_tuples) {
    DCHECK(nullable_tuples.size() == row_tuples.size());
    DCHECK_GT(row_tuples.size(), 0);

    for (int row_tuple : row_tuples) {
        TupleDescriptor* tupleDesc = desc_tbl.get_tuple_descriptor(row_tuple);
        _tuple_desc_map.push_back(tupleDesc);
        DCHECK(_tuple_desc_map.back() != nullptr);
    }

    init_tuple_idx_map();
}

RowDescriptor::RowDescriptor(TupleDescriptor* tuple_desc, bool is_nullable)
        : _tuple_desc_map(1, tuple_desc), _tuple_idx_nullable_map(1, is_nullable) {
    init_tuple_idx_map();
}

void RowDescriptor::init_tuple_idx_map() {
    // find max id
    TupleId max_id = 0;
    for (auto& i : _tuple_desc_map) {
        max_id = std::max(i->id(), max_id);
    }

    _tuple_idx_map.resize(max_id + 1, INVALID_IDX);
    for (int i = 0; i < _tuple_desc_map.size(); ++i) {
        _tuple_idx_map[_tuple_desc_map[i]->id()] = i;
    }
}

int RowDescriptor::get_tuple_idx(TupleId id) const {
    DCHECK_LT(id, _tuple_idx_map.size()) << "RowDescriptor: " << debug_string();
    return _tuple_idx_map[id];
}

void RowDescriptor::to_thrift(std::vector<TTupleId>* row_tuple_ids) {
    row_tuple_ids->clear();

    for (auto& i : _tuple_desc_map) {
        row_tuple_ids->push_back(i->id());
    }
}

void RowDescriptor::to_protobuf(google::protobuf::RepeatedField<google::protobuf::int32>* row_tuple_ids) {
    row_tuple_ids->Clear();
    for (auto desc : _tuple_desc_map) {
        row_tuple_ids->Add(desc->id());
    }
}

bool RowDescriptor::is_prefix_of(const RowDescriptor& other_desc) const {
    if (_tuple_desc_map.size() > other_desc._tuple_desc_map.size()) {
        return false;
    }

    for (int i = 0; i < _tuple_desc_map.size(); ++i) {
        // pointer comparison okay, descriptors are unique
        if (_tuple_desc_map[i] != other_desc._tuple_desc_map[i]) {
            return false;
        }
    }

    return true;
}

bool RowDescriptor::equals(const RowDescriptor& other_desc) const {
    if (_tuple_desc_map.size() != other_desc._tuple_desc_map.size()) {
        return false;
    }

    for (int i = 0; i < _tuple_desc_map.size(); ++i) {
        // pointer comparison okay, descriptors are unique
        if (_tuple_desc_map[i] != other_desc._tuple_desc_map[i]) {
            return false;
        }
    }

    return true;
}

std::string RowDescriptor::debug_string() const {
    std::stringstream ss;

    ss << "tuple_desc_map: [";
    for (int i = 0; i < _tuple_desc_map.size(); ++i) {
        ss << _tuple_desc_map[i]->debug_string();
        if (i != _tuple_desc_map.size() - 1) {
            ss << ", ";
        }
    }
    ss << "] ";

    ss << "tuple_id_map: [";
    for (int i = 0; i < _tuple_idx_map.size(); ++i) {
        ss << _tuple_idx_map[i];
        if (i != _tuple_idx_map.size() - 1) {
            ss << ", ";
        }
    }
    ss << "] ";

    ss << "tuple_is_nullable: [";
    for (int i = 0; i < _tuple_idx_nullable_map.size(); ++i) {
        ss << _tuple_idx_nullable_map[i];
        if (i != _tuple_idx_nullable_map.size() - 1) {
            ss << ", ";
        }
    }
    ss << "] ";

    return ss.str();
}

Status DescriptorTbl::create(RuntimeState* state, ObjectPool* pool, const TDescriptorTable& thrift_tbl,
                             DescriptorTbl** tbl, int32_t chunk_size) {
    *tbl = pool->add(new DescriptorTbl());

    // deserialize table descriptors first, they are being referenced by tuple descriptors
    for (const auto& tdesc : thrift_tbl.tableDescriptors) {
        TableDescriptor* desc = nullptr;

        switch (tdesc.tableType) {
        case TTableType::MYSQL_TABLE:
            desc = pool->add(new MySQLTableDescriptor(tdesc));
            break;

        case TTableType::OLAP_TABLE:
        case TTableType::MATERIALIZED_VIEW:
            desc = pool->add(new OlapTableDescriptor(tdesc));
            break;

        case TTableType::SCHEMA_TABLE:
            desc = pool->add(new SchemaTableDescriptor(tdesc));
            break;
        case TTableType::BROKER_TABLE:
            desc = pool->add(new BrokerTableDescriptor(tdesc));
            break;
        case TTableType::ES_TABLE:
            desc = pool->add(new EsTableDescriptor(tdesc));
            break;
        case TTableType::HDFS_TABLE: {
            auto* hdfs_desc = pool->add(new HdfsTableDescriptor(tdesc, pool));
            RETURN_IF_ERROR(hdfs_desc->create_key_exprs(state, pool, chunk_size));
            desc = hdfs_desc;
            break;
        }
        case TTableType::FILE_TABLE: {
            desc = pool->add(new FileTableDescriptor(tdesc, pool));
            break;
        }
        case TTableType::ICEBERG_TABLE: {
            auto* iceberg_desc = pool->add(new IcebergTableDescriptor(tdesc, pool));
            RETURN_IF_ERROR(iceberg_desc->set_partition_desc_map(tdesc.icebergTable, pool));
            RETURN_IF_ERROR(iceberg_desc->create_key_exprs(state, pool, chunk_size));
            desc = iceberg_desc;
            break;
        }
        case TTableType::DELTALAKE_TABLE: {
            auto* delta_lake_desc = pool->add(new DeltaLakeTableDescriptor(tdesc, pool));
            RETURN_IF_ERROR(delta_lake_desc->create_key_exprs(state, pool, chunk_size));
            desc = delta_lake_desc;
            break;
        }
        case TTableType::HUDI_TABLE: {
            auto* hudi_desc = pool->add(new HudiTableDescriptor(tdesc, pool));
            RETURN_IF_ERROR(hudi_desc->create_key_exprs(state, pool, chunk_size));
            desc = hudi_desc;
            break;
        }
        case TTableType::PAIMON_TABLE: {
            desc = pool->add(new PaimonTableDescriptor(tdesc, pool));
            break;
        }
        case TTableType::JDBC_TABLE: {
            desc = pool->add(new JDBCTableDescriptor(tdesc));
            break;
        }
        default:
            DCHECK(false) << "invalid table type: " << tdesc.tableType;
        }

        (*tbl)->_tbl_desc_map[tdesc.id] = desc;
    }

    for (const auto& tdesc : thrift_tbl.tupleDescriptors) {
        TupleDescriptor* desc = pool->add(new TupleDescriptor(tdesc));

        // fix up table pointer
        if (tdesc.__isset.tableId) {
            desc->_table_desc = (*tbl)->get_table_descriptor(tdesc.tableId);
            DCHECK(desc->_table_desc != nullptr);
        }

        (*tbl)->_tuple_desc_map[tdesc.id] = desc;
    }

    for (const auto& tdesc : thrift_tbl.slotDescriptors) {
        SlotDescriptor* slot_d = pool->add(new SlotDescriptor(tdesc));
        (*tbl)->_slot_desc_map[tdesc.id] = slot_d;

        // link to parent
        auto entry = (*tbl)->_tuple_desc_map.find(tdesc.parent);

        if (entry == (*tbl)->_tuple_desc_map.end()) {
            return Status::InternalError("unknown tid in slot descriptor msg");
        }

        entry->second->add_slot(slot_d);
    }

    return Status::OK();
}

TableDescriptor* DescriptorTbl::get_table_descriptor(TableId id) const {
    auto i = _tbl_desc_map.find(id);
    if (i == _tbl_desc_map.end()) {
        return nullptr;
    } else {
        return i->second;
    }
}

TupleDescriptor* DescriptorTbl::get_tuple_descriptor(TupleId id) const {
    auto i = _tuple_desc_map.find(id);
    if (i == _tuple_desc_map.end()) {
        return nullptr;
    } else {
        return i->second;
    }
}

SlotDescriptor* DescriptorTbl::get_slot_descriptor(SlotId id) const {
    // TODO: is there some boost function to do exactly this?
    auto i = _slot_desc_map.find(id);

    if (i == _slot_desc_map.end()) {
        return nullptr;
    } else {
        return i->second;
    }
}

// return all registered tuple descriptors
void DescriptorTbl::get_tuple_descs(std::vector<TupleDescriptor*>* descs) const {
    descs->clear();

    for (auto i : _tuple_desc_map) {
        descs->push_back(i.second);
    }
}

std::string DescriptorTbl::debug_string() const {
    std::stringstream out;
    out << "tuples:\n";

    for (auto i : _tuple_desc_map) {
        out << i.second->debug_string() << '\n';
    }

    return out.str();
}

} // namespace starrocks
