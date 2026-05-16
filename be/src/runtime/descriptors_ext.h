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

#include <map>
#include <memory_resource>
#include <optional>
#include <shared_mutex>
#include <string>
#include <vector>

#include "common/status.h"
#include "common/statusor.h"
#include "runtime/descriptors.h"

namespace starrocks {

class HdfsPartitionDescriptor {
public:
    HdfsPartitionDescriptor(const THdfsPartition& thrift_partition,
                            std::pmr::memory_resource* mr = std::pmr::get_default_resource());
    int64_t id() const { return _id; }
    THdfsFileFormat::type file_format() { return _file_format; }
    std::string_view location() { return _location; }
    // Thrift partition-key expressions (constant/literal, e.g. for hdfs://path/x=1/y=2/zzz
    // the partition slots are [x, y] and the partition key values are [1, 2]).
    //
    // Opened ExprContexts are not stored on the descriptor: ExprContext::prepare captures
    // a fragment RuntimeState, so the live evaluators must be built and closed within
    // fragment scope. Consumers (see HiveDataSource::_init_partition_values) build their
    // own ExprContexts from this thrift in a fragment-local pool.
    const std::vector<TExpr>& thrift_partition_key_exprs() const { return _thrift_partition_key_exprs; }
    std::string debug_string() const;

private:
    int64_t _id = 0;
    THdfsFileFormat::type _file_format;
    std::pmr::string _location;

    const std::vector<TExpr> _thrift_partition_key_exprs;
};

class HiveTableDescriptor : public TableDescriptor {
public:
    HiveTableDescriptor(const TTableDescriptor& tdesc, ObjectPool* pool,
                        std::pmr::memory_resource* mr = std::pmr::get_default_resource());
    virtual bool has_partition() const = 0;
    virtual bool is_partition_col(const SlotDescriptor* slot) const;
    virtual int get_partition_col_index(const SlotDescriptor* slot) const;
    virtual HdfsPartitionDescriptor* get_partition(int64_t partition_id) const;
    virtual bool has_base_path() const { return false; }
    virtual std::string_view get_base_path() const { return _table_location; }

    StatusOr<TPartitionMap*> deserialize_partition_map(const TCompressedPartitionMap& compressed_partition_map,
                                                       ObjectPool* pool);

    Status add_partition_value(ObjectPool* pool, int64_t id, const THdfsPartition& thrift_partition);
    std::optional<std::string> get_column_default_value(const SlotDescriptor* slot) const;

protected:
    std::pmr::string _hdfs_base_path;
    std::vector<TColumn> _columns;
    std::vector<TColumn> _partition_columns;
    mutable std::shared_mutex _map_mutex;
    std::map<int64_t, HdfsPartitionDescriptor*> _partition_id_to_desc_map;
    std::pmr::string _table_location;
    std::pmr::memory_resource* _mr;
};

class HdfsTableDescriptor : public HiveTableDescriptor {
public:
    HdfsTableDescriptor(const TTableDescriptor& tdesc, ObjectPool* pool,
                        std::pmr::memory_resource* mr = std::pmr::get_default_resource());
    ~HdfsTableDescriptor() override = default;
    bool has_partition() const override { return true; }
    std::string_view get_hive_column_names() const;
    std::string_view get_hive_column_types() const;
    std::string_view get_input_format() const;
    std::string_view get_serde_lib() const;
    const std::map<std::string, std::string> get_serde_properties() const;
    std::string_view get_time_zone() const;

private:
    std::pmr::string _serde_lib;
    std::pmr::string _input_format;
    std::pmr::string _hive_column_names;
    std::pmr::string _hive_column_types;
    std::map<std::string, std::string> _serde_properties;
    std::pmr::string _time_zone;
};

class IcebergTableDescriptor : public HiveTableDescriptor {
public:
    IcebergTableDescriptor(const TTableDescriptor& tdesc, ObjectPool* pool,
                           std::pmr::memory_resource* mr = std::pmr::get_default_resource());
    ~IcebergTableDescriptor() override = default;
    bool has_partition() const override { return false; }
    const TIcebergSchema* get_iceberg_schema() const { return &_t_iceberg_schema; }
    bool is_unpartitioned_table() { return _partition_column_names.empty(); }
    const std::vector<std::string>& partition_column_names() { return _partition_column_names; }
    const std::vector<TExpr>& get_partition_exprs() { return _partition_exprs; }
    const std::vector<std::string>& get_transform_exprs() { return _transform_exprs; }
    const std::vector<std::string> full_column_names();
    std::vector<int32_t> partition_source_index_in_schema();
    bool has_base_path() const override { return true; }
    const TSortOrder& sort_order() const { return _t_sort_order; }

    Status set_partition_desc_map(const TIcebergTable& thrift_table, ObjectPool* pool);

    const std::vector<std::string>& partition_source_column_names() { return _source_column_names; }

private:
    TIcebergSchema _t_iceberg_schema;
    std::vector<std::string> _source_column_names; // partition transform column's source column name
    std::vector<std::string> _partition_column_names;
    std::vector<std::string> _transform_exprs;
    std::vector<TExpr> _partition_exprs;
    TSortOrder _t_sort_order;
};

class FileTableDescriptor : public HiveTableDescriptor {
public:
    FileTableDescriptor(const TTableDescriptor& tdesc, ObjectPool* pool,
                        std::pmr::memory_resource* mr = std::pmr::get_default_resource());
    ~FileTableDescriptor() override = default;
    bool has_partition() const override { return false; }
    std::string_view get_table_locations() const;
    std::string_view get_hive_column_names() const;
    std::string_view get_hive_column_types() const;
    std::string_view get_input_format() const;
    std::string_view get_serde_lib() const;
    std::string_view get_time_zone() const;

private:
    std::pmr::string _serde_lib;
    std::pmr::string _input_format;
    std::pmr::string _hive_column_names;
    std::pmr::string _hive_column_types;
    std::pmr::string _time_zone;
};

class DeltaLakeTableDescriptor : public HiveTableDescriptor {
public:
    DeltaLakeTableDescriptor(const TTableDescriptor& tdesc, ObjectPool* pool,
                             std::pmr::memory_resource* mr = std::pmr::get_default_resource());
    ~DeltaLakeTableDescriptor() override = default;
    bool has_partition() const override { return true; }
    bool has_base_path() const override { return true; }
};

class HudiTableDescriptor : public HiveTableDescriptor {
public:
    HudiTableDescriptor(const TTableDescriptor& tdesc, ObjectPool* pool,
                        std::pmr::memory_resource* mr = std::pmr::get_default_resource());
    ~HudiTableDescriptor() override = default;
    bool has_partition() const override { return true; }
    std::string_view get_instant_time() const;
    std::string_view get_hive_column_names() const;
    std::string_view get_hive_column_types() const;
    std::string_view get_input_format() const;
    std::string_view get_serde_lib() const;
    std::string_view get_time_zone() const;

private:
    std::pmr::string _hudi_instant_time;
    std::pmr::string _hive_column_names;
    std::pmr::string _hive_column_types;
    std::pmr::string _input_format;
    std::pmr::string _serde_lib;
    std::pmr::string _time_zone;
};

class PaimonTableDescriptor : public HiveTableDescriptor {
public:
    PaimonTableDescriptor(const TTableDescriptor& tdesc, ObjectPool* pool,
                          std::pmr::memory_resource* mr = std::pmr::get_default_resource());
    ~PaimonTableDescriptor() override = default;
    bool has_partition() const override { return false; }
    std::string_view get_paimon_native_table() const;
    std::string_view get_time_zone() const;
    const TIcebergSchema* get_paimon_schema() const { return &_t_paimon_schema; }

private:
    std::pmr::string _paimon_native_table;
    std::pmr::string _time_zone;
    TIcebergSchema _t_paimon_schema;
};

class OdpsTableDescriptor : public HiveTableDescriptor {
public:
    OdpsTableDescriptor(const TTableDescriptor& tdesc, ObjectPool* pool,
                        std::pmr::memory_resource* mr = std::pmr::get_default_resource());
    ~OdpsTableDescriptor() override = default;
    bool has_partition() const override { return false; }
    std::string_view get_database_name() const;
    std::string_view get_table_name() const;
    std::string_view get_time_zone() const;

private:
    std::pmr::string _database_name;
    std::pmr::string _table_name;
    std::pmr::string _time_zone;
};

class IcebergMetadataTableDescriptor : public HiveTableDescriptor {
public:
    IcebergMetadataTableDescriptor(const TTableDescriptor& tdesc, ObjectPool* pool,
                                   std::pmr::memory_resource* mr = std::pmr::get_default_resource());
    ~IcebergMetadataTableDescriptor() override = default;
    std::string_view get_hive_column_names() const;
    std::string_view get_hive_column_types() const;
    std::string_view get_time_zone() const;
    bool has_partition() const override { return false; }

private:
    std::pmr::string _hive_column_names;
    std::pmr::string _hive_column_types;
    std::pmr::string _time_zone;
};

class KuduTableDescriptor : public HiveTableDescriptor {
public:
    KuduTableDescriptor(const TTableDescriptor& tdesc, ObjectPool* pool,
                        std::pmr::memory_resource* mr = std::pmr::get_default_resource());
    ~KuduTableDescriptor() override = default;
    bool has_partition() const override { return false; }
};

class OlapTableDescriptor : public TableDescriptor {
public:
    OlapTableDescriptor(const TTableDescriptor& tdesc,
                        std::pmr::memory_resource* mr = std::pmr::get_default_resource());
    std::string debug_string() const override;
};

class SchemaTableDescriptor : public TableDescriptor {
public:
    SchemaTableDescriptor(const TTableDescriptor& tdesc,
                          std::pmr::memory_resource* mr = std::pmr::get_default_resource());
    ~SchemaTableDescriptor() override;
    std::string debug_string() const override;
    TSchemaTableType::type schema_table_type() const { return _schema_table_type; }

private:
    TSchemaTableType::type _schema_table_type;
};

class BrokerTableDescriptor : public TableDescriptor {
public:
    BrokerTableDescriptor(const TTableDescriptor& tdesc,
                          std::pmr::memory_resource* mr = std::pmr::get_default_resource());
    ~BrokerTableDescriptor() override;
    std::string debug_string() const override;
};

class EsTableDescriptor : public TableDescriptor {
public:
    EsTableDescriptor(const TTableDescriptor& tdesc, std::pmr::memory_resource* mr = std::pmr::get_default_resource());
    ~EsTableDescriptor() override;
    std::string debug_string() const override;
};

class MySQLTableDescriptor : public TableDescriptor {
public:
    MySQLTableDescriptor(const TTableDescriptor& tdesc,
                         std::pmr::memory_resource* mr = std::pmr::get_default_resource());
    std::string debug_string() const override;
    std::string_view mysql_db() const { return _mysql_db; }
    std::string_view mysql_table() const { return _mysql_table; }
    std::string_view host() const { return _host; }
    std::string_view port() const { return _port; }
    std::string_view user() const { return _user; }
    std::string_view passwd() const { return _passwd; }

private:
    std::pmr::string _mysql_db;
    std::pmr::string _mysql_table;
    std::pmr::string _host;
    std::pmr::string _port;
    std::pmr::string _user;
    std::pmr::string _passwd;
};

class JDBCTableDescriptor : public TableDescriptor {
public:
    JDBCTableDescriptor(const TTableDescriptor& tdesc,
                        std::pmr::memory_resource* mr = std::pmr::get_default_resource());
    std::string debug_string() const override;
    std::string_view jdbc_driver_name() const { return _jdbc_driver_name; }
    std::string_view jdbc_driver_url() const { return _jdbc_driver_url; }
    std::string_view jdbc_driver_checksum() const { return _jdbc_driver_checksum; }
    std::string_view jdbc_driver_class() const { return _jdbc_driver_class; }
    std::string_view jdbc_url() const { return _jdbc_url; }
    std::string_view jdbc_table() const { return _jdbc_table; }
    std::string_view jdbc_user() const { return _jdbc_user; }
    std::string_view jdbc_passwd() const { return _jdbc_passwd; }

private:
    std::pmr::string _jdbc_driver_name;
    std::pmr::string _jdbc_driver_url;
    std::pmr::string _jdbc_driver_checksum;
    std::pmr::string _jdbc_driver_class;

    std::pmr::string _jdbc_url;
    std::pmr::string _jdbc_table;
    std::pmr::string _jdbc_user;
    std::pmr::string _jdbc_passwd;
};

} // namespace starrocks
