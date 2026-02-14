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
#include <shared_mutex>
#include <string>
#include <vector>

#include "runtime/descriptors.h"

namespace starrocks {

class ExprContext;

class HdfsPartitionDescriptor {
public:
    HdfsPartitionDescriptor(const THdfsPartition& thrift_partition);
    int64_t id() const { return _id; }
    THdfsFileFormat::type file_format() { return _file_format; }
    std::string& location() { return _location; }
    // ExprContext is constant/literal for sure
    // such as hdfs://path/x=1/y=2/zzz, then
    // partition slots would be [x, y]
    // partition key values wold be [1, 2]
    std::vector<ExprContext*>& partition_key_value_evals() { return _partition_key_value_evals; }
    const std::vector<TExpr>& thrift_partition_key_exprs() const { return _thrift_partition_key_exprs; }
    Status create_part_key_exprs(RuntimeState* state, ObjectPool* pool);
    std::string debug_string() const;

private:
    int64_t _id = 0;
    THdfsFileFormat::type _file_format;
    std::string _location;

    // holding thrift exprs for partition keys for duplication check during runtime
    const std::vector<TExpr> _thrift_partition_key_exprs;
    std::vector<ExprContext*> _partition_key_value_evals;
};

class HiveTableDescriptor : public TableDescriptor {
public:
    HiveTableDescriptor(const TTableDescriptor& tdesc, ObjectPool* pool);
    virtual bool has_partition() const = 0;
    virtual bool is_partition_col(const SlotDescriptor* slot) const;
    virtual int get_partition_col_index(const SlotDescriptor* slot) const;
    virtual HdfsPartitionDescriptor* get_partition(int64_t partition_id) const;
    virtual bool has_base_path() const { return false; }
    virtual const std::string& get_base_path() const { return _table_location; }

    Status create_key_exprs(RuntimeState* state, ObjectPool* pool) {
        for (auto& part : _partition_id_to_desc_map) {
            RETURN_IF_ERROR(part.second->create_part_key_exprs(state, pool));
        }
        return Status::OK();
    }

    StatusOr<TPartitionMap*> deserialize_partition_map(const TCompressedPartitionMap& compressed_partition_map,
                                                       ObjectPool* pool);

    Status add_partition_value(RuntimeState* runtime_state, ObjectPool* pool, int64_t id,
                               const THdfsPartition& thrift_partition);

protected:
    std::string _hdfs_base_path;
    std::vector<TColumn> _columns;
    std::vector<TColumn> _partition_columns;
    mutable std::shared_mutex _map_mutex;
    std::map<int64_t, HdfsPartitionDescriptor*> _partition_id_to_desc_map;
    std::string _table_location;
};

class HdfsTableDescriptor : public HiveTableDescriptor {
public:
    HdfsTableDescriptor(const TTableDescriptor& tdesc, ObjectPool* pool);
    ~HdfsTableDescriptor() override = default;
    bool has_partition() const override { return true; }
    const std::string& get_hive_column_names() const;
    const std::string& get_hive_column_types() const;
    const std::string& get_input_format() const;
    const std::string& get_serde_lib() const;
    const std::map<std::string, std::string> get_serde_properties() const;
    const std::string& get_time_zone() const;

private:
    std::string _serde_lib;
    std::string _input_format;
    std::string _hive_column_names;
    std::string _hive_column_types;
    std::map<std::string, std::string> _serde_properties;
    std::string _time_zone;
};

class IcebergTableDescriptor : public HiveTableDescriptor {
public:
    IcebergTableDescriptor(const TTableDescriptor& tdesc, ObjectPool* pool);
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
    FileTableDescriptor(const TTableDescriptor& tdesc, ObjectPool* pool);
    ~FileTableDescriptor() override = default;
    bool has_partition() const override { return false; }
    const std::string& get_table_locations() const;
    const std::string& get_hive_column_names() const;
    const std::string& get_hive_column_types() const;
    const std::string& get_input_format() const;
    const std::string& get_serde_lib() const;
    const std::string& get_time_zone() const;

private:
    std::string _serde_lib;
    std::string _input_format;
    std::string _hive_column_names;
    std::string _hive_column_types;
    std::string _time_zone;
};

class DeltaLakeTableDescriptor : public HiveTableDescriptor {
public:
    DeltaLakeTableDescriptor(const TTableDescriptor& tdesc, ObjectPool* pool);
    ~DeltaLakeTableDescriptor() override = default;
    bool has_partition() const override { return true; }
    bool has_base_path() const override { return true; }
};

class HudiTableDescriptor : public HiveTableDescriptor {
public:
    HudiTableDescriptor(const TTableDescriptor& tdesc, ObjectPool* pool);
    ~HudiTableDescriptor() override = default;
    bool has_partition() const override { return true; }
    const std::string& get_instant_time() const;
    const std::string& get_hive_column_names() const;
    const std::string& get_hive_column_types() const;
    const std::string& get_input_format() const;
    const std::string& get_serde_lib() const;
    const std::string& get_time_zone() const;

private:
    std::string _hudi_instant_time;
    std::string _hive_column_names;
    std::string _hive_column_types;
    std::string _input_format;
    std::string _serde_lib;
    std::string _time_zone;
};

class PaimonTableDescriptor : public HiveTableDescriptor {
public:
    PaimonTableDescriptor(const TTableDescriptor& tdesc, ObjectPool* pool);
    ~PaimonTableDescriptor() override = default;
    bool has_partition() const override { return false; }
    const std::string& get_paimon_native_table() const;
    const std::string& get_time_zone() const;
    const TIcebergSchema* get_paimon_schema() const { return &_t_paimon_schema; }

private:
    std::string _paimon_native_table;
    std::string _time_zone;
    TIcebergSchema _t_paimon_schema;
};

class OdpsTableDescriptor : public HiveTableDescriptor {
public:
    OdpsTableDescriptor(const TTableDescriptor& tdesc, ObjectPool* pool);
    ~OdpsTableDescriptor() override = default;
    bool has_partition() const override { return false; }
    const std::string& get_database_name() const;
    const std::string& get_table_name() const;
    const std::string& get_time_zone() const;

private:
    std::string _database_name;
    std::string _table_name;
    std::string _time_zone;
};

class IcebergMetadataTableDescriptor : public HiveTableDescriptor {
public:
    IcebergMetadataTableDescriptor(const TTableDescriptor& tdesc, ObjectPool* pool);
    ~IcebergMetadataTableDescriptor() override = default;
    const std::string& get_hive_column_names() const;
    const std::string& get_hive_column_types() const;
    const std::string& get_time_zone() const;
    bool has_partition() const override { return false; }

private:
    std::string _hive_column_names;
    std::string _hive_column_types;
    std::string _time_zone;
};

class KuduTableDescriptor : public HiveTableDescriptor {
public:
    KuduTableDescriptor(const TTableDescriptor& tdesc, ObjectPool* pool);
    ~KuduTableDescriptor() override = default;
    bool has_partition() const override { return false; }
};

class OlapTableDescriptor : public TableDescriptor {
public:
    OlapTableDescriptor(const TTableDescriptor& tdesc);
    std::string debug_string() const override;
};

class SchemaTableDescriptor : public TableDescriptor {
public:
    SchemaTableDescriptor(const TTableDescriptor& tdesc);
    ~SchemaTableDescriptor() override;
    std::string debug_string() const override;
    TSchemaTableType::type schema_table_type() const { return _schema_table_type; }

private:
    TSchemaTableType::type _schema_table_type;
};

class BrokerTableDescriptor : public TableDescriptor {
public:
    BrokerTableDescriptor(const TTableDescriptor& tdesc);
    ~BrokerTableDescriptor() override;
    std::string debug_string() const override;
};

class EsTableDescriptor : public TableDescriptor {
public:
    EsTableDescriptor(const TTableDescriptor& tdesc);
    ~EsTableDescriptor() override;
    std::string debug_string() const override;
};

class MySQLTableDescriptor : public TableDescriptor {
public:
    MySQLTableDescriptor(const TTableDescriptor& tdesc);
    std::string debug_string() const override;
    const std::string mysql_db() const { return _mysql_db; }
    const std::string mysql_table() const { return _mysql_table; }
    const std::string host() const { return _host; }
    const std::string port() const { return _port; }
    const std::string user() const { return _user; }
    const std::string passwd() const { return _passwd; }

private:
    std::string _mysql_db;
    std::string _mysql_table;
    std::string _host;
    std::string _port;
    std::string _user;
    std::string _passwd;
};

class JDBCTableDescriptor : public TableDescriptor {
public:
    JDBCTableDescriptor(const TTableDescriptor& tdesc);
    std::string debug_string() const override;
    const std::string jdbc_driver_name() const { return _jdbc_driver_name; }
    const std::string jdbc_driver_url() const { return _jdbc_driver_url; }
    const std::string jdbc_driver_checksum() const { return _jdbc_driver_checksum; }
    const std::string jdbc_driver_class() const { return _jdbc_driver_class; }
    const std::string jdbc_url() const { return _jdbc_url; }
    const std::string jdbc_table() const { return _jdbc_table; }
    const std::string jdbc_user() const { return _jdbc_user; }
    const std::string jdbc_passwd() const { return _jdbc_passwd; }

private:
    std::string _jdbc_driver_name;
    std::string _jdbc_driver_url;
    std::string _jdbc_driver_checksum;
    std::string _jdbc_driver_class;

    std::string _jdbc_url;
    std::string _jdbc_table;
    std::string _jdbc_user;
    std::string _jdbc_passwd;
};

} // namespace starrocks
