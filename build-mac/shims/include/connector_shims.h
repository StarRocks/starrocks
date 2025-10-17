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

#pragma once

#include <memory>
#include <string>
#include <vector>
#include <unordered_map>

// Forward declarations for compatibility
namespace arrow {
class Schema;
}

namespace starrocks {

// Forward declarations
class Status;
class RuntimeState;
class ObjectPool;
class TExpr;
class TDataSink;
class MemTracker;
class ExprContext;

namespace vectorized {
class Chunk;
using ChunkPtr = std::shared_ptr<Chunk>;
}

namespace pipeline {
class OperatorFactory;
class PipelineBuilderContext;
}

// Additional forward declarations
struct TDataSourceScanDesc;
class RuntimeDescriptor;

}

namespace starrocks {

class RuntimeState;
class ObjectPool;
class TExpr;
class TDataSink;
class MemTracker;
class ExprContext;

namespace connector {

class DataSource {
public:
    static const std::string PROFILE_NAME;

    DataSource();
    virtual ~DataSource();

    virtual Status open(RuntimeState* state) = 0;
    virtual std::shared_ptr<arrow::Schema> arrow_schema() = 0;
    virtual Status get_next(RuntimeState* state, vectorized::ChunkPtr* chunk, bool* eof) = 0;
};

class Connector {
public:
    Connector();
    virtual ~Connector();

    virtual std::unique_ptr<DataSource> create_data_source(const TDataSourceScanDesc& scan_desc,
                                                          const RuntimeDescriptor& desc) = 0;
};

class DataSourceProvider {
public:
    DataSourceProvider();
    virtual ~DataSourceProvider();

    virtual std::shared_ptr<DataSource> create_data_source(const TDataSourceScanDesc& scan_desc,
                                                          const RuntimeDescriptor& desc) = 0;
};

class ConnectorManager {
public:
    static ConnectorManager* default_instance();

    ConnectorManager();
    ~ConnectorManager();

    bool register_connector(const std::string& name, std::unique_ptr<Connector> connector);
    std::shared_ptr<DataSourceProvider> get_data_source_provider(const std::string& name);

private:
    std::unordered_map<std::string, std::unique_ptr<Connector>> _connectors;
};

class SinkMemoryManager {
public:
    SinkMemoryManager();
    ~SinkMemoryManager();

    size_t get_memory_usage() const;
    void update_memory_usage(size_t usage);

private:
    size_t _memory_usage;
};

class ConnectorSink {
public:
    ConnectorSink();
    virtual ~ConnectorSink();

    virtual Status open(RuntimeState* state) = 0;
    virtual Status send_chunk(RuntimeState* state, vectorized::Chunk* chunk) = 0;
    virtual Status close(RuntimeState* status, Status exec_status) = 0;

    virtual void decompose_to_pipeline(std::vector<std::shared_ptr<pipeline::OperatorFactory>>& operators,
                                     const TDataSink& sink,
                                     pipeline::PipelineBuilderContext* context) = 0;
};

} // namespace connector
} // namespace starrocks
