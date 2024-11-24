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

package com.starrocks.connector.delta;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.cache.LoadingCache;
import com.starrocks.common.Pair;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.JsonHandler;
import io.delta.kernel.engine.ParquetHandler;
import io.delta.kernel.types.StructType;
import org.apache.hadoop.conf.Configuration;

import java.util.List;

public class DeltaLakeEngine extends DefaultEngine {
    private final Configuration hadoopConf;
    private final DeltaLakeCatalogProperties properties;
    // Cache for checkpoint metadata, key is file path and read schema, value is list of ColumnarBatch
    private final LoadingCache<Pair<DeltaLakeFileStatus, StructType>, List<ColumnarBatch>> checkpointCache;
    // Cache for json metadata, key is file path, value is list of JsonNode
    private final LoadingCache<DeltaLakeFileStatus, List<JsonNode>> jsonCache;

    protected DeltaLakeEngine(Configuration hadoopConf, DeltaLakeCatalogProperties properties,
                              LoadingCache<Pair<DeltaLakeFileStatus, StructType>, List<ColumnarBatch>> checkpointCache,
                              LoadingCache<DeltaLakeFileStatus, List<JsonNode>> jsonCache) {
        super(hadoopConf);
        this.hadoopConf = hadoopConf;
        this.properties = properties;
        this.checkpointCache = checkpointCache;
        this.jsonCache = jsonCache;
    }

    @Override
    public JsonHandler getJsonHandler() {
        return properties.isEnableDeltaLakeJsonMetaCache() ? new DeltaLakeJsonHandler(hadoopConf, jsonCache) :
                new TraceDefaultJsonHandler(hadoopConf);
    }

    @Override
    public ParquetHandler getParquetHandler() {
        return properties.isEnableDeltaLakeCheckpointMetaCache() ? new DeltaLakeParquetHandler(hadoopConf, checkpointCache) :
                new TraceDefaultParquetHandler(hadoopConf);
    }

    public static DeltaLakeEngine create(Configuration hadoopConf, DeltaLakeCatalogProperties properties,
                                         LoadingCache<Pair<DeltaLakeFileStatus, StructType>, List<ColumnarBatch>> checkpointCache,
                                         LoadingCache<DeltaLakeFileStatus, List<JsonNode>> jsonCache) {
        return new DeltaLakeEngine(hadoopConf, properties, checkpointCache, jsonCache);
    }
}
