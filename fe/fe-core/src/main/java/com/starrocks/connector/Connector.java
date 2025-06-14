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

package com.starrocks.connector;

import com.starrocks.common.Pair;
import com.starrocks.connector.config.ConnectorConfig;
import com.starrocks.memory.MemoryTrackable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public interface Connector extends MemoryTrackable {
    /**
     * Get the connector meta of connector
     *
     * @return a ConnectorMetadata instance of connector
     */
    ConnectorMetadata getMetadata();

    /**
     * Shutdown the connector by releasing any held resources such as
     * threads, sockets, etc. This method will only be called when no
     * queries are using the connector. After this method is called,
     * no methods will be called on the connector or any objects that
     * have been returned from the connector.
     */
    default void shutdown() {
    }

    /**
     * check connector config
     */
    default void bindConfig(ConnectorConfig config) {
    }

    default boolean supportMemoryTrack() {
        return false;
    }

    default Map<String, Long> estimateCount() {
        return new HashMap<>();
    }

    default List<Pair<List<Object>, Long>> getSamples() {
        return new ArrayList<>();
    }
}
