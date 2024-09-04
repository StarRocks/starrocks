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
import com.starrocks.connector.informationschema.InformationSchemaConnector;
<<<<<<< HEAD
=======
import com.starrocks.connector.metadata.TableMetaConnector;

import java.util.List;
import java.util.Map;
>>>>>>> f0cb5e97c8 ([Enhancement] Optimize memory tracker (#49841))

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class CatalogConnector implements Connector {
    private final Connector normalConnector;
    private final Connector informationSchemaConnector;

    public CatalogConnector(Connector normalConnector, InformationSchemaConnector informationSchemaConnector) {
        requireNonNull(normalConnector, "normalConnector is null");
        requireNonNull(informationSchemaConnector, "informationSchemaConnector is null");
        checkArgument(!(normalConnector instanceof InformationSchemaConnector), "normalConnector is InformationSchemaConnector");
        this.normalConnector = normalConnector;
        this.informationSchemaConnector = informationSchemaConnector;
    }

    public ConnectorMetadata getMetadata() {
        return new CatalogConnectorMetadata(
                normalConnector.getMetadata(),
                informationSchemaConnector.getMetadata()
        );
    }

    public void shutdown() {
        normalConnector.shutdown();
    }
<<<<<<< HEAD
=======

    @Override
    public boolean supportMemoryTrack() {
        return normalConnector.supportMemoryTrack();
    }

    @Override
    public Map<String, Long> estimateCount() {
        return normalConnector.estimateCount();
    }

    @Override
    public List<Pair<List<Object>, Long>> getSamples() {
        return normalConnector.getSamples();
    }

    public String normalConnectorClassName() {
        if (normalConnector instanceof LazyConnector) {
            return ((LazyConnector) normalConnector).getRealConnectorClassName();
        } else {
            return normalConnector.getClass().getSimpleName();
        }
    }
>>>>>>> f0cb5e97c8 ([Enhancement] Optimize memory tracker (#49841))
}
