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
import com.starrocks.connector.exception.StarRocksConnectorException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;

public class LazyConnector implements Connector {
    private static final Logger LOG = LogManager.getLogger(LazyConnector.class);
    private Connector delegate;
    private final ConnectorContext context;

    public LazyConnector(ConnectorContext context) {
        this.context = context;
    }

    @Override
    public ConnectorMetadata getMetadata() {
        initIfNeeded();
        return delegate.getMetadata();
    }

    public void initIfNeeded() {
        synchronized (this) {
            if (delegate == null) {
                try {
                    delegate = ConnectorFactory.createRealConnector(context);
                } catch (Exception e) {
                    LOG.error("Failed to init connector [type: {}, name: {}]",
                            context.getType(), context.getCatalogName(), e);
                    throw new StarRocksConnectorException("Failed to init connector [type: %s, name: %s]. msg: %s",
                            context.getType(), context.getCatalogName(), e.getMessage());
                }
            }
        }
    }

    @Override
    public void shutdown() {
        synchronized (this) {
            if (delegate != null) {
                delegate.shutdown();
            }
        }
    }

    @Override
    public boolean supportMemoryTrack() {
        initIfNeeded();
        return delegate.supportMemoryTrack();
    }

    @Override
    public Map<String, Long> estimateCount() {
        initIfNeeded();
        return delegate.estimateCount();
    }

    @Override
    public List<Pair<List<Object>, Long>> getSamples() {
        initIfNeeded();
        return delegate.getSamples();
    }

    public String getRealConnectorClassName() {
        if (delegate != null) {
            return delegate.getClass().getSimpleName();
        } else {
            return LazyConnector.class.getSimpleName();
        }
    }
}
