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
package com.starrocks.connector.delta.cache;

import com.starrocks.connector.delta.CachingDeltaLakeMetadata;
import io.delta.standalone.DeltaScan;
import io.delta.standalone.actions.AddFile;
import io.delta.standalone.actions.Metadata;
import io.delta.standalone.data.CloseableIterator;
import io.delta.standalone.expressions.Expression;
import io.delta.standalone.internal.util.DeltaPartitionUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class CachingDeltaScanImpl implements DeltaScan {

    private static final Logger LOG = LogManager.getLogger(CachingDeltaScanImpl.class);
    private DeltaLakeTableName deltaTbl;
    private Expression expression;
    private CachingDeltaLakeMetadata caching;


    public CachingDeltaScanImpl(DeltaLakeTableName deltaTbl,
                                CachingDeltaLakeMetadata caching) {
        this.deltaTbl = deltaTbl;
        this.caching = caching;
    }

    public CachingDeltaScanImpl(DeltaLakeTableName deltaTbl,
                                CachingDeltaLakeMetadata caching,
                                Expression expression) {
        this(deltaTbl, caching);
        this.expression = expression;
    }

    @Override
    public CloseableIterator<AddFile> getFiles() {
        try {
            Iterator<AddFile> iterator;
            if (null == expression) {
                iterator = caching.getAllFiles(deltaTbl).iterator();
            } else {
                Set<Map<String, String>> allPartitions = caching.getAllPartitions(deltaTbl);
                Metadata metadata = caching.getMetadata(deltaTbl);

                Set<String> pushedPartitionValues = DeltaPartitionUtil
                        .getAllPushedPartitionValues(metadata, allPartitions, expression);
                if (pushedPartitionValues.isEmpty()) {
                    iterator = Collections.emptyIterator();
                } else {
                    iterator = caching
                            .getPushedAddFiles(deltaTbl, pushedPartitionValues)
                            .iterator();
                }
            }
            return new CloseableIterator<AddFile>() {
                @Override
                public boolean hasNext() {
                    return iterator.hasNext();
                }

                @Override
                public AddFile next() {
                    return iterator.next();
                }

                @Override
                public void close() {

                }
            };
        } catch (Exception e) {
            LOG.error("If the delta lake table is a partitioned table, " +
                    "the query condition must include the partition field", e);
            throw new IllegalArgumentException("If the delta lake table is a partitioned table, " +
                    "the query condition must include the partition field");
        }
    }

    @Override
    public Optional<Expression> getInputPredicate() {
        return Optional.of(expression);
    }

    @Override
    public Optional<Expression> getPushedPredicate() {
        return Optional.of(expression);
    }

    @Override
    public Optional<Expression> getResidualPredicate() {
        return Optional.of(expression);
    }

    private Set<String> getAllPushedPartitionValues() throws Exception {
        Set<Map<String, String>> allPartitions = caching.getAllPartitions(deltaTbl);
        Metadata metadata = caching.getMetadata(deltaTbl);
        return DeltaPartitionUtil.getAllPushedPartitionValues(metadata, allPartitions, expression);
    }
}
