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

package com.starrocks.data.load.stream;

import com.starrocks.data.load.stream.http.StreamLoadEntityMeta;
import com.starrocks.data.load.stream.properties.StreamLoadTableProperties;
import io.airlift.log.Logger;
import io.trino.plugin.starrocks.StarRocksOperationApplier;

import java.io.Serializable;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.atomic.AtomicLong;

public class StreamTableRegion
        implements Serializable
{
    private static final Logger log = Logger.get(StreamTableRegion.class);
    private static final byte[] END_STREAM = new byte[0];
    private final String uniqueKey;
    private final String database;
    private final String table;
    private final Optional<String> temporaryTableName;
    private final StarRocksOperationApplier applier;
    private final long chunkLimit;
    private final String labelPrefix;
    private final StreamLoadDataFormat dataFormat;
    private final BlockingQueue<byte[]> buffer = new LinkedTransferQueue<>();
    private final AtomicLong cacheBytes = new AtomicLong();
    private final AtomicLong flushBytes = new AtomicLong();
    private final AtomicLong flushRows = new AtomicLong();
    private volatile String label;
    private volatile boolean flushing;

    public StreamTableRegion(String uniqueKey,
                             String database,
                             String table,
                             Optional<String> temporaryTableName,
                             String labelPrefix,
                             StarRocksOperationApplier applier,
                             StreamLoadTableProperties properties)
    {
        this.uniqueKey = uniqueKey;
        this.database = database;
        this.table = table;
        this.temporaryTableName = temporaryTableName;
        this.applier = applier;
        this.dataFormat = properties.getDataFormat();
        this.chunkLimit = properties.getChunkLimit();
        this.labelPrefix = labelPrefix;
    }

    public String getUniqueKey()
    {
        return uniqueKey;
    }

    public String getDatabase()
    {
        return database;
    }

    public String getTable()
    {
        return table;
    }

    public Optional<String> getTemporaryTableName()
    {
        return temporaryTableName;
    }

    public void setLabel(String label)
    {
        this.label = label;
    }

    public String getLabel()
    {
        return label;
    }

    public StreamLoadEntityMeta getEntityMeta()
    {
        return StreamLoadEntityMeta.CHUNK_ENTITY_META;
    }

    public int write(byte[] row)
    {
        try {
            buffer.put(row);
            if (row != END_STREAM) {
                cacheBytes.addAndGet(row.length);
            }
            else {
                log.info("Write EOF");
            }
            return row.length;
        }
        catch (InterruptedException ignored) {
        }
        return 0;
    }

    private final AtomicLong totalFlushBytes = new AtomicLong();
    private volatile boolean endStream;
    private volatile byte[] next;

    public byte[] read()
    {
        if (!flushing) {
            flushing = true;
        }
        try {
            byte[] row;
            if (next == null) {
                row = buffer.take();
            }
            else {
                row = next;
            }
            if (row == END_STREAM) {
                endStream = true;
                log.info("Read EOF");
                return null;
            }
            int delimiterL = dataFormat.delimiter() == null ? 0 : dataFormat.delimiter().length;

            if (totalFlushBytes.get() + row.length + delimiterL > chunkLimit) {
                next = row;
                log.info("Read part EOF");
                return null;
            }
            next = null;
            totalFlushBytes.addAndGet(row.length + delimiterL);
            cacheBytes.addAndGet(-row.length);
            flushBytes.addAndGet(row.length);
            flushRows.incrementAndGet();
            return row;
        }
        catch (InterruptedException e) {
            log.info("read queue interrupted, msg : %s", e.getMessage());
        }
        return null;
    }

    protected void flip()
    {
        flushBytes.set(0L);
        flushRows.set(0L);

        final int initSize = (dataFormat.first() == null ? 0 : dataFormat.first().length)
                + (dataFormat.end() == null ? 0 : dataFormat.end().length)
                - (dataFormat.delimiter() == null ? 0 : dataFormat.delimiter().length);
        totalFlushBytes.set(initSize);
        endStream = false;
    }

    public boolean commit()
    {
        if (isReadable()) {
            flip();
            write(END_STREAM);
            setLabel(genLabel());
            return Boolean.TRUE;
        }
        return Boolean.FALSE;
    }

    private String genLabel()
    {
        if (labelPrefix != null) {
            return labelPrefix + UUID.randomUUID();
        }
        return UUID.randomUUID().toString();
    }

    public void callback(StreamLoadResponse response)
    {
        applier.callback(response);
    }

    public void callback(Throwable e)
    {
        applier.callback(e);
    }

    public void complete(StreamLoadResponse response)
    {
        response.setFlushBytes(flushBytes.get());
        response.setFlushRows(flushRows.get());

        callback(response);

        log.info("Stream load flushed, label : %s", label);
        if (!endStream) {
            log.info("Stream load continue");
            streamLoad();
            return;
        }
        log.info("Stream load completed");
    }

    public boolean isReadable()
    {
        return cacheBytes.get() > 0;
    }

    protected boolean streamLoad()
    {
        try {
            flip();
            applier.send();
            return true;
        }
        catch (Exception e) {
            callback(e);
        }

        return false;
    }
}
