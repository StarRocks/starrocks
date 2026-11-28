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

package com.starrocks.connector.paimon;

import com.starrocks.catalog.PaimonTable;
import com.starrocks.catalog.TableName;
import com.starrocks.connector.metadata.MetadataCollectJob;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.QueryState;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.SimpleExecutor;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.thrift.TResultBatch;
import com.starrocks.thrift.TResultSinkType;
import com.starrocks.thrift.TRowFormat;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.utils.RoaringNavigableMap64;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.protocol.TCompactProtocol;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.BitSet;
import java.util.function.Consumer;

/** Collects every shard through the shared metadata executor without retaining result batches. */
public final class PaimonGlobalIndexCollectJob extends MetadataCollectJob {
    // Reserve space for mutable bitmap containers, merge state and SDK split-range conversion.
    private static final int MEMORY_BYTES_PER_ROW = 512;
    private final PaimonTable origin;
    private final PaimonGlobalIndexRequest request;
    private final long budget;
    private long receivedBytes;
    private long receivedRows;
    private final BitSet received = new BitSet();
    private final RoaringNavigableMap64 aggregate = new RoaringNavigableMap64();

    public PaimonGlobalIndexCollectJob(PaimonTable origin, PaimonGlobalIndexRequest request, long budget) {
        super(origin.getCatalogName(), origin.getCatalogDBName(), origin.getCatalogTableName(), TResultSinkType.CUSTOMIZED);
        this.origin = origin;
        this.request = request;
        this.budget = budget;
    }

    @Override
    protected String buildCollectMetadataSQL() {
        return "SELECT shard_id, index_result FROM " + quote(getCatalogName()) + "." + quote(getDbName()) + "."
                + quote(getTableName() + PaimonGlobalIndexMetadataTable.SUFFIX)
                + " WHERE predicate = '" + request.toTransportString() + "'";
    }

    private static String quote(String name) {
        return "`" + name.replace("`", "``") + "`";
    }

    @Override
    public Consumer<TResultBatch> getResultConsumer() {
        return this::consume;
    }

    record CollectedIndex(GlobalIndexResult result, PaimonGlobalIndexPlanInfo.Details details) {
    }

    CollectedIndex collect(ConnectContext caller) {
        SimpleExecutor.checkInternalQueryState(caller);
        // Check policies in the user's context before creating the trusted internal task.
        TableName name = new TableName(getCatalogName(), getDbName(), getTableName());
        var masks = Authorizer.getColumnMaskingPolicy(caller, name, origin.getFullSchema());
        if (Authorizer.getRowAccessPolicy(caller, name) != null || masks != null && !masks.isEmpty()) {
            throw new IllegalArgumentException("Paimon index evaluation is unavailable with row or masking policies");
        }
        // Reuse Iceberg's internal identity; the outer query authorizes the base table.
        init(caller);
        var variables = getContext().getSessionVariable();
        variables.setSqlSelectLimit(SessionVariable.DEFAULT_SELECT_LIMIT);
        variables.setEnableShortCircuit(false);
        // Bound each RPC result batch as well as the aggregate; a large combined chunk
        // would already be allocated by the RPC client before the consumer can reject it.
        getContext().getSessionVariable().setChunkSize(1);
        getContext().getSessionVariable().setPipelineDop(1);
        asyncCollectMetadata();
        SimpleExecutor.checkInternalQueryState(getContext());
        if (getContext().getState().getStateType() != QueryState.MysqlStateType.EOF || getMetadataJobCoord() == null) {
            throw new IllegalStateException("Incomplete Paimon Global Index result: "
                    + getContext().getState().getErrorMessage());
        }
        var scans = getMetadataJobCoord().getScanNodes();
        if (scans.size() != 1 || !(scans.get(0) instanceof com.starrocks.planner.PaimonGlobalIndexScanNode)) {
            throw new IllegalStateException("Unexpected Paimon Global Index metadata plan");
        }
        int expected = scans.get(0).getScanRangeLocations(0).size();
        if (expected == 0 || received.cardinality() != expected || received.length() != expected) {
            throw new IllegalStateException("Incomplete Paimon Global Index shard results");
        }
        var details = new PaimonGlobalIndexPlanInfo.Details("btree", request.getIndexedColumns(),
                request.getSnapshotId(), expected, aggregate.getLongCardinality());
        return new CollectedIndex(GlobalIndexResult.create(aggregate), details);
    }

    private void consume(TResultBatch batch) {
        for (ByteBuffer row : batch.getRows()) {
            if (row.remaining() > request.getResultBytesLimit() + 1024 || receivedBytes > budget / 4 - row.remaining()) {
                throw new IllegalStateException("Paimon Global Index result exceeds the FE collection budget");
            }
            receivedBytes += row.remaining();
            try {
                byte[] wire = new byte[row.remaining()];
                row.duplicate().get(wire);
                TRowFormat format = new TRowFormat();
                new TDeserializer(new TCompactProtocol.Factory()).deserialize(format, wire);
                ByteBuffer nulls = format.bufferForNull_bits();
                if (nulls.remaining() != 2 || nulls.get() != 1 || nulls.get() != 1) {
                    throw new IllegalArgumentException("Missing Paimon Global Index shard result");
                }
                ByteBuffer data = format.bufferForPacked_data().order(ByteOrder.LITTLE_ENDIAN);
                long id = data.getLong();
                long length = data.getLong();
                if (id < 0 || id >= PaimonGlobalIndexPlanner.MAX_SHARDS || received.get((int) id) || length != data.remaining()) {
                    throw new IllegalArgumentException("Invalid or duplicate Paimon Global Index shard result");
                }
                byte[] serialized = new byte[data.remaining()];
                data.get(serialized);
                RoaringNavigableMap64 partial = deserialize(serialized, budget / MEMORY_BYTES_PER_ROW - receivedRows);
                long cardinality = partial.getLongCardinality();
                if (cardinality > budget / MEMORY_BYTES_PER_ROW - receivedRows) {
                    throw new IllegalStateException("Paimon Global Index bitmap exceeds the FE collection budget");
                }
                receivedRows += cardinality;
                aggregate.or(partial);
                received.set((int) id);
            } catch (Exception e) {
                String reason = e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage();
                throw new IllegalStateException("Cannot collect Paimon Global Index result: " + reason, e);
            }
        }
    }

    private static RoaringNavigableMap64 deserialize(byte[] serialized, long remainingRows) throws java.io.IOException {
        // Validate the length envelope before calling the SDK, which allocates from its embedded length.
        ByteBuffer input = ByteBuffer.wrap(serialized).order(ByteOrder.BIG_ENDIAN);
        int version = input.getInt();
        if (version != 1 && version != 2 || version == 2 && input.get() != 0) {
            throw new IllegalArgumentException("Unsupported Paimon scalar index result version or kind");
        }
        int length = input.getInt();
        if (length < 0 || length != input.remaining() - Integer.BYTES || input.getInt(input.limit() - 4) != 0) {
            throw new IllegalArgumentException("Invalid Paimon scalar index result length or scores");
        }
        ByteBuffer portable = input.slice().order(ByteOrder.LITTLE_ENDIAN);
        portable.limit(length);
        long highKeys = portable.getLong();
        if (highKeys < 0 || highKeys > portable.remaining() / 12) {
            throw new IllegalArgumentException("Invalid portable Paimon bitmap header");
        }
        long cardinality = 0;
        for (long i = 0; i < highKeys; i++) {
            portable.getInt();
            ImmutableRoaringBitmap view = new ImmutableRoaringBitmap(portable.slice());
            long rows = view.getLongCardinality();
            if (rows > remainingRows - cardinality) {
                throw new IllegalStateException("Paimon Global Index bitmap exceeds the FE collection budget");
            }
            cardinality += rows;
            int size = view.serializedSizeInBytes();
            if (size < 0 || size > portable.remaining()) {
                throw new IllegalArgumentException("Invalid portable Paimon bitmap length");
            }
            portable.position(portable.position() + size);
        }
        if (portable.hasRemaining()) {
            throw new IllegalArgumentException("Trailing bytes in portable Paimon bitmap");
        }
        byte[] bitmap = new byte[length];
        input.get(bitmap);
        RoaringNavigableMap64 result = new RoaringNavigableMap64();
        result.deserialize(bitmap);
        return result;
    }
}
