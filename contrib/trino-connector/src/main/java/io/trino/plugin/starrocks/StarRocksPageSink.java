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

package io.trino.plugin.starrocks;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.plugin.jdbc.JdbcOutputTableHandle;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSinkId;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;

import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.starrocks.data.load.stream.StreamLoadConstants.DATETIME_FORMATTER;
import static com.starrocks.data.load.stream.StreamLoadConstants.DATE_FORMATTER;
import static com.starrocks.data.load.stream.StreamLoadUtils.toLocalDateTime;
import static io.trino.plugin.starrocks.StarRocksErrorCode.STAR_ROCKS_WRITE_ERROR;
import static io.trino.spi.type.DateType.DATE;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class StarRocksPageSink
        implements ConnectorPageSink
{
    private final ObjectMapper objectMapper;
    private final JdbcOutputTableHandle handle;
    private final StarRocksOperationApplier applier;
    private final ConnectorPageSinkId pageSinkId;

    public StarRocksPageSink(JdbcOutputTableHandle handle, StarRocksOperationApplier applier, ConnectorPageSinkId pageSinkId)
    {
        this.handle = handle;
        this.applier = applier;
        this.objectMapper = new ObjectMapperProvider().get();
        this.pageSinkId = pageSinkId;
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        try {
            for (int position = 0; position < page.getPositionCount(); position++) {
                ObjectNode objectNode = objectMapper.createObjectNode();
                for (int channel = 0; channel < page.getChannelCount(); channel++) {
                    appendColumn(page, position, channel, objectNode);
                }
                if (handle.getPageSinkIdColumnName().isPresent()) {
                    objectNode.put(handle.getPageSinkIdColumnName().get(), pageSinkId.getId());
                }
                String row = objectMapper.writeValueAsString(objectNode);
                applier.applyOperationAsync(row);
            }
        }
        catch (SQLException | JsonProcessingException e) {
            throw new TrinoException(STAR_ROCKS_WRITE_ERROR, e);
        }
        return NOT_BLOCKED;
    }

    private void appendColumn(Page page, int position, int channel, ObjectNode objectNode)
            throws SQLException
    {
        Block block = page.getBlock(channel);
        if (block.isNull(position)) {
            return;
        }

        List<Type> columnTypes = handle.getColumnTypes();
        List<String> columnNames = handle.getColumnNames();
        Type type = columnTypes.get(channel);
        String columnName = columnNames.get(channel);
        Class<?> javaType = type.getJavaType();
        if (javaType == boolean.class) {
            objectNode.put(columnName, type.getBoolean(block, position));
        }
        else if (javaType == long.class) {
            long value = type.getLong(block, position);
            if (type.equals(DATE)) {
                objectNode.put(columnName, LocalDate.ofEpochDay(value).format(DATE_FORMATTER));
            }
            else if (type instanceof TimestampType) {
                LocalDateTime dateTime = toLocalDateTime(((TimestampType) type), block, position);
                objectNode.put(columnName, dateTime.format(DATETIME_FORMATTER));
            }
            else {
                objectNode.put(columnName, value);
            }
        }
        else if (javaType == double.class) {
            objectNode.put(columnName, type.getDouble(block, position));
        }
        else if (javaType == Slice.class) {
            objectNode.put(columnName, type.getSlice(block, position).toStringUtf8());
        }
        else {
            objectNode.put(columnName, type.getObject(block, position).toString());
        }
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        applier.close();
        return completedFuture(ImmutableList.of(Slices.wrappedLongArray(pageSinkId.getId())));
    }

    @SuppressWarnings("unused")
    @Override
    public void abort()
    {
    }
}
