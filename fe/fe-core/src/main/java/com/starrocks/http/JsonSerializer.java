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

package com.starrocks.http;

import com.google.gson.JsonObject;
import com.google.gson.stream.JsonWriter;
import com.starrocks.analysis.Expr;
import com.starrocks.catalog.Column;
import com.starrocks.proto.PQueryStatistics;
import com.starrocks.proto.QueryStatisticsItemPB;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.qe.ShowResultSetMetaData;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * used for serialize memory data to byte stream(or byte array) of Json
 */
public class JsonSerializer {
    private static final Logger LOG = LogManager.getLogger(JsonSerializer.class);
    private static final String META_OBJ_NAME = "meta";
    private static final String META_FIELD_NAME = "name";
    private static final String META_FIELD_TYPE = "type";

    private static final String DATA_OBJ_NAME = "data";

    private static final String STATISTICS_OBJ_NAME = "statistics";
    private static final String STATISTICS_SCAN_ROWS = "scanRows";
    private static final String STATISTICS_SCAN_BYTES = "scanBytes";
    private static final String STATISTICS_RETURN_ROWS = "returnRows";

    public static ByteBuf getShowResult(ShowResultSet showResultSet) throws IOException {
        ByteArrayOutputStream resultStream = new ByteArrayOutputStream();
        JsonWriter jsonWriter = new JsonWriter(new OutputStreamWriter(resultStream));

        jsonWriter.beginObject();
        final ShowResultSetMetaData metaData = showResultSet.getMetaData();
        // serialize metadata
        jsonWriter.name(META_OBJ_NAME).beginArray();
        for (Column column : metaData.getColumns()) {
            jsonWriter.beginObject();
            jsonWriter.name(META_FIELD_NAME).value(column.getName());
            jsonWriter.name(META_FIELD_TYPE).value(column.getType().toSql());
            jsonWriter.endObject();
        }
        jsonWriter.endArray();

        // serialize result data
        jsonWriter.name(DATA_OBJ_NAME).beginArray();
        for (List<String> resultRow : showResultSet.getResultRows()) {
            jsonWriter.beginObject();
            for (int i = 0; i < metaData.getColumnCount(); i++) {
                jsonWriter.name(metaData.getColumn(i).getName()).value(resultRow.get(i));
            }
            jsonWriter.endObject();
        }
        jsonWriter.endArray();

        // serialize statistics
        jsonWriter.name(STATISTICS_OBJ_NAME).beginObject();
        jsonWriter.name(STATISTICS_SCAN_ROWS).value(0);
        jsonWriter.name(STATISTICS_SCAN_BYTES).value(0);
        jsonWriter.name(STATISTICS_RETURN_ROWS).value(showResultSet.getResultRows().size());
        jsonWriter.endObject();

        jsonWriter.endObject();
        jsonWriter.flush();
        return Unpooled.wrappedBuffer(resultStream.toByteArray());
    }

    public static ByteBuf getConnectId(int connectionId) {
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("connectionId", connectionId);
        String str = jsonObject + "\n";
        return Unpooled.wrappedBuffer(str.getBytes(StandardCharsets.UTF_8));
    }

    public static ByteBuf getMetaData(List<String> colNames, List<Expr> exprs) throws IOException {
        ByteArrayOutputStream resultStream = new ByteArrayOutputStream();
        OutputStreamWriter outputStreamWriter = new OutputStreamWriter(resultStream);
        JsonWriter jsonWriter = new JsonWriter(outputStreamWriter);

        jsonWriter.beginObject();
        jsonWriter.name(META_OBJ_NAME).beginArray();
        for (int i = 0; i < colNames.size(); i++) {
            jsonWriter.beginObject();
            jsonWriter.name(META_FIELD_NAME).value(colNames.get(i));
            jsonWriter.name(META_FIELD_TYPE).value(exprs.get(i).getType().toSql());
            jsonWriter.endObject();
        }
        jsonWriter.endArray();
        jsonWriter.endObject();

        outputStreamWriter.write("\n");
        jsonWriter.flush();

        return Unpooled.wrappedBuffer(resultStream.toByteArray());
    }

    public static ByteBuf getStatistic(PQueryStatistics queryStatistics) throws IOException {
        ByteArrayOutputStream resultStream = new ByteArrayOutputStream();
        OutputStreamWriter outputStreamWriter = new OutputStreamWriter(resultStream);
        JsonWriter jsonWriter = new JsonWriter(outputStreamWriter);

        jsonWriter.beginObject();
        jsonWriter.name(STATISTICS_OBJ_NAME).beginObject();
        long scanRows = 0;
        long scanBytes = 0;
        long returnRows = 0;
        if (null != queryStatistics && null != queryStatistics.statsItems) {
            for (QueryStatisticsItemPB item : queryStatistics.statsItems) {
                scanRows += item.scanRows;
                scanBytes += item.scanBytes;
            }
            returnRows = queryStatistics.returnedRows;
        }

        jsonWriter.name(STATISTICS_SCAN_ROWS).value(scanRows);
        jsonWriter.name(STATISTICS_SCAN_BYTES).value(scanBytes);
        jsonWriter.name(STATISTICS_RETURN_ROWS).value(returnRows);
        jsonWriter.endObject();
        jsonWriter.endObject();

        outputStreamWriter.write("\n");
        jsonWriter.flush();

        return Unpooled.wrappedBuffer(resultStream.toByteArray());
    }
}
