// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.http;

import com.google.gson.stream.JsonWriter;
import com.starrocks.analysis.Expr;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Type;
import com.starrocks.proto.PQueryStatistics;
import com.starrocks.proto.QueryStatisticsItemPB;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.thrift.TResultBatch;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * used for serialize memory data to byte stream(or byte array) of Json
 */
public class JsonSerializer {
    private static final int NULL_LENGTH = ~0;
    private static final String META_OBJ_NAME = "meta";
    private static final String META_FIELD_NAME = "name";
    private static final String META_FIELD_TYPE = "type";

    private static final String DATA_OBJ_NAME = "data";

    private static final String STATISTICS_OBJ_NAME = "statistics";
    private static final String STATISTICS_SCAN_ROWS = "scanRows";
    private static final String STATISTICS_SCAN_BYTES = "scanBytes";
    private static final String STATISTICS_RETURN_ROWS = "returnRows";

    private ByteArrayOutputStream out;
    private JsonWriter jsonWriter;

    private JsonSerializer(ByteArrayOutputStream byteArrayOutputStream) {
        this.out = byteArrayOutputStream;
        this.jsonWriter = new JsonWriter(new OutputStreamWriter(out));
    }

    public static JsonSerializer newInstance() throws IOException {
        final JsonSerializer jsonSerializer = new JsonSerializer(new ByteArrayOutputStream());
        jsonSerializer.jsonWriter.beginObject();
        return jsonSerializer;
    }

    public static ByteBuf getChunkedBytes(ShowResultSet showResultSet) throws IOException {
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

    // can only call once in a query, and writeResultBatch should be called closely followed
    public void writeMetaData(List<String> colNames, List<Expr> exprs) throws IOException {
        jsonWriter.name(META_OBJ_NAME).beginArray();
        for (int i = 0; i < colNames.size(); i++) {
            jsonWriter.beginObject();
            jsonWriter.name(META_FIELD_NAME).value(colNames.get(i));
            jsonWriter.name(META_FIELD_TYPE).value(exprs.get(i).getType().toSql());
            jsonWriter.endObject();
        }
        jsonWriter.endArray();

        // prepared for result data
        jsonWriter.name(DATA_OBJ_NAME).beginArray();
    }

    // writeMetaData calls must be made before writeResultBatch first call
    public void writeResultBatch(TResultBatch resultBatch, List<String> colNames, List<Expr> exprs) throws IOException {
        for (ByteBuffer row : resultBatch.getRows()) {
            jsonWriter.beginObject();
            writeRowData(row, colNames, exprs);
            jsonWriter.endObject();
        }
    }

    // can only call once in a query, and should be called closely follow with last writeResultBatch call
    public void writeStatistic(PQueryStatistics queryStatistics) throws IOException {
        jsonWriter.endArray();

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
    }

    public ByteBuf getChunkedBytes() throws IOException {
        jsonWriter.flush();
        return Unpooled.copiedBuffer(out.toByteArray());
    }

    public ByteBuf end() throws IOException {
        jsonWriter.endObject();
        jsonWriter.flush();
        return Unpooled.copiedBuffer(out.toByteArray());
    }

    public void reset() {
        out.reset();
    }

    private void writeRowData(ByteBuffer row, List<String> colNames, List<Expr> exprs) throws IOException {
        for (int i = 0; i < colNames.size(); i++) {
            jsonWriter.name(colNames.get(i));

            int len = getLength(row);
            if (len == NULL_LENGTH) {
                jsonWriter.nullValue();
            } else if (len == 0) {
                jsonWriter.beginArray();
                jsonWriter.endArray();
            } else {
                byte[] tmpCol = new byte[len];
                row.get(tmpCol, 0, len);
                String colValue = new String(tmpCol, StandardCharsets.UTF_8);
                Type colType = exprs.get(i).getOriginType();
                if (colType.isNumericType()) {
                    if (colType.getDecimalDigits() == 0) {
                        jsonWriter.value(Long.valueOf(colValue));
                    } else if (colType.getDecimalDigits() < 15) {
                        jsonWriter.value(Double.valueOf(colValue));
                    } else {
                        // Decimal send as String to avoid loss of accuracy
                        jsonWriter.value(colValue);
                    }
                } else if (colType.isArrayType()) {
                    jsonWriter.jsonValue(colValue);
                } else {
                    jsonWriter.value(colValue);
                }
            }
        }
    }

    /**
     * // the first byte:
     * // <= 250: length
     * // = 251: NULL
     * // = 252: the next two byte is length
     * // = 253: the next three byte is length
     * // = 254: the next eighth byte is length
     * <p>
     * ref mysql_row_buffer.cpp in BE
     */
    private int getLength(ByteBuffer row) {
        int len = 0;
        int sw = row.get() & 0xff;
        switch (sw) {
            case 251:
                len = NULL_LENGTH;
                break;

            case 252:
                len = (row.get() & 0xff)
                        | ((row.get() & 0xff) << 8);
                break;

            case 253:
                len = (row.get() & 0xff)
                        | ((row.get() & 0xff) << 8)
                        | ((row.get() & 0xff) << 16);
                break;

            case 254:
                len = (int) ((row.get() & 0xff)
                        | ((long) (row.get() & 0xff) << 8)
                        | ((long) (row.get() & 0xff) << 16)
                        | ((long) (row.get() & 0xff) << 24)
                        | ((long) (row.get() & 0xff) << 32)
                        | ((long) (row.get() & 0xff) << 40)
                        | ((long) (row.get() & 0xff) << 48)
                        | ((long) (row.get() & 0xff) << 56));
                break;

            default:
                len = sw;
        }
        return len;
    }
}
