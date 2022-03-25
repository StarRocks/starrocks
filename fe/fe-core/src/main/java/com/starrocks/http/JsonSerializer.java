// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.http;

import com.google.gson.stream.JsonWriter;
import com.starrocks.analysis.Expr;
import com.starrocks.catalog.Type;
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

    private ByteArrayOutputStream out;
    private JsonWriter jsonWriter;

    private JsonSerializer(ByteArrayOutputStream byteArrayOutputStream) {
        this.out = byteArrayOutputStream;
        this.jsonWriter = new JsonWriter(new OutputStreamWriter(out));
    }

    public static JsonSerializer newInstance() throws IOException {
        final JsonSerializer jsonSerializer = new JsonSerializer(new ByteArrayOutputStream());
        jsonSerializer.jsonWriter.beginArray();
        return jsonSerializer;
    }

    public static ByteBuf getChunkedBytes(ShowResultSet showResultSet) throws IOException {
        ByteArrayOutputStream resultStream = new ByteArrayOutputStream();
        JsonWriter jsonWriter = new JsonWriter(new OutputStreamWriter(resultStream));

        jsonWriter.beginArray();
        final ShowResultSetMetaData metaData = showResultSet.getMetaData();
        for (List<String> resultRow : showResultSet.getResultRows()) {
            jsonWriter.beginObject();
            for (int i = 0; i < metaData.getColumnCount(); i++) {
                jsonWriter.name(metaData.getColumn(i).getName()).value(resultRow.get(i));

            }
            jsonWriter.endObject();
        }
        jsonWriter.endArray();

        jsonWriter.flush();
        return Unpooled.wrappedBuffer(resultStream.toByteArray());
    }

    public void writeResultBatch(TResultBatch resultBatch, List<String> colNames, List<Expr> exprs) throws IOException {
        for (ByteBuffer row : resultBatch.getRows()) {
            jsonWriter.beginObject();
            writeRowData(row, colNames, exprs);
            jsonWriter.endObject();
        }
    }

    public ByteBuf getChunkedBytes() throws IOException {
        jsonWriter.flush();
        return Unpooled.copiedBuffer(out.toByteArray());
    }

    public ByteBuf endArray() throws IOException {
        jsonWriter.endArray();
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
