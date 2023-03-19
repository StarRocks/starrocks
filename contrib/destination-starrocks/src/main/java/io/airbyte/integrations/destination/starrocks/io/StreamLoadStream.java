package io.airbyte.integrations.destination.starrocks.io;

import com.fasterxml.jackson.core.io.JsonStringEncoder;
import io.airbyte.commons.json.Jsons;
import io.airbyte.protocol.models.v0.AirbyteRecordMessage;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.UUID;

import static io.airbyte.integrations.destination.starrocks.StarRocksConstants.CsvFormat;

public class StreamLoadStream extends InputStream {

    private static final int DEFAULT_BUFFER_SIZE = 2048;

    private final Iterator<AirbyteRecordMessage> recordIter;

    private ByteBuffer buffer;
    private byte[] cache;
    private int pos;
    private boolean endStream = false;
    private JsonStringEncoder jsonEncoder;

    public StreamLoadStream(Iterator<AirbyteRecordMessage> recordIter) {
        this.recordIter = recordIter;
        jsonEncoder = JsonStringEncoder.getInstance();

        buffer = ByteBuffer.allocate(DEFAULT_BUFFER_SIZE);
        buffer.position(buffer.capacity());
    }


    @Override
    public int read() throws IOException {
        byte[] bytes = new byte[1];
        int ws = read(bytes);
        if (ws == -1) {
            return -1;
        }
        return bytes[0];
    }

    @Override
    public int read(byte[] b) throws IOException {
        return read(b, 0, b.length);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (len == 0) {
            return 0;
        }

        if (!buffer.hasRemaining()) {
            if (cache == null && endStream) {
                return -1;
            }
            fillBuffer();
        }

        int size = len - off;
        int ws = Math.min(size, buffer.remaining());
        buffer.get(b, off, ws);

        return ws;
    }

    @Override
    public void close() throws IOException {
        buffer = null;
        cache = null;
        pos = 0;
        endStream = true;
    }

    private void fillBuffer() {
        buffer.clear();
        if (cache != null) {
            writeBuffer(cache, pos);
        }

        if (endStream || !buffer.hasRemaining()) {
            buffer.flip();
            return;
        }

        byte[] bytes;
        while ((bytes = nextCsvRow()) != null) {
            writeBuffer(bytes, 0);
            bytes = null;
            if (!buffer.hasRemaining()) {
                break;
            }
        }
        if (buffer.position() == 0) {
            buffer.position(buffer.limit());
        } else {
            buffer.flip();
        }
    }

    private void writeBuffer(byte[] bytes, int pos) {
        int size = bytes.length - pos;

        int remain = buffer.remaining();

        int ws = Math.min(size, remain);
        buffer.put(bytes, pos, ws);
        if (size > remain) {
            this.cache = bytes;
            this.pos = pos + ws;
        } else {
            this.cache = null;
            this.pos = 0;
        }
    }


    private byte[] nextCsvRow(){
        if (recordIter.hasNext()){
            AirbyteRecordMessage record = recordIter.next();
            return String.format(CsvFormat.LINE_PATTERN
                    , UUID.randomUUID()
                    , record.getEmittedAt()
                    , new String(jsonEncoder.quoteAsString(Jsons.serialize(record.getData())))).getBytes(StandardCharsets.UTF_8);
        }else {
            endStream = true;
            return CsvFormat.LINE_DELIMITER_BYTE;
        }
    }
}
