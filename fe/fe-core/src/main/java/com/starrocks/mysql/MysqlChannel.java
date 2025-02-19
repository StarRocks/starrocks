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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/mysql/MysqlChannel.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.mysql;

import com.starrocks.common.util.NetUtils;
import com.starrocks.mysql.nio.ReadListener;
import com.starrocks.mysql.ssl.SSLChannel;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.ConnectProcessor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.xnio.StreamConnection;
import org.xnio.channels.Channels;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

/**
 * This class used to read/write MySQL logical packet.
 * MySQL protocol will split one logical packet more than 16MB to many packets.
 * http://dev.mysql.com/doc/internals/en/sending-more-than-16mbyte.html
 */
public class MysqlChannel {
    // max length which one MySQL physical can hold, if one logical packet is bigger than this,
    // one packet will split to many packets
    protected static final int MAX_PHYSICAL_PACKET_LENGTH = 0xffffff;
    // MySQL packet header length
    protected static final int PACKET_HEADER_LEN = 4;
    protected static final int DEFAULT_BUFFER_SIZE = 16 * 1024;
    // logger for this class
    protected static final Logger LOG = LogManager.getLogger(MysqlChannel.class);
    // next sequence id to receive or send
    protected int sequenceId;
    private StreamConnection conn;
    // used to receive/send header, avoiding new this many time.
    protected ByteBuffer headerByteBuffer = ByteBuffer.allocate(PACKET_HEADER_LEN);
    // default packet byte buffer for most packet
    protected ByteBuffer defaultBuffer = ByteBuffer.allocate(DEFAULT_BUFFER_SIZE);
    protected ByteBuffer sendBuffer;

    private SSLChannel sslChannel;

    // for log and show
    protected String remoteHostPortString;
    protected String remoteIp;
    protected boolean isSend;
    protected boolean closed;

    public MysqlChannel(StreamConnection connection) {
        this.conn = connection;
        this.closed = false;
        this.sequenceId = 0;
        this.isSend = false;
        this.remoteHostPortString = "";
        this.remoteIp = "";

        if (connection != null) {
            if (connection.getPeerAddress() instanceof InetSocketAddress) {
                InetSocketAddress address = (InetSocketAddress) connection.getPeerAddress();
                remoteHostPortString = NetUtils.getHostPortInAccessibleFormat(address.getHostString(), address.getPort());
                remoteIp = address.getAddress().getHostAddress();
            } else {
                // Reach here, what's it?
                remoteHostPortString = connection.getPeerAddress().toString();
                remoteIp = connection.getPeerAddress().toString();
            }
        }
    }

    public void setSequenceId(int sequenceId) {
        this.sequenceId = sequenceId;
    }

    public String getRemoteIp() {
        return remoteIp;
    }

    private int packetId() {
        byte[] header = headerByteBuffer.array();
        return header[3] & 0xFF;
    }

    private int packetLen() {
        byte[] header = headerByteBuffer.array();
        return (header[0] & 0xFF) | ((header[1] & 0XFF) << 8) | ((header[2] & 0XFF) << 16);
    }

    private void accSequenceId() {
        sequenceId++;
        if (sequenceId > 255) {
            sequenceId = 0;
        }
    }

    // Close channel
    public synchronized void close() {
        if (closed) {
            return;
        }
        try {
            conn.close();
        } catch (IOException e) {
            LOG.warn("Close channel exception, ignore.");
        } finally {
            closed = true;
        }
    }

    public void setSSLChannel(SSLChannel sslChannel) {
        this.sslChannel = sslChannel;
    }

    protected int readAll(ByteBuffer dstBuf) throws IOException {
        if (sslChannel != null) {
            return sslChannel.readAll(dstBuf);
        } else {
            return readAllPlain(dstBuf);
        }
    }

    protected int readAllPlain(ByteBuffer dstBuf) throws IOException {
        int readLen = 0;
        while (dstBuf.remaining() != 0) {
            int ret = realNetRead(dstBuf);
            // return -1 when remote peer close the channel
            if (ret == -1) {
                return readLen;
            }
            readLen += ret;
        }
        return readLen;
    }

    public int realNetRead(ByteBuffer dstBuf) throws IOException {
        return Channels.readBlocking(conn.getSourceChannel(), dstBuf);
    }

    // read one logical mysql protocol packet
    // null for channel is closed.
    // NOTE: all of the following code is assumed that the channel is in block mode.
    public ByteBuffer fetchOnePacket() throws IOException {
        int readLen;
        ByteBuffer result = defaultBuffer;
        result.clear();

        while (true) {
            headerByteBuffer.clear();
            readLen = readAll(headerByteBuffer);
            if (readLen != PACKET_HEADER_LEN) {
                // remote has close this channel
                LOG.info("Receive packet header failed, " +
                        "remote {} may close the channel.", remoteHostPortString);
                return null;
            }
            if (packetId() != sequenceId) {
                LOG.warn("receive packet sequence id[" + packetId() + "] want to get[" + sequenceId + "]");
                throw new IOException("Bad packet sequence.");
            }
            int packetLen = packetLen();
            if ((result.capacity() - result.position()) < packetLen) {
                // byte buffer is not enough, new one packet
                ByteBuffer tmp;
                if (packetLen < MAX_PHYSICAL_PACKET_LENGTH) {
                    // last packet, enough to this packet is OK.
                    tmp = ByteBuffer.allocate(packetLen + result.position());
                } else {
                    // already have packet, to allocate two packet.
                    tmp = ByteBuffer.allocate(2 * packetLen + result.position());
                }
                tmp.put(result.array(), 0, result.position());
                result = tmp;
            }

            // read one physical packet
            // before read, set limit to make read only one packet
            result.limit(result.position() + packetLen);
            readLen = readAll(result);
            if (readLen != packetLen) {
                LOG.warn("Length of received packet content(" + readLen
                        + ") is not equal with length in head.(" + packetLen + ")");
                return null;
            }
            accSequenceId();
            if (packetLen != MAX_PHYSICAL_PACKET_LENGTH) {
                result.flip();
                break;
            }
        }
        return result;
    }

    private void send(ByteBuffer buffer) throws IOException {
        if (sslChannel != null) {
            sslChannel.write(buffer);
        } else {
            realNetSend(buffer);
        }
        isSend = true;
    }

    public void realNetSend(ByteBuffer buffer) throws IOException {
        long bufLen = buffer.remaining();
        long writeLen = Channels.writeBlocking(conn.getSinkChannel(), buffer);
        if (bufLen != writeLen) {
            throw new IOException("Write mysql packet failed.[write=" + writeLen
                    + ", needToWrite=" + bufLen + "]");
        }
        Channels.flushBlocking(conn.getSinkChannel());
        isSend = true;
    }

    public void flush() throws IOException {
        if (null == sendBuffer || sendBuffer.position() == 0) {
            // Nothing to send
            return;
        }

        sendBuffer.flip();
        try {
            send(sendBuffer);
        } finally {
            sendBuffer.clear();
        }

        isSend = true;
    }

    public void initBuffer(int bufferSize) {
        if (this.sendBuffer == null) {
            // The buffer size shouldn't too large or shouldn't too small
            bufferSize = Math.min(bufferSize, 2 * 1024 * 1024);
            bufferSize = Math.max(bufferSize, 256 * 1024);
            this.sendBuffer = ByteBuffer.allocate(bufferSize);
        }
    }

    public boolean isSendBufferNull() {
        return this.sendBuffer == null;
    }

    private void writeHeader(int length) throws IOException {
        if ((sendBuffer.capacity() - sendBuffer.position()) < 4) {
            flush();
        }

        long newLen = length;
        for (int i = 0; i < 3; ++i) {
            sendBuffer.put((byte) newLen);
            newLen >>= 8;
        }
        sendBuffer.put((byte) sequenceId);
    }

    private void writeBuffer(ByteBuffer buffer) throws IOException {
        // If too long for buffer, send buffered data.
        if (sendBuffer.remaining() < buffer.remaining()) {
            // Flush data in buffer.
            flush();
        }
        // Send this buffer if large enough
        if (buffer.remaining() > sendBuffer.remaining()) {
            send(buffer);
            return;
        }
        // Put it to
        sendBuffer.put(buffer);
    }

    public void sendOnePacket(ByteBuffer packet) throws IOException {
        initBuffer(DEFAULT_BUFFER_SIZE);
        int bufLen;
        int oldLimit = packet.limit();
        while (oldLimit - packet.position() >= MAX_PHYSICAL_PACKET_LENGTH) {
            bufLen = MAX_PHYSICAL_PACKET_LENGTH;
            packet.limit(packet.position() + bufLen);
            writeHeader(bufLen);
            writeBuffer(packet);
            accSequenceId();
        }
        writeHeader(oldLimit - packet.position());
        packet.limit(oldLimit);
        writeBuffer(packet);
        accSequenceId();
    }

    public void sendAndFlush(ByteBuffer packet) throws IOException {
        sendOnePacket(packet);
        flush();
    }

    // Call this function before send query before
    public void reset() {
        isSend = false;
        if (null != sendBuffer) {
            sendBuffer.clear();
        }
    }

    public boolean isSend() {
        return isSend;
    }

    public String getRemoteHostPortString() {
        return remoteHostPortString;
    }

    public void startAcceptQuery(ConnectContext connectContext, ConnectProcessor connectProcessor) {
        conn.getSourceChannel().setReadListener(new ReadListener(connectContext, connectProcessor));
        conn.getSourceChannel().resumeReads();
    }

    public void suspendAcceptQuery() {
        conn.getSourceChannel().suspendReads();
    }

    public void resumeAcceptQuery() {
        conn.getSourceChannel().resumeReads();
    }

    public void stopAcceptQuery() throws IOException {
        conn.getSourceChannel().shutdownReads();
    }
}
