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

package com.starrocks.mysql.ssl;

import com.starrocks.mysql.MysqlChannel;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSession;

public class SSLChannelImp implements SSLChannel {
    protected static final Logger LOG = LogManager.getLogger(SSLChannelImp.class);
    private final SSLEngine sslEngine;
    private final MysqlChannel channel;
    private ByteBuffer myNetData;
    private ByteBuffer peerNetData;
    private ByteBuffer peerAppData;
    private boolean haveCachedPeerAppData = false;
    private boolean haveCachedPeerNetData = false;


    /**
     * Will be used to execute tasks that may emerge during handshake in parallel with the server's main thread.
     */
    protected ExecutorService executor;

    public SSLChannelImp(SSLEngine sslEngine, MysqlChannel channel) {
        this.sslEngine = sslEngine;
        this.channel = channel;
    }

    @Override
    public boolean init() throws IOException {
        SSLSession session = sslEngine.getSession();
        myNetData = ByteBuffer.allocate(session.getPacketBufferSize());
        peerAppData = ByteBuffer.allocate(session.getApplicationBufferSize());
        peerNetData = ByteBuffer.allocate(session.getPacketBufferSize());

        executor = Executors.newSingleThreadExecutor();

        sslEngine.setUseClientMode(false);
        sslEngine.setNeedClientAuth(false);
        sslEngine.setWantClientAuth(false);
        sslEngine.beginHandshake();

        // do handshake
        return doHandshake();
    }

    @Override
    public int readAll(ByteBuffer dstBuf) throws IOException {
        int readLen = 0;
        while (dstBuf.remaining() > 0) {
            if (haveCachedPeerAppData) {
                readLen += readFromAppData(dstBuf);
            }

            if (dstBuf.remaining() <= 0) {
                return readLen;
            }

            // the net data may be cached from handshake stage
            int bytesRead;
            if (haveCachedPeerNetData) {
                bytesRead = peerNetData.remaining();
                haveCachedPeerNetData = false;
            } else {
                peerNetData.clear();
                bytesRead = channel.realNetRead(peerNetData);
                peerNetData.flip();
            }
            if (bytesRead > 0) {
                while (peerNetData.hasRemaining()) {
                    SSLEngineResult result = sslEngine.unwrap(peerNetData, peerAppData);
                    switch (result.getStatus()) {
                        case OK:
                            if (peerNetData.hasRemaining()) {
                                peerNetData.compact();
                                peerNetData.flip();
                                haveCachedPeerNetData = true;
                            } else {
                                haveCachedPeerNetData = false;
                            }
                            peerAppData.flip();
                            readLen += readFromAppData(dstBuf);
                            if (dstBuf.remaining() <= 0) {
                                return readLen;
                            }
                            break;
                        case BUFFER_OVERFLOW:
                            peerAppData = handleBufferOverflow(sslEngine, peerAppData);
                            break;
                        case BUFFER_UNDERFLOW:
                            peerNetData = handleBufferUnderflow(sslEngine, peerNetData);
                            bytesRead = channel.realNetRead(peerNetData);
                            if (bytesRead <= 0) {
                                LOG.warn("Received end of stream.");
                                return readLen;
                            }
                            peerNetData.flip();
                            break;
                        case CLOSED:
                            LOG.debug("Client wants to close connection...");
                            return readLen;
                        default:
                            throw new IllegalStateException("Invalid SSL status: " + result.getStatus());
                    }
                }
            } else {
                LOG.warn("Received end of stream.");
                return readLen;
            }
        }

        return readLen;
    }

    private int readFromAppData(ByteBuffer dstBuf) {
        int readLen = Math.min(peerAppData.remaining(), dstBuf.remaining());
        if (peerAppData.remaining() > dstBuf.remaining()) {
            int remain = dstBuf.remaining();
            for (int i = 0; i < remain; i++) {
                dstBuf.put(peerAppData.get());
            }
            haveCachedPeerAppData = true;
        } else {
            haveCachedPeerAppData = false;
            dstBuf.put(peerAppData);
            peerAppData.clear();
        }
        return readLen;
    }

    @Override
    public void write(ByteBuffer data) throws IOException {
        while (data.hasRemaining()) {
            // The loop has a meaning for (outgoing) messages larger than 16KB.
            // Every wrap call will remove 16KB from the original message and send it to the remote peer.
            myNetData.clear();
            SSLEngineResult result = sslEngine.wrap(data, myNetData);
            switch (result.getStatus()) {
                case OK:
                    myNetData.flip();
                    channel.realNetSend(myNetData);
                    break;
                case BUFFER_OVERFLOW:
                    myNetData = enlargePacketBuffer(sslEngine, myNetData);
                    break;
                case BUFFER_UNDERFLOW:
                    throw new SSLException("Buffer underflow occured after a wrap. " +
                            "I don't think we should ever get here.");
                case CLOSED:
                    throw new IOException("client closed the connection");
                default:
                    throw new IllegalStateException("Invalid SSL status: " + result.getStatus());
            }
        }
    }

    /**
     * Implements the handshake protocol between two peers, required for the establishment of the SSL/TLS connection.
     * During the handshake, encryption configuration information
     * - such as the list of available cipher suites - will be exchanged
     * and if the handshake is successful will lead to an established SSL/TLS session.
     *
     * <p/>
     * A typical handshake will usually contain the following steps:
     *
     * <ul>
     *   <li>1. wrap:     ClientHello</li>
     *   <li>2. unwrap:   ServerHello/Cert/ServerHelloDone</li>
     *   <li>3. wrap:     ClientKeyExchange</li>
     *   <li>4. wrap:     ChangeCipherSpec</li>
     *   <li>5. wrap:     Finished</li>
     *   <li>6. unwrap:   ChangeCipherSpec</li>
     *   <li>7. unwrap:   Finished</li>
     * </ul>
     * <p/>
     * Handshake is also used during the end of the session,
     * in order to properly close the connection between the two peers.
     * A proper connection close will typically include the one peer sending a CLOSE message to another,
     * and then wait for the other's CLOSE message to close the transport link.
     * The other peer from his perspective would read a CLOSE message
     * from his peer and then enter the handshake procedure to send his own CLOSE message as well.
     *
     * @return True if the connection handshake was successful or false if an error occurred.
     * @throws IOException - if an error occurs during read/write to the socket channel.
     */
    private boolean doHandshake() throws IOException {
        SSLEngineResult result;
        SSLEngineResult.HandshakeStatus handshakeStatus;

        // NioSslPeer's fields myAppData and peerAppData are supposed
        // to be large enough to hold all message data the peer
        // will send and expects to receive from the other peer respectively.
        // Since the messages to be exchanged will usually be less
        // than 16KB long the capacity of these fields should also be smaller.
        // Here we initialize these two local buffers
        // to be used for the handshake, while keeping client's buffers at the same size.
        int appBufferSize = sslEngine.getSession().getApplicationBufferSize();
        ByteBuffer myAppData = ByteBuffer.allocate(appBufferSize);
        ByteBuffer peerAppData = ByteBuffer.allocate(appBufferSize);
        myNetData.clear();
        peerNetData.clear();

        handshakeStatus = sslEngine.getHandshakeStatus();
        while (handshakeStatus != SSLEngineResult.HandshakeStatus.FINISHED
                && handshakeStatus != SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING) {
            switch (handshakeStatus) {
                case NEED_UNWRAP:
                    if (!haveCachedPeerNetData) {
                        peerNetData.clear();
                        if (channel.realNetRead(peerNetData) <= 0) {
                            if (sslEngine.isInboundDone() && sslEngine.isOutboundDone()) {
                                LOG.warn("in bound and out bound are done");
                                return false;
                            }
                            try {
                                sslEngine.closeInbound();
                            } catch (SSLException e) {
                                LOG.error("This engine was forced to close inbound, without having received the proper " +
                                        "SSL/TLS close notification message from the peer, due to end of stream.");
                            }
                            sslEngine.closeOutbound();
                            // After closeOutbound the engine will be set to WRAP state,
                            // in order to try to send a close message to the client.
                            handshakeStatus = sslEngine.getHandshakeStatus();
                            break;
                        }
                        peerNetData.flip();
                    }
                    try {
                        LOG.debug("#handshake, peerNetData size: {}, is cached data: {}",
                                peerNetData.remaining(), haveCachedPeerNetData);
                        result = sslEngine.unwrap(peerNetData, peerAppData);
                        haveCachedPeerNetData = peerNetData.remaining() > 0;
                        LOG.debug("#handshake unwrap, bytesConsumed: {}, bytesProduced: {}, remaining: {}",
                                result.bytesConsumed(), result.bytesProduced(), peerNetData.remaining());
                        handshakeStatus = result.getHandshakeStatus();
                    } catch (SSLException sslException) {
                        LOG.error("A problem was encountered while processing the data that " +
                                        "caused the SSLEngine to abort. Will try to properly close connection...",
                                sslException);
                        sslEngine.closeOutbound();
                        handshakeStatus = sslEngine.getHandshakeStatus();
                        break;
                    }
                    switch (result.getStatus()) {
                        case OK:
                            break;
                        case BUFFER_OVERFLOW:
                            // Will occur when peerAppData's capacity is
                            // smaller than the data derived from peerNetData's unwrap.
                            peerAppData = enlargeApplicationBuffer(sslEngine, peerAppData);
                            break;
                        case BUFFER_UNDERFLOW:
                            // Will occur either when no data was read from the peer
                            // or when the peerNetData buffer was too small to hold all peer's data.
                            LOG.debug("handle buffer under flow");
                            peerNetData = handleBufferUnderflow(sslEngine, peerNetData);
                            if (channel.realNetRead(peerNetData) <= 0) {
                                if (sslEngine.isInboundDone() && sslEngine.isOutboundDone()) {
                                    LOG.warn("in bound and out bound are done");
                                    return false;
                                }
                                try {
                                    sslEngine.closeInbound();
                                } catch (SSLException e) {
                                    LOG.error("This engine was forced to close inbound, without having received the proper " +
                                            "SSL/TLS close notification message from the peer, due to end of stream.");
                                }
                                sslEngine.closeOutbound();
                                // After closeOutbound the engine will be set to WRAP state,
                                // in order to try to send a close message to the client.
                                handshakeStatus = sslEngine.getHandshakeStatus();
                                break;
                            }
                            peerNetData.flip();
                            haveCachedPeerNetData = true;
                            break;
                        case CLOSED:
                            if (sslEngine.isOutboundDone()) {
                                LOG.warn("in bound is done");
                                return false;
                            } else {
                                sslEngine.closeOutbound();
                                handshakeStatus = sslEngine.getHandshakeStatus();
                                break;
                            }
                        default:
                            throw new IllegalStateException("Invalid SSL status: " + result.getStatus());
                    }
                    break;
                case NEED_WRAP:
                    myNetData.clear();
                    try {
                        result = sslEngine.wrap(myAppData, myNetData);
                        handshakeStatus = result.getHandshakeStatus();
                    } catch (SSLException sslException) {
                        LOG.error("A problem was encountered while processing the data " +
                                "that caused the SSLEngine to abort. Will try to properly close connection...");
                        sslEngine.closeOutbound();
                        handshakeStatus = sslEngine.getHandshakeStatus();
                        break;
                    }
                    switch (result.getStatus()) {
                        case OK :
                            myNetData.flip();
                            while (myNetData.hasRemaining()) {
                                channel.realNetSend(myNetData);
                            }
                            break;
                        case BUFFER_OVERFLOW:
                            // Will occur if there is not enough space in myNetData buffer
                            // to write all the data that would be generated by the method wrap.
                            // Since myNetData is set to session's packet size we should not get
                            // to this point because SSLEngine is supposed
                            // to produce messages smaller or equal to that,
                            // but a general handling would be the following:
                            myNetData = enlargePacketBuffer(sslEngine, myNetData);
                            break;
                        case BUFFER_UNDERFLOW:
                            throw new SSLException("Buffer underflow occured after a wrap. " +
                                    "I don't think we should ever get here.");
                        case CLOSED:
                            try {
                                myNetData.flip();
                                while (myNetData.hasRemaining()) {
                                    channel.realNetSend(myNetData);
                                }
                                // At this point the handshake status will probably be NEED_UNWRAP
                                // so we make sure that peerNetData is clear to read.
                                peerNetData.clear();
                            } catch (Exception e) {
                                LOG.error("Failed to send server's CLOSE message due to socket channel's failure.");
                                handshakeStatus = sslEngine.getHandshakeStatus();
                            }
                            break;
                        default:
                            throw new IllegalStateException("Invalid SSL status: " + result.getStatus());
                    }
                    break;
                case NEED_TASK:
                    Runnable task;
                    while ((task = sslEngine.getDelegatedTask()) != null) {
                        task.run();
                    }
                    handshakeStatus = sslEngine.getHandshakeStatus();
                    break;
                default:
                    throw new IllegalStateException("Invalid SSL status: " + handshakeStatus);
            }
        }

        return true;
    }

    protected ByteBuffer enlargePacketBuffer(SSLEngine engine, ByteBuffer buffer) {
        return enlargeBuffer(buffer, engine.getSession().getPacketBufferSize());
    }

    protected ByteBuffer enlargeApplicationBuffer(SSLEngine engine, ByteBuffer buffer) {
        return enlargeBuffer(buffer, engine.getSession().getApplicationBufferSize());
    }

    /**
     * Compares <code>sessionProposedCapacity<code> with buffer's capacity. If buffer's capacity is smaller,
     * returns a buffer with the proposed capacity. If it's equal or larger, returns a buffer
     * with capacity twice the size of the initial one.
     *
     * @param buffer - the buffer to be enlarged.
     * @param sessionProposedCapacity - the minimum size of the new buffer, proposed by {@link SSLSession}.
     * @return A new buffer with a larger capacity.
     */
    protected ByteBuffer enlargeBuffer(ByteBuffer buffer, int sessionProposedCapacity) {
        if (sessionProposedCapacity > buffer.capacity()) {
            buffer = ByteBuffer.allocate(sessionProposedCapacity);
        } else {
            buffer = ByteBuffer.allocate(buffer.capacity() * 2);
        }
        return buffer;
    }

    /**
     * Handles {@link SSLEngineResult.Status#BUFFER_UNDERFLOW}. Will check if the buffer is already filled,
     * and if there is no space problem will return the same buffer, so the client tries to read again.
     * If the buffer is already filled will try to enlarge the buffer either to
     * session's proposed size or to a larger capacity.
     * A buffer underflow can happen only after an unwrap, so the buffer will always be a peerNetData buffer.
     *
     * @param buffer - will always be peerNetData buffer.
     * @param engine - the engine used for encryption/decryption of the data exchanged between the two peers.
     * @return The same buffer if there is no space problem or a new buffer with the same data but more space.
     * @throws Exception
     */
    protected ByteBuffer handleBufferUnderflow(SSLEngine engine, ByteBuffer buffer) {
        if (buffer.position() > 0) {
            buffer.compact();
            buffer.flip();
        }
        if (buffer.capacity() - buffer.limit() > 2 * engine.getSession().getPacketBufferSize()) {
            buffer.position(buffer.limit());
            buffer.limit(buffer.capacity());
            return buffer;
        }
        ByteBuffer replaceBuffer = enlargePacketBuffer(engine, buffer);
        replaceBuffer.put(buffer);
        return replaceBuffer;
    }

    protected ByteBuffer handleBufferOverflow(SSLEngine engine, ByteBuffer buffer) {
        ByteBuffer replaceBuffer = enlargeApplicationBuffer(engine, buffer);
        buffer.flip();
        replaceBuffer.put(buffer);
        return replaceBuffer;
    }
}
