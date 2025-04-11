package com.starrocks.plugins.ssl;

import com.starrocks.mysql.ssl.SSLChannel;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;


public class SSLChannelImp implements SSLChannel {
    private final SocketChannel socketChannel;
    private final SSLEngine sslEngine;
    private ByteBuffer appData;
    private ByteBuffer netData;
    private ByteBuffer peerAppData;
    private ByteBuffer peerNetData;

    public SSLChannelImp(SocketChannel socketChannel, SSLContext sslContext) throws SSLException {
        this.socketChannel = socketChannel;
        this.sslEngine = sslContext.createSSLEngine();
        this.sslEngine.setUseClientMode(false);

        // Initialize SSL/TLS session
        sslEngine.beginHandshake();

        // Initialize buffers
        createBuffers();
    }

    private void createBuffers() {
        // Get session sizes
        int appBufferSize = sslEngine.getSession().getApplicationBufferSize();
        int netBufferSize = sslEngine.getSession().getPacketBufferSize();

        // Allocate buffers
        appData = ByteBuffer.allocate(appBufferSize);
        netData = ByteBuffer.allocate(netBufferSize);
        peerAppData = ByteBuffer.allocate(appBufferSize);
        peerNetData = ByteBuffer.allocate(netBufferSize);
    }

    @Override
    public boolean init() throws IOException {
        // Perform SSL/TLS handshake
        return doHandshake();
    }

    @Override
    public int readAll(ByteBuffer dst) throws IOException {
        return readSSL(dst);
    }

    @Override
    public void write(ByteBuffer src) throws IOException {
        writeSSL(src);
    }

    private boolean doHandshake() throws IOException {
        SSLEngineResult.HandshakeStatus handshakeStatus = sslEngine.getHandshakeStatus();

        while (handshakeStatus != SSLEngineResult.HandshakeStatus.FINISHED &&
                handshakeStatus != SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING) {

            switch (handshakeStatus) {
                case NEED_UNWRAP:
                    if (socketChannel.read(peerNetData) < 0) {
                        if (sslEngine.isInboundDone() && sslEngine.isOutboundDone()) {
                            return false;
                        }
                        try {
                            sslEngine.closeInbound();
                        } catch (SSLException e) {
                            // Handle exception
                        }
                        sslEngine.closeOutbound();
                        return false;
                    }
                    peerNetData.flip();
                    SSLEngineResult unwrapResult = sslEngine.unwrap(peerNetData, peerAppData);
                    peerNetData.compact();
                    handshakeStatus = unwrapResult.getHandshakeStatus();
                    break;

                case NEED_WRAP:
                    netData.clear();
                    SSLEngineResult wrapResult = sslEngine.wrap(appData, netData);
                    netData.flip();
                    while (netData.hasRemaining()) {
                        socketChannel.write(netData);
                    }
                    handshakeStatus = wrapResult.getHandshakeStatus();
                    break;

                case NEED_TASK:
                    Runnable task;
                    while ((task = sslEngine.getDelegatedTask()) != null) {
                        task.run();
                    }
                    handshakeStatus = sslEngine.getHandshakeStatus();
                    break;

                default:
                    throw new IllegalStateException("Invalid SSL handshake status: " + handshakeStatus);
            }
        }
        return true;
    }

    private int readSSL(ByteBuffer dst) throws IOException {
        int totalRead = 0;

        while (dst.hasRemaining()) {
            if (peerNetData.position() == 0 || !peerNetData.hasRemaining()) {
                peerNetData.clear();
                int bytesRead = socketChannel.read(peerNetData);
                if (bytesRead == -1) {
                    sslEngine.closeInbound();
                    return -1;
                }
                peerNetData.flip();
            }

            SSLEngineResult result = sslEngine.unwrap(peerNetData, dst);
            switch (result.getStatus()) {
                case OK:
                    totalRead += result.bytesProduced();
                    break;

                case BUFFER_OVERFLOW:
                    // Expand application buffer
                    ByteBuffer newDst = ByteBuffer.allocate(dst.capacity() * 2);
                    dst.flip();
                    newDst.put(dst);
                    dst = newDst;
                    break;

                case BUFFER_UNDERFLOW:
                    // Need more data
                    if (!peerNetData.hasRemaining()) {
                        peerNetData.compact();
                        int bytesRead = socketChannel.read(peerNetData);
                        if (bytesRead == -1) {
                            sslEngine.closeInbound();
                            return -1;
                        }
                        peerNetData.flip();
                    }
                    break;

                case CLOSED:
                    sslEngine.closeInbound();
                    return -1;

                default:
                    throw new SSLException("Invalid SSL status: " + result.getStatus());
            }

            if (result.getHandshakeStatus() == SSLEngineResult.HandshakeStatus.NEED_TASK) {
                Runnable task;
                while ((task = sslEngine.getDelegatedTask()) != null) {
                    task.run();
                }
            }
        }

        return totalRead;
    }

    private void writeSSL(ByteBuffer src) throws IOException {
        while (src.hasRemaining()) {
            netData.clear();
            SSLEngineResult result = sslEngine.wrap(src, netData);
            netData.flip();

            while (netData.hasRemaining()) {
                socketChannel.write(netData);
            }

            switch (result.getStatus()) {
                case OK:
                    break;

                case BUFFER_OVERFLOW:
                    // Expand network buffer
                    ByteBuffer newNetData = ByteBuffer.allocate(netData.capacity() * 2);
                    netData.flip();
                    newNetData.put(netData);
                    netData = newNetData;
                    break;

                case BUFFER_UNDERFLOW:
                    // Should not occur on wrap
                    throw new SSLException("Buffer underflow occurred after a wrap.");

                case CLOSED:
                    sslEngine.closeOutbound();
                    break;

                default:
                    throw new SSLException("Invalid SSL status: " + result.getStatus());
            }

            if (result.getHandshakeStatus() == SSLEngineResult.HandshakeStatus.NEED_TASK) {
                Runnable task;
                while ((task = sslEngine.getDelegatedTask()) != null) {
                    task.run();
                }
            }
        }
    }
}
