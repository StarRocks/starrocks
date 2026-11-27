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

package com.starrocks.rpc;

import org.apache.thrift.TConfiguration;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.net.Socket;

/**
 * TSocket and TServerSocket wrappers that dynamically honor FE Thrift size and recursion
 * settings ({@code thrift_max_message_size}, {@code thrift_max_frame_size},
 * {@code thrift_max_recursion_depth}), including pooled connection reuse and treating
 * {@code thrift_max_message_size <= 0} as unlimited.
 */
public class ConfigurableTSocket extends TSocket {

    public ConfigurableTSocket(Socket socket) throws TTransportException {
        super(socket);
        refreshConfiguration();
    }

    public ConfigurableTSocket(String host, int port, int timeout) throws TTransportException {
        super(ConfigurableTProtocolFactory.buildTConfiguration(), host, port, timeout);
    }

    @Override
    protected long getMaxMessageSize() {
        return ConfigurableTProtocolFactory.effectiveMaxMessageSizeBytes();
    }

    @Override
    public void checkReadBytesAvailable(long numBytes) throws TTransportException {
        long currentLimit = getMaxMessageSize();
        long consumed = Math.max(0L, knownMessageSize - remainingMessageSize);
        long available = currentLimit - consumed;
        if (available < numBytes || numBytes < 0) {
            throw new TTransportException(
                    TTransportException.MESSAGE_SIZE_LIMIT,
                    "Message size exceeds limit: " + currentLimit);
        }
    }

    @Override
    public void open() throws TTransportException {
        super.open();
        refreshConfiguration();
    }

    public void refreshConfiguration() throws TTransportException {
        TConfiguration config = getConfiguration();
        if (config != null) {
            config.setMaxMessageSize(ConfigurableTProtocolFactory.effectiveMaxMessageSizeConfig());
            config.setMaxFrameSize(ConfigurableTProtocolFactory.effectiveMaxFrameSizeConfig());
            config.setRecursionLimit(ConfigurableTProtocolFactory.effectiveRecursionLimitConfig());
        }
        resetConsumedMessageSize(-1);
    }

    public static void refreshTransport(TTransport transport) {
        if (transport == null) {
            return;
        }
        try {
            if (transport instanceof ConfigurableTSocket) {
                ((ConfigurableTSocket) transport).refreshConfiguration();
            } else {
                TConfiguration config = transport.getConfiguration();
                if (config != null) {
                    config.setMaxMessageSize(ConfigurableTProtocolFactory.effectiveMaxMessageSizeConfig());
                    config.setMaxFrameSize(ConfigurableTProtocolFactory.effectiveMaxFrameSizeConfig());
                    config.setRecursionLimit(ConfigurableTProtocolFactory.effectiveRecursionLimitConfig());
                }
                transport.updateKnownMessageSize(0);
            }
        } catch (TTransportException ignored) {
            // Best-effort refresh before starting a new RPC read/write cycle.
        }
    }

    public static class ServerSocket extends TServerSocket {
        private final int clientTimeoutMs;

        public ServerSocket(ServerSocketTransportArgs args, int clientTimeoutMs) throws TTransportException {
            super(args);
            this.clientTimeoutMs = clientTimeoutMs;
        }

        @Override
        public TSocket accept() throws TTransportException {
            java.net.ServerSocket serverSocket = getServerSocket();
            if (serverSocket == null) {
                throw new TTransportException(TTransportException.NOT_OPEN, "No underlying server socket.");
            }
            Socket result;
            try {
                result = serverSocket.accept();
            } catch (Exception e) {
                throw new TTransportException(e);
            }
            if (result == null) {
                throw new TTransportException("Blocking server's accept() may not return NULL");
            }
            ConfigurableTSocket socket = new ConfigurableTSocket(result);
            socket.setTimeout(clientTimeoutMs);
            return socket;
        }
    }
}
