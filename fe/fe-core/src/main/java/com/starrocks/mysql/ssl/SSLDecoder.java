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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;

public class SSLDecoder {
    private static final Logger LOG = LogManager.getLogger(SSLDecoder.class);

    private final SSLEngine sslEngine;

    // Encrypted data buffer (network)
    private ByteBuffer netBuffer;
    // Decrypted plaintext buffer
    private ByteBuffer appBuffer;

    public SSLDecoder(SSLEngine sslEngine, ByteBuffer netBuffer, ByteBuffer appBuffer) {
        this.sslEngine = sslEngine;
        this.netBuffer = netBuffer;
        this.appBuffer = appBuffer;
        this.netBuffer.compact();
    }

    public void feed(ByteBuffer encryptedInput) throws IOException {
        // Check if netBuffer has enough remaining capacity
        if (encryptedInput.remaining() > netBuffer.remaining()) {
            // Expand netBuffer if needed
            ByteBuffer newBuf = ByteBuffer.allocate(
                    Math.max(netBuffer.capacity() * 2, netBuffer.position() + encryptedInput.remaining()));
            netBuffer.flip();
            newBuf.put(netBuffer);
            netBuffer = newBuf;
        }
        netBuffer.put(encryptedInput);
    }

    public ByteBuffer decode() throws IOException {
        if (!appBuffer.hasRemaining()) {
            appBuffer.clear();
        }
        netBuffer.flip();

        while (netBuffer.hasRemaining()) {
            SSLEngineResult result = sslEngine.unwrap(netBuffer, appBuffer);
            switch (result.getStatus()) {
                case OK:
                    // No progress (no bytes consumed or produced), break the loop to avoid busy-wait
                    if (result.bytesConsumed() == 0 && result.bytesProduced() == 0) {
                        netBuffer.compact();
                        appBuffer.flip();
                        return appBuffer;
                    }
                    break;
                case BUFFER_OVERFLOW:
                    // appBuffer is too small, expand it and retry
                    expandAppBuffer();
                    continue;
                case BUFFER_UNDERFLOW:
                    // Not enough encrypted data, preserve remaining data in netBuffer
                    // and exit. More data will be fed in the next read event.
                    netBuffer.compact();
                    appBuffer.flip();
                    return appBuffer;
                case CLOSED:
                    netBuffer.compact();
                    appBuffer.flip();
                    return appBuffer;
                default:
                    throw new IllegalStateException("Unexpected SSL status: " + result.getStatus());
            }
        }

        // All encrypted data consumed, flip appBuffer for reading
        netBuffer.compact();
        appBuffer.flip();
        return appBuffer;
    }

    private void expandAppBuffer() {
        int newCapacity = appBuffer.capacity() * 2;
        ByteBuffer newBuf = ByteBuffer.allocateDirect(newCapacity);
        appBuffer.flip();
        newBuf.put(appBuffer);
        appBuffer = newBuf;
    }

}
