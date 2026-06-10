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

package com.starrocks.mysql;

import org.junit.jupiter.api.Test;
import org.xnio.StreamConnection;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ProxyProtocolParserTest {

    // Minimal channel stub: realNetReadWithTimeout returns EOF (-1) immediately,
    // causing parse(MysqlChannel, long) to throw "Connection closed" IOException (line 63).
    private static class EofMysqlChannel extends MysqlChannel {
        EofMysqlChannel() {
            super((StreamConnection) null);
        }

        @Override
        protected int realNetReadWithTimeout(ByteBuffer dst, long timeoutMs) {
            return -1;
        }
    }

    private ProxyProtocolParser.ByteReader readerFor(byte[] data) {
        AtomicInteger pos = new AtomicInteger(0);
        return n -> {
            int start = pos.getAndAdd(n);
            if (start + n > data.length) {
                throw new IOException("Unexpected end of test data");
            }
            return Arrays.copyOfRange(data, start, start + n);
        };
    }

    private ProxyProtocolParser.ByteReader readerFor(String ascii) {
        return readerFor(ascii.getBytes(StandardCharsets.US_ASCII));
    }

    @Test
    public void testV1Ipv4() throws IOException {
        ProxyProtocolParser.Result r =
                ProxyProtocolParser.parse(readerFor("PROXY TCP4 1.2.3.4 5.6.7.8 1000 9030\r\n"));
        assertEquals("1.2.3.4", r.ip);
        assertEquals(1000, r.port);
    }

    @Test
    public void testV1Ipv6() throws IOException {
        ProxyProtocolParser.Result r = ProxyProtocolParser.parse(
                readerFor("PROXY TCP6 ::1 ::2 4567 9030\r\n"));
        assertEquals("::1", r.ip);
        assertEquals(4567, r.port);
    }

    @Test
    public void testV1Unknown() throws IOException {
        // long form (with optional address fields)
        assertNull(ProxyProtocolParser.parse(readerFor("PROXY UNKNOWN 1.2.3.4 5.6.7.8 1000 9030\r\n")));
        // short form — canonical health-check line, must not throw
        assertNull(ProxyProtocolParser.parse(readerFor("PROXY UNKNOWN\r\n")));
    }

    @Test
    public void testInvalidHeaders() {
        StringBuilder tooLong = new StringBuilder("PROXY TCP4 ");
        while (tooLong.length() < 110) {
            tooLong.append("1");
        }
        String[] bad = {
                tooLong.toString(),                                    // exceeds max line length
                "PROXY TCP4 1.2.3.4\r\n",                            // missing fields
                "PROXY TCP4 1.2.3.4 5.6.7.8 abc 9030\r\n",           // non-numeric port
                "GET / HTTP/1.1\r\n",                                  // unrecognized protocol
                "PRO",                                                  // truncated before version detection
                "PROXY TCP4 not-an-ip 5.6.7.8 1000 9030\r\n",        // invalid IP address
                "PROXY TCP4 ::1 ::2 1000 9030\r\n",                   // TCP4 with IPv6 address
                "PROXY TCP6 1.2.3.4 5.6.7.8 1000 9030\r\n",          // TCP6 with IPv4 address
                "PROXY UDP4 1.2.3.4 5.6.7.8 1000 9030\r\n",          // unsupported protocol family
                "PROXY \r\n",                                          // only "PROXY" token after trim (line 102)
                "PROXY TCP4 1.2.3.4 5.6.7.8 99999 9030\r\n",         // port out of range (line 140)
        };
        for (String input : bad) {
            assertThrows(IOException.class, () -> ProxyProtocolParser.parse(readerFor(input)));
        }
    }

    // Covers line 63: channel returns EOF before a full header is delivered.
    @Test
    public void testChannelEofBeforeHeader() {
        assertThrows(IOException.class, () -> ProxyProtocolParser.parse(new EofMysqlChannel(), 1000));
    }
}
