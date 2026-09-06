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

package com.starrocks.qe;

import com.starrocks.common.ErrorCode;
import com.starrocks.mysql.MysqlCapability;
import com.starrocks.mysql.MysqlErrPacket;
import com.starrocks.mysql.MysqlSerializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

// SHOW ERRORS must report exactly what the client already received in the ERR packet for the same
// statement. MysqlErrPacket applies two substitutions of its own before the packet goes on the
// wire: code 1064 when QueryState carries no ErrorCode, and "Unknown error" when the message is
// null or empty. QueryWarning.fromErrorState repeats both, so these tests decode the packet and
// compare against the diagnostic instead of restating the expected values in a second place, which
// would let the two drift apart without any test noticing.
public class QueryWarningTest {

    @Test
    public void testFromErrorStateMirrorsErrPacket() {
        QueryState state = new QueryState();
        state.setErrorCode(ErrorCode.ERR_BAD_DB_ERROR);
        state.setError("Unknown database 'no_such_db'");

        QueryWarning diagnostic = QueryWarning.fromErrorState(state);
        ErrPacket packet = serializeErrPacket(state);

        Assertions.assertTrue(diagnostic.isError());
        Assertions.assertEquals("Error", diagnostic.getLevel());
        Assertions.assertEquals(String.valueOf(packet.code), diagnostic.getCode());
        Assertions.assertEquals(packet.message, diagnostic.getMessage());

        // The ErrorCode carried by the state, not the 1064 fallback.
        Assertions.assertEquals("5501", diagnostic.getCode());
        Assertions.assertEquals("Unknown database 'no_such_db'", diagnostic.getMessage());
    }

    // A statement can reach the error path without either field set, for example when it fails
    // before an ErrorCode is attached. The client then reads 1064 / "Unknown error" off the wire,
    // so SHOW ERRORS has to report the same rather than an empty message and a missing code.
    @Test
    public void testFromErrorStateSubstitutesTheSameDefaultsAsErrPacket() {
        QueryState emptyMessage = new QueryState();

        QueryState nullMessage = new QueryState();
        nullMessage.resetError();

        for (QueryState state : new QueryState[] {emptyMessage, nullMessage}) {
            ErrPacket packet = serializeErrPacket(state);
            Assertions.assertEquals(1064, packet.code);
            Assertions.assertEquals("Unknown error", packet.message);

            QueryWarning diagnostic = QueryWarning.fromErrorState(state);
            Assertions.assertEquals("Error", diagnostic.getLevel());
            Assertions.assertEquals(String.valueOf(packet.code), diagnostic.getCode());
            Assertions.assertEquals(packet.message, diagnostic.getMessage());
        }
    }

    private static ErrPacket serializeErrPacket(QueryState state) {
        MysqlSerializer serializer = MysqlSerializer.newInstance(MysqlCapability.DEFAULT_CAPABILITY);
        new MysqlErrPacket(state).writeTo(serializer);
        ByteBuffer buffer = serializer.toByteBuffer();

        Assertions.assertEquals((byte) 0xFF, buffer.get());
        int code = (buffer.get() & 0xFF) | ((buffer.get() & 0xFF) << 8);

        // DEFAULT_CAPABILITY advertises CLIENT_PROTOCOL_41, so the marker and the five byte SQL
        // state sit between the code and the message.
        Assertions.assertEquals((byte) '#', buffer.get());
        buffer.position(buffer.position() + 5);

        byte[] message = new byte[buffer.remaining()];
        buffer.get(message);
        return new ErrPacket(code, new String(message, StandardCharsets.UTF_8));
    }

    private static final class ErrPacket {
        private final int code;
        private final String message;

        private ErrPacket(int code, String message) {
            this.code = code;
            this.message = message;
        }
    }
}
