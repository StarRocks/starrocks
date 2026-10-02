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

package org.apache.iceberg.encryption;

import java.nio.ByteBuffer;

/**
 * Package-bridge into Iceberg's {@link StandardKeyMetadata}, which is package-private.
 *
 * <p>StarRocks' BE generates the per-file Data Encryption Key (DEK) and AAD prefix when
 * writing an encrypted Parquet file, and unwraps them when reading. To interoperate with
 * Iceberg/Spark/Trino, the DEK+AAD must be serialized into / parsed from the exact byte
 * layout Iceberg writes into the manifest's {@code key_metadata} column. That layout is
 * produced by {@code StandardKeyMetadata}, whose constructor and {@code parse} are not
 * public. This helper lives in {@code org.apache.iceberg.encryption} so it can call them,
 * delegating all serialization to Iceberg's own encoder (no hand-rolled crypto/Avro).
 */
public final class StarRocksKeyMetadata {
    private StarRocksKeyMetadata() {}

    /** Serialize a per-file (DEK, AAD prefix) into Iceberg's committable key_metadata bytes. */
    public static ByteBuffer serialize(byte[] dek, byte[] aadPrefix) {
        return new StandardKeyMetadata(dek, aadPrefix).buffer();
    }

    /** Parse Iceberg key_metadata bytes back into (DEK, AAD prefix). */
    public static Parsed parse(ByteBuffer keyMetadata) {
        StandardKeyMetadata parsed = StandardKeyMetadata.parse(keyMetadata);
        return new Parsed(toBytes(parsed.encryptionKey()), toBytes(parsed.aadPrefix()));
    }

    private static byte[] toBytes(ByteBuffer buffer) {
        if (buffer == null) {
            return null;
        }
        ByteBuffer dup = buffer.duplicate();
        byte[] out = new byte[dup.remaining()];
        dup.get(out);
        return out;
    }

    /** The (DEK, AAD prefix) pair unwrapped from an Iceberg key_metadata blob. */
    public static final class Parsed {
        public final byte[] dek;
        public final byte[] aadPrefix;

        Parsed(byte[] dek, byte[] aadPrefix) {
            this.dek = dek;
            this.aadPrefix = aadPrefix;
        }
    }
}
