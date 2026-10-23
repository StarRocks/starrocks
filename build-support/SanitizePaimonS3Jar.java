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

import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

/** Removes a dnsjava service descriptor that cannot be loaded from Paimon's embedded plugin layout. */
public final class SanitizePaimonS3Jar {
    private static final String INET_ADDRESS_RESOLVER_PROVIDER =
            "/paimon-plugin-s3/META-INF/services/java.net.spi.InetAddressResolverProvider";

    private SanitizePaimonS3Jar() {
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            throw new IllegalArgumentException("Usage: SanitizePaimonS3Jar <paimon-s3.jar>");
        }

        Path jar = Path.of(args[0]).toAbsolutePath();
        if (!Files.isRegularFile(jar)) {
            throw new IOException("Paimon S3 JAR does not exist: " + jar);
        }

        URI jarUri = URI.create("jar:" + jar.toUri());
        try (FileSystem zip = FileSystems.newFileSystem(jarUri, Map.of())) {
            Files.deleteIfExists(zip.getPath(INET_ADDRESS_RESOLVER_PROVIDER));
        }

        try (FileSystem zip = FileSystems.newFileSystem(jarUri, Map.of())) {
            if (Files.exists(zip.getPath(INET_ADDRESS_RESOLVER_PROVIDER))) {
                throw new IOException("Failed to remove incompatible InetAddressResolver provider from " + jar);
            }
        }
    }
}
