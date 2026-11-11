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

package com.starrocks.jni.connector;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ScannerHelperClassLoaderTest {
    @TempDir
    Path home;

    @Test
    void moduleJarTakesPrecedenceOverDependencies() throws Exception {
        Path module = Files.createDirectories(home.resolve("lib/paimon-reader-lib"));
        Files.createDirectories(home.resolve("lib/common-runtime-lib"));
        Files.createDirectories(home.resolve("lib/hadoop/common"));
        writeJar(module.resolve("hive.jar"), "hive");
        writeJar(module.resolve("starrocks-paimon-reader.jar"), "reader");

        ClassLoader loader = ScannerHelper.createModuleClassLoader(home.toFile(), "paimon-reader-lib",
                "starrocks-paimon-reader.jar");
        try (var stream = loader.getResourceAsStream("compatibility-test.txt")) {
            assertEquals("reader", new String(stream.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8));
        }
    }

    @Test
    void moduleJarsTakePrecedenceWithoutPreferredJar() throws Exception {
        Path module = Files.createDirectories(home.resolve("lib/paimon-reader-lib"));
        Path common = Files.createDirectories(home.resolve("lib/common-runtime-lib"));
        Files.createDirectories(home.resolve("lib/hadoop/common"));
        writeJar(module.resolve("module.jar"), "module");
        writeJar(common.resolve("dependency.jar"), "dependency");

        ClassLoader loader = ScannerHelper.createModuleClassLoader(home.toFile(), "paimon-reader-lib", null);
        try (var stream = loader.getResourceAsStream("compatibility-test.txt")) {
            assertEquals("module", new String(stream.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8));
        }
    }

    @Test
    void missingPreferredJarFailsFast() throws IOException {
        Files.createDirectories(home.resolve("lib/paimon-reader-lib"));
        assertThrows(IllegalStateException.class, () -> ScannerHelper.createModuleClassLoader(
                home.toFile(), "paimon-reader-lib", "starrocks-paimon-reader.jar"));
    }

    private static void writeJar(Path path, String value) throws IOException {
        try (JarOutputStream jar = new JarOutputStream(Files.newOutputStream(path))) {
            jar.putNextEntry(new JarEntry("compatibility-test.txt"));
            jar.write(value.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            jar.closeEntry();
        }
    }
}
