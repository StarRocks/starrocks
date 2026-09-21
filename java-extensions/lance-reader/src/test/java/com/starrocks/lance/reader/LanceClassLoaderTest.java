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

package com.starrocks.lance.reader;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LanceClassLoaderTest {
    @TempDir
    public Path tempDir;

    @Test
    public void testNativeDispatcherWithIsolatedReaderClassLoader() throws Exception {
        String classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
        String testClasses = Path.of(getClass().getProtectionDomain().getCodeSource().getLocation().toURI()).toString();
        Path output = tempDir.resolve("isolated-reader.log");
        Process process = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "--add-opens=java.base/java.nio=ALL-UNNAMED", "-cp", testClasses,
                IsolatedReader.class.getName(), classpath, tempDir.toString())
                .redirectErrorStream(true).redirectOutput(output.toFile()).start();
        try {
            assertTrue(process.waitFor(60, TimeUnit.SECONDS), "Isolated Lance reader timed out");
            String log = Files.readString(output);
            assertEquals(0, process.exitValue(), log);
            // Lance 7 can panic on a background native dispatcher while synchronous reads still succeed.
            assertFalse(log.contains("NoClassDefFoundError"), log);
            assertFalse(log.contains("panicked"), log);
            assertTrue(log.contains("Isolated scan passed"), log);
        } finally {
            process.destroyForcibly();
        }
    }

    public static class IsolatedReader {
        public static void main(String[] args) throws Exception {
            URL[] urls = Arrays.stream(args[0].split(File.pathSeparator)).map(path -> {
                try {
                    return Path.of(path).toUri().toURL();
                } catch (java.net.MalformedURLException e) {
                    throw new IllegalArgumentException(e);
                }
            }).toArray(URL[]::new);
            // The application/context loader deliberately cannot see Lance, as in BE's module loader.
            try (URLClassLoader loader = new URLClassLoader(urls, ClassLoader.getPlatformClassLoader())) {
                Class<?> fixture = loader.loadClass("com.starrocks.lance.reader.LanceSplitScannerTest");
                Object test = fixture.getConstructor().newInstance();
                fixture.getField("tempDir").set(test, Path.of(args[1]));
                fixture.getMethod("enableTestAllocator").invoke(test);
                try {
                    fixture.getMethod("testReadRealDatasetAcrossChunks").invoke(test);
                } finally {
                    fixture.getMethod("restoreTestAllocator").invoke(test);
                }
            }
            System.out.println("Isolated scan passed");
        }
    }
}
