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

package com.starrocks.connector.lance;

import com.starrocks.connector.exception.StarRocksConnectorException;

import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

/** Loads Lance's native runtime with its own Arrow dependencies, exchanging only JSON and strings. */
class LanceCatalogBridge {
    private static class Holder {
        private static final Class<?> CLIENT = load();

        private static Class<?> load() {
            String home = System.getenv("STARROCKS_HOME");
            if (home == null) {
                throw new IllegalStateException("STARROCKS_HOME is required for Lance REST metadata");
            }
            try (var files = Files.list(Path.of(home, "lib", "lance-reader-lib"))) {
                URL[] urls = files.filter(p -> p.toString().endsWith(".jar")).sorted().map(p -> {
                    try {
                        return p.toUri().toURL();
                    } catch (Exception e) {
                        throw new IllegalStateException("Invalid Lance reader library path");
                    }
                }).toArray(URL[]::new);
                // Keep the loader alive for the FE process: Lance's native library binds to it.
                return new URLClassLoader(urls, ClassLoader.getPlatformClassLoader())
                        .loadClass("com.starrocks.lance.reader.LanceRestCatalog");
            } catch (Exception e) {
                throw new IllegalStateException("Install the Lance reader libraries in FE lib/lance-reader-lib");
            }
        }
    }

    String invoke(String method, String... args) {
        ClassLoader previous = Thread.currentThread().getContextClassLoader();
        try {
            Class<?> client = Holder.CLIENT;
            Thread.currentThread().setContextClassLoader(client.getClassLoader());
            Class<?>[] types = new Class<?>[args.length];
            Arrays.fill(types, String.class);
            return (String) client.getMethod(method, types).invoke(null, (Object[]) args);
        } catch (InvocationTargetException e) {
            // Third-party failures can include response bodies or signed storage URLs.
            throw new StarRocksConnectorException("Lance catalog metadata request failed");
        } catch (ReflectiveOperationException | LinkageError e) {
            throw new StarRocksConnectorException(
                    "Cannot initialize the FE Lance metadata reader; check its libraries and JVM options");
        } finally {
            Thread.currentThread().setContextClassLoader(previous);
        }
    }
}
