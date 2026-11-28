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

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

/** Loads Lance's native runtime with its own Arrow dependencies, exchanging only JSON and strings. */
class LanceDirectoryCatalog {
    private static class Holder {
        // The SDK's native library binds to this loader, so it belongs to the FE process, not a catalog or request.
        private static final URLClassLoader LOADER = createLoader();
        private static final Class<?> CLIENT = loadClientClass(LOADER);

        private static URLClassLoader createLoader() {
            String home = System.getenv("STARROCKS_HOME");
            if (home == null) {
                throw new IllegalStateException("STARROCKS_HOME is required for Lance directory metadata");
            }
            try (var files = Files.list(Path.of(home, "lib", "lance-metadata-lib"))) {
                URL[] urls = files.filter(p -> p.toString().endsWith(".jar")).sorted().map(p -> {
                    try {
                        return p.toUri().toURL();
                    } catch (Exception e) {
                        throw new IllegalStateException("Invalid Lance reader library path");
                    }
                }).toArray(URL[]::new);
                return new URLClassLoader(urls, ClassLoader.getPlatformClassLoader());
            } catch (Exception e) {
                throw new IllegalStateException("Install the Lance metadata libraries in FE lib/lance-metadata-lib");
            }
        }
    }

    static Class<?> loadClientClass(URLClassLoader loader) {
        try {
            return loader.loadClass("com.starrocks.lance.metadata.LanceDirectoryNamespace");
        } catch (Exception | LinkageError e) {
            IllegalStateException failure =
                    new IllegalStateException("Install the Lance metadata libraries in FE lib/lance-metadata-lib");
            // A failed initialization cannot publish a usable client; release any JARs already opened.
            try {
                loader.close();
            } catch (IOException closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            throw failure;
        }
    }

    String listTables(String warehouse, String options) {
        return invoke("listTables", warehouse, options);
    }

    String describeTable(String warehouse, String options, String table) {
        return invoke("describeTable", warehouse, options, table);
    }

    private String invoke(String method, String... args) {
        ClassLoader previous = Thread.currentThread().getContextClassLoader();
        try {
            Class<?> client = Holder.CLIENT;
            Thread.currentThread().setContextClassLoader(client.getClassLoader());
            Class<?>[] types = new Class<?>[args.length];
            Arrays.fill(types, String.class);
            return (String) client.getMethod(method, types).invoke(null, (Object[]) args);
        } catch (InvocationTargetException e) {
            // Third-party failures can include response bodies or signed storage URLs.
            throw new StarRocksConnectorException("Lance directory " + method
                    + " failed; check storage access and dataset validity ("
                    + e.getCause().getClass().getSimpleName() + ")");
        } catch (ReflectiveOperationException | LinkageError e) {
            throw new StarRocksConnectorException(
                    "Cannot initialize the FE Lance metadata reader; check its libraries and JVM options");
        } finally {
            Thread.currentThread().setContextClassLoader(previous);
        }
    }
}
