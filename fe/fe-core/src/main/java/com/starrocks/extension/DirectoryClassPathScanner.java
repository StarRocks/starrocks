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

package com.starrocks.extension;

import com.google.api.client.util.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

public class DirectoryClassPathScanner {
    private static final Logger LOG = LogManager.getLogger(JarScanner.class);
    private static final List<String> BASE_PACKAGES = List.of(
            "com.starrocks.extension",
            "com.starrocks.epack"
    );

    public Set<Class<?>> scan(String path) {
        Path root = Paths.get(path);
        if (!Files.isDirectory(root)) {
            return Set.of();
        }

        Set<Class<?>> result = Sets.newHashSet();

        try (Stream<Path> paths = Files.walk(root)) {
            paths.filter(this::isClassFile)
                    .filter(this::isUnderBasePackage)
                    .forEach(p -> handleClassFile(root, p, result));
        } catch (IOException e) {
            LOG.warn("Failed to scan directory {}", path, e);
        }

        return result;
    }

    private boolean isClassFile(Path path) {
        return path.toString().endsWith(".class");
    }

    private boolean isUnderBasePackage(Path classFile) {
        String normalized =
                classFile.toString().replace(File.separatorChar, '/');
        return BASE_PACKAGES.stream().anyMatch(normalized::contains);
    }

    private static String toClassName(Path baseDir, Path classFile) {
        Path relative = baseDir.relativize(classFile);
        return relative.toString().replace(File.separatorChar, '.').replace(".class", "");
    }

    private void handleClassFile(
            Path baseDir,
            Path classFile,
            Set<Class<?>> output) {
        String className = toClassName(baseDir, classFile);
        try {
            Class<?> clazz = Class.forName(className);
            if (StarRocksExtension.class.isAssignableFrom(clazz) &&
                    clazz.isAnnotationPresent(SRModule.class)) {
                output.add(clazz);
            }
        } catch (Throwable t) {
            LOG.warn("Failed to load class {}", className, t);
        }
    }

}
