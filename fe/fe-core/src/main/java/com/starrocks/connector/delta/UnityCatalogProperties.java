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

package com.starrocks.connector.delta;

import com.starrocks.connector.exception.StarRocksConnectorException;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Locale;
import java.util.Map;

public final class UnityCatalogProperties {
    public static final String UNITY_CATALOG_URI = "unity.catalog.uri";
    public static final String UNITY_CATALOG_NAME = "unity.catalog.name";

    private final URI uri;
    private final String catalogName;

    public UnityCatalogProperties(Map<String, String> properties) {
        try {
            String value = properties.getOrDefault(UNITY_CATALOG_URI, "");
            uri = new URI(value.endsWith("/") ? value.substring(0, value.length() - 1) : value);
            if (!("https".equals(uri.getScheme()) || "http".equals(uri.getScheme()))
                    || uri.getHost() == null || uri.getUserInfo() != null
                    || uri.getQuery() != null || uri.getFragment() != null || !uri.equals(uri.normalize())
                    || uri.getRawPath().contains("%")) {
                throw new URISyntaxException("", "Invalid catalog URI");
            }
        } catch (URISyntaxException e) {
            throw new StarRocksConnectorException("unity.catalog.uri must be an HTTP(S) API base URI without credentials");
        }
        catalogName = properties.getOrDefault(UNITY_CATALOG_NAME, "");
        validateName(catalogName);
    }

    public URI getUri() {
        return uri;
    }

    public String getCatalogName() {
        return catalogName;
    }

    public static void validateName(String name) {
        if (name == null || name.isBlank() || name.length() > 255 || name.contains(".") || name.contains("/")
                || name.contains("\\") || name.chars().anyMatch(c -> Character.isISOControl(c))) {
            throw new StarRocksConnectorException("Unity Catalog requires nonempty catalog, schema and table names without dots");
        }
    }

    /** Returns an ADLS Gen2 URI with no credentials or ambiguous path segments. */
    public static URI validateStorageLocation(String location) {
        if (location == null) {
            throw new StarRocksConnectorException("Unity Catalog requires an ADLS Gen2 storage location");
        }
        try {
            URI value = new URI(location);
            String host = value.getHost() == null ? "" : value.getHost().toLowerCase(Locale.ROOT);
            String path = value.getPath();
            String container = value.getUserInfo();
            if (!host.matches("[a-z0-9]{3,24}\\.dfs\\.core\\.windows\\.net") || value.getPort() != -1
                    || value.getRawQuery() != null || value.getRawFragment() != null || path == null
                    || value.getRawPath().toLowerCase(Locale.ROOT).matches(".*%(2f|5c).*")) {
                throw new IllegalArgumentException();
            }
            if ("https".equals(value.getScheme()) && container == null) {
                int separator = path.indexOf('/', 1);
                if (separator < 0) {
                    throw new IllegalArgumentException();
                }
                container = path.substring(1, separator);
                path = path.substring(separator);
            } else if (!"abfss".equals(value.getScheme())) {
                throw new IllegalArgumentException();
            }
            if (container == null || !container.matches("[a-z0-9][a-z0-9-]{1,61}[a-z0-9]")
                    || !path.startsWith("/") || path.contains("\\") || path.contains("%")
                    || path.chars().anyMatch(c -> Character.isISOControl(c))) {
                throw new IllegalArgumentException();
            }
            while (path.length() > 1 && path.endsWith("/")) {
                path = path.substring(0, path.length() - 1);
            }
            if (!path.equals("/")) {
                for (String segment : path.substring(1).split("/", -1)) {
                    if (segment.isEmpty() || segment.equals(".") || segment.equals("..")) {
                        throw new IllegalArgumentException();
                    }
                }
            }
            return new URI("abfss", container, host, -1, path, null, null);
        } catch (URISyntaxException | IllegalArgumentException e) {
            throw new StarRocksConnectorException("Unity Catalog requires an uncredentialed ADLS Gen2 storage location");
        }
    }

    public static void requireDescendant(String root, String file) {
        URI rootUri = validateStorageLocation(root);
        URI fileUri = validateStorageLocation(file);
        String prefix = rootUri.getPath().endsWith("/") ? rootUri.getPath() : rootUri.getPath() + "/";
        if (!rootUri.getAuthority().equals(fileUri.getAuthority()) || !fileUri.getPath().startsWith(prefix)
                || rootUri.getPath().equals(fileUri.getPath())) {
            throw new StarRocksConnectorException("Delta file is outside the Unity Catalog table location");
        }
    }
}
