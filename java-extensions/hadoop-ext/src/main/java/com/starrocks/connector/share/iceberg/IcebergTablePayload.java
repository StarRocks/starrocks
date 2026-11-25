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

package com.starrocks.connector.share.iceberg;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.SnapshotRefParser;
import org.apache.iceberg.encryption.PlaintextEncryptionManager;
import org.apache.iceberg.hadoop.HadoopConfigurable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.StorageCredential;
import org.apache.iceberg.io.SupportsStorageCredentials;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * The table of an Iceberg metadata table scan in a JSON form that FE and BE of different versions can both read:
 * {@code {"format": 1, "build": <FE build>, "table": <table>}}. The predicate travels apart, as
 * ExpressionParser JSON, so that queries with different predicates share the table the BE caches by payload.
 *
 * <p>The FE ships a table to the BE metadata scanners as a Java-serialized {@link SerializableTable}. Its graph
 * holds Iceberg classes (ResolvingFileIO, S3FileIO, AwsProperties, SerializableConfiguration...) whose
 * serialVersionUID changes between Iceberg releases, so a BE fails to read the table of an FE built with
 * another Iceberg version, as during a rolling upgrade. The FE ships this payload as well; the BE reads it
 * when it was not built from the same build as the FE, or when the Java form fails to deserialize.
 *
 * <p>Compatibility rules, which every change of this format must keep:
 * <ul>
 *   <li>fields are only added, never removed, renamed or given another meaning;</li>
 *   <li>a reader ignores the fields it does not know and gives the missing ones their defaults;</li>
 *   <li>{@code build} comes before {@code table}, so a reader gets it without parsing the table (its Hadoop
 *       configuration makes it large);</li>
 *   <li>the schema, specs, sort order and refs are the JSON forms of the Iceberg spec (SchemaParser,
 *       PartitionSpecParser, SortOrderParser, SnapshotRefParser), stable across Iceberg releases.</li>
 * </ul>
 * The payload carries storage credentials: never log it.
 */
public final class IcebergTablePayload {
    public static final int FORMAT_VERSION = 1;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String FORMAT = "format";
    private static final String BUILD = "build";
    private static final String TABLE = "table";
    private static final String NAME = "name";
    private static final String LOCATION = "location";
    private static final String UUID_FIELD = "uuid";
    private static final String METADATA_FILE_LOCATION = "metadata_file_location";
    private static final String PROPERTIES = "properties";
    private static final String SCHEMA = "schema";
    private static final String DEFAULT_SPEC_ID = "default_spec_id";
    private static final String SPECS = "specs";
    private static final String SORT_ORDER = "sort_order";
    private static final String REFS = "refs";
    private static final String FILE_IO = "file_io";
    private static final String IMPL = "impl";
    private static final String CONF = "conf";
    private static final String CREDENTIALS = "credentials";
    private static final String PREFIX = "prefix";
    private static final String CONFIG = "config";

    private IcebergTablePayload() {
    }

    /**
     * The JSON form of {@code table}, stamped with the build of the writer, or {@code null} when the table cannot
     * be expressed in it (an encrypted table, or a FileIO that does not expose its properties). The BE then reads
     * the Java form only, as before.
     */
    public static String toJson(SerializableTable table, String build) {
        if (!(table.encryption() instanceof PlaintextEncryptionManager)) {
            return null;
        }
        FileIO io = table.rawIO();
        Map<String, String> ioProperties;
        try {
            ioProperties = io.properties();
        } catch (UnsupportedOperationException e) {
            return null;
        }

        ObjectNode envelope = MAPPER.createObjectNode();
        envelope.put(FORMAT, FORMAT_VERSION);
        envelope.put(BUILD, build);
        ObjectNode root = envelope.putObject(TABLE);
        root.put(NAME, table.name());
        root.put(LOCATION, table.location());
        if (table.uuid() != null) {
            root.put(UUID_FIELD, table.uuid().toString());
        }
        root.put(METADATA_FILE_LOCATION, table.metadataFileLocation());
        root.set(PROPERTIES, toNode(table.properties()));
        root.put(SCHEMA, table.schemaAsJson());
        root.put(DEFAULT_SPEC_ID, table.defaultSpecId());
        ObjectNode specs = root.putObject(SPECS);
        table.specAsJsonMap().forEach((specId, json) -> specs.put(String.valueOf(specId), json));
        root.put(SORT_ORDER, table.sortOrderAsJson());
        ObjectNode refs = root.putObject(REFS);
        table.refs().forEach((name, ref) -> refs.put(name, SnapshotRefParser.toJson(ref)));

        ObjectNode fileIO = root.putObject(FILE_IO);
        fileIO.put(IMPL, io.getClass().getName());
        fileIO.set(PROPERTIES, toNode(ioProperties));
        if (io instanceof HadoopConfigurable) {
            // The whole configuration the FE built, vended credentials included: the BE must not rebuild it.
            Configuration conf = ((HadoopConfigurable) io).getConf();
            if (conf != null) {
                Map<String, String> confAsMap = new HashMap<>(conf.size());
                conf.forEach(entry -> confAsMap.put(entry.getKey(), entry.getValue()));
                fileIO.set(CONF, toNode(confAsMap));
            }
        }
        if (io instanceof SupportsStorageCredentials) {
            List<StorageCredential> credentials = ((SupportsStorageCredentials) io).credentials();
            if (credentials != null && !credentials.isEmpty()) {
                ArrayNode array = fileIO.putArray(CREDENTIALS);
                for (StorageCredential credential : credentials) {
                    ObjectNode node = array.addObject();
                    node.put(PREFIX, credential.prefix());
                    node.set(CONFIG, toNode(credential.config()));
                }
            }
        }

        try {
            return MAPPER.writeValueAsString(envelope);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** The build of the FE that wrote the payload, read without parsing the table; "" when absent. */
    public static String readBuild(String json) {
        JsonNode build = readHeaderField(json, BUILD);
        return build == null || build.isNull() ? "" : build.asText();
    }

    /** Rebuilds the table of a payload written by {@link #toJson}, loading its FileIO on this side. */
    public static SerializableTable fromJson(String json) {
        JsonNode envelope;
        try {
            envelope = MAPPER.readTree(json);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to parse the iceberg table payload", e);
        }
        checkFormat(envelope.path(FORMAT).asInt(0));
        JsonNode root = envelope.get(TABLE);
        if (root == null || !root.isObject()) {
            throw new IllegalArgumentException("The iceberg table payload has no table");
        }

        Map<Integer, String> specs = new HashMap<>();
        root.path(SPECS).fields().forEachRemaining(
                e -> specs.put(Integer.parseInt(e.getKey()), e.getValue().asText()));
        Map<String, SnapshotRef> refs = new HashMap<>();
        root.path(REFS).fields().forEachRemaining(
                e -> refs.put(e.getKey(), SnapshotRefParser.fromJson(e.getValue().asText())));
        String uuid = text(root, UUID_FIELD);

        return new SerializableTable(
                text(root, NAME),
                text(root, LOCATION),
                uuid == null ? null : UUID.fromString(uuid),
                text(root, METADATA_FILE_LOCATION),
                toMap(root.path(PROPERTIES)),
                text(root, SCHEMA),
                root.path(DEFAULT_SPEC_ID).asInt(0),
                specs,
                text(root, SORT_ORDER),
                loadFileIO(root.path(FILE_IO)),
                PlaintextEncryptionManager.instance(),
                refs);
    }

    // Streams the top-level fields up to {@code field}, skipping the values before it, so reading the header does
    // not parse the table that follows it.
    private static JsonNode readHeaderField(String json, String field) {
        try (JsonParser parser = MAPPER.getFactory().createParser(json)) {
            if (parser.nextToken() != JsonToken.START_OBJECT) {
                throw new IllegalArgumentException("The iceberg table payload is not a JSON object");
            }
            while (parser.nextToken() == JsonToken.FIELD_NAME) {
                String name = parser.currentName();
                parser.nextToken();
                if (FORMAT.equals(name)) {
                    checkFormat(parser.getIntValue());
                } else if (field.equals(name)) {
                    return MAPPER.readTree(parser);
                } else {
                    parser.skipChildren();
                }
            }
            return null;
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to parse the iceberg table payload", e);
        }
    }

    private static void checkFormat(int format) {
        if (format < 1) {
            throw new IllegalArgumentException("Unknown iceberg table payload format " + format);
        }
    }

    private static FileIO loadFileIO(JsonNode node) {
        String impl = text(node, IMPL);
        if (impl == null) {
            throw new IllegalArgumentException("The iceberg table payload has no FileIO");
        }
        Configuration conf = null;
        if (node.has(CONF)) {
            conf = new Configuration(false);
            Configuration target = conf;
            toMap(node.get(CONF)).forEach(target::set);
        }
        List<StorageCredential> credentials = new ArrayList<>();
        for (JsonNode credential : node.path(CREDENTIALS)) {
            credentials.add(StorageCredential.create(text(credential, PREFIX), toMap(credential.path(CONFIG))));
        }
        return CatalogUtil.loadFileIO(impl, toMap(node.path(PROPERTIES)), conf, credentials);
    }

    private static String text(JsonNode node, String field) {
        JsonNode value = node.get(field);
        return value == null || value.isNull() ? null : value.asText();
    }

    private static ObjectNode toNode(Map<String, String> map) {
        ObjectNode node = MAPPER.createObjectNode();
        if (map != null) {
            map.forEach((key, value) -> {
                if (value != null) {
                    node.put(key, value);
                }
            });
        }
        return node;
    }

    private static Map<String, String> toMap(JsonNode node) {
        Map<String, String> map = new HashMap<>();
        Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            if (!field.getValue().isNull()) {
                map.put(field.getKey(), field.getValue().asText());
            }
        }
        return map;
    }
}
