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

package com.starrocks.connector.paimon;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.starrocks.connector.index.ConnectorIndexDescriptor.IndexType;
import com.starrocks.connector.index.IndexCondition;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorSerializer;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.DataField;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/** Versioned FE/BE request carried by the virtual Paimon index table. */
public final class PaimonGlobalIndexRequest {
    public static final int CURRENT_VERSION = 3;
    private static final Gson GSON = new Gson();

    private int version;
    private Long snapshotId;
    private Long schemaId;
    private Long schemaTimeMillis;
    private JsonObject predicate;
    private String tablePath;
    private String branch;
    private Map<String, IndexReference> indexes;

    private static final class IndexReference {
        private Integer fieldId;
        private String provider;

        private IndexReference(int fieldId) {
            this.fieldId = fieldId;
            this.provider = "btree";
        }
    }

    private PaimonGlobalIndexRequest() {
    }

    public static PaimonGlobalIndexRequest create(
            String tablePath, String branch, long snapshotId, long schemaId, long schemaTimeMillis,
            IndexCondition condition) {
        if (condition == null || condition.getPredicate() == null) {
            throw new IllegalArgumentException("Paimon distributed execution requires a scalar index condition");
        }
        PaimonGlobalIndexRequest request = new PaimonGlobalIndexRequest();
        request.version = CURRENT_VERSION;
        request.snapshotId = snapshotId;
        request.schemaId = schemaId;
        request.schemaTimeMillis = schemaTimeMillis;
        request.tablePath = tablePath;
        request.branch = branch;
        request.predicate = GSON.toJsonTree(
                ScalarOperatorSerializer.toJson(condition.getPredicate())).getAsJsonObject();
        request.indexes = new LinkedHashMap<>();
        for (ColumnRefOperator column : condition.getPredicate().getColumnRefs()) {
            Integer fieldId = condition.getFieldIds().get(column.getId());
            if (fieldId == null || fieldId < 0) {
                throw new IllegalArgumentException("Missing stable field binding for " + column);
            }
            IndexReference previous = request.indexes.put(column.getName(), new IndexReference(fieldId));
            if (previous != null && !previous.fieldId.equals(fieldId)) {
                throw new IllegalArgumentException("Ambiguous index expression column " + column.getName());
            }
        }
        request.validate();
        return request;
    }

    private void validate() {
        if (version != CURRENT_VERSION || snapshotId == null || snapshotId < 0 || tablePath == null || tablePath.isBlank()
                || schemaId == null || schemaId < 0 || schemaTimeMillis == null || schemaTimeMillis < 0
                || !"main".equals(branch) || predicate == null || indexes == null || indexes.isEmpty()) {
            throw new IllegalArgumentException("Invalid Paimon Global Index request");
        }
        getRequiredIndexes();
    }

    public static PaimonGlobalIndexRequest parse(String json) {
        try {
            PaimonGlobalIndexRequest request = GSON.fromJson(json, PaimonGlobalIndexRequest.class);
            if (request == null) {
                throw new IllegalArgumentException("Invalid Paimon Global Index request");
            }
            request.validate();
            return request;
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("Invalid Paimon Global Index request JSON", e);
        }
    }

    public static PaimonGlobalIndexRequest parseTransport(String encoded) {
        try {
            byte[] json = Base64.getDecoder().decode(encoded);
            return parse(new String(json, StandardCharsets.UTF_8));
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("Invalid Paimon Global Index request transport", e);
        }
    }

    public String toJson() {
        return GSON.toJson(this);
    }

    public String toTransportString() {
        return Base64.getEncoder().encodeToString(toJson().getBytes(StandardCharsets.UTF_8));
    }

    public long getSnapshotId() {
        return snapshotId;
    }

    public boolean matchesSchema(long id, long timeMillis) {
        return schemaId == id && schemaTimeMillis == timeMillis;
    }

    /** args is caller input. Bind it to the actual schema before creating any native index readers. */
    public void validatePredicate(TableSchema schema) {
        try {
            validateAgainstSchema(schema);
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("Invalid Paimon Global Index predicate: " + e.getMessage(), e);
        }
    }

    private void validateAgainstSchema(TableSchema schema) {
        Map<String, String> types = new LinkedHashMap<>();
        indexes.forEach((name, reference) -> {
            DataField field = schema.idToFieldMap().get(reference.fieldId);
            if (field == null || !name.equals(field.name())) {
                throw new IllegalArgumentException("Invalid Paimon Global Index field binding: " + name);
            }
            String type = switch (field.type().getTypeRoot()) {
                case BOOLEAN -> "boolean";
                case TINYINT -> "tinyint";
                case SMALLINT -> "smallint";
                case INTEGER -> "int";
                case BIGINT -> "bigint";
                case FLOAT -> "float";
                case DOUBLE -> "double";
                case CHAR, VARCHAR -> "string";
                default -> throw new IllegalArgumentException("Unsupported Paimon Global Index field type");
            };
            types.put(name, type);
        });
        validatePredicate(predicate, types, 0);
    }

    private static String stringMember(JsonObject node, String key) {
        JsonElement value = node.get(key);
        if (value == null || !value.isJsonPrimitive() || !value.getAsJsonPrimitive().isString()) {
            throw new IllegalArgumentException("Invalid Paimon Global Index predicate member: " + key);
        }
        return value.getAsString();
    }

    private static String literalType(JsonObject node) {
        String type = stringMember(node, "t");
        if (type.length() > 64) {
            throw new IllegalArgumentException("Paimon Global Index type token is too long");
        }
        type = type.toLowerCase(java.util.Locale.ROOT);
        if (type.matches("(?:string|varchar|char)(?:\\([0-9]+\\))?")) {
            return "string";
        }
        return type;
    }

    private static void validatePredicate(JsonObject node, Map<String, String> types, int depth) {
        if (depth > 128) {
            throw new IllegalArgumentException("Paimon Global Index predicate is too deep");
        }
        String operator = stringMember(node, "o");
        if (operator.equals("cp")) {
            if (!Set.of("AND", "OR").contains(stringMember(node, "ct"))) {
                throw new IllegalArgumentException("Unsupported Paimon Global Index compound predicate");
            }
            JsonArray children = node.getAsJsonArray("c");
            if (children == null || children.size() != 2) {
                throw new IllegalArgumentException("Invalid Paimon Global Index compound predicate");
            }
            for (JsonElement child : children) {
                validatePredicate(child.getAsJsonObject(), types, depth + 1);
            }
            return;
        }
        JsonArray children = node.getAsJsonArray(operator.equals("ca") ? "a" : "c");
        int expected = operator.equals("isn") ? 1 : 2;
        if (children == null || (operator.equals("ip") ? children.size() < 2 : children.size() != expected)) {
            throw new IllegalArgumentException("Invalid Paimon Global Index predicate children");
        }
        JsonObject column = children.get(0).getAsJsonObject();
        String type = types.get(stringMember(column, "n"));
        if (!"cr".equals(stringMember(column, "o")) || type == null || !type.equals(literalType(column))) {
            throw new IllegalArgumentException("Paimon Global Index column does not match schema");
        }
        switch (operator) {
            case "b":
                if (!Set.of("EQ", "NE", "LT", "LE", "GT", "GE").contains(stringMember(node, "b"))) {
                    throw new IllegalArgumentException("Unsupported Paimon Global Index binary predicate");
                }
                break;
            case "ip":
            case "isn":
                JsonElement negated = node.get("ng");
                if (negated == null || !negated.isJsonPrimitive() || !negated.getAsJsonPrimitive().isBoolean()) {
                    throw new IllegalArgumentException("Invalid Paimon Global Index negation");
                }
                break;
            case "ca":
                if (!"starts_with".equalsIgnoreCase(stringMember(node, "f")) || !type.equals("string")) {
                    throw new IllegalArgumentException("Unsupported Paimon Global Index function or field type");
                }
                break;
            default:
                throw new IllegalArgumentException("Unsupported Paimon Global Index predicate");
        }
        for (int i = 1; i < children.size(); i++) {
            validateLiteral(children.get(i).getAsJsonObject(), type);
        }
    }

    private static void validateLiteral(JsonObject node, String type) {
        JsonElement value = node.get("v");
        if (!"co".equals(stringMember(node, "o")) || !type.equals(literalType(node))
                || value == null || !value.isJsonPrimitive()) {
            throw new IllegalArgumentException("Paimon Global Index literal does not match schema");
        }
        if (type.equals("string") && value.getAsJsonPrimitive().isString()
                || type.equals("boolean") && value.getAsJsonPrimitive().isBoolean()) {
            return;
        }
        if (!value.getAsJsonPrimitive().isNumber()) {
            throw new IllegalArgumentException("Invalid Paimon Global Index literal value");
        }
        BigDecimal number = value.getAsBigDecimal();
        int bits = switch (type) {
            case "tinyint" -> 8;
            case "smallint" -> 16;
            case "int" -> 32;
            case "bigint" -> 64;
            default -> 0;
        };
        if (bits != 0) {
            BigInteger limit = BigInteger.ONE.shiftLeft(bits - 1);
            // Check exponent/range before any integer conversion: a short caller-supplied
            // value such as 1e1000000000 must not allocate a billion-digit BigInteger.
            if (number.compareTo(new BigDecimal(limit.negate())) >= 0
                    && number.compareTo(new BigDecimal(limit.subtract(BigInteger.ONE))) <= 0
                    && number.stripTrailingZeros().scale() <= 0) {
                return;
            }
        } else if (type.equals("float") && Float.isFinite(number.floatValue())
                && (number.signum() == 0 || number.floatValue() != 0)
                || type.equals("double") && Double.isFinite(number.doubleValue())
                && (number.signum() == 0 || number.doubleValue() != 0)) {
            return;
        }
        throw new IllegalArgumentException("Paimon Global Index literal is out of range");
    }

    public String getTablePath() {
        return tablePath;
    }

    public String getBranch() {
        return branch;
    }

    public Map<Integer, IndexType> getRequiredIndexes() {
        Map<Integer, IndexType> result = new LinkedHashMap<>();
        indexes.forEach((column, reference) -> {
            if (column == null || column.isEmpty() || reference == null
                    || reference.fieldId == null || reference.fieldId < 0 || !"btree".equals(reference.provider)) {
                throw new IllegalArgumentException("Invalid scalar index field binding");
            }
            result.put(reference.fieldId, IndexType.RANGE);
        });
        return Map.copyOf(result);
    }
}
