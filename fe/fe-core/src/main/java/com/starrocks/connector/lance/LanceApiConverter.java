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

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.starrocks.catalog.Column;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.type.ArrayType;
import com.starrocks.type.StructField;
import com.starrocks.type.StructType;
import com.starrocks.type.Type;
import com.starrocks.type.TypeFactory;

import java.util.ArrayList;
import java.util.List;

import static com.starrocks.type.BooleanType.BOOLEAN;
import static com.starrocks.type.DateType.DATE;
import static com.starrocks.type.DateType.DATETIME;
import static com.starrocks.type.FloatType.DOUBLE;
import static com.starrocks.type.FloatType.FLOAT;
import static com.starrocks.type.IntegerType.BIGINT;
import static com.starrocks.type.IntegerType.INT;
import static com.starrocks.type.IntegerType.SMALLINT;
import static com.starrocks.type.IntegerType.TINYINT;
import static com.starrocks.type.VarbinaryType.VARBINARY;
import static com.starrocks.type.VarcharType.VARCHAR;

public class LanceApiConverter {
    private static final String CHILDREN = "children";

    /** Convert the schema read by Lance; never silently substitute a type or discard a field. */
    static List<Column> fromSchema(JsonObject schema) {
        if (schema == null || !schema.has("fields")) {
            throw new StarRocksConnectorException("Lance dataset has no schema");
        }
        List<Column> columns = new ArrayList<>();
        for (JsonElement value : schema.getAsJsonArray("fields")) {
            JsonObject field = value.getAsJsonObject();
            columns.add(new Column(field.get("name").getAsString(), fromFieldType(field),
                    field.get("nullable").getAsBoolean()));
        }
        if (columns.isEmpty()) {
            throw new StarRocksConnectorException("Lance dataset has an empty schema");
        }
        return columns;
    }

    private static Type fromFieldType(JsonObject field) {
        JsonObject type = field.getAsJsonObject("type");
        String name = type.get("name").getAsString();
        switch (name) {
            case "list":
            case "largelist":
            case "fixedsizelist":
                if (field.getAsJsonArray(CHILDREN).size() != 1) {
                    throw new StarRocksConnectorException("Invalid Lance list field");
                }
                return new ArrayType(fromFieldType(field.getAsJsonArray(CHILDREN).get(0).getAsJsonObject()));
            case "struct":
                ArrayList<StructField> fields = new ArrayList<>();
                for (JsonElement value : field.getAsJsonArray(CHILDREN)) {
                    JsonObject child = value.getAsJsonObject();
                    fields.add(new StructField(child.get("name").getAsString(), fromFieldType(child)));
                }
                return new StructType(fields);
            case "int":
                int bits = type.get("bitWidth").getAsInt();
                if (List.of(8, 16, 32, 64).contains(bits)) {
                    return parseType((type.get("isSigned").getAsBoolean() ? "int" : "uint") + bits);
                }
                break;
            case "floatingpoint":
                switch (type.get("precision").getAsString()) {
                    case "HALF":
                    case "SINGLE":
                        return FLOAT;
                    case "DOUBLE":
                        return DOUBLE;
                    default:
                        break;
                }
                break;
            case "decimal":
                int precision = type.get("precision").getAsInt();
                int scale = type.get("scale").getAsInt();
                if (type.get("bitWidth").getAsInt() == 128 && precision > 0 && precision <= 38
                        && scale >= 0 && scale <= precision) {
                    return TypeFactory.createUnifiedDecimalType(precision, scale);
                }
                break;
            case "timestamp":
                return DATETIME;
            case "date":
                return "DAY".equals(type.get("unit").getAsString()) ? DATE : DATETIME;
            case "bool":
                return BOOLEAN;
            case "utf8":
            case "largeutf8":
                return VARCHAR;
            case "binary":
            case "largebinary":
                return VARBINARY;
            default:
                break;
        }
        throw new StarRocksConnectorException("Unsupported Lance field type: " + name);
    }

    /**
     * Maps safe string representations of Apache Arrow / Lance data types to StarRocks Types recursively.
     * In Phase 1, we parse a structured string description of types (e.g., "list<float32>", "struct<a:int32,b:string>")
     * into native StarRocks Type representation.
     */
    public static Type parseType(String typeStr) {
        String clean = typeStr.trim();
        String lower = clean.toLowerCase();
        if (lower.equals("boolean") || lower.equals("bool")) {
            return BOOLEAN;
        } else if (lower.equals("int8") || lower.equals("tinyint")) {
            return TINYINT;
        } else if (lower.equals("int16") || lower.equals("smallint")) {
            return SMALLINT;
        } else if (lower.equals("int32") || lower.equals("int")) {
            return INT;
        } else if (lower.equals("int64") || lower.equals("bigint")) {
            return BIGINT;
        } else if (lower.equals("uint8")) {
            return SMALLINT;
        } else if (lower.equals("uint16")) {
            return INT;
        } else if (lower.equals("uint32")) {
            return BIGINT;
        } else if (lower.equals("uint64")) {
            return TypeFactory.createUnifiedDecimalType(20, 0);
        } else if (lower.equals("float16") || lower.equals("float32") || lower.equals("float")) {
            return FLOAT;
        } else if (lower.equals("float64") || lower.equals("double")) {
            return DOUBLE;
        } else if (lower.equals("string") || lower.equals("utf8") || lower.equals("varchar") || lower.equals("large_string")
                || lower.equals("large_utf8")) {
            return VARCHAR;
        } else if (lower.equals("binary") || lower.equals("varbinary") || lower.equals("large_binary")) {
            return VARBINARY;
        } else if (lower.equals("date") || lower.equals("date32")) {
            return DATE;
        } else if (lower.equals("timestamp") || lower.startsWith("timestamp[") || lower.equals("datetime")) {
            return DATETIME;
        }

        if (lower.startsWith("list<") && lower.endsWith(">")) {
            String inner = clean.substring(5, clean.length() - 1);
            return new ArrayType(parseType(stripTopLevelFieldLabel(inner)));
        }

        if (lower.startsWith("fixed_size_list<")) {
            // "fixed_size_list<float32, 128>" and "fixed_size_list<item: float>[128]" -> Array of item type
            int closeIdx = clean.lastIndexOf(">");
            if (closeIdx > 16) {
                List<String> parts = splitTopLevel(clean.substring(16, closeIdx), ',');
                if (!parts.isEmpty()) {
                    return new ArrayType(parseType(stripTopLevelFieldLabel(parts.get(0))));
                }
            }
        }

        if (lower.startsWith("fixed_size_list:")) {
            // "fixed_size_list:float:128" -> mapped to Array of Float
            int sizeSeparatorIdx = clean.lastIndexOf(":");
            if (sizeSeparatorIdx > 16) {
                String inner = clean.substring(16, sizeSeparatorIdx).trim();
                return new ArrayType(parseType(inner));
            }
        }

        if (lower.startsWith("struct<") && lower.endsWith(">")) {
            String inner = clean.substring(7, clean.length() - 1);
            ArrayList<StructField> fields = new ArrayList<>();
            for (String field : splitTopLevel(inner, ',')) {
                parseAndAddField(field, fields);
            }
            return new StructType(fields);
        }

        return VARCHAR; // Fallback
    }

    static List<String> splitTopLevel(String value, char delimiter) {
        ArrayList<String> parts = new ArrayList<>();
        int angleBracketDepth = 0;
        int squareBracketDepth = 0;
        StringBuilder partBuilder = new StringBuilder();
        for (int i = 0; i < value.length(); i++) {
            char ch = value.charAt(i);
            if (ch == '<') {
                angleBracketDepth++;
            }
            if (ch == '>') {
                angleBracketDepth--;
            }
            if (ch == '[') {
                squareBracketDepth++;
            }
            if (ch == ']') {
                squareBracketDepth--;
            }
            if (ch == delimiter && angleBracketDepth == 0 && squareBracketDepth == 0) {
                addPart(parts, partBuilder);
            } else {
                partBuilder.append(ch);
            }
        }
        addPart(parts, partBuilder);
        return parts;
    }

    private static void addPart(ArrayList<String> parts, StringBuilder partBuilder) {
        String part = partBuilder.toString().trim();
        if (!part.isEmpty()) {
            parts.add(part);
        }
        partBuilder.setLength(0);
    }

    private static String stripTopLevelFieldLabel(String typeStr) {
        int colonIdx = topLevelDelimiterIndex(typeStr, ':');
        if (colonIdx > 0) {
            return typeStr.substring(colonIdx + 1).trim();
        }
        return typeStr.trim();
    }

    private static int topLevelDelimiterIndex(String value, char delimiter) {
        int angleBracketDepth = 0;
        int squareBracketDepth = 0;
        for (int i = 0; i < value.length(); i++) {
            char ch = value.charAt(i);
            if (ch == '<') {
                angleBracketDepth++;
            }
            if (ch == '>') {
                angleBracketDepth--;
            }
            if (ch == '[') {
                squareBracketDepth++;
            }
            if (ch == ']') {
                squareBracketDepth--;
            }
            if (ch == delimiter && angleBracketDepth == 0 && squareBracketDepth == 0) {
                return i;
            }
        }
        return -1;
    }

    private static void parseAndAddField(String fieldStr, ArrayList<StructField> fields) {
        int colonIdx = fieldStr.indexOf(':');
        if (colonIdx > 0) {
            String name = fieldStr.substring(0, colonIdx).trim();
            String typePart = fieldStr.substring(colonIdx + 1).trim();
            fields.add(new StructField(name, parseType(typePart)));
        }
    }
}
