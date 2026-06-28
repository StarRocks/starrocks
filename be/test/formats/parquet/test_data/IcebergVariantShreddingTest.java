package com.starrocks.lab;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.variants.PhysicalType;
import org.apache.iceberg.variants.ShreddedObject;
import org.apache.iceberg.variants.Variant;
import org.apache.iceberg.variants.VariantMetadata;
import org.apache.iceberg.variants.Variants;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;

/**
 * Example: Using pure Java Iceberg API (1.10.0) to create a table with a variant column and write some fake data (by writing
 * Parquet data files and then appending to the table)
 * <p>
 * Note: Different Iceberg versions may have variations in certain helper methods/class names. If you encounter compilation
 * errors regarding missing methods, let me know the specific error and I will help you adjust.
 */
public class IcebergVariantShreddingTest {

    private static final Namespace NAMESPACE = Namespace.of("zya");
    private static final String SHREDDING_TABLE = "test_shredding_variant";
    private static final String SPARSE_SHREDDING_TABLE = "test_shredding_variant_sparse";
    private static final String NO_SHREDDING_TABLE = "test_noshredding_variant";
    // Schema includes top-level physical columns (id, age, city) alongside the variant
    // column so that predicates on them trigger Phase 2 (has_filter=true) while
    // predicates on variant sub-fields are evaluated in Phase 4.  The physical columns
    // mirror fields inside the variant to enable cross-validation.
    private static final Schema SCHEMA = new Schema(
            Types.NestedField.optional(1, "data", Types.VariantType.get()),
            Types.NestedField.optional(2, "id", Types.LongType.get()),
            Types.NestedField.optional(3, "age", Types.IntegerType.get()),
            Types.NestedField.optional(4, "city", Types.StringType.get())
    );

    public static void main(String[] args) throws IOException {
        String homeDir = System.getProperty("user.home");

        // warehouse: Consistent with the path in your catalog SQL
        String warehousePath = System.getProperty("iceberg.warehouse");
        if (warehousePath == null || warehousePath.isEmpty()) {
            warehousePath = System.getenv("ICEBERG_WAREHOUSE");
        }
        if (warehousePath == null || warehousePath.isEmpty()) {
            warehousePath = homeDir + "/data/iceberg/warehouse";
        }

        // Hadoop configuration
        Configuration conf = new Configuration();
        // If needed, you can set fs.defaultFS, but file:/// usually doesn't require it:
        conf.set("fs.defaultFS", "file:///");

        // Create HadoopCatalog
        Catalog catalog = new HadoopCatalog(conf, warehousePath);

        runWithShredding(catalog, warehousePath);
        runWithSparseShredding(catalog, warehousePath);
        runWithoutShredding(catalog, warehousePath);
    }

    /**
     * Enhanced variant shredding definition: project only some keys into the Parquet typed_value group,
     * leaving others as raw variant storage. We intentionally include a mixed-type field (score) that
     * is sometimes INT and sometimes STRING to observe shredding behavior.
     */
    private static Type shreddedTypeForVariant(int fieldId, String name) {
        Type metricsTyped = org.apache.parquet.schema.Types.optionalGroup()
                .addField(shreddedField("views",
                        org.apache.parquet.schema.Types.optional(PrimitiveTypeName.BINARY).named("value"),
                        org.apache.parquet.schema.Types.optional(PrimitiveTypeName.INT32).named("typed_value")))
                .addField(shreddedField("ratio",
                        org.apache.parquet.schema.Types.optional(PrimitiveTypeName.BINARY).named("value"),
                        org.apache.parquet.schema.Types.optional(PrimitiveTypeName.DOUBLE).named("typed_value")))
                .named("typed_value");

        Type profileTyped =
                org.apache.parquet.schema.Types.optionalGroup()
                        .addField(shreddedField("salary",
                                org.apache.parquet.schema.Types.optional(PrimitiveTypeName.BINARY).named("value"),
                                org.apache.parquet.schema.Types.optional(PrimitiveTypeName.DOUBLE)
                                        .named("typed_value")))
                        // price: DECIMAL(5,2) → DECIMAL4 (INT32). Rows 0-4: 10.50,11.00,11.50,12.00,12.50
                        // Out-of-range: price > 100 → 0 rows; price < 0 → 0 rows; >= 11.50 → 3 rows
                        .addField(shreddedField("price",
                                org.apache.parquet.schema.Types.optional(PrimitiveTypeName.BINARY).named("value"),
                                org.apache.parquet.schema.Types.optional(PrimitiveTypeName.INT32)
                                        .as(OriginalType.DECIMAL)
                                        .precision(5)
                                        .scale(2)
                                        .named("typed_value")))
                        // amount: DECIMAL(16,2) → DECIMAL8 (INT64). Rows 0-4: 10000000000.10, 20000000000.20, ...
                        // Out-of-range: amount > 99999999999999.99 → 0 rows; >= 30000000000.30 → 3 rows
                        .addField(shreddedField("amount",
                                org.apache.parquet.schema.Types.optional(PrimitiveTypeName.BINARY).named("value"),
                                org.apache.parquet.schema.Types.optional(PrimitiveTypeName.INT64)
                                        .as(OriginalType.DECIMAL)
                                        .precision(16)
                                        .scale(2)
                                        .named("typed_value")))
                        // balance: DECIMAL(22,4) → DECIMAL16 (FIXED_LEN_BYTE_ARRAY(16)).
                        // Rows 0-4: 1000000000000000.0001, 2000000000000000.0002, ...
                        // Out-of-range: balance > 9999999999999999.9999 → 0 rows; >= 3000000000000000.0003 → 3 rows
                        .addField(shreddedField("balance",
                                org.apache.parquet.schema.Types.optional(PrimitiveTypeName.BINARY).named("value"),
                                org.apache.parquet.schema.Types.optional(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
                                        .length(10)
                                        .as(OriginalType.DECIMAL)
                                        .precision(22)
                                        .scale(4)
                                        .named("typed_value")))
                        .addField(shreddedField("department",
                                org.apache.parquet.schema.Types.optional(PrimitiveTypeName.BINARY).named("value"),
                                org.apache.parquet.schema.Types.optional(PrimitiveTypeName.BINARY)
                                        .named("typed_value")))
                        .addField(shreddedField("rank",
                                org.apache.parquet.schema.Types.optional(PrimitiveTypeName.BINARY).named("value"),
                                org.apache.parquet.schema.Types.optional(PrimitiveTypeName.INT32).named("typed_value")))
                        .addField(shreddedField("metrics",
                                org.apache.parquet.schema.Types.optional(PrimitiveTypeName.BINARY).named("value"),
                                metricsTyped))
                        .named("typed_value");
        // Intentionally omit one field from typed_value so array element.value keeps
        // a non-empty base object for overlay reconstruction tests.
        Type eventObjectTyped = org.apache.parquet.schema.Types.optionalGroup()
                .addField(shreddedField("type",
                        org.apache.parquet.schema.Types.optional(PrimitiveTypeName.BINARY).named("value"),
                        org.apache.parquet.schema.Types.optional(PrimitiveTypeName.BINARY).named("typed_value")))
                .addField(shreddedField("count",
                        org.apache.parquet.schema.Types.optional(PrimitiveTypeName.BINARY).named("value"),
                        org.apache.parquet.schema.Types.optional(PrimitiveTypeName.INT32).named("typed_value")))
                .named("typed_value");

        // groups: array of objects where each element contains name (STRING) and scores (nested INT32 array).
        // This exercises the array-of-arrays shredding code path: _collect_overlays_for_array_element
        // must recurse into ARRAY-kinded child nodes to reconstruct scores within each group element.
        Type scoresTypedValue = shreddedScalarArrayTypedValue(
                org.apache.parquet.schema.Types.optional(PrimitiveTypeName.BINARY).named("value"),
                org.apache.parquet.schema.Types.optional(PrimitiveTypeName.INT32).named("typed_value"));
        // Same idea for groups: keep one unshredded element field so list.element.value
        // is materialized in the Parquet file.
        Type groupElementTyped = org.apache.parquet.schema.Types.optionalGroup()
                .addField(shreddedField("name",
                        org.apache.parquet.schema.Types.optional(PrimitiveTypeName.BINARY).named("value"),
                        org.apache.parquet.schema.Types.optional(PrimitiveTypeName.BINARY).named("typed_value")))
                .addField(shreddedField("scores",
                        org.apache.parquet.schema.Types.optional(PrimitiveTypeName.BINARY).named("value"),
                        scoresTypedValue))
                .named("typed_value");

        return org.apache.parquet.schema.Types
                .optionalGroup()
                // typed_value holds shredded fields; must itself be a group
                .id(fieldId)
                .addField(shreddedField("id",
                        org.apache.parquet.schema.Types.optional(PrimitiveTypeName.BINARY).named("value"),
                        org.apache.parquet.schema.Types.optional(PrimitiveTypeName.INT64).named("typed_value")))
                .addField(shreddedField("created_at",
                        org.apache.parquet.schema.Types.optional(PrimitiveTypeName.BINARY).named("value"),
                        org.apache.parquet.schema.Types.optional(PrimitiveTypeName.INT32)
                                .as(OriginalType.DATE)
                                .named("typed_value")))
                .addField(shreddedField("updated_at",
                        org.apache.parquet.schema.Types.optional(PrimitiveTypeName.BINARY).named("value"),
                        org.apache.parquet.schema.Types.optional(PrimitiveTypeName.INT64)
                                .as(OriginalType.TIMESTAMP_MICROS)
                                .named("typed_value")))
                .addField(shreddedField("age",
                        org.apache.parquet.schema.Types.optional(PrimitiveTypeName.BINARY).named("value"),
                        org.apache.parquet.schema.Types.optional(PrimitiveTypeName.INT32).named("typed_value")))
                // city/status/name/email are intentionally omitted to keep them only in raw value.
                .addField(shreddedField("score",
                        org.apache.parquet.schema.Types.optional(PrimitiveTypeName.BINARY).named("value"),
                        org.apache.parquet.schema.Types.optional(PrimitiveTypeName.INT32).named("typed_value")))
                .addField(shreddedField("profile",
                        org.apache.parquet.schema.Types.optional(PrimitiveTypeName.BINARY).named("value"),
                        profileTyped))
                .addField(shreddedField("events",
                        org.apache.parquet.schema.Types.optional(PrimitiveTypeName.BINARY).named("value"),
                        shreddedArrayTypedValue(
                                org.apache.parquet.schema.Types.optional(PrimitiveTypeName.BINARY).named("value"),
                                eventObjectTyped)))
                // Scalar array shredding: array of primitive integers (no nested shredded paths)
                .addField(shreddedScalarArrayField("numbers",
                        org.apache.parquet.schema.Types.optional(PrimitiveTypeName.BINARY).named("value"),
                        org.apache.parquet.schema.Types.optional(PrimitiveTypeName.INT32).named("typed_value")))
                // Array-of-objects with nested scalar array: exercises array-of-arrays shredding
                .addField(shreddedField("groups",
                        org.apache.parquet.schema.Types.optional(PrimitiveTypeName.BINARY).named("value"),
                        shreddedArrayTypedValue(
                                org.apache.parquet.schema.Types.optional(PrimitiveTypeName.BINARY).named("value"),
                                groupElementTyped)))
                .named("typed_value");
    }

    /**
     * Build a shredded field group:
     * optional group <fieldName> {
     * optional binary value;        // raw variant storage
     * optional <type> typed_value;  // strongly typed projection
     * }
     */
    private static Type shreddedField(String fieldName, Type valueField, Type typedValueField) {
        return org.apache.parquet.schema.Types.optionalGroup()
                .addField(valueField)
                .addField(typedValueField)
                .named(fieldName);
    }

    /**
     * Build just the typed_value LIST group for a scalar array nested inside another object's typed_value.
     * Analogous to shreddedArrayTypedValue but for scalar element types.
     * Use when a scalar array (e.g. scores: [int, int, ...]) is shredded as a sub-field of an
     * object that is itself an array element, exercising the array-of-arrays code path.
     */
    private static Type shreddedScalarArrayTypedValue(Type elementValueField, Type elementTypedValueField) {
        Type elementInList = org.apache.parquet.schema.Types.optionalGroup()
                .addField(elementValueField)
                .addField(elementTypedValueField)
                .named("element");
        Type listGroup = org.apache.parquet.schema.Types.repeatedGroup()
                .addField(elementInList)
                .named("list");
        return org.apache.parquet.schema.Types.optionalGroup()
                .as(OriginalType.LIST)
                .addField(listGroup)
                .named("typed_value");
    }

    private static Type shreddedArrayTypedValue(Type elementValueField, Type elementTypedValueField) {
        Type elementGroup = org.apache.parquet.schema.Types.requiredGroup()
                .addField(elementValueField)
                .addField(elementTypedValueField)
                .named("element");
        Type listGroup = org.apache.parquet.schema.Types.repeatedGroup()
                .addField(elementGroup)
                .named("list");
        return org.apache.parquet.schema.Types.optionalGroup()
                .as(OriginalType.LIST)
                .addField(listGroup)
                .named("typed_value");
    }

    /**
     * Build a shredded field for scalar arrays (e.g., [1, 2, 3]).
     * Unlike object arrays, scalar arrays have no nested shredded paths,
     * so the typed_value directly contains the array elements without children.
     * <p>
     * optional group <fieldName> {
     * optional binary value;             // raw variant storage
     * optional group typed_value {       // strongly typed array projection
     * repeated group list {
     * required group element {
     * optional <type> value;        // scalar element value
     * }
     * }
     * }
     * }
     */
    private static Type shreddedScalarArrayField(String fieldName, Type valueField, Type elementTypedValueField) {
        Type elementValueInList = org.apache.parquet.schema.Types.optionalGroup()
                .addField(valueField)
                .addField(elementTypedValueField)
                .named("element");
        Type listGroup = org.apache.parquet.schema.Types.repeatedGroup()
                .addField(elementValueInList)
                .named("list");
        Type typedValue = org.apache.parquet.schema.Types.optionalGroup()
                .as(OriginalType.LIST)
                .addField(listGroup)
                .named("typed_value");
        return org.apache.parquet.schema.Types.optionalGroup()
                .addField(valueField)
                .addField(typedValue)
                .named(fieldName);
    }

    private static void runWithShredding(Catalog catalog, String warehousePath) throws IOException {
        TableIdentifier identifier = TableIdentifier.of(NAMESPACE, SHREDDING_TABLE);
        dropTableIfExists(catalog, identifier, warehousePath);

        PartitionSpec spec = PartitionSpec.unpartitioned();
        Map<String, String> tableProperties = new HashMap<>();
        tableProperties.put("format-version", "3");
        tableProperties.put("iceberg.enableVariantShredding", "true");

        Table table = catalog.createTable(identifier, SCHEMA, spec, tableProperties);
        System.out.println("Created table with shredding: " + identifier);

        List<Record> records = buildRecords(true);
        long recordCount = writeDataFile(table, spec, records, true, "data_shredding.parquet");
        System.out.println("Wrote " + recordCount + " records with variant shredding.");

        Table refreshed = catalog.loadTable(identifier);
        System.out.println("Table location: " + refreshed.location());
        System.out.println("Current snapshot id: " + refreshed.currentSnapshot().snapshotId());
        try {
            readAndPrint(refreshed, "shredding");
        } catch (RuntimeException re) {
            System.err.println("Read failed for shredded table (mixed-type fields may be the cause): " + re.getMessage());
            re.printStackTrace(System.err);
        }
    }

    private static void runWithSparseShredding(Catalog catalog, String warehousePath) throws IOException {
        TableIdentifier identifier = TableIdentifier.of(NAMESPACE, SPARSE_SHREDDING_TABLE);
        dropTableIfExists(catalog, identifier, warehousePath);

        PartitionSpec spec = PartitionSpec.unpartitioned();
        Map<String, String> tableProperties = new HashMap<>();
        tableProperties.put("format-version", "3");
        tableProperties.put("iceberg.enableVariantShredding", "true");

        Table table = catalog.createTable(identifier, SCHEMA, spec, tableProperties);
        System.out.println("Created table with sparse shredding: " + identifier);

        List<Record> records = buildSparseRecords(true);
        long recordCount = writeDataFile(table, spec, records, true, "data_shredding_sparse.parquet");
        System.out.println("Wrote " + recordCount + " records with sparse variant shredding.");

        Table refreshed = catalog.loadTable(identifier);
        System.out.println("Table location: " + refreshed.location());
        System.out.println("Current snapshot id: " + refreshed.currentSnapshot().snapshotId());
        try {
            readAndPrint(refreshed, "sparse shredding");
        } catch (RuntimeException re) {
            System.err.println("Read failed for sparse shredded table: " + re.getMessage());
            re.printStackTrace(System.err);
        }
    }

    private static void runWithoutShredding(Catalog catalog, String warehousePath) throws IOException {
        TableIdentifier identifier = TableIdentifier.of(NAMESPACE, NO_SHREDDING_TABLE);
        dropTableIfExists(catalog, identifier, warehousePath);

        PartitionSpec spec = PartitionSpec.unpartitioned();
        Map<String, String> tableProperties = new HashMap<>();
        tableProperties.put("format-version", "3");

        Table table = catalog.createTable(identifier, SCHEMA, spec, tableProperties);
        System.out.println("Created table without shredding: " + identifier);

        List<Record> records = buildRecords(true);
        long recordCount = writeDataFile(table, spec, records, false, "data_noshredding.parquet");
        System.out.println("Wrote " + recordCount + " records without variant shredding.");

        Table refreshed = catalog.loadTable(identifier);
        System.out.println("Table location: " + refreshed.location());
        System.out.println("Current snapshot id: " + refreshed.currentSnapshot().snapshotId());
        readAndPrint(refreshed, "no shredding");
    }

    private static List<Record> buildRecords(boolean multiTypes) {
        List<Record> records = new ArrayList<>();
        VariantMetadata metadata = Variants.metadata("id", "age", "city", "score", "status", "name", "email", "profile",
                "salary", "price", "amount", "balance", "department", "rank", "metrics", "views", "ratio", "events",
                "type", "count", "numbers", "groups", "scores", "detail", "note", "created_at", "updated_at");
        // created_at: 2024-01-01 to 2024-01-05 (DATE, shredded INT32+DATE)
        //   2024-01-01 = 19723 days since Unix epoch (1970-01-01)
        // updated_at: 2024-01-01 00:00 to 04:00 UTC (TIMESTAMP, shredded INT64+TIMESTAMP_MICROS)
        //   2024-01-01 00:00:00 UTC = 1703980800000000 μs since epoch
        // Out-of-range tests: created_at > '2025-01-01' → 0 rows; created_at < '2023-01-01' → 0 rows
        // In-range test: created_at >= '2024-01-03' → 3 rows (rows 2,3,4)
        final int BASE_DATE_DAYS = 19723; // 2024-01-01 in days since 1970-01-01
        final long BASE_TS_MICROS = 1704067200_000000L; // 2024-01-01 00:00:00 UTC in μs since epoch
        for (int i = 0; i < 5; i++) {
            Record rec = GenericRecord.create(SCHEMA.asStruct());
            ShreddedObject obj = Variants.object(metadata);
            obj.put("id", Variants.of(1000L + i));
            obj.put("age", Variants.of(20 + i));
            obj.put("city", Variants.of("city_" + i));
            // Mixed-type field: INT on even rows, STRING on odd rows
            if (i % 2 == 0 || !multiTypes) {
                obj.put("score", Variants.of(80 + i));
            } else {
                obj.put("score", Variants.of("S" + (80 + i)));
            }
            obj.put("status", Variants.of(i % 2 == 0 ? "active" : "inactive"));
            obj.put("name", Variants.of("name_" + i));  // not shredded
            org.apache.iceberg.variants.ValueArray events = Variants.array();
            ShreddedObject event0 = Variants.object(metadata);
            event0.put("type", Variants.of("view"));
            event0.put("count", Variants.of(i + 1));
            event0.put("detail", Variants.of("detail_view_" + i));
            events.add(event0);
            ShreddedObject event1 = Variants.object(metadata);
            event1.put("type", Variants.of("click"));
            event1.put("count", Variants.of((i + 1) * 2));
            event1.put("detail", Variants.of("detail_click_" + i));
            events.add(event1);
            obj.put("events", events);
            // Add scalar array for testing fully-typed array reconstruction
            org.apache.iceberg.variants.ValueArray numbers = Variants.array();
            numbers.add(Variants.of(1 + i));
            numbers.add(Variants.of(2 + i));
            numbers.add(Variants.of(3 + i));
            obj.put("numbers", numbers);
            ShreddedObject profile = Variants.object(metadata);
            profile.put("salary", Variants.of(50000.0 + i * 1000));
            // price: DECIMAL(5,2) → DECIMAL4 (INT32), precision=4, row i: 10.50 + i*0.50
            profile.put("price",
                    Variants.of(new BigDecimal("10.50").add(new BigDecimal("0.50").multiply(BigDecimal.valueOf(i)))));
            // amount: DECIMAL(16,2) → DECIMAL8 (INT64), precision=14, row i: (i+1)*10000000000.10
            profile.put("amount", Variants.of(new BigDecimal("10000000000.10").multiply(BigDecimal.valueOf(i + 1))));
            // balance: DECIMAL(22,4) → DECIMAL16 (FLBA 16 bytes), row i: (i+1)*1000000000000000.0001
            profile.put("balance",
                    Variants.of(new BigDecimal("1000000000000000.0001").multiply(BigDecimal.valueOf(i + 1))));
            profile.put("department", Variants.of("dept_" + i));
            if (i % 2 == 0 || !multiTypes) {
                profile.put("rank", Variants.of(i + 1));
            } else {
                profile.put("rank", Variants.of("L" + (i + 1)));
            }
            ShreddedObject metrics = Variants.object(metadata);
            metrics.put("views", Variants.of(100 + i * 10));
            metrics.put("ratio", Variants.of(0.1 + i * 0.05));
            profile.put("metrics", metrics);
            obj.put("profile", profile);  // merged details + profile
            // groups: array of 2 group objects, each with name (STRING) and scores (nested INT32 array).
            // Exercises the array-of-arrays shredding code path in the reader.
            org.apache.iceberg.variants.ValueArray groups = Variants.array();
            for (int g = 0; g < 2; g++) {
                ShreddedObject groupObj = Variants.object(metadata);
                groupObj.put("name", Variants.of("group_" + g));
                groupObj.put("note", Variants.of("note_" + i + "_" + g));
                org.apache.iceberg.variants.ValueArray scores = Variants.array();
                scores.add(Variants.of(10 + i + g * 30));
                scores.add(Variants.of(20 + i + g * 30));
                scores.add(Variants.of(30 + i + g * 30));
                groupObj.put("scores", scores);
                groups.add(groupObj);
            }
            obj.put("groups", groups);
            obj.put("email", Variants.of("user" + i + "@example.com"));
            // DATE: 2024-01-01 + i days → shredded into INT32+DATE typed_value
            obj.put("created_at", Variants.of(PhysicalType.DATE, BASE_DATE_DAYS + i));
            // TIMESTAMP: 2024-01-01 T(i*1h) UTC → shredded into INT64+TIMESTAMP_MICROS typed_value
            obj.put("updated_at", Variants.of(PhysicalType.TIMESTAMPTZ, BASE_TS_MICROS + (long) i * 3600 * 1_000_000L));
            Variant value = Variant.of(metadata, obj);
            rec.setField("data", value);
            // Mirror key fields as top-level physical columns for Phase-2 filter testing
            // and cross-validation against the variant sub-fields.
            rec.setField("id", 1000L + i);
            rec.setField("age", 20 + i);
            rec.setField("city", "city_" + i);
            records.add(rec);
        }
        return records;
    }

    private static List<Record> buildSparseRecords(boolean multiTypes) {
        List<Record> records = new ArrayList<>();
        VariantMetadata metadata = Variants.metadata("id", "age", "city", "score", "status", "name", "email", "profile",
                "salary", "price", "amount", "balance", "department", "rank", "metrics", "views", "ratio", "events",
                "type", "count", "numbers", "groups", "scores", "detail", "note", "created_at", "updated_at");
        final int BASE_DATE_DAYS = 19723;
        final long BASE_TS_MICROS = 1704067200_000000L;
        for (int i = 0; i < 5; i++) {
            Record rec = GenericRecord.create(SCHEMA.asStruct());
            if (i == 4) {
                rec.setField("data", null);
                rec.setField("id", null);
                rec.setField("age", null);
                rec.setField("city", null);
                records.add(rec);
                continue;
            }

            ShreddedObject obj = Variants.object(metadata);
            if (i != 3) {
                obj.put("id", Variants.of(1000L + i));
            }
            obj.put("age", Variants.of(20 + i));
            obj.put("city", Variants.of("city_" + i));
            if (i % 2 == 0 || !multiTypes) {
                obj.put("score", Variants.of(80 + i));
            } else {
                obj.put("score", Variants.of("S" + (80 + i)));
            }
            obj.put("status", Variants.of(i % 2 == 0 ? "active" : "inactive"));
            obj.put("name", Variants.of("name_" + i));

            org.apache.iceberg.variants.ValueArray events = Variants.array();
            ShreddedObject event0 = Variants.object(metadata);
            event0.put("type", Variants.of("view"));
            event0.put("count", Variants.of(i + 1));
            event0.put("detail", Variants.of("detail_view_" + i));
            events.add(event0);
            ShreddedObject event1 = Variants.object(metadata);
            event1.put("type", Variants.of("click"));
            event1.put("count", Variants.of((i + 1) * 2));
            event1.put("detail", Variants.of("detail_click_" + i));
            events.add(event1);
            obj.put("events", events);

            org.apache.iceberg.variants.ValueArray numbers = Variants.array();
            numbers.add(Variants.of(1 + i));
            numbers.add(Variants.of(2 + i));
            numbers.add(Variants.of(3 + i));
            obj.put("numbers", numbers);

            ShreddedObject profile = Variants.object(metadata);
            if (i != 2) {
                profile.put("salary", Variants.of(50000.0 + i * 1000));
            }
            profile.put("price",
                    Variants.of(new BigDecimal("10.50").add(new BigDecimal("0.50").multiply(BigDecimal.valueOf(i)))));
            profile.put("amount", Variants.of(new BigDecimal("10000000000.10").multiply(BigDecimal.valueOf(i + 1))));
            profile.put("balance",
                    Variants.of(new BigDecimal("1000000000000000.0001").multiply(BigDecimal.valueOf(i + 1))));
            profile.put("department", Variants.of("dept_" + i));
            if (i % 2 == 0 || !multiTypes) {
                profile.put("rank", Variants.of(i + 1));
            } else {
                profile.put("rank", Variants.of("L" + (i + 1)));
            }
            ShreddedObject metrics = Variants.object(metadata);
            metrics.put("views", Variants.of(100 + i * 10));
            metrics.put("ratio", Variants.of(0.1 + i * 0.05));
            profile.put("metrics", metrics);
            obj.put("profile", profile);

            org.apache.iceberg.variants.ValueArray groups = Variants.array();
            for (int g = 0; g < 2; g++) {
                ShreddedObject groupObj = Variants.object(metadata);
                groupObj.put("name", Variants.of("group_" + g));
                groupObj.put("note", Variants.of("note_" + i + "_" + g));
                org.apache.iceberg.variants.ValueArray scores = Variants.array();
                scores.add(Variants.of(10 + i + g * 30));
                scores.add(Variants.of(20 + i + g * 30));
                scores.add(Variants.of(30 + i + g * 30));
                groupObj.put("scores", scores);
                groups.add(groupObj);
            }
            obj.put("groups", groups);
            obj.put("email", Variants.of("user" + i + "@example.com"));
            obj.put("created_at", Variants.of(PhysicalType.DATE, BASE_DATE_DAYS + i));
            obj.put("updated_at", Variants.of(PhysicalType.TIMESTAMPTZ, BASE_TS_MICROS + (long) i * 3600 * 1_000_000L));

            Variant value = Variant.of(metadata, obj);
            rec.setField("data", value);
            rec.setField("id", i == 3 ? null : 1000L + i);
            rec.setField("age", 20 + i);
            rec.setField("city", "city_" + i);
            records.add(rec);
        }
        return records;
    }

    private static long writeDataFile(Table table,
                                      PartitionSpec spec,
                                      List<Record> records,
                                      boolean enableShredding,
                                      String outputFileName) throws IOException {
        File dataDir = new File(table.location() + "/data");
        if (!dataDir.exists()) {
            dataDir.mkdirs();
        }
        String outPath = dataDir + "/" + outputFileName;

        OutputFile out = org.apache.iceberg.Files.localOutput(new File(outPath));
        long recordCount = 0L;

        Parquet.WriteBuilder builder = Parquet.write(out)
                .schema(SCHEMA)
                .forTable(table)
                .createWriterFunc(GenericParquetWriter::create);

        if (enableShredding) {
            builder = builder.variantShreddingFunc(IcebergVariantShreddingTest::shreddedTypeForVariant);
        }

        try (FileAppender<Record> appender = builder.build()) {
            for (Record r : records) {
                appender.add(r);
                recordCount++;
            }
        }

        File file = new File(outPath);
        long fileSize = file.length();

        DataFile dataFile = DataFiles.builder(spec)
                .withPath(outPath)
                .withFileSizeInBytes(fileSize)
                .withRecordCount(recordCount)
                .build();

        table.newAppend()
                .appendFile(dataFile)
                .commit();

        return recordCount;
    }

    private static void readAndPrint(Table table, String label) {
        System.out.println("Reading rows (" + label + "):");
        try (CloseableIterable<Record> rows = IcebergGenerics.read(table).build()) {
            for (Record row : rows) {
                Variant variant = (Variant) row.getField("data");
                System.out.println(variant == null ? "null" : Variant.toString(variant));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void dropTableIfExists(Catalog catalog, TableIdentifier id, String warehousePath) throws IOException {
        try {
            catalog.dropTable(id, true);
        } catch (Exception e) {
            System.err.println("dropTable warning: " + e.getMessage());
        }

        Path tablePath = Paths.get(warehousePath, id.namespace().levels()).resolve(id.name());
        if (Files.exists(tablePath)) {
            Files.walk(tablePath)
                    .sorted(Comparator.reverseOrder())
                    .forEach(p -> {
                        try {
                            Files.deleteIfExists(p);
                        } catch (IOException ioe) {
                            System.err.println("Failed to delete " + p + ": " + ioe.getMessage());
                        }
                    });
        }
    }
}
