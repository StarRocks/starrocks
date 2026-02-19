package com.starrocks.lab;

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
import org.apache.iceberg.variants.ShreddedObject;
import org.apache.iceberg.variants.Variant;
import org.apache.iceberg.variants.VariantMetadata;
import org.apache.iceberg.variants.Variants;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
    private static final String NO_SHREDDING_TABLE = "test_noshredding_variant";
    private static final Schema SCHEMA = new Schema(
            Types.NestedField.optional(1, "data", Types.VariantType.get())
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

        Type profileTyped = org.apache.parquet.schema.Types.optionalGroup()
                .addField(shreddedField("salary",
                        org.apache.parquet.schema.Types.optional(PrimitiveTypeName.BINARY).named("value"),
                        org.apache.parquet.schema.Types.optional(PrimitiveTypeName.DOUBLE).named("typed_value")))
                .addField(shreddedField("department",
                        org.apache.parquet.schema.Types.optional(PrimitiveTypeName.BINARY).named("value"),
                        org.apache.parquet.schema.Types.optional(PrimitiveTypeName.BINARY).named("typed_value")))
                .addField(shreddedField("rank",
                        org.apache.parquet.schema.Types.optional(PrimitiveTypeName.BINARY).named("value"),
                        org.apache.parquet.schema.Types.optional(PrimitiveTypeName.INT32).named("typed_value")))
                .addField(shreddedField("metrics",
                        org.apache.parquet.schema.Types.optional(PrimitiveTypeName.BINARY).named("value"),
                        metricsTyped))
                .named("typed_value");
        Type eventObjectTyped = org.apache.parquet.schema.Types.optionalGroup()
                .addField(shreddedField("type",
                        org.apache.parquet.schema.Types.optional(PrimitiveTypeName.BINARY).named("value"),
                        org.apache.parquet.schema.Types.optional(PrimitiveTypeName.BINARY).named("typed_value")))
                .addField(shreddedField("count",
                        org.apache.parquet.schema.Types.optional(PrimitiveTypeName.BINARY).named("value"),
                        org.apache.parquet.schema.Types.optional(PrimitiveTypeName.INT32).named("typed_value")))
                .named("typed_value");

        return org.apache.parquet.schema.Types.optionalGroup()
                // typed_value holds shredded fields; must itself be a group
                .id(fieldId)
                .addField(shreddedField("id",
                        org.apache.parquet.schema.Types.optional(PrimitiveTypeName.BINARY).named("value"),
                        org.apache.parquet.schema.Types.optional(PrimitiveTypeName.INT64).named("typed_value")))
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
        long recordCount = writeDataFile(table, spec, records, true);
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

    private static void runWithoutShredding(Catalog catalog, String warehousePath) throws IOException {
        TableIdentifier identifier = TableIdentifier.of(NAMESPACE, NO_SHREDDING_TABLE);
        dropTableIfExists(catalog, identifier, warehousePath);

        PartitionSpec spec = PartitionSpec.unpartitioned();
        Map<String, String> tableProperties = new HashMap<>();
        tableProperties.put("format-version", "3");

        Table table = catalog.createTable(identifier, SCHEMA, spec, tableProperties);
        System.out.println("Created table without shredding: " + identifier);

        List<Record> records = buildRecords(true);
        long recordCount = writeDataFile(table, spec, records, false);
        System.out.println("Wrote " + recordCount + " records without variant shredding.");

        Table refreshed = catalog.loadTable(identifier);
        System.out.println("Table location: " + refreshed.location());
        System.out.println("Current snapshot id: " + refreshed.currentSnapshot().snapshotId());
        readAndPrint(refreshed, "no shredding");
    }

    private static List<Record> buildRecords(boolean multiTypes) {
        List<Record> records = new ArrayList<>();
        VariantMetadata metadata = Variants.metadata(
                "id", "age", "city", "score", "status", "name", "email", "profile",
                "salary", "department", "rank", "metrics", "views", "ratio",
                "events", "type", "count");
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
            events.add(event0);
            ShreddedObject event1 = Variants.object(metadata);
            event1.put("type", Variants.of("click"));
            event1.put("count", Variants.of((i + 1) * 2));
            events.add(event1);
            obj.put("events", events);
            ShreddedObject profile = Variants.object(metadata);
            profile.put("salary", Variants.of(50000.0 + i * 1000));
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
            obj.put("email", Variants.of("user" + i + "@example.com"));
            Variant value = Variant.of(metadata, obj);
            rec.setField("data", value);
            records.add(rec);
        }
        return records;
    }

    private static long writeDataFile(Table table,
                                      PartitionSpec spec,
                                      List<Record> records,
                                      boolean enableShredding) throws IOException {
        File dataDir = new File(table.location() + "/data");
        if (!dataDir.exists()) {
            dataDir.mkdirs();
        }
        String outPath =
                dataDir + "/data-" + (enableShredding ? "shred" : "noshred") + ".parquet";

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
                System.out.println(Variant.toString(variant));
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
