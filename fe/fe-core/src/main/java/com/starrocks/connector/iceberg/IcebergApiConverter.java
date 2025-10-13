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

package com.starrocks.connector.iceberg;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.starrocks.catalog.ArrayType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.IcebergView;
import com.starrocks.catalog.MapType;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.StructField;
import com.starrocks.catalog.StructType;
import com.starrocks.catalog.Type;
import com.starrocks.common.DdlException;
import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
import com.starrocks.connector.ConnectorViewDefinition;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.hive.RemoteFileInputFormat;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.ListPartitionDesc;
import com.starrocks.sql.ast.OrderByElement;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.thrift.TIcebergColumnStats;
import com.starrocks.thrift.TIcebergDataFile;
import com.starrocks.thrift.TIcebergSchema;
import com.starrocks.thrift.TIcebergSchemaField;
import org.apache.commons.lang3.StringUtils;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ManifestEvaluator;
import org.apache.iceberg.expressions.Projections;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.transforms.PartitionSpecVisitor;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.view.SQLViewRepresentation;
import org.apache.iceberg.view.View;
import org.apache.iceberg.view.ViewVersion;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.starrocks.connector.ColumnTypeConverter.fromIcebergType;
import static com.starrocks.connector.ConnectorTableId.CONNECTOR_ID_GENERATOR;
import static com.starrocks.connector.iceberg.IcebergCatalogProperties.ICEBERG_CATALOG_TYPE;
import static com.starrocks.connector.iceberg.IcebergMetadata.COMPRESSION_CODEC;
import static com.starrocks.connector.iceberg.IcebergMetadata.FILE_FORMAT;
import static com.starrocks.server.CatalogMgr.ResourceMappingCatalog.toResourceName;
import static com.starrocks.sql.ast.OutFileClause.PARQUET_COMPRESSION_TYPE_MAP;
import static java.lang.String.format;
import static org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap.copyOf;
import static org.apache.iceberg.view.ViewProperties.COMMENT;

public class IcebergApiConverter {
    private static final Logger LOG = LogManager.getLogger(IcebergApiConverter.class);
    public static final String PARTITION_NULL_VALUE = "null";
    private static final Pattern ICEBERG_BUCKET_PATTERN = Pattern.compile("bucket\\[(\\d+)]");
    private static final Pattern ICEBERG_TRUNCATE_PATTERN = Pattern.compile("truncate\\[(\\d+)]");
    private static final int FAKE_FIELD_ID = -1;

    public static IcebergTable toIcebergTable(Table nativeTbl, String catalogName, String remoteDbName,
                                              String remoteTableName, String nativeCatalogType) {
        IcebergTable.Builder tableBuilder = IcebergTable.builder()
                .setId(CONNECTOR_ID_GENERATOR.getNextId().asInt())
                .setSrTableName(remoteTableName)
                .setCatalogName(catalogName)
                .setResourceName(toResourceName(catalogName, "iceberg"))
                .setCatalogDBName(remoteDbName)
                .setCatalogTableName(remoteTableName)
                .setComment(nativeTbl.properties().getOrDefault("common", ""))
                .setNativeTable(nativeTbl)
                .setFullSchema(toFullSchemas(nativeTbl.schema(), nativeTbl))
                .setIcebergProperties(toIcebergProps(
                        nativeTbl.properties() != null ? Optional.of(copyOf(nativeTbl.properties())) : Optional.empty(),
                        nativeCatalogType));

        return tableBuilder.build();
    }

    public static Schema toIcebergApiSchema(List<Column> columns) {
        List<Types.NestedField> icebergColumns = new ArrayList<>();
        for (Column column : columns) {
            if (column.getName().startsWith(FeConstants.GENERATED_PARTITION_COLUMN_PREFIX)) {
                //do not record this aux column into iceberg meta
                continue;
            }
            int index = icebergColumns.size();
            org.apache.iceberg.types.Type type = toIcebergColumnType(column.getType());
            String colComment = StringUtils.defaultIfBlank(column.getComment(), null);
            Types.NestedField field = Types.NestedField.of(
                    index, column.isAllowNull(), column.getName(), type, colComment);
            icebergColumns.add(field);
        }

        org.apache.iceberg.types.Type icebergSchema = Types.StructType.of(icebergColumns);
        AtomicInteger nextFieldId = new AtomicInteger(1);
        icebergSchema = TypeUtil.assignFreshIds(icebergSchema, nextFieldId::getAndIncrement);
        return new Schema(icebergSchema.asStructType().fields());
    }

    public static SortOrder toIcebergSortOrder(Schema schema, List<OrderByElement> orderByElements) throws DdlException {
        if (orderByElements == null) {
            return null;
        }

        Set<String> addedSortKey = new HashSet<>();
        SortOrder.Builder builder = SortOrder.builderFor(schema);
        for (OrderByElement orderByElement : orderByElements) {
            String columnName = orderByElement.castAsSlotRef();
            Preconditions.checkNotNull(columnName);
            NullOrder nullOrder = orderByElement.getNullsFirstParam() ? NullOrder.NULLS_FIRST : NullOrder.NULLS_LAST;
            if (orderByElement.getIsAsc()) {
                builder.asc(columnName, nullOrder);
            } else {
                builder.desc(columnName, nullOrder);
            }
            if (!addedSortKey.add(columnName)) {
                throw new DdlException("Duplicate sort key column " + columnName + " is not allowed.");
            }
        }

        SortOrder sortOrder = null;
        try {
            sortOrder = builder.build();
        } catch (Exception e) {
            throw new StarRocksConnectorException("Fail to build Iceberg sortOrder, msg: %s", e.getMessage());
        }
        return sortOrder;
    }

    // TODO(stephen): support iceberg transform partition like `partition by day(dt)`
    public static PartitionSpec parsePartitionFields(Schema schema, ListPartitionDesc desc) {
        PartitionSpec.Builder builder = PartitionSpec.builderFor(schema);
        if (desc == null) {
            return builder.build();
        }
        int idx = 0;
        for (String colName : desc.getPartitionColNames()) {
            if (colName.startsWith(FeConstants.GENERATED_PARTITION_COLUMN_PREFIX)) {
                Expr partExpr = desc.getPartitionExprs().get(idx++);
                if (partExpr instanceof FunctionCallExpr) {
                    String fn = ((FunctionCallExpr) partExpr).getFnName().getFunction();
                    Expr child = ((FunctionCallExpr) partExpr).getChild(0);
                    if (child instanceof SlotRef) {
                        colName = ((SlotRef) child).getColumnName();
                        if (fn.equalsIgnoreCase("year")) {
                            builder.year(colName);
                        } else if (fn.equalsIgnoreCase("month")) {
                            builder.month(colName);
                        } else if (fn.equalsIgnoreCase("day")) {
                            builder.day(colName);
                        } else if (fn.equalsIgnoreCase("hour")) {
                            builder.hour(colName);
                        } else if (fn.equalsIgnoreCase("void")) {
                            builder.alwaysNull(colName); 
                            // not supported for V2 format
                        } else if (fn.equalsIgnoreCase("identity")) {
                            builder.identity(colName); 
                        } else if (fn.equalsIgnoreCase("truncate")) {
                            IntLiteral w = (IntLiteral) ((FunctionCallExpr) partExpr).getChild(1);
                            builder.truncate(colName, (int) w.getValue());
                        } else if (fn.equalsIgnoreCase("bucket")) {
                            IntLiteral w = (IntLiteral) ((FunctionCallExpr) partExpr).getChild(1);
                            builder.bucket(colName, (int) w.getValue());
                        } else {
                            throw new SemanticException(
                                    "Unsupported partition transform %s for column %s", fn, colName);
                        }
                    } else {
                        throw new SemanticException("Unsupported partition transform %s for arguments", fn);
                    }
                } else {
                    throw new SemanticException("Unsupported partition definition");
                }
            } else if (schema.findField(colName) == null) {
                throw new SemanticException("Column %s not found in schema", colName);
            } else {
                builder.identity(colName);
            }
        }
        return builder.build();
    }

    public static org.apache.iceberg.types.Type toIcebergColumnType(Type type) {
        if (type.isScalarType()) {
            PrimitiveType primitiveType = type.getPrimitiveType();

            switch (primitiveType) {
                case BOOLEAN:
                    return Types.BooleanType.get();
                case TINYINT:
                case SMALLINT:
                case INT:
                    return Types.IntegerType.get();
                case BIGINT:
                    return Types.LongType.get();
                case FLOAT:
                    return Types.FloatType.get();
                case DOUBLE:
                    return Types.DoubleType.get();
                case DATE:
                    return Types.DateType.get();
                case DATETIME:
                    return Types.TimestampType.withoutZone();
                case VARCHAR:
                case CHAR:
                    return Types.StringType.get();
                case VARBINARY:
                    return Types.BinaryType.get();
                case DECIMAL32:
                case DECIMAL64:
                case DECIMAL128:
                    ScalarType scalarType = (ScalarType) type;
                    return Types.DecimalType.of(scalarType.getScalarPrecision(), scalarType.getScalarScale());
                case TIME:
                    return Types.TimeType.get();
                default:
                    throw new StarRocksConnectorException("Unsupported primitive column type %s", primitiveType);
            }
        }

        // handle complex type
        // it's ok to use FAKE_FIELD_ID here because TypeUtil.assignFreshIds will assign ground-truth ids later.
        if (type.isArrayType()) {
            ArrayType arrayType = (ArrayType) type;
            return Types.ListType.ofOptional(FAKE_FIELD_ID, toIcebergColumnType(arrayType.getItemType()));
        }

        if (type.isMapType()) {
            MapType mapType = (MapType) type;
            return Types.MapType.ofOptional(FAKE_FIELD_ID, FAKE_FIELD_ID, toIcebergColumnType(mapType.getKeyType()),
                    toIcebergColumnType(mapType.getValueType()));
        }

        if (type.isStructType()) {
            StructType structType = (StructType) type;
            List<Types.NestedField> fields = new ArrayList<>();
            for (StructField structField : structType.getFields()) {
                org.apache.iceberg.types.Type subtype = toIcebergColumnType(structField.getType());
                Types.NestedField field = Types.NestedField.of(
                        FAKE_FIELD_ID, true, structField.getName(), subtype, structField.getComment());
                fields.add(field);
            }
            return Types.StructType.of(fields);
        }

        throw new StarRocksConnectorException("Unsupported complex column type %s", type);
    }

    public static List<Column> toFullSchemas(Schema schema, Table table) {
        List<Column> fullSchema = toFullSchemas(schema);
        if (table instanceof BaseTable) {
            if (((BaseTable) table).operations().current().formatVersion() >= 3) {
                boolean hasRowId = fullSchema.stream().anyMatch(column -> column.getName().equals(IcebergTable.ROW_ID));
                if (!hasRowId) {
                    Column column = new Column(IcebergTable.ROW_ID, Type.BIGINT, true);
                    column.setIsHidden(true);
                    fullSchema.add(column);
                }
            }
        }
        return fullSchema;
    }

    public static List<Column> toFullSchemas(Schema schema) {
        List<Column> fullSchema = Lists.newArrayList();
        List<Types.NestedField> columns;
        try {
            columns = schema.columns();
        } catch (NullPointerException e) {
            throw new StarRocksConnectorException(e.getMessage());
        }

        for (Types.NestedField field : columns) {
            Type srType;
            try {
                srType = fromIcebergType(field.type());
            } catch (InternalError | Exception e) {
                LOG.error("Failed to convert iceberg type {}", field.type().toString(), e);
                srType = Type.UNKNOWN_TYPE;
            }
            Column column = new Column(field.name(), srType, true);
            column.setComment(field.doc());
            fullSchema.add(column);
        }
        return fullSchema;
    }

    public static Map<String, String> toIcebergProps(Optional<Map<String, String>> properties, String nativeCatalogType) {
        AtomicReference<Map<String, String>> options = new AtomicReference<>();
        properties.ifPresentOrElse(value -> {
            options.set(new HashMap<>(value));
        },
                () -> options.set(new HashMap<>()));

        options.get().put(ICEBERG_CATALOG_TYPE, nativeCatalogType);
        return options.get();
    }

    public static RemoteFileInputFormat getHdfsFileFormat(FileFormat format) {
        switch (format) {
            case ORC:
                return RemoteFileInputFormat.ORC;
            case PARQUET:
                return RemoteFileInputFormat.PARQUET;
            default:
                throw new StarRocksConnectorException("Unexpected file format: " + format);
        }
    }

    public static TIcebergSchema getTIcebergSchema(Schema schema) {
        Types.StructType rootType = schema.asStruct();
        TIcebergSchema tIcebergSchema = new TIcebergSchema();
        List<TIcebergSchemaField> fields = new ArrayList<>(rootType.fields().size());
        for (Types.NestedField nestedField : rootType.fields()) {
            fields.add(getTIcebergSchemaField(nestedField));
        }
        tIcebergSchema.setFields(fields);
        return tIcebergSchema;
    }

    private static TIcebergSchemaField getTIcebergSchemaField(Types.NestedField nestedField) {
        TIcebergSchemaField tIcebergSchemaField = new TIcebergSchemaField();
        tIcebergSchemaField.setField_id(nestedField.fieldId());
        tIcebergSchemaField.setName(nestedField.name());
        if (nestedField.type().isNestedType()) {
            List<TIcebergSchemaField> children = new ArrayList<>(nestedField.type().asNestedType().fields().size());
            for (Types.NestedField child : nestedField.type().asNestedType().fields()) {
                children.add(getTIcebergSchemaField(child));
            }
            tIcebergSchemaField.setChildren(children);
        }
        return tIcebergSchemaField;
    }

    public static void reverseBuffer(ByteBuffer buf) {
        if (buf == null || buf.remaining() <= 1) {
            return; // nothing to reverse
        }
        int lo = buf.position();
        int hi = buf.limit() - 1;

        while (lo < hi) {
            byte bLo = buf.get(lo);
            byte bHi = buf.get(hi);
            buf.put(lo, bHi);
            buf.put(hi, bLo);
            lo++;
            hi--;
        }
    }

    public static Metrics buildDataFileMetrics(TIcebergDataFile dataFile, org.apache.iceberg.Table nativeTable) {
        Map<Integer, Long> columnSizes = new HashMap<>();
        Map<Integer, Long> valueCounts = new HashMap<>();
        Map<Integer, Long> nullValueCounts = new HashMap<>();
        Map<Integer, ByteBuffer> lowerBounds = new HashMap<>();
        Map<Integer, ByteBuffer> upperBounds = new HashMap<>();
        if (dataFile.isSetColumn_stats()) {
            TIcebergColumnStats stats = dataFile.column_stats;
            if (stats.isSetColumn_sizes()) {
                columnSizes = stats.column_sizes;
            }
            if (stats.isSetValue_counts()) {
                valueCounts = stats.value_counts;
            }
            if (stats.isSetNull_value_counts()) {
                nullValueCounts = stats.null_value_counts;
            }
            if (stats.isSetLower_bounds()) {
                lowerBounds = stats.lower_bounds;
            }
            if (stats.isSetUpper_bounds()) {
                upperBounds = stats.upper_bounds;
            }
        }

        for (Types.NestedField field : nativeTable.schema().columns()) {
            if (field.type() instanceof Types.DecimalType || field.type() == Types.UUIDType.get()) {
                //change to BigEndian
                reverseBuffer(lowerBounds.get(field.fieldId()));
                reverseBuffer(upperBounds.get(field.fieldId()));
            }
        }

        return new Metrics(dataFile.record_count, columnSizes, valueCounts,
                nullValueCounts, null, lowerBounds, upperBounds);
    }

    public static Map<String, String> rebuildCreateTableProperties(Map<String, String> createProperties) {
        ImmutableMap.Builder<String, String> tableProperties = ImmutableMap.builder();
        createProperties.entrySet().forEach(tableProperties::put);
        String fileFormat = createProperties.getOrDefault(FILE_FORMAT, TableProperties.DEFAULT_FILE_FORMAT_DEFAULT);
        String compressionCodec = null;

        if ("parquet".equalsIgnoreCase(fileFormat)) {
            tableProperties.put(TableProperties.DEFAULT_FILE_FORMAT, "parquet");
            compressionCodec =
                    createProperties.getOrDefault(COMPRESSION_CODEC, TableProperties.PARQUET_COMPRESSION_DEFAULT);
            tableProperties.put(TableProperties.PARQUET_COMPRESSION, compressionCodec);
        } else if ("avro".equalsIgnoreCase(fileFormat)) {
            tableProperties.put(TableProperties.DEFAULT_FILE_FORMAT, "avro");
            compressionCodec =
                    createProperties.getOrDefault(COMPRESSION_CODEC, TableProperties.AVRO_COMPRESSION_DEFAULT);
            tableProperties.put(TableProperties.AVRO_COMPRESSION, compressionCodec);
        } else if ("orc".equalsIgnoreCase(fileFormat)) {
            tableProperties.put(TableProperties.DEFAULT_FILE_FORMAT, "orc");
            compressionCodec =
                    createProperties.getOrDefault(COMPRESSION_CODEC, TableProperties.ORC_COMPRESSION_DEFAULT);
            tableProperties.put(TableProperties.ORC_COMPRESSION, compressionCodec);
        } else if (fileFormat != null) {
            throw new IllegalArgumentException("Unsupported format in USING: " + fileFormat);
        }

        if (!PARQUET_COMPRESSION_TYPE_MAP.containsKey(compressionCodec.toLowerCase(Locale.ROOT))) {
            throw new IllegalArgumentException("Unsupported compression codec in USING: " + compressionCodec);
        }
        tableProperties.put(TableProperties.FORMAT_VERSION, "1");

        return tableProperties.build();
    }

    public static List<ManifestFile> filterManifests(List<ManifestFile> manifests,
                                               org.apache.iceberg.Table table, Expression filter) {
        Map<Integer, ManifestEvaluator> evalCache = specCache(table, filter);

        return manifests.stream()
                .filter(manifest -> manifest.hasAddedFiles() || manifest.hasExistingFiles())
                .filter(manifest -> evalCache.get(manifest.partitionSpecId()).eval(manifest))
                .collect(Collectors.toList());
    }

    private static Map<Integer, ManifestEvaluator> specCache(org.apache.iceberg.Table table, Expression filter) {
        Map<Integer, ManifestEvaluator> cache = new ConcurrentHashMap<>();

        for (Map.Entry<Integer, PartitionSpec> entry : table.specs().entrySet()) {
            Integer spedId = entry.getKey();
            PartitionSpec spec = entry.getValue();

            Expression projection = Projections.inclusive(spec, false).project(filter);
            ManifestEvaluator evaluator = ManifestEvaluator.forPartitionFilter(projection, spec, false);

            cache.put(spedId, evaluator);
        }
        return cache;
    }

    public static boolean mayHaveEqualityDeletes(Snapshot snapshot) {
        String count = snapshot.summary().get(SnapshotSummary.TOTAL_EQ_DELETES_PROP);
        return count == null || !count.equals("0");
    }

    public static IcebergView toView(String catalogName, String dbName, View icebergView) {
        SQLViewRepresentation sqlView = icebergView.sqlFor("starrocks");
        String comment = icebergView.properties().get(COMMENT);
        List<Column> columns = toFullSchemas(icebergView.schema());
        ViewVersion currentVersion = icebergView.currentVersion();
        String defaultCatalogName = currentVersion.defaultCatalog();
        String defaultDbName = currentVersion.defaultNamespace().level(0);
        String viewName = icebergView.name();
        String location = icebergView.location();
        IcebergView view = new IcebergView(CONNECTOR_ID_GENERATOR.getNextId().asInt(), catalogName, dbName, viewName,
                columns, sqlView.sql(), defaultCatalogName, defaultDbName, location);
        view.setComment(comment);
        return view;
    }

    public static List<String> toPartitionFields(PartitionSpec spec, Boolean withTransfomPrefix) {
        return spec.fields().stream()
                .map(field -> toPartitionField(spec, field, withTransfomPrefix))
                .collect(toImmutableList());
    }

    public static List<Pair<Integer, Integer>> getBucketSourceIdWithBucketNum(PartitionSpec spec) {
        return PartitionSpecVisitor.visit(spec, new BucketPartitionSpecVisitor()).stream()
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    private static class BucketPartitionSpecVisitor
            implements PartitionSpecVisitor<Pair<Integer, Integer>> {
        @Override
        public Pair<Integer, Integer> identity(int fieldId, String sourceName, int sourceId) {
            return null;
        }

        @Override
        public Pair<Integer, Integer> bucket(
                int fieldId, String sourceName, int sourceId, int numBuckets) {
            return new Pair<>(sourceId, numBuckets);
        }

        @Override
        public Pair<Integer, Integer> truncate(
                int fieldId, String sourceName, int sourceId, int width) {
            return null;
        }

        @Override
        public Pair<Integer, Integer> year(int fieldId, String sourceName, int sourceId) {
            return null;
        }

        @Override
        public Pair<Integer, Integer> month(int fieldId, String sourceName, int sourceId) {
            return null;
        }

        @Override
        public Pair<Integer, Integer> day(int fieldId, String sourceName, int sourceId) {
            return null;
        }

        @Override
        public Pair<Integer, Integer> hour(int fieldId, String sourceName, int sourceId) {
            return null;
        }

        @Override
        public Pair<Integer, Integer> alwaysNull(int fieldId, String sourceName, int sourceId) {
            return null;
        }

        @Override
        public Pair<Integer, Integer> unknown(
                int fieldId, String sourceName, int sourceId, String transform) {
            return null;
        }
    }

    public static String toPartitionField(PartitionSpec spec, PartitionField field, Boolean withTransfomPrefix) {
        String name = spec.schema().findColumnName(field.sourceId());
        String escapedName =  "`" + name + "`";
        String transform = field.transform().toString();
        String prefix = withTransfomPrefix ? FeConstants.ICEBERG_TRANSFORM_EXPRESSION_PREFIX : "";

        switch (transform) {
            case "identity":
                return escapedName;
            case "year":
            case "month":
            case "day":
            case "hour":
            case "void":
                return prefix + format("%s(%s)", transform, escapedName);
        }

        Matcher matcher = ICEBERG_BUCKET_PATTERN.matcher(transform);
        if (matcher.matches()) {
            return prefix + format("bucket(%s, %s)", escapedName, matcher.group(1));
        }

        matcher = ICEBERG_TRUNCATE_PATTERN.matcher(transform);
        if (matcher.matches()) {
            return prefix + format("truncate(%s, %s)", escapedName, matcher.group(1));
        }

        throw new StarRocksConnectorException("Unsupported partition transform: " + field);
    }

    public static List<StructField> getPartitionColumns(List<PartitionField> fields, Schema schema) {
        if (fields.isEmpty()) {
            return Lists.newArrayList();
        }

        List<StructField> partitionColumns = Lists.newArrayList();
        for (PartitionField field : fields) {
            Type srType;
            org.apache.iceberg.types.Type icebergType = field.transform().getResultType(schema.findType(field.sourceId()));
            try {
                srType = fromIcebergType(icebergType);
            } catch (InternalError | Exception e) {
                LOG.error("Failed to convert iceberg type {}", icebergType, e);
                throw new StarRocksConnectorException("Failed to convert iceberg type %s", icebergType);
            }
            StructField column = new StructField(field.name(), srType);
            partitionColumns.add(column);
        }
        return partitionColumns;
    }

    public static Namespace convertDbNameToNamespace(String dbName) {
        return Namespace.of(dbName.split("\\."));
    }

    public static Map<String, String> buildViewProperties(ConnectorViewDefinition definition, String catalogName) {
        ConnectContext connectContext = ConnectContext.get();
        if (connectContext == null) {
            throw new StarRocksConnectorException("not found connect context when building iceberg view properties");
        }

        String queryId = connectContext.getQueryId().toString();

        Map<String, String> properties = com.google.common.collect.ImmutableMap.of(
                "queryId", queryId,
                "starrocksCatalog", catalogName,
                "starrocksVersion", GlobalStateMgr.getCurrentState().getNodeMgr().getMySelf().getFeVersion());

        if (!Strings.isNullOrEmpty(definition.getComment())) {
            properties.put(IcebergMetadata.COMMENT, definition.getComment());
        }

        return properties;
    }
}
