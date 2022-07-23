// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.hudi.reader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.hadoop.realtime.HoodieRealtimeFileSplit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.stream.Collectors.toList;

public class HudiSliceRecordReader {

    private final String basePath;
    private final String hiveColumnNames;
    private final String[] hiveColumnTypes;
    private final String[] requiredFields;
    private final String instantTime;
    private final String[] deltaFilePaths;
    private final String dataFilePath;
    private final long dataFileLenth;
    private final String serde;
    private final String inputFormat;
    private final int fetchSize;
    private RecordReader<NullWritable, ArrayWritable> reader;
    private boolean hasNext;
    private StructObjectInspector rowInspector;
    private ObjectInspector[] fieldInspectors;
    private StructField[] structFields;
    private Deserializer deserializer;
    private String[] requiredTypes;
    private OffHeapTable offHeapTable;

    public HudiSliceRecordReader(String basePath,
                                 String hiveColumnNames,
                                 String hiveColumnTypes,
                                 String[] requiredFields,
                                 String instantTime,
                                 String[] deltaFilePaths,
                                 String dataFilePath,
                                 long dataFileLenth,
                                 String serde,
                                 String inputFormat,
                                 int fetchSize) {
        this.basePath = basePath;
        this.hiveColumnNames = hiveColumnNames;
        this.hiveColumnTypes = hiveColumnTypes.split(":");
        this.requiredFields = requiredFields;
        this.instantTime = instantTime;
        this.deltaFilePaths = deltaFilePaths;
        this.dataFilePath = dataFilePath;
        this.dataFileLenth = dataFileLenth;
        this.serde = serde;
        this.inputFormat = inputFormat;
        this.fetchSize = fetchSize;
        this.hasNext = true;
        this.fieldInspectors = new ObjectInspector[requiredFields.length];
        this.structFields = new StructField[requiredFields.length];
        this.requiredTypes = new String[requiredFields.length];
    }

    public void open() throws IOException {
        try {
            Properties properties = new Properties();
            Configuration conf = new Configuration();
            conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
            conf.setBoolean("dfs.client.use.legacy.blockreader", false);
            JobConf jobConf = new JobConf(conf);
            jobConf.setBoolean("hive.io.file.read.all.columns", false);
            String[] hiveColumnNames = this.hiveColumnNames.split(",");
            Map<String, Integer> hiveColumnNameToIndex = new HashMap<>();
            Map<String, String> hiveColumnNameToType = new HashMap<>();
            for (int i = 0; i < hiveColumnNames.length; i++) {
                hiveColumnNameToIndex.put(hiveColumnNames[i], i);
                hiveColumnNameToType.put(hiveColumnNames[i], hiveColumnTypes[i]);
            }
            StringBuilder columnIdBuilder = new StringBuilder();
            boolean isFirst = true;
            for (int i = 0; i < requiredFields.length; i++) {
                if (!isFirst) {
                    columnIdBuilder.append(",");
                }
                columnIdBuilder.append(hiveColumnNameToIndex.get(requiredFields[i]));
                requiredTypes[i] = hiveColumnNameToType.get(requiredFields[i]);
                isFirst = false;
            }

            properties.setProperty("hive.io.file.readcolumn.ids", columnIdBuilder.toString());
            properties.setProperty("hive.io.file.readcolumn.names", String.join(",", this.requiredFields));
            properties.setProperty("columns", this.hiveColumnNames);
            properties.setProperty("columns.types", String.join(",", this.hiveColumnTypes));
            properties.setProperty("serialization.lib", this.serde);
            properties.stringPropertyNames().forEach(name -> jobConf.set(name, properties.getProperty(name)));

            // dataFileLenth==-1 or dataFilePath == "" means logs only scan
            String realtimePath = dataFileLenth != -1 ? dataFilePath : deltaFilePaths[0];
            long realtimeLength = dataFileLenth != -1 ? dataFileLenth : 0;
            Path path = new Path(realtimePath);
            FileSplit fileSplit = new FileSplit(path, 0, realtimeLength, new String[] {""});
            List<HoodieLogFile> logFiles = Arrays.stream(deltaFilePaths).map(HoodieLogFile::new).collect(toList());
            FileSplit hudiSplit = new HoodieRealtimeFileSplit(fileSplit, basePath, logFiles, instantTime, false, Option.empty());

            InputFormat<?, ?> inputFormatClass = createInputFormat(jobConf, inputFormat);
            reader = (RecordReader<NullWritable, ArrayWritable>) inputFormatClass
                    .getRecordReader(hudiSplit, jobConf, Reporter.NULL);

            deserializer = getDeserializer(jobConf, properties, serde);
            rowInspector = getTableObjectInspector(deserializer);
            for (int i = 0; i < requiredFields.length; i++) {
                StructField field = rowInspector.getStructFieldRef(requiredFields[i]);
                structFields[i] = field;
                fieldInspectors[i] = field.getFieldObjectInspector();
            }

        } catch (Exception e) {
            e.printStackTrace();
            throw new IOException("Cannot read hudi file slice");
        }
    }

    public void close() throws IOException {
        reader.close();
        if (offHeapTable != null) {
            offHeapTable.close();
        }
    }

    public boolean hasNext() {
        return hasNext;
    }

    public long nextChunkOffHeap() throws IOException {
        this.offHeapTable = new OffHeapTable(requiredTypes, fetchSize);
        try {
            int numRows = 0;
            for (; numRows < fetchSize && hasNext(); numRows++) {
                NullWritable key = reader.createKey();
                ArrayWritable value = reader.createValue();
                if (!reader.next(key, value)) {
                    hasNext = false;
                    break;
                }
                Object rowData = deserializer.deserialize(value);
                for (int i = 0; i < requiredFields.length; i++) {
                    Object fieldData = rowInspector.getStructFieldData(rowData, structFields[i]);
                    if (fieldData == null) {
                        offHeapTable.appendData(i, null);
                    } else {
                        Object fieldValue = ((PrimitiveObjectInspector) fieldInspectors[i]).getPrimitiveJavaObject(fieldData);
                        offHeapTable.appendData(i, fieldValue);
                    }
                }
            }
            offHeapTable.setNumRows(numRows);
            return offHeapTable.getMetaNativeAddress();
        } catch (Exception e) {
            e.printStackTrace();
            throw new IOException("Cannot get hudi next chunk");
        }
    }

    public void releaseOffHeapColumnVector(int fieldId) {
        offHeapTable.releaseOffHeapColumnVector(fieldId);
    }

    private InputFormat<?, ?> createInputFormat(Configuration conf, String inputFormat) throws Exception {
        try {
            Class<?> clazz = conf.getClassByName(inputFormat);
            Class<? extends InputFormat<?, ?>> cls = (Class<? extends InputFormat<?, ?>>) clazz.asSubclass(InputFormat.class);
            return ReflectionUtils.newInstance(cls, conf);
        } catch (ClassNotFoundException | RuntimeException e) {
            throw new Exception("Unable to create input format " + inputFormat, e);
        }
    }

    private Deserializer getDeserializer(Configuration configuration, Properties properties, String name) throws Exception {
        Class<? extends Deserializer> deserializerClass = Class.forName(name, true, JavaUtils.getClassLoader())
                .asSubclass(Deserializer.class);
        Deserializer deserializer = deserializerClass.getConstructor().newInstance();
        deserializer.initialize(configuration, properties);
        return deserializer;
    }

    private StructObjectInspector getTableObjectInspector(Deserializer deserializer) {
        try {
            ObjectInspector inspector = deserializer.getObjectInspector();
            checkArgument(inspector.getCategory() == ObjectInspector.Category.STRUCT,
                    "expected STRUCT: %s", inspector.getCategory());
            return (StructObjectInspector) inspector;
        } catch (SerDeException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * For test only
     */
    public List<List<Object>> nextChunk() throws IOException {
        try {
            int columnCount = requiredFields.length;
            List<List<Object>> chunk = new ArrayList<>(columnCount);
            for (int i = 0; i < columnCount; i++) {
                chunk.add(new ArrayList<Object>(fetchSize));
            }
            for (int numRows = 0; numRows < fetchSize && hasNext(); numRows++) {
                NullWritable key = reader.createKey();
                ArrayWritable value = reader.createValue();
                if (!reader.next(key, value)) {
                    hasNext = false;
                    break;
                }
                Object rowData = deserializer.deserialize(value);
                for (int i = 0; i < columnCount; i++) {
                    Object fieldData = rowInspector.getStructFieldData(rowData, structFields[i]);
                    if (fieldData == null) {
                        chunk.get(i).add(null);
                    } else {
                        Object fieldValue = ((PrimitiveObjectInspector) fieldInspectors[i]).getPrimitiveJavaObject(fieldData);
                        chunk.get(i).add(fieldValue);
                    }
                }
            }
            return chunk;
        } catch (Exception e) {
            e.printStackTrace();
            throw new IOException("Cannot get hudi next chunk");
        }
    }

    /**
     * For test only
     */
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("basePath: ");
        sb.append(basePath);
        sb.append("\n");
        sb.append("hiveColumnNames: ");
        sb.append(hiveColumnNames);
        sb.append("\n");
        sb.append("hiveColumnTypes: ");
        sb.append(Arrays.toString(hiveColumnTypes));
        sb.append("\n");
        sb.append("requiredFields: ");
        sb.append(Arrays.toString(requiredFields));
        sb.append("\n");
        sb.append("instantTime: ");
        sb.append(instantTime);
        sb.append("\n");
        sb.append("deltaFilePaths: ");
        sb.append(Arrays.toString(deltaFilePaths));
        sb.append("\n");
        sb.append("dataFilePath: ");
        sb.append(dataFilePath);
        sb.append("\n");
        sb.append("dataFileLenth: ");
        sb.append(dataFileLenth);
        sb.append("\n");
        sb.append("serde: ");
        sb.append(serde);
        sb.append("\n");
        sb.append("inputFormat: ");
        sb.append(inputFormat);
        sb.append("\n");
        return sb.toString();
    }
}