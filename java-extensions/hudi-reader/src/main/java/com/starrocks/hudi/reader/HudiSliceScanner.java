// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.hudi.reader;

import com.starrocks.jni.connector.ConnectorScanner;
import com.starrocks.jni.connector.TypeMapping;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.serde2.Deserializer;
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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.stream.Collectors.toList;

public class HudiSliceScanner extends ConnectorScanner {

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
    private RecordReader<NullWritable, ArrayWritable> reader;
    private StructObjectInspector rowInspector;
    private ObjectInspector[] fieldInspectors;
    private StructField[] structFields;
    private Deserializer deserializer;
    private final int fetchSize;

    public HudiSliceScanner(int fetchSize, Map<String, String> params) {
        this.fetchSize = fetchSize;
        this.basePath = params.get("base_path");
        this.hiveColumnNames = params.get("hive_column_names");
        this.hiveColumnTypes = params.get("hive_column_types").split(":");
        this.requiredFields = params.get("required_fields").split(",");
        this.instantTime = params.get("instant_time");
        if (params.get("delta_file_paths").length() == 0) {
            this.deltaFilePaths = new String[0];
        } else {
            this.deltaFilePaths = params.get("delta_file_paths").split(",");
        }
        this.dataFilePath = params.get("data_file_path");
        this.dataFileLenth = Long.parseLong(params.get("data_file_length"));
        this.serde = params.get("serde");
        this.inputFormat = params.get("input_format");;
        this.fieldInspectors = new ObjectInspector[requiredFields.length];
        this.structFields = new StructField[requiredFields.length];
    }

    @Override
    public void open() throws IOException {
        try {
            Properties properties = new Properties();
            Configuration conf = new Configuration();
            conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
            conf.setBoolean("dfs.client.use.legacy.blockreader", false);
            JobConf jobConf = new JobConf(conf);
            jobConf.setBoolean("hive.io.file.read.all.columns", false);
            String[] hiveColumnNames = this.hiveColumnNames.split(",");
            HashMap<String, Integer> hiveColumnNameToIndex = new HashMap<>();
            HashMap<String, String> hiveColumnNameToType = new HashMap<>();
            for (int i = 0; i < hiveColumnNames.length; i++) {
                hiveColumnNameToIndex.put(hiveColumnNames[i], i);
                hiveColumnNameToType.put(hiveColumnNames[i], hiveColumnTypes[i]);
            }

            String[] requiredTypes = new String[requiredFields.length];
            StringBuilder columnIdBuilder = new StringBuilder();
            boolean isFirst = true;
            for (int i = 0; i < requiredFields.length; i++) {
                if (!isFirst) {
                    columnIdBuilder.append(",");
                }
                columnIdBuilder.append(hiveColumnNameToIndex.get(requiredFields[i]));
                String typeStr = hiveColumnNameToType.get(requiredFields[i]);
                // convert decimal(x,y) to decimal
                if (typeStr.startsWith("decimal")) {
                    typeStr = "decimal";
                }
                requiredTypes[i] = typeStr;
                isFirst = false;
            }
            initOffHeapTableWriter(requiredTypes, fetchSize, TypeMapping.hiveTypeMappings);

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
            close();
            e.printStackTrace();
            throw new IOException("Failed to open the hudi MOR slice reader.", e);
        }
    }

    @Override
    public void close() throws IOException {
        try {
            if (reader != null) {
                reader.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new IOException("Failed to close the hudi MOR slice reader.", e);
        }
    }

    @Override
    public int getNext() throws IOException {
        try {
            NullWritable key = reader.createKey();
            ArrayWritable value = reader.createValue();
            int numRows = 0;
            for (; numRows < getTableSize(); numRows++) {
                if (!reader.next(key, value)) {
                    break;
                }
                Object rowData = deserializer.deserialize(value);
                for (int i = 0; i < requiredFields.length; i++) {
                    Object fieldData = rowInspector.getStructFieldData(rowData, structFields[i]);
                    if (fieldData == null) {
                        scanData(i, null);
                    } else {
                        Object fieldValue = ((PrimitiveObjectInspector) fieldInspectors[i]).getPrimitiveJavaObject(fieldData);
                        scanData(i, fieldValue);
                    }
                }
            }
            return numRows;
        } catch (Exception e) {
            close();
            e.printStackTrace();
            throw new IOException("Failed to get the next off-heap table chunk of hudi.", e);
        }
    }

    private InputFormat<?, ?> createInputFormat(Configuration conf, String inputFormat) throws Exception {
        Class<?> clazz = conf.getClassByName(inputFormat);
        Class<? extends InputFormat<?, ?>> cls = (Class<? extends InputFormat<?, ?>>) clazz.asSubclass(InputFormat.class);
        return ReflectionUtils.newInstance(cls, conf);
    }

    private Deserializer getDeserializer(Configuration configuration, Properties properties, String name) throws Exception {
        Class<? extends Deserializer> deserializerClass = Class.forName(name, true, JavaUtils.getClassLoader())
                .asSubclass(Deserializer.class);
        Deserializer deserializer = deserializerClass.getConstructor().newInstance();
        deserializer.initialize(configuration, properties);
        return deserializer;
    }

    private StructObjectInspector getTableObjectInspector(Deserializer deserializer) throws Exception {
        ObjectInspector inspector = deserializer.getObjectInspector();
        checkArgument(inspector.getCategory() == ObjectInspector.Category.STRUCT,
                "expected STRUCT: %s", inspector.getCategory());
        return (StructObjectInspector) inspector;
    }

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