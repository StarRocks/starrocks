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

package com.starrocks.hudi.reader;

import com.starrocks.jni.connector.ColumnType;
import com.starrocks.jni.connector.ColumnValue;
import com.starrocks.jni.connector.ConnectorScanner;
import com.starrocks.jni.connector.ScannerHelper;
import com.starrocks.jni.connector.SelectedFields;
import com.starrocks.utils.loader.ThreadContextClassLoader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.starrocks.hudi.reader.HudiScannerUtils.HIVE_TYPE_MAPPING;
import static java.util.stream.Collectors.toList;

public class HudiSliceScanner extends ConnectorScanner {

    private static final Logger LOG = LogManager.getLogger(HudiSliceScanner.class);

    private final String basePath;
    private final String hiveColumnNames;
    private final String[] hiveColumnTypes;
    private final String[] requiredFields;
    private int[] requiredColumnIds;
    private ColumnType[] requiredTypes;
    private final String[] nestedFields;
    private final String instantTime;
    private final String[] deltaFilePaths;
    private final String dataFilePath;
    private final long dataFileLength;
    private final String serde;
    private final String inputFormat;
    private RecordReader<NullWritable, ArrayWritable> reader;
    private StructObjectInspector rowInspector;
    private ObjectInspector[] fieldInspectors;
    private StructField[] structFields;
    private Deserializer deserializer;
    private final int fetchSize;
    private final ClassLoader classLoader;
    private final String fsOptionsProps;

    public HudiSliceScanner(int fetchSize, Map<String, String> params) {
        this.fetchSize = fetchSize;
        this.hiveColumnNames = params.get("hive_column_names");
        this.hiveColumnTypes = params.get("hive_column_types").split("#");
        this.requiredFields = params.get("required_fields").split(",");
        this.nestedFields = params.getOrDefault("nested_fields", "").split(",");
        this.instantTime = params.get("instant_time");
        if (params.get("delta_file_paths").length() == 0) {
            this.deltaFilePaths = new String[0];
        } else {
            this.deltaFilePaths = params.get("delta_file_paths").split(",");
        }
        this.basePath = params.get("base_path");
        this.dataFilePath = params.get("data_file_path");
        this.dataFileLength = Long.parseLong(params.get("data_file_length"));
        this.serde = params.get("serde");
        this.inputFormat = params.get("input_format");
        this.fieldInspectors = new ObjectInspector[requiredFields.length];
        this.structFields = new StructField[requiredFields.length];
        this.classLoader = this.getClass().getClassLoader();
        this.fsOptionsProps = params.get("fs_options_props");
        for (Map.Entry<String, String> kv : params.entrySet()) {
            LOG.debug("key = " + kv.getKey() + ", value = " + kv.getValue());
        }
    }

    private JobConf makeJobConf(Properties properties) {
        Configuration conf = new Configuration();
        JobConf jobConf = new JobConf(conf);
        jobConf.setBoolean("hive.io.file.read.all.columns", false);
        properties.stringPropertyNames().forEach(name -> jobConf.set(name, properties.getProperty(name)));
        return jobConf;
    }

    private void parseRequiredTypes() {
        String[] hiveColumnNames = this.hiveColumnNames.split(",");
        HashMap<String, Integer> hiveColumnNameToIndex = new HashMap<>();
        HashMap<String, String> hiveColumnNameToType = new HashMap<>();
        for (int i = 0; i < hiveColumnNames.length; i++) {
            hiveColumnNameToIndex.put(hiveColumnNames[i], i);
            hiveColumnNameToType.put(hiveColumnNames[i], hiveColumnTypes[i]);
        }

        requiredTypes = new ColumnType[requiredFields.length];
        requiredColumnIds = new int[requiredFields.length];
        for (int i = 0; i < requiredFields.length; i++) {
            requiredColumnIds[i] = hiveColumnNameToIndex.get(requiredFields[i]);
            String type = hiveColumnNameToType.get(requiredFields[i]);
            requiredTypes[i] = new ColumnType(requiredFields[i], type);
        }

        // prune fields
        SelectedFields ssf = new SelectedFields();
        for (String nestField : nestedFields) {
            ssf.addNestedPath(nestField);
        }
        for (int i = 0; i < requiredFields.length; i++) {
            ColumnType type = requiredTypes[i];
            String name = requiredFields[i];
            type.pruneOnField(ssf, name);
        }
    }

    private Properties makeProperties() {
        Properties properties = new Properties();
        properties.setProperty("hive.io.file.readcolumn.ids",
                Arrays.stream(this.requiredColumnIds).mapToObj(String::valueOf)
                        .collect(Collectors.joining(",")));
        properties.setProperty("hive.io.file.readcolumn.names", String.join(",", this.requiredFields));
        // build `readNestedColumn.paths` spec.
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < requiredFields.length; i++) {
            String name = requiredFields[i];
            ColumnType type = requiredTypes[i];
            type.buildNestedFieldsSpec(name, sb);
        }
        if (sb.length() > 0) {
            sb.deleteCharAt(sb.length() - 1);
            properties.setProperty("hive.io.file.readNestedColumn.paths", sb.toString());
        }
        properties.setProperty("columns", this.hiveColumnNames);
        // recover INT64 based timestamp mark to hive type, TimestampMicros/TimestampMillis => timestamp
        List<String> types = new ArrayList<>();
        for (int i = 0; i < this.hiveColumnTypes.length; i++) {
            String type = this.hiveColumnTypes[i];
            if (HIVE_TYPE_MAPPING.containsKey(type)) {
                type = HIVE_TYPE_MAPPING.get(type);
            }
            types.add(type);
        }
        properties.setProperty("columns.types", types.stream().collect(Collectors.joining(",")));
        properties.setProperty("serialization.lib", this.serde);

        ScannerHelper.parseFSOptionsProps(fsOptionsProps, kv -> {
            properties.put(kv[0], kv[1]);
            return null;
        }, t -> {
            LOG.warn("Invalid hudi scanner fs options props argument: " + t);
            return null;
        });
        return properties;
    }

    private void initReader(JobConf jobConf, Properties properties) throws Exception {
        // dataFileLenth==-1 or dataFilePath == "" means logs only scan
        String realtimePath = dataFileLength != -1 ? dataFilePath : deltaFilePaths[0];
        long realtimeLength = dataFileLength != -1 ? dataFileLength : 0;
        Path path = new Path(realtimePath);
        FileSplit fileSplit = new FileSplit(path, 0, realtimeLength, (String[]) null);
        List<HoodieLogFile> logFiles = Arrays.stream(deltaFilePaths).map(HoodieLogFile::new).collect(toList());
        FileSplit hudiSplit =
                new HoodieRealtimeFileSplit(fileSplit, basePath, logFiles, instantTime, false, Option.empty());

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
    }

    @Override
    public void open() throws IOException {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            parseRequiredTypes();
            initOffHeapTableWriter(requiredTypes, requiredFields, fetchSize);
            Properties properties = makeProperties();
            JobConf jobConf = makeJobConf(properties);
            initReader(jobConf, properties);
        } catch (Exception e) {
            close();
            LOG.error("Failed to open the hudi MOR slice reader.", e);
            throw new IOException("Failed to open the hudi MOR slice reader.", e);
        }
    }

    @Override
    public void close() throws IOException {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            if (reader != null) {
                reader.close();
            }
        } catch (IOException e) {
            LOG.error("Failed to close the hudi MOR slice reader.", e);
            throw new IOException("Failed to close the hudi MOR slice reader.", e);
        }
    }

    @Override
    public int getNext() throws IOException {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
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
                        appendData(i, null);
                    } else {
                        ColumnValue fieldValue = new HudiColumnValue(fieldInspectors[i], fieldData);
                        appendData(i, fieldValue);
                    }
                }
            }
            return numRows;
        } catch (Exception e) {
            close();
            LOG.error("Failed to get the next off-heap table chunk of hudi.", e);
            throw new IOException("Failed to get the next off-heap table chunk of hudi.", e);
        }
    }

    private InputFormat<?, ?> createInputFormat(Configuration conf, String inputFormat) throws Exception {
        Class<?> clazz = conf.getClassByName(inputFormat);
        Class<? extends InputFormat<?, ?>> cls =
                (Class<? extends InputFormat<?, ?>>) clazz.asSubclass(InputFormat.class);
        return ReflectionUtils.newInstance(cls, conf);
    }

    private Deserializer getDeserializer(Configuration configuration, Properties properties, String name)
            throws Exception {
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
        sb.append(dataFileLength);
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