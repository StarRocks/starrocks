package com.starrocks.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

import java.util.Base64;

public class UDFBase64ToBitmap extends GenericUDF {
    private transient StringObjectInspector inspector;

    @Override
    public ObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
        if (args.length != 1) {
            throw new UDFArgumentException("Argument number of base64_to_bitmap should be 1");
        }

        ObjectInspector arg0 = args[0];
        if (!(arg0 instanceof StringObjectInspector)) {
            throw new UDFArgumentException("First argument should be string type");
        }
        this.inspector = (StringObjectInspector) args[0];

        return PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] args) throws HiveException {
        if (args[0].get() == null) {
            return null;
        }

        String str = inspector.getPrimitiveJavaObject(args[0].get());
        return Base64.getDecoder().decode(str);
    }

    @Override
    public String getDisplayString(String[] strings) {
        return "USAGE: base64_to_bitmap(bitmap)";
    }
}
