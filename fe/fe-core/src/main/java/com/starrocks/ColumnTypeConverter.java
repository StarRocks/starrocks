package com.starrocks;

import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
import com.starrocks.external.hive.Utils;

public class ColumnTypeConverter {

    public static Type fromHiveType(String hiveType) {
        String typeUpperCase = Utils.getTypeKeyword(hiveType).toUpperCase();
        PrimitiveType primitiveType;
        switch (typeUpperCase) {
            case "TINYINT":
                primitiveType = PrimitiveType.TINYINT;
                break;
            case "SMALLINT":
                primitiveType = PrimitiveType.SMALLINT;
                break;
            case "INT":
            case "INTEGER":
                primitiveType = PrimitiveType.INT;
                break;
            case "BIGINT":
                primitiveType = PrimitiveType.BIGINT;
                break;
            case "FLOAT":
                primitiveType = PrimitiveType.FLOAT;
                break;
            case "DOUBLE":
            case "DOUBLE PRECISION":
                primitiveType = PrimitiveType.DOUBLE;
                break;
            case "DECIMAL":
            case "NUMERIC":
                primitiveType = PrimitiveType.DECIMAL32;
                break;
            case "TIMESTAMP":
                primitiveType = PrimitiveType.DATETIME;
                break;
            case "DATE":
                primitiveType = PrimitiveType.DATE;
                break;
            case "STRING":
                return ScalarType.createDefaultString();
            case "VARCHAR":
                return ScalarType.createVarcharType(Utils.getVarcharLength(hiveType));
            case "CHAR":
                return ScalarType.createCharType(Utils.getCharLength(hiveType));
            case "BOOLEAN":
                primitiveType = PrimitiveType.BOOLEAN;
                break;
            case "ARRAY":
                Type type = Utils.convertToArrayType(hiveType);
                if (type.isArrayType()) {
                    return type;
                }
            default:
                primitiveType = PrimitiveType.UNKNOWN_TYPE;
                break;
        }

        if (primitiveType != PrimitiveType.DECIMAL32) {
            return ScalarType.createType(primitiveType);
        } else {
            int[] parts = Utils.getPrecisionAndScale(hiveType);
            return ScalarType.createUnifiedDecimalType(parts[0], parts[1]);
        }
    }
}
