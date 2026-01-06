# Type Checker XML Configuration Guide

## Overview

The StarRocks Type Checker system now supports XML-based configuration for defining Java type to StarRocks logical type mappings. This provides a flexible, maintainable way to manage type conversions without requiring code changes and recompilation.

## Features

- **Dynamic Configuration**: Type mappings can be updated via XML without recompiling
- **Backward Compatible**: Falls back to hardcoded configuration if XML is unavailable
- **Extensible**: Easy to add new type mappings or modify existing ones
- **Validated**: Comprehensive error checking for malformed XML or invalid configurations

## Configuration File Location

The type checker configuration is loaded from one of the following locations (in priority order):

1. **Environment Variable**: `STARROCKS_TYPE_CHECKER_CONFIG`
   ```bash
   export STARROCKS_TYPE_CHECKER_CONFIG=/path/to/custom/type_checker_config.xml
   ```

2. **Default Location**: `$STARROCKS_HOME/conf/type_checker_config.xml`
   ```bash
   export STARROCKS_HOME=/opt/starrocks
   # Configuration will be loaded from /opt/starrocks/conf/type_checker_config.xml
   ```

3. **Fallback**: Hardcoded configuration (no XML required)

## XML Configuration Format

### Basic Structure

```xml
<?xml version="1.0" encoding="UTF-8"?>
<type-checkers>
  <type-mapping java_class="<java-class-name>" checker="<checker-class-name>"/>
  <!-- Additional type mappings -->
</type-checkers>
```

### Example Configuration

```xml
<?xml version="1.0" encoding="UTF-8"?>
<type-checkers>
  <!-- Basic Java types -->
  <type-mapping java_class="java.lang.Byte" checker="ByteTypeChecker"/>
  <type-mapping java_class="java.lang.Short" checker="ShortTypeChecker"/>
  <type-mapping java_class="java.lang.Integer" checker="IntegerTypeChecker"/>
  <type-mapping java_class="java.lang.Long" checker="LongTypeChecker"/>
  <type-mapping java_class="java.lang.Boolean" checker="BooleanTypeChecker"/>
  <type-mapping java_class="java.lang.Float" checker="FloatTypeChecker"/>
  <type-mapping java_class="java.lang.Double" checker="DoubleTypeChecker"/>
  
  <!-- String types -->
  <type-mapping java_class="java.lang.String" checker="StringTypeChecker"/>
  
  <!-- Temporal types -->
  <type-mapping java_class="java.sql.Date" checker="DateTypeChecker"/>
  <type-mapping java_class="java.sql.Time" checker="TimeTypeChecker"/>
  <type-mapping java_class="java.sql.Timestamp" checker="TimestampTypeChecker"/>
  <type-mapping java_class="java.time.LocalDate" checker="LocalDateTypeChecker"/>
  <type-mapping java_class="java.time.LocalDateTime" checker="LocalDateTimeTypeChecker"/>
  
  <!-- Numeric types -->
  <type-mapping java_class="java.math.BigInteger" checker="BigIntegerTypeChecker"/>
  <type-mapping java_class="java.math.BigDecimal" checker="BigDecimalTypeChecker"/>
  
  <!-- Binary types -->
  <type-mapping java_class="byte[]" checker="ByteArrayTypeChecker"/>
  <type-mapping java_class="[B" checker="ByteArrayTypeChecker"/>
  
  <!-- Database-specific types -->
  <type-mapping java_class="oracle.sql.TIMESTAMP" checker="OracleTimestampClassTypeChecker"/>
  <type-mapping java_class="oracle.jdbc.OracleBlob" checker="ByteArrayTypeChecker"/>
  <type-mapping java_class="com.clickhouse.data.value.UnsignedByte" checker="ClickHouseUnsignedByteTypeChecker"/>
  <type-mapping java_class="microsoft.sql.DateTimeOffset" checker="SqlServerDateTimeOffsetTypeChecker"/>
</type-checkers>
```

## Supported Type Checkers

The following type checker implementations are available:

| Checker Name | Description | Target Type |
|-------------|-------------|-------------|
| `ByteTypeChecker` | Java Byte type | TINYINT/BOOLEAN |
| `ShortTypeChecker` | Java Short type | SMALLINT |
| `IntegerTypeChecker` | Java Integer type | INT |
| `LongTypeChecker` | Java Long type | BIGINT |
| `BigIntegerTypeChecker` | Java BigInteger type | LARGEINT/VARCHAR |
| `BooleanTypeChecker` | Java Boolean type | BOOLEAN |
| `FloatTypeChecker` | Java Float type | FLOAT |
| `DoubleTypeChecker` | Java Double type | DOUBLE |
| `StringTypeChecker` | Java String type | VARCHAR |
| `DateTypeChecker` | java.sql.Date | DATE |
| `TimeTypeChecker` | java.sql.Time | TIME |
| `TimestampTypeChecker` | java.sql.Timestamp | DATETIME/VARCHAR |
| `LocalDateTypeChecker` | java.time.LocalDate | DATE |
| `LocalDateTimeTypeChecker` | java.time.LocalDateTime | DATETIME |
| `BigDecimalTypeChecker` | java.math.BigDecimal | DECIMAL/VARCHAR |
| `ByteArrayTypeChecker` | byte[] and similar | VARBINARY |
| `OracleTimestampClassTypeChecker` | Oracle timestamp types | VARCHAR |
| `SqlServerDateTimeOffsetTypeChecker` | SQL Server DateTimeOffset | VARCHAR |
| `ClickHouseUnsignedByteTypeChecker` | ClickHouse UnsignedByte | SMALLINT |
| `ClickHouseUnsignedShortTypeChecker` | ClickHouse UnsignedShort | INT |
| `ClickHouseUnsignedIntegerTypeChecker` | ClickHouse UnsignedInteger | BIGINT |
| `ClickHouseUnsignedLongTypeChecker` | ClickHouse UnsignedLong | LARGEINT |
| `DefaultTypeChecker` | Fallback for unknown types | VARCHAR |

## Usage

### For Administrators

1. **Using Default Configuration**
   - The default configuration is provided at `conf/type_checker_config.xml`
   - No additional setup required if using standard type mappings

2. **Custom Configuration**
   ```bash
   # Create custom configuration file
   cp conf/type_checker_config.xml /path/to/custom_config.xml
   
   # Edit the custom configuration
   vim /path/to/custom_config.xml
   
   # Set environment variable
   export STARROCKS_TYPE_CHECKER_CONFIG=/path/to/custom_config.xml
   
   # Start StarRocks BE
   ./bin/start_be.sh
   ```

3. **Validation**
   - Check BE logs for configuration loading messages:
   ```
   INFO: TypeCheckerManager initialized from XML configuration: /path/to/config.xml
   ```
   - Or for fallback:
   ```
   INFO: TypeCheckerManager using hardcoded configuration
   ```

### For Developers

The type checker system is used automatically when processing JDBC query results:

```cpp
#include "types/type_checker_manager.h"

TypeCheckerManager& manager = TypeCheckerManager::getInstance();
StatusOr<LogicalType> result = manager.checkType("java.lang.Integer", slot_desc);

if (!result.ok()) {
    // Handle type mismatch error
    LOG(WARNING) << result.status().message();
} else {
    LogicalType logical_type = result.value();
    // Use the logical type for data conversion
}
```

## Error Handling

The XML loader provides detailed error messages for common issues:

### Missing Configuration File
```
Status: NotFound
Message: XML configuration file not found: /path/to/config.xml
Behavior: Falls back to hardcoded configuration
```

### Malformed XML
```
Status: InvalidArgument  
Message: Invalid type-mapping element: <malformed line>
Behavior: Initialization fails, falls back to hardcoded configuration
```

### Empty Configuration
```
Status: InvalidArgument
Message: No valid type mappings found in XML configuration
Behavior: Falls back to hardcoded configuration
```

### Unknown Checker Type
```
Warning: Unknown checker type in XML configuration: UnknownChecker
Behavior: Mapping is skipped, processing continues with other mappings
```

## Testing

### Unit Tests

Run the type checker tests:
```bash
# Run all type checker tests
./be/test/types/type_checker_test

# Run XML loader specific tests
./be/test/types/type_checker_xml_loader_test
```

### Manual Testing

1. Create a test XML configuration:
```xml
<?xml version="1.0" encoding="UTF-8"?>
<type-checkers>
  <type-mapping java_class="java.lang.String" checker="StringTypeChecker"/>
</type-checkers>
```

2. Set environment variable and verify loading:
```bash
export STARROCKS_TYPE_CHECKER_CONFIG=/tmp/test_config.xml
# Start BE and check logs
```

## Migration Guide

### From Hardcoded to XML Configuration

If you're currently using the hardcoded configuration:

1. **No Changes Required**: The system automatically uses hardcoded configuration as fallback
2. **Optional Migration**: To use XML configuration:
   - Copy the default `conf/type_checker_config.xml`
   - Customize as needed
   - Set `STARROCKS_TYPE_CHECKER_CONFIG` or place at default location
   - Restart BE

### Adding Custom Type Mappings

To add a new type mapping:

1. Implement a new type checker class (if needed) following the existing pattern
2. Add the checker to `TypeCheckerXMLLoader::create_checker()`
3. Add the mapping to your XML configuration:
   ```xml
   <type-mapping java_class="com.example.CustomType" checker="CustomTypeChecker"/>
   ```

## Best Practices

1. **Version Control**: Keep your XML configuration in version control
2. **Validation**: Validate XML changes before deployment
3. **Comments**: Use XML comments to document custom type mappings
4. **Testing**: Test configuration changes in a non-production environment first
5. **Backup**: Keep a backup of working configuration before making changes

## Troubleshooting

### Configuration Not Loading

1. Check environment variables:
   ```bash
   echo $STARROCKS_TYPE_CHECKER_CONFIG
   echo $STARROCKS_HOME
   ```

2. Verify file permissions:
   ```bash
   ls -la $STARROCKS_HOME/conf/type_checker_config.xml
   ```

3. Check BE logs for error messages

### Type Checker Not Working

1. Verify the Java class name is correct in XML
2. Ensure the checker name matches exactly (case-sensitive)
3. Check that the checker is supported (see Supported Type Checkers table)
4. Review BE logs for warnings about unknown checkers

## See Also

- Type Checker Implementation: `be/src/types/checker/type_checker.h`
- XML Loader: `be/src/types/checker/type_checker_xml_loader.h`
- Type Checker Manager: `be/src/types/type_checker_manager.h`
- Unit Tests: `be/test/types/type_checker_test.cpp`, `be/test/types/type_checker_xml_loader_test.cpp`
