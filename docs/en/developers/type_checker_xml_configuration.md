# Type Checker XML Configuration Guide

## Overview

The StarRocks Type Checker system uses **mandatory** XML-based configuration for defining Java type to StarRocks logical type mappings. All type checkers are now defined via XML with configurable type rules, providing maximum flexibility and maintainability without requiring code changes or recompilation.

## Features

- **Fully XML-Configurable**: All type mappings defined in XML with explicit type validation rules
- **Industry-Standard Parser**: Uses libxml2 for robust, standards-compliant XML parsing
- **No Hardcoded Fallback**: XML configuration is mandatory - ensures all type mappings are explicitly defined
- **Flexible Type Rules**: Each type can have multiple type-rule mappings for different StarRocks types
- **Extensible**: Easy to add new type mappings or modify existing ones via XML
- **Validated**: Comprehensive error checking for malformed XML or invalid configurations

## Configuration File Location

The type checker configuration is loaded from the following location:

1. **Default Location**: `$STARROCKS_HOME/conf/type_checker_config.xml`
   ```bash
   export STARROCKS_HOME=/opt/starrocks
   # Configuration will be loaded from /opt/starrocks/conf/type_checker_config.xml
   ```

2. **Relative Fallback**: `conf/type_checker_config.xml` (if `STARROCKS_HOME` is not set)

**IMPORTANT**: XML configuration is **mandatory**. The configuration file must exist at one of the above locations. If the file is not found or fails to parse, the system will log errors and continue with an empty checker map (default checker handles unknown types).

## XML Configuration Format

### Structure with Type Rules

All type checkers now use a configurable format with explicit type-rule elements:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<type-checkers>
  <type-mapping java_class="<java-class-name>" display_name="<display-name>">
    <type-rule allowed_type="<starrocks-type>" return_type="<return-type>"/>
    <!-- Additional type rules -->
  </type-mapping>
  <!-- Additional type mappings -->
</type-checkers>
```

### XML Elements

- **`<type-checkers>`**: Root element containing all type mappings
- **`<type-mapping>`**: Defines a mapping for a specific Java class
  - `java_class` attribute: Fully qualified Java class name (e.g., `java.lang.Integer`)
  - `display_name` attribute: Human-readable name used in error messages
- **`<type-rule>`**: Defines an allowed type conversion rule
  - `allowed_type` attribute: StarRocks type that can accept this Java type (e.g., `TYPE_INT`)
  - `return_type` attribute: StarRocks type to return for this conversion

### Example Configuration

```xml
<?xml version="1.0" encoding="UTF-8"?>
<type-checkers>
  <!-- Java Integer type -->
  <type-mapping java_class="java.lang.Integer" display_name="Integer">
    <type-rule allowed_type="TYPE_TINYINT" return_type="TYPE_INT"/>
    <type-rule allowed_type="TYPE_SMALLINT" return_type="TYPE_INT"/>
    <type-rule allowed_type="TYPE_INT" return_type="TYPE_INT"/>
    <type-rule allowed_type="TYPE_BIGINT" return_type="TYPE_INT"/>
  </type-mapping>
  
  <!-- Java String type -->
  <type-mapping java_class="java.lang.String" display_name="String">
    <type-rule allowed_type="TYPE_CHAR" return_type="TYPE_VARCHAR"/>
    <type-rule allowed_type="TYPE_VARCHAR" return_type="TYPE_VARCHAR"/>
  </type-mapping>
  
  <!-- Java Boolean type -->
  <type-mapping java_class="java.lang.Boolean" display_name="Boolean">
    <type-rule allowed_type="TYPE_BOOLEAN" return_type="TYPE_BOOLEAN"/>
    <type-rule allowed_type="TYPE_SMALLINT" return_type="TYPE_BOOLEAN"/>
    <type-rule allowed_type="TYPE_INT" return_type="TYPE_BOOLEAN"/>
    <type-rule allowed_type="TYPE_BIGINT" return_type="TYPE_BOOLEAN"/>
  </type-mapping>
  
  <!-- ClickHouse UnsignedByte type -->
  <type-mapping java_class="com.clickhouse.data.value.UnsignedByte" display_name="UnsignedByte">
    <type-rule allowed_type="TYPE_SMALLINT" return_type="TYPE_SMALLINT"/>
    <type-rule allowed_type="TYPE_INT" return_type="TYPE_SMALLINT"/>
    <type-rule allowed_type="TYPE_BIGINT" return_type="TYPE_SMALLINT"/>
  </type-mapping>
</type-checkers>
```

## Supported StarRocks Types

The following StarRocks logical types can be used in type rules:

| Type Name | Description |
|-----------|-------------|
| `TYPE_BOOLEAN` | Boolean type |
| `TYPE_TINYINT` | 8-bit signed integer |
| `TYPE_SMALLINT` | 16-bit signed integer |
| `TYPE_INT` | 32-bit signed integer |
| `TYPE_BIGINT` | 64-bit signed integer |
| `TYPE_LARGEINT` | 128-bit signed integer |
| `TYPE_FLOAT` | 32-bit floating point |
| `TYPE_DOUBLE` | 64-bit floating point |
| `TYPE_VARCHAR` | Variable-length string |
| `TYPE_CHAR` | Fixed-length string |
| `TYPE_BINARY` | Fixed-length binary |
| `TYPE_VARBINARY` | Variable-length binary |
| `TYPE_DATE` | Date type |
| `TYPE_DATETIME` | Datetime type |
| `TYPE_TIME` | Time type |
| `TYPE_DECIMAL32` | 32-bit decimal |
| `TYPE_DECIMAL64` | 64-bit decimal |
| `TYPE_DECIMAL128` | 128-bit decimal |
| `TYPE_DECIMAL256` | 256-bit decimal |

## Default Type Mappings

The default `conf/type_checker_config.xml` includes mappings for:

- **Java Primitives**: Byte, Short, Integer, Long, Boolean, Float, Double
- **Java Objects**: String, BigInteger, BigDecimal
- **Temporal Types**: java.sql.Date, java.sql.Time, java.sql.Timestamp, java.time.LocalDate, java.time.LocalDateTime
- **Binary Types**: byte[], [B, java.util.UUID
- **Database-Specific**: Oracle (TIMESTAMP, TIMESTAMPLTZ, TIMESTAMPTZ, OracleBlob), SQL Server (DateTimeOffset), ClickHouse (UnsignedByte, UnsignedShort, UnsignedInteger, UnsignedLong)

See `conf/type_checker_config.xml` for the complete configuration.

## Usage

### For Administrators

1. **Using Default Configuration**
   - The default configuration is provided at `conf/type_checker_config.xml`
   - Ensure this file exists before starting StarRocks BE
   - No additional setup required if using standard type mappings

2. **Custom Configuration**
   ```bash
   # Edit the configuration file
   vim $STARROCKS_HOME/conf/type_checker_config.xml
   
   # Or copy to a different location and symlink
   cp conf/type_checker_config.xml /path/to/custom_config.xml
   ln -s /path/to/custom_config.xml $STARROCKS_HOME/conf/type_checker_config.xml
   
   # Start StarRocks BE
   ./bin/start_be.sh
   ```

3. **Validation**
   - Check BE logs for configuration loading messages:
   ```
   INFO: TypeCheckerManager initialized from XML configuration: /path/to/config.xml
   ```
   - Or for errors:
   ```
   ERROR: Failed to load type checker configuration from XML: /path/to/config.xml
   ERROR: All type checkers must be defined in XML. Please ensure the configuration file exists.
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
Message: Failed to parse XML configuration file: /path/to/config.xml
Behavior: System logs ERROR and continues with empty checker map
```

### Malformed XML
```
Status: InvalidArgument  
Message: Invalid XML: root element must be <type-checkers>
Behavior: XML loading fails, system logs ERROR
```

### Missing Attributes
```
Status: InvalidArgument
Message: type-mapping element missing java_class attribute
Behavior: XML loading fails with detailed error message
```

### Invalid Type Names
```
Status: InvalidArgument
Message: Invalid logical type in type-rule: allowed=TYPE_UNKNOWN, return=TYPE_INVALID
Behavior: XML loading fails with detailed error message
```

### Empty Configuration
```
Status: InvalidArgument
Message: No valid type mappings found in XML configuration
Behavior: XML loading fails, system logs ERROR
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
  <type-mapping java_class="java.lang.String" display_name="String">
    <type-rule allowed_type="TYPE_VARCHAR" return_type="TYPE_VARCHAR"/>
  </type-mapping>
</type-checkers>
```

2. Verify loading:
```bash
# Place your test XML at the default location
cp /tmp/test_config.xml $STARROCKS_HOME/conf/type_checker_config.xml
# Or without STARROCKS_HOME
cp /tmp/test_config.xml conf/type_checker_config.xml
# Start BE and check logs
```

## Adding Custom Type Mappings

To add a new type mapping:

1. Edit your XML configuration file (or `conf/type_checker_config.xml`)
2. Add a new `<type-mapping>` element with appropriate type rules:
   ```xml
   <type-mapping java_class="com.example.CustomType" display_name="CustomType">
     <type-rule allowed_type="TYPE_VARCHAR" return_type="TYPE_VARCHAR"/>
     <!-- Add more type rules as needed -->
   </type-mapping>
   ```
3. Restart StarRocks BE
4. Verify loading in BE logs

**No C++ code changes required** - all type checking logic is in the XML configuration!

## Best Practices

1. **Version Control**: Keep your XML configuration in version control
2. **Validation**: Validate XML changes before deployment using XML validators
3. **Comments**: Use XML comments to document custom type mappings
4. **Testing**: Test configuration changes in a non-production environment first
5. **Backup**: Keep a backup of working configuration before making changes
6. **Documentation**: Document any custom type rules and their rationale

## Troubleshooting

### Configuration Not Loading

1. Check environment variable:
   ```bash
   echo $STARROCKS_HOME
   ```

2. Verify file exists and has correct permissions:
   ```bash
   ls -la $STARROCKS_HOME/conf/type_checker_config.xml
   # Or if STARROCKS_HOME is not set
   ls -la conf/type_checker_config.xml
   ```

3. Check BE logs for error messages

4. Validate XML syntax:
   ```bash
   xmllint --noout conf/type_checker_config.xml
   ```

### Type Checker Not Working

1. Verify the Java class name is correct in XML (case-sensitive, fully qualified)
2. Ensure the `display_name` attribute is present
3. Check that at least one `<type-rule>` element is defined
4. Verify type names match exactly (e.g., `TYPE_INT` not `int`)
5. Review BE logs for warnings about failed checker creation

### XML Parsing Errors

1. Ensure XML is well-formed (matching open/close tags)
2. Verify all attributes are properly quoted
3. Check for special characters that need escaping
4. Use XML validator: `xmllint conf/type_checker_config.xml`

## Migration from Previous Versions

If you're upgrading from a version with hardcoded type checkers:

1. **XML Configuration is Now Mandatory**: Ensure `conf/type_checker_config.xml` exists
2. **No Hardcoded Fallback**: The system will not fall back to hardcoded configuration
3. **New XML Format**: Update any custom XML to use `<type-rule>` elements instead of `checker` attribute
4. **Verify Configuration**: Test your XML configuration before deployment

### Old Format (No Longer Supported)
```xml
<!-- Old format - NOT SUPPORTED -->
<type-mapping java_class="java.lang.Integer" checker="IntegerTypeChecker"/>
```

### New Format (Required)
```xml
<!-- New format - REQUIRED -->
<type-mapping java_class="java.lang.Integer" display_name="Integer">
  <type-rule allowed_type="TYPE_INT" return_type="TYPE_INT"/>
</type-mapping>
```

## Architecture

The type checker system consists of:

- **`ConfigurableTypeChecker`**: Single type checker class that validates based on configured rules
- **`TypeCheckerXMLLoader`**: Parses XML using libxml2 and creates ConfigurableTypeChecker instances
- **`TypeCheckerManager`**: Manages type checker registry and routes type checking requests

All type validation logic is now data-driven via XML configuration, eliminating the need for specialized type checker classes.

## See Also

- Type Checker Implementation: `be/src/types/checker/type_checker.h`
- XML Loader: `be/src/types/checker/type_checker_xml_loader.h`
- Type Checker Manager: `be/src/types/type_checker_manager.h`
- Default Configuration: `conf/type_checker_config.xml`
- Unit Tests: `be/test/types/type_checker_test.cpp`, `be/test/types/type_checker_xml_loader_test.cpp`

