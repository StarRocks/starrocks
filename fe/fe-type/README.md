# StarRocks FE Type System (fe-type)

This module contains the Frontend (FE) logical type system. It defines the full set of SQL-visible types and related helpers used by the analyzer, optimizer, catalog, and planner.

## Responsibilities
- Define the type hierarchy in `com.starrocks.type`:
  - Core: `Type`, `ScalarType`, `PrimitiveType`
  - Complex: `ArrayType`, `MapType`, `StructType`, `StructField`, `JsonType`
  - String and binary: `CharType`, `VarcharType`, `StringType`, `VarbinaryType`
  - Numerics and date/time: `IntegerType`, `FloatType`, `DecimalType`, `DateType`
  - Special/aggregate: `BitmapType`, `HLLType`, `PercentileType`, `FunctionType`, `VariantType`, `AggStateDesc`
  - Wildcards and helpers: `Any*` types, `ComplexTypeAccessPath`, `ComplexTypeAccessPathType`
- Keep this module free of FE business logic; it is a pure model layer used across FE.

## Relationship to other FE modules
- Construction entry points live in `fe/fe-core` via `TypeFactory` (e.g., `TypeFactory.createCharType`, `createUnifiedDecimalType`, `createDecimalV3Type`, etc.).
- Analyzer, optimizer, catalog, and planner consume `com.starrocks.type.*` from this module.
- Connectors/plugins should not expose FE types over SPI; avoid leaking `com.starrocks.type.*` across SPI boundaries.

## Decimal notes
- Decimal V2 vs V3 is selected by configuration (`Config.enable_decimal_v3`).
- Large precision handling may fall back to `DOUBLE` or cap to `DECIMAL256` based on session variables (see `TypeFactory`).

## Usage examples
- Prefer factory methods in FE core:
  - `TypeFactory.createCharType(10)`
  - `TypeFactory.createVarcharType(255)`
  - `TypeFactory.createUnifiedDecimalType(10, 2)`
  - `new ArrayType(TypeFactory.createVarcharType(100))`

## Development guidelines
- Do not add FE-specific behavior or I/O here; keep types immutable except for well-scoped, package-private setters used by `TypeFactory`.
- Respect `Type.MAX_NESTING_DEPTH` to avoid pathological structures.

## References
- FE overview: `fe/README.md`
- FE core factory: `fe/fe-core/src/main/java/com/starrocks/type/TypeFactory.java`
- Package: `com.starrocks.type`
