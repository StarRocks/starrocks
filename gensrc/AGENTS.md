# AGENTS.md - StarRocks Generated Source Code

> Guidelines for AI coding agents working with generated source code.
> **Important**: Do NOT manually edit generated files.

## Overview

This directory contains generated source code from Thrift and Protobuf definitions. These files are auto-generated during the build process.

## Directory Structure

```
gensrc/
├── thrift/              # Thrift definition files (.thrift)
├── proto/               # Protobuf definition files (.proto)
└── build/               # Generated output (created during build)
    ├── gen-cpp/         # Generated C++ code
    └── gen-java/        # Generated Java code
```

## Golden Rule

**NEVER manually edit files in `gensrc/build/`**

These files are regenerated on every build. Any manual changes will be lost.

## When to Modify

Modify the **source definition files**, not the generated output:

| To change... | Edit this file | Not this |
|--------------|----------------|----------|
| FE-BE RPC interface | `gensrc/thrift/*.thrift` | `build/gen-*/` |
| Storage format | `gensrc/proto/*.proto` | `build/gen-*/` |

## Regenerating Code

Code is regenerated automatically during build:

```bash
# Full rebuild (regenerates all)
./build.sh --fe --be --clean

# Regenerate Thrift only
cd gensrc && ./gen_thrift.sh

# Regenerate Protobuf only
cd gensrc && ./gen_proto.sh
```

## Thrift Guidelines

### File Naming
- Lowercase with underscores: `my_service.thrift`

### Struct Naming
- Prefix with `T`: `TMyStruct`

```thrift
struct TTabletInfo {
    1: optional i64 tablet_id;
    2: optional i64 schema_hash;
}
```

### Field Rules
- **NEVER** use `required` (breaks forward/backward compatibility)
- **NEVER** change field ordinals (numbers)
- Always use `optional`

```thrift
// Good
struct TMyStruct {
    1: optional i64 field_one;
    2: optional string field_two;
    // Adding new field - use next available number
    3: optional bool field_three;
}

// BAD - Don't do this
struct TMyStruct {
    1: required i64 field_one;     // Never use required!
    3: optional string field_two;  // Changed from 2 to 3 - breaks compatibility!
}
```

### Service Definition

```thrift
service BackendService {
    TStatus submit_task(1: TTaskRequest request);
}
```

## Protobuf Guidelines

### File Naming
- Lowercase with underscores: `my_message.proto`

### Message Naming
- PascalCase with `PB` suffix: `MyMessagePB`

```protobuf
message TabletInfoPB {
    optional int64 tablet_id = 1;
    optional int64 schema_hash = 2;
}
```

### Field Rules
- **NEVER** use `required` (deprecated in proto3)
- **NEVER** change field numbers
- Use `optional` (proto2) or implicit optional (proto3)

```protobuf
// Good
message MyMessagePB {
    optional int64 field_one = 1;
    optional string field_two = 2;
    // Adding new field
    optional bool field_three = 3;
}

// BAD
message MyMessagePB {
    required int64 field_one = 1;  // Never use required!
    optional string field_two = 3; // Changed from 2 to 3 - breaks!
}
```

### Deprecating Fields

Don't remove fields, mark as deprecated:

```protobuf
message MyMessagePB {
    optional int64 field_one = 1;
    optional string field_two = 2 [deprecated = true];
    optional string field_two_v2 = 3;  // New replacement
}
```

## Common Tasks

### Adding a New RPC Method

1. Edit the `.thrift` file in `gensrc/thrift/`
2. Run `./build.sh --fe --be` to regenerate
3. Implement the handler in BE (`be/src/service/`)
4. Implement the caller in FE (`fe/fe-core/.../rpc/`)

### Adding a New Storage Field

1. Edit the `.proto` file in `gensrc/proto/`
2. Run `./build.sh --be` to regenerate
3. Update reading/writing code in storage layer

### Checking Generated Files

After regenerating, verify:
```bash
# Check generated C++ compiles
./build.sh --be

# Check generated Java compiles
./build.sh --fe
```

## Version Compatibility

Thrift/Protobuf changes affect upgrade/downgrade compatibility:

| Change Type | Safe? | Notes |
|-------------|-------|-------|
| Add optional field | Yes | Use next available number |
| Remove field | No | Mark deprecated instead |
| Change field number | No | Breaks all existing data |
| Change field type | No | Breaks compatibility |
| Rename field | Careful | Number must stay same |

## Debugging Generated Code

If you see serialization errors:

1. Check field numbers match between versions
2. Verify no required fields were added
3. Check for type mismatches

## Related Documentation

- Thrift guide: `docs/en/developers/code-style-guides/thrift-guides.md`
- Protobuf guide: `docs/en/developers/code-style-guides/protobuf-guides.md`
