# JSON Data Generator

A Python tool for generating JSONL (JSON Lines) data files with configurable characteristics.

## Parameters

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `--num-records` | int | 1000 | Number of JSON records to generate |
| `--num-fields` | int | 10 | Number of top-level fields per record |
| `--sparsity` | float | 0.0 | Sparsity ratio (0.0 to 1.0). Fields are randomly omitted based on this ratio |
| `--max-depth` | int | 4 | Maximum nesting depth for nested objects |
| `--nest-probability` | float | 0.2 | Probability of creating nested objects (0.0 to 1.0) |
| `--field-types` | string | `string,int,bool` | Comma-separated list of field types: string, int, bool, datetime, array, object |
| `--high-cardinality-fields` | int | 0 | Number of fields with high cardinality (unique values) |
| `--low-cardinality-fields` | int | 0 | Number of fields with low cardinality (limited value sets) |
| `--seed` | int | None | Random seed for reproducible data generation |
| `--output` | string | None | Output file path (default: stdout) |
| `--pretty` | flag | False | Pretty-print JSON |

## Example

```bash
python json_data_generator.py \
  --num-records 1000 \
  --num-fields 50 \
  --sparsity 0.3 \
  --max-depth 5 \
  --nest-probability 0.2 \
  --field-types string,int,bool,datetime,array,object \
  --high-cardinality-fields 10 \
  --low-cardinality-fields 15 \
  --seed 42 \
  --output data.jsonl
```

This generates 1000 JSONL records with 50 fields per record, where the first 10 fields have high cardinality, the next 15 fields have low cardinality, 30% of fields are randomly omitted, and nested objects can be up to 5 levels deep.
