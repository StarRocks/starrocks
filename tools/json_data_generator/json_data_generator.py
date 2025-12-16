#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import argparse
import json
import random
import string
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple


class FieldSchema:
    """Represents a field schema with type and cardinality information."""
    
    def __init__(self, name: str, field_type: str, cardinality: str = 'medium'):
        self.name = name
        self.field_type = field_type
        self.cardinality = cardinality  # 'high', 'low', or 'medium'
    
    def __repr__(self):
        return f"FieldSchema(name={self.name}, type={self.field_type}, cardinality={self.cardinality})"


class ValueGenerator:
    """Generates values based on field type and cardinality."""
    
    def __init__(self, seed: Optional[int] = None):
        if seed is not None:
            random.seed(seed)
        self._low_cardinality_strings = [
            "red", "green", "blue", "yellow", "purple", "orange", "pink", "brown", "black", "white"
        ]
        self._low_cardinality_ints = [100, 200, 300, 400, 500]
        self._string_counter = 0
        self._int_counter = 0
    
    def generate_string(self, cardinality: str) -> str:
        """Generate a string value based on cardinality."""
        if cardinality == 'high':
            # Generate unique or near-unique values
            self._string_counter += 1
            random_part = ''.join(random.choices(string.ascii_letters + string.digits, k=8))
            return f"unique_value_{self._string_counter}_{random_part}"
        elif cardinality == 'low':
            # Generate from limited set
            return random.choice(self._low_cardinality_strings)
        else:  # medium
            # Generate semi-unique values with some repetition
            base = random.choice(["value", "data", "item", "record", "entry"])
            num = random.randint(1, 100)
            return f"{base}_{num}"
    
    def generate_int(self, cardinality: str) -> int:
        """Generate an integer value based on cardinality."""
        if cardinality == 'high':
            # Generate unique or near-unique values
            self._int_counter += 1
            return random.randint(100000000, 999999999) + self._int_counter
        elif cardinality == 'low':
            # Generate from limited set
            return random.choice(self._low_cardinality_ints)
        else:  # medium
            # Generate from moderate range
            return random.randint(1, 10000)
    
    def generate_bool(self) -> bool:
        """Generate a boolean value."""
        return random.choice([True, False])
    
    def generate_datetime(self) -> str:
        """Generate a datetime-formatted string."""
        start_date = datetime(2020, 1, 1)
        end_date = datetime(2025, 12, 31)
        random_date = start_date + timedelta(
            seconds=random.randint(0, int((end_date - start_date).total_seconds()))
        )
        return random_date.strftime('%Y-%m-%d %H:%M:%S')
    
    def generate_array(self, field_type: str, cardinality: str, size: int = 3) -> List[Any]:
        """Generate an array of values."""
        array = []
        for _ in range(size):
            if field_type == 'string':
                array.append(self.generate_string(cardinality))
            elif field_type == 'int':
                array.append(self.generate_int(cardinality))
            elif field_type == 'bool':
                array.append(self.generate_bool())
            elif field_type == 'datetime':
                array.append(self.generate_datetime())
            else:
                array.append(self.generate_string(cardinality))
        return array


class JSONDataGenerator:
    """Main class for generating JSON data with configurable characteristics."""
    
    def __init__(self, 
                 num_fields: int = 10,
                 sparsity: float = 0.0,
                 max_depth: int = 4,
                 nest_probability: float = 0.2,
                 field_types: List[str] = None,
                 high_cardinality_fields: int = 0,
                 low_cardinality_fields: int = 0,
                 seed: Optional[int] = None):
        self.num_fields = num_fields
        self.sparsity = sparsity
        self.max_depth = max_depth
        self.nest_probability = nest_probability
        self.field_types = field_types or ['string', 'int', 'bool']
        self.high_cardinality_fields = high_cardinality_fields
        self.low_cardinality_fields = low_cardinality_fields
        self.seed = seed
        
        if seed is not None:
            random.seed(seed)
        
        self.value_generator = ValueGenerator(seed)
        self.field_schemas = self._generate_field_schemas()
    
    def _generate_field_schemas(self) -> List[FieldSchema]:
        """Generate field schemas with types and cardinality assignments."""
        schemas = []
        
        # Determine cardinality for each field
        field_cardinalities = []
        for i in range(self.num_fields):
            if i < self.high_cardinality_fields:
                field_cardinalities.append('high')
            elif i < self.high_cardinality_fields + self.low_cardinality_fields:
                field_cardinalities.append('low')
            else:
                field_cardinalities.append('medium')
        
        # Generate field schemas
        for i in range(self.num_fields):
            field_name = f"field_{i+1}"
            field_type = random.choice(self.field_types)
            cardinality = field_cardinalities[i]
            schemas.append(FieldSchema(field_name, field_type, cardinality))
        
        return schemas
    
    def _generate_value(self, schema: FieldSchema, depth: int = 0) -> Any:
        """Generate a value for a field schema, supporting nesting."""
        # Check if we should create a nested object
        if (schema.field_type == 'object' or 
            (schema.field_type in self.field_types and 
             depth < self.max_depth and 
             random.random() < self.nest_probability)):
            return self._generate_nested_object(depth + 1)
        
        # Generate value based on type
        if schema.field_type == 'string':
            return self.value_generator.generate_string(schema.cardinality)
        elif schema.field_type == 'int':
            return self.value_generator.generate_int(schema.cardinality)
        elif schema.field_type == 'bool':
            return self.value_generator.generate_bool()
        elif schema.field_type == 'datetime':
            return self.value_generator.generate_datetime()
        elif schema.field_type == 'array':
            # For arrays, use the first non-array type or default to string
            array_type = next((t for t in self.field_types if t != 'array'), 'string')
            return self.value_generator.generate_array(array_type, schema.cardinality)
        elif schema.field_type == 'object':
            return self._generate_nested_object(depth + 1)
        else:
            return self.value_generator.generate_string(schema.cardinality)
    
    def _generate_nested_object(self, depth: int = 0) -> Dict[str, Any]:
        """Generate a nested object with random fields."""
        if depth >= self.max_depth:
            # At max depth, generate a simple value
            return self.value_generator.generate_string('medium')
        
        nested_obj = {}
        num_nested_fields = random.randint(2, min(5, self.num_fields))
        
        for i in range(num_nested_fields):
            nested_field_name = f"nested_{depth}_{i+1}"
            # Choose a type for nested field
            nested_type = random.choice(self.field_types)
            
            # Determine cardinality (use medium for nested fields by default)
            nested_cardinality = 'medium'
            if random.random() < 0.3:  # 30% chance of high cardinality
                nested_cardinality = 'high'
            elif random.random() < 0.5:  # 50% chance of low cardinality
                nested_cardinality = 'low'
            
            nested_schema = FieldSchema(nested_field_name, nested_type, nested_cardinality)
            
            # Recursively generate nested value
            if nested_type == 'object' and depth + 1 < self.max_depth:
                nested_obj[nested_field_name] = self._generate_nested_object(depth + 1)
            else:
                nested_obj[nested_field_name] = self._generate_value(nested_schema, depth + 1)
        
        return nested_obj
    
    def generate_record(self) -> Dict[str, Any]:
        """Generate a single JSON record."""
        record = {}
        
        for schema in self.field_schemas:
            # Apply sparsity: randomly omit fields
            if random.random() < self.sparsity:
                continue  # Omit this field
            
            record[schema.name] = self._generate_value(schema)
        
        return record
    
    def generate(self, num_records: int, output_file: Optional[str] = None, pretty: bool = False):
        """Generate JSONL records and write to file or stdout."""
        output = open(output_file, 'w') if output_file else sys.stdout
        
        try:
            for i in range(num_records):
                record = self.generate_record()
                if pretty:
                    json_str = json.dumps(record, indent=2, ensure_ascii=False)
                    output.write(json_str + '\n')
                else:
                    json_str = json.dumps(record, ensure_ascii=False)
                    output.write(json_str + '\n')
        finally:
            if output_file and output != sys.stdout:
                output.close()


def parse_field_types(field_types_str: str) -> List[str]:
    """Parse field types from comma-separated string."""
    valid_types = ['string', 'int', 'bool', 'datetime', 'array', 'object']
    types = [t.strip() for t in field_types_str.split(',')]
    
    for t in types:
        if t not in valid_types:
            raise ValueError(f"Invalid field type: {t}. Valid types are: {', '.join(valid_types)}")
    
    return types


def main():
    parser = argparse.ArgumentParser(
        description='Generate JSONL data files with configurable characteristics',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate 1000 records with 50 fields, 30% sparsity
  python json_data_generator.py --num-records 1000 --num-fields 50 --sparsity 0.3 --output data.jsonl
  
  # Generate with high and low cardinality fields
  python json_data_generator.py --num-records 100 --num-fields 20 \\
    --high-cardinality-fields 5 --low-cardinality-fields 10 --output data.jsonl
  
  # Generate with nested objects
  python json_data_generator.py --num-records 500 --max-depth 5 \\
    --nest-probability 0.3 --field-types string,int,object --output nested.jsonl
        """
    )
    
    parser.add_argument('--num-records', type=int, default=1000,
                       help='Number of JSON records to generate (default: 1000)')
    parser.add_argument('--num-fields', type=int, default=10,
                       help='Number of top-level fields per record (default: 10)')
    parser.add_argument('--sparsity', type=float, default=0.0,
                       help='Sparsity ratio (0.0 to 1.0). Fields are randomly omitted based on this ratio (default: 0.0)')
    parser.add_argument('--max-depth', type=int, default=4,
                       help='Maximum nesting depth for nested objects (default: 4)')
    parser.add_argument('--nest-probability', type=float, default=0.2,
                       help='Probability of creating nested objects (0.0 to 1.0) (default: 0.2)')
    parser.add_argument('--field-types', type=str, default='string,int,bool',
                       help='Comma-separated list of field types: string,int,bool,datetime,array,object (default: string,int,bool)')
    parser.add_argument('--high-cardinality-fields', type=int, default=0,
                       help='Number of fields with high cardinality (default: 0)')
    parser.add_argument('--low-cardinality-fields', type=int, default=0,
                       help='Number of fields with low cardinality (default: 0)')
    parser.add_argument('--seed', type=int, default=None,
                       help='Random seed for reproducible data generation (default: None)')
    parser.add_argument('--output', type=str, default=None,
                       help='Output file path (default: stdout)')
    parser.add_argument('--pretty', action='store_true',
                       help='Pretty-print JSON (default: False)')
    
    args = parser.parse_args()
    
    # Validate arguments
    if args.num_records < 0:
        parser.error('--num-records must be non-negative')
    if args.num_fields < 0:
        parser.error('--num-fields must be non-negative')
    if not 0.0 <= args.sparsity <= 1.0:
        parser.error('--sparsity must be between 0.0 and 1.0')
    if args.max_depth < 1:
        parser.error('--max-depth must be at least 1')
    if not 0.0 <= args.nest_probability <= 1.0:
        parser.error('--nest-probability must be between 0.0 and 1.0')
    if args.high_cardinality_fields < 0:
        parser.error('--high-cardinality-fields must be non-negative')
    if args.low_cardinality_fields < 0:
        parser.error('--low-cardinality-fields must be non-negative')
    if args.high_cardinality_fields + args.low_cardinality_fields > args.num_fields:
        parser.error('Sum of --high-cardinality-fields and --low-cardinality-fields cannot exceed --num-fields')
    
    # Parse field types
    try:
        field_types = parse_field_types(args.field_types)
    except ValueError as e:
        parser.error(str(e))
    
    # Create generator
    generator = JSONDataGenerator(
        num_fields=args.num_fields,
        sparsity=args.sparsity,
        max_depth=args.max_depth,
        nest_probability=args.nest_probability,
        field_types=field_types,
        high_cardinality_fields=args.high_cardinality_fields,
        low_cardinality_fields=args.low_cardinality_fields,
        seed=args.seed
    )
    
    # Generate data
    generator.generate(args.num_records, args.output, args.pretty)


if __name__ == '__main__':
    main()
