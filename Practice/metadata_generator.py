#!/usr/bin/env python3
"""
CSV Metadata Generator for C Program Integration
==================================================

This script analyzes CSV files and generates metadata files containing field names
and C-compatible data types. The metadata files are designed to be consumed by
a generic C program for CSV-to-binary conversion.

Author: Production Data Pipeline Team
Version: 1.0.0
Date: 2025-12-31

Usage:
    python csv_metadata_generator.py <csv_file_path> [--max-rows MAX_ROWS] [--log-file LOG_FILE]

Arguments:
    csv_file_path    : Full path to the CSV file to analyze
    --max-rows       : Maximum number of rows to sample (default: 10000)
    --log-file       : Path to log file (default: csv_metadata_generator.log)

Output:
    Creates a .description file with the same base name as the CSV file
    containing field names and C-compatible data types.

Exit Codes:
    0 - Success
    1 - File not found or permission error
    2 - Data type inconsistency detected
    3 - Invalid CSV format
    4 - Other processing errors
"""

import sys
import csv
import argparse
import logging
import re
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Set, Tuple, Optional
from collections import defaultdict
import traceback


# C-compatible data type mapping
C_TYPE_MAPPING = {
    'integer': 'int',           # 32-bit signed integer
    'long': 'long long',        # 64-bit signed integer
    'float': 'float',           # 32-bit floating point
    'double': 'double',         # 64-bit floating point
    'string': 'char*',          # Null-terminated string
    'date': 'char*',            # ISO date format string (YYYY-MM-DD)
    'datetime': 'char*',        # ISO datetime format string
    'boolean': 'int',           # Boolean as integer (0/1)
}


class DataTypeAnalyzer:
    """
    Analyzes data values to determine their appropriate C data type.
    
    Type inference hierarchy (from most specific to most general):
    1. NULL/Empty
    2. Boolean (true/false, yes/no, 0/1)
    3. Integer (whole numbers within int range)
    4. Long (whole numbers beyond int range)
    5. Float (decimal numbers with precision <= 7 digits)
    6. Double (decimal numbers with precision > 7 digits)
    7. Date (YYYY-MM-DD format)
    8. DateTime (ISO 8601 format)
    9. String (default fallback)
    """
    
    # Regular expression patterns for type detection
    INTEGER_PATTERN = re.compile(r'^-?\d+$')
    FLOAT_PATTERN = re.compile(r'^-?\d*\.\d+$|^-?\d+\.\d*$')
    DATE_PATTERN = re.compile(r'^\d{4}-\d{2}-\d{2}$')
    DATETIME_PATTERN = re.compile(r'^\d{4}-\d{2}-\d{2}[T\s]\d{2}:\d{2}:\d{2}')
    BOOLEAN_PATTERN = re.compile(r'^(true|false|yes|no|0|1)$', re.IGNORECASE)
    
    # Integer range limits for C int type (-2^31 to 2^31-1)
    INT_MIN = -2147483648
    INT_MAX = 2147483647
    
    @staticmethod
    def infer_type(value: str) -> str:
        """
        Infer the C-compatible data type of a given value.
        
        Args:
            value: String value to analyze
            
        Returns:
            String representing the inferred data type
        """
        # Handle empty or NULL values
        if not value or value.strip() == '' or value.upper() in ('NULL', 'NONE', 'NA', 'N/A'):
            return 'null'
        
        value = value.strip()
        
        # Check for boolean
        if DataTypeAnalyzer.BOOLEAN_PATTERN.match(value):
            return 'boolean'
        
        # Check for integer
        if DataTypeAnalyzer.INTEGER_PATTERN.match(value):
            try:
                int_val = int(value)
                if DataTypeAnalyzer.INT_MIN <= int_val <= DataTypeAnalyzer.INT_MAX:
                    return 'integer'
                else:
                    return 'long'
            except (ValueError, OverflowError):
                return 'long'
        
        # Check for floating point
        if DataTypeAnalyzer.FLOAT_PATTERN.match(value):
            try:
                float_val = float(value)
                # Determine precision to decide between float and double
                decimal_places = len(value.split('.')[-1]) if '.' in value else 0
                if decimal_places <= 7 and abs(float_val) < 3.4e38:
                    return 'float'
                else:
                    return 'double'
            except (ValueError, OverflowError):
                return 'string'
        
        # Check for date (YYYY-MM-DD)
        if DataTypeAnalyzer.DATE_PATTERN.match(value):
            try:
                datetime.strptime(value, '%Y-%m-%d')
                return 'date'
            except ValueError:
                pass
        
        # Check for datetime
        if DataTypeAnalyzer.DATETIME_PATTERN.match(value):
            return 'datetime'
        
        # Default to string
        return 'string'
    
    @staticmethod
    def is_compatible(type1: str, type2: str) -> bool:
        """
        Check if two data types are compatible or can be promoted.
        
        Type promotion hierarchy:
        - null can be promoted to any type
        - integer can be promoted to long, float, double, or string
        - long can be promoted to double or string
        - float can be promoted to double or string
        - boolean can be promoted to integer, long, or string
        - date/datetime can be promoted to string
        - string is the universal fallback
        
        Args:
            type1: First data type
            type2: Second data type
            
        Returns:
            True if types are compatible, False otherwise
        """
        if type1 == type2:
            return True
        
        if 'null' in (type1, type2):
            return True
        
        # Define promotion paths
        promotions = {
            'boolean': {'integer', 'long', 'string'},
            'integer': {'long', 'float', 'double', 'string'},
            'long': {'double', 'string'},
            'float': {'double', 'string'},
            'double': {'string'},
            'date': {'datetime', 'string'},
            'datetime': {'string'},
        }
        
        return type2 in promotions.get(type1, set()) or type1 in promotions.get(type2, set())
    
    @staticmethod
    def promote_type(type1: str, type2: str) -> str:
        """
        Determine the promoted type when two types conflict.
        
        Args:
            type1: First data type
            type2: Second data type
            
        Returns:
            The promoted data type
        """
        if type1 == type2:
            return type1
        
        if type1 == 'null':
            return type2
        if type2 == 'null':
            return type1
        
        # Promotion hierarchy (order matters)
        hierarchy = ['boolean', 'integer', 'long', 'float', 'double', 'date', 'datetime', 'string']
        
        try:
            idx1 = hierarchy.index(type1)
            idx2 = hierarchy.index(type2)
            return hierarchy[max(idx1, idx2)]
        except ValueError:
            return 'string'


class CSVMetadataGenerator:
    """
    Main class for generating metadata files from CSV files.
    """
    
    def __init__(self, csv_file_path: str, max_rows: int = 10000, log_file: str = None):
        """
        Initialize the metadata generator.
        
        Args:
            csv_file_path: Path to the CSV file to analyze
            max_rows: Maximum number of rows to sample for type inference
            log_file: Path to log file (optional)
        """
        self.csv_file_path = Path(csv_file_path)
        self.max_rows = max_rows
        self.log_file = log_file or 'csv_metadata_generator.log'
        self.logger = self._setup_logging()
        
        # Metadata storage
        self.field_names: List[str] = []
        self.field_types: Dict[str, str] = {}
        self.field_type_samples: Dict[str, List[str]] = defaultdict(list)
        self.rows_processed = 0
        self.inconsistencies: List[str] = []
        
    def _setup_logging(self) -> logging.Logger:
        """
        Configure logging for the application.
        
        Returns:
            Configured logger instance
        """
        logger = logging.getLogger('CSVMetadataGenerator')
        logger.setLevel(logging.DEBUG)
        
        # File handler
        fh = logging.FileHandler(self.log_file, mode='w', encoding='utf-8')
        fh.setLevel(logging.DEBUG)
        
        # Console handler
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(logging.INFO)
        
        # Formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)
        
        logger.addHandler(fh)
        logger.addHandler(ch)
        
        return logger
    
    def validate_csv_file(self) -> bool:
        """
        Validate that the CSV file exists and is readable.
        
        Returns:
            True if valid, False otherwise
        """
        if not self.csv_file_path.exists():
            self.logger.error(f"CSV file not found: {self.csv_file_path}")
            return False
        
        if not self.csv_file_path.is_file():
            self.logger.error(f"Path is not a file: {self.csv_file_path}")
            return False
        
        try:
            with open(self.csv_file_path, 'r', encoding='utf-8') as f:
                f.read(1)
            return True
        except PermissionError:
            self.logger.error(f"Permission denied reading file: {self.csv_file_path}")
            return False
        except Exception as e:
            self.logger.error(f"Error accessing file: {e}")
            return False
    
    def analyze_csv(self) -> bool:
        """
        Analyze the CSV file to infer data types for each field.
        
        Returns:
            True if analysis succeeded, False otherwise
        """
        self.logger.info(f"Starting analysis of: {self.csv_file_path}")
        self.logger.info(f"Maximum rows to sample: {self.max_rows}")
        
        try:
            with open(self.csv_file_path, 'r', encoding='utf-8', newline='') as csvfile:
                # Auto-detect CSV dialect
                sample = csvfile.read(8192)
                csvfile.seek(0)
                
                try:
                    dialect = csv.Sniffer().sniff(sample)
                    self.logger.debug(f"Detected CSV dialect: delimiter='{dialect.delimiter}'")
                except csv.Error:
                    dialect = csv.excel
                    self.logger.debug("Using default Excel dialect")
                
                reader = csv.DictReader(csvfile, dialect=dialect)
                
                # Extract field names
                self.field_names = reader.fieldnames
                if not self.field_names:
                    self.logger.error("No field names found in CSV file")
                    return False
                
                self.logger.info(f"Found {len(self.field_names)} fields: {', '.join(self.field_names)}")
                
                # Initialize field types
                for field in self.field_names:
                    self.field_types[field] = 'null'
                
                # Process rows
                for row_num, row in enumerate(reader, start=2):  # Start at 2 (1 is header)
                    if row_num > self.max_rows + 1:
                        self.logger.info(f"Reached maximum row limit: {self.max_rows}")
                        break
                    
                    self.rows_processed += 1
                    
                    # Analyze each field in the row
                    for field in self.field_names:
                        value = row.get(field, '')
                        inferred_type = DataTypeAnalyzer.infer_type(value)
                        current_type = self.field_types[field]
                        
                        # Track type samples for inconsistency detection
                        if inferred_type != 'null':
                            self.field_type_samples[field].append(inferred_type)
                        
                        # Check compatibility and promote if necessary
                        if current_type == 'null':
                            self.field_types[field] = inferred_type
                        elif inferred_type != 'null' and current_type != inferred_type:
                            if DataTypeAnalyzer.is_compatible(current_type, inferred_type):
                                promoted_type = DataTypeAnalyzer.promote_type(current_type, inferred_type)
                                if promoted_type != current_type:
                                    self.logger.warning(
                                        f"Field '{field}' type promoted from '{current_type}' to '{promoted_type}' "
                                        f"at row {row_num} (value: '{value}')"
                                    )
                                    self.field_types[field] = promoted_type
                            else:
                                msg = (
                                    f"INCONSISTENCY DETECTED: Field '{field}' has incompatible types - "
                                    f"'{current_type}' vs '{inferred_type}' at row {row_num} (value: '{value}')"
                                )
                                self.logger.error(msg)
                                self.inconsistencies.append(msg)
                    
                    # Progress logging every 10000 rows
                    if row_num % 10000 == 0:
                        self.logger.debug(f"Processed {row_num - 1} rows...")
                
                self.logger.info(f"Successfully analyzed {self.rows_processed} rows")
                
                # Check for inconsistencies
                if self.inconsistencies:
                    self.logger.error(f"Found {len(self.inconsistencies)} data type inconsistencies")
                    return False
                
                # Finalize types (convert 'null' fields to 'string' as safe default)
                for field in self.field_names:
                    if self.field_types[field] == 'null':
                        self.field_types[field] = 'string'
                        self.logger.warning(f"Field '{field}' had all NULL values, defaulting to 'string'")
                
                return True
                
        except csv.Error as e:
            self.logger.error(f"CSV parsing error: {e}")
            return False
        except Exception as e:
            self.logger.error(f"Unexpected error during analysis: {e}")
            self.logger.debug(traceback.format_exc())
            return False
    
    def generate_metadata_file(self) -> bool:
        """
        Generate the .description metadata file with C-compatible types.
        
        The metadata file format:
        - Line 1: Number of fields
        - Subsequent lines: field_name|c_data_type
        
        Returns:
            True if metadata file was created successfully, False otherwise
        """
        output_file = self.csv_file_path.with_suffix(self.csv_file_path.suffix + '.description')
        
        try:
            self.logger.info(f"Generating metadata file: {output_file}")
            
            with open(output_file, 'w', encoding='utf-8') as f:
                # Write number of fields
                f.write(f"{len(self.field_names)}\n")
                
                # Write field definitions
                for field in self.field_names:
                    inferred_type = self.field_types[field]
                    c_type = C_TYPE_MAPPING.get(inferred_type, 'char*')
                    f.write(f"{field}|{c_type}\n")
            
            self.logger.info(f"Metadata file created successfully: {output_file}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error writing metadata file: {e}")
            self.logger.debug(traceback.format_exc())
            return False
    
    def print_summary(self):
        """
        Print a summary of the metadata generation process.
        """
        print("\n" + "=" * 80)
        print("CSV METADATA GENERATION SUMMARY")
        print("=" * 80)
        print(f"CSV File:           {self.csv_file_path}")
        print(f"Rows Processed:     {self.rows_processed:,}")
        print(f"Total Fields:       {len(self.field_names)}")
        print(f"Max Rows Sampled:   {self.max_rows:,}")
        print(f"Log File:           {self.log_file}")
        print("-" * 80)
        print("FIELD METADATA:")
        print("-" * 80)
        print(f"{'Field Name':<30} {'Inferred Type':<15} {'C Type':<15}")
        print("-" * 80)
        
        for field in self.field_names:
            inferred_type = self.field_types[field]
            c_type = C_TYPE_MAPPING.get(inferred_type, 'char*')
            print(f"{field:<30} {inferred_type:<15} {c_type:<15}")
        
        print("-" * 80)
        
        if self.inconsistencies:
            print(f"\n⚠️  WARNING: {len(self.inconsistencies)} data type inconsistencies detected!")
            print("Review the log file for details.")
        else:
            print("\n✓ No data type inconsistencies detected.")
        
        output_file = self.csv_file_path.with_suffix(self.csv_file_path.suffix + '.description')
        print(f"\nMetadata file: {output_file}")
        print("=" * 80 + "\n")


def main():
    """
    Main entry point for the CSV metadata generator.
    """
    parser = argparse.ArgumentParser(
        description='Generate C-compatible metadata files from CSV files',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python csv_metadata_generator.py data/customers.csv
  python csv_metadata_generator.py data/transactions.csv --max-rows 50000
  python csv_metadata_generator.py data/large_file.csv --max-rows 100000 --log-file analysis.log
        """
    )
    
    parser.add_argument(
        'csv_file',
        type=str,
        help='Full path to the CSV file to analyze'
    )
    
    parser.add_argument(
        '--max-rows',
        type=int,
        default=10000,
        help='Maximum number of rows to sample for type inference (default: 10000)'
    )
    
    parser.add_argument(
        '--log-file',
        type=str,
        default='csv_metadata_generator.log',
        help='Path to log file (default: csv_metadata_generator.log)'
    )
    
    args = parser.parse_args()
    
    # Create generator instance
    generator = CSVMetadataGenerator(
        csv_file_path=args.csv_file,
        max_rows=args.max_rows,
        log_file=args.log_file
    )
    
    # Validate CSV file
    if not generator.validate_csv_file():
        sys.exit(1)
    
    # Analyze CSV
    if not generator.analyze_csv():
        generator.print_summary()
        sys.exit(2)
    
    # Generate metadata file
    if not generator.generate_metadata_file():
        generator.print_summary()
        sys.exit(4)
    
    # Print summary
    generator.print_summary()
    
    sys.exit(0)


if __name__ == '__main__':
    main()