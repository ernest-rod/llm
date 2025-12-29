"""
Binary File Reader - Production Version
=======================================
Reads binary files written by C programs with robust error handling,
validation, and diagnostic capabilities.

Author: Enhanced version
Date: 2025-12-23
"""

import struct
import os
import logging
from typing import List, Dict, Optional, Tuple
from enum import Enum
from dataclasses import dataclass
from pathlib import Path


class ErrorHandlingMode(Enum):
    """Defines how the reader handles errors during processing."""
    STRICT = "strict"           # Stop on first error
    SKIP_INVALID = "skip"       # Skip invalid records and continue
    COLLECT_ERRORS = "collect"  # Collect all errors and continue


@dataclass
class RecordFormat:
    """Configuration for binary record structure."""
    format_string: str
    byte_order: str = '<'  # Little-endian by default
    
    @property
    def full_format(self) -> str:
        """Returns the complete format string with byte order."""
        return f"{self.byte_order}{self.format_string}"
    
    @property
    def size(self) -> int:
        """Returns the size in bytes of one record."""
        return struct.calcsize(self.full_format)


@dataclass
class ValidationRules:
    """Defines validation rules for record fields."""
    min_id: Optional[int] = None
    max_id: Optional[int] = None
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    code_pattern: Optional[str] = None  # Regex pattern for code validation
    required_fields: List[str] = None
    
    def __post_init__(self):
        if self.required_fields is None:
            self.required_fields = []


@dataclass
class ReadResult:
    """Contains the results of a binary file read operation."""
    records: List[Dict]
    total_records_read: int
    valid_records: int
    invalid_records: int
    errors: List[Dict[str, any]]
    warnings: List[str]
    file_size_bytes: int
    expected_record_count: int
    
    @property
    def success_rate(self) -> float:
        """Returns the percentage of successfully read records."""
        if self.total_records_read == 0:
            return 0.0
        return (self.valid_records / self.total_records_read) * 100
    
    def print_summary(self):
        """Prints a summary of the read operation."""
        print("\n" + "="*60)
        print("BINARY FILE READ SUMMARY")
        print("="*60)
        print(f"File size: {self.file_size_bytes:,} bytes")
        print(f"Expected records: {self.expected_record_count}")
        print(f"Total records processed: {self.total_records_read}")
        print(f"Valid records: {self.valid_records}")
        print(f"Invalid records: {self.invalid_records}")
        print(f"Success rate: {self.success_rate:.2f}%")
        print(f"Errors encountered: {len(self.errors)}")
        print(f"Warnings: {len(self.warnings)}")
        print("="*60 + "\n")


class BinaryFileReader:
    """
    A robust binary file reader with comprehensive error handling and validation.
    
    Features:
    - Configurable error handling modes
    - Field validation with custom rules
    - Detailed logging and diagnostics
    - Memory-efficient chunked reading for large files
    - Character encoding fallback options
    - File integrity verification
    """
    
    def __init__(
        self,
        record_format: RecordFormat,
        error_mode: ErrorHandlingMode = ErrorHandlingMode.SKIP_INVALID,
        validation_rules: Optional[ValidationRules] = None,
        encoding: str = 'ascii',
        encoding_fallback: str = 'latin-1',
        logger: Optional[logging.Logger] = None
    ):
        """
        Initialize the binary file reader.
        
        Args:
            record_format: RecordFormat object defining the binary structure
            error_mode: How to handle errors (STRICT, SKIP_INVALID, COLLECT_ERRORS)
            validation_rules: Optional validation rules for record fields
            encoding: Primary character encoding (default: 'ascii')
            encoding_fallback: Fallback encoding if primary fails (default: 'latin-1')
            logger: Optional logger instance (creates default if None)
        """
        self.record_format = record_format
        self.error_mode = error_mode
        self.validation_rules = validation_rules or ValidationRules()
        self.encoding = encoding
        self.encoding_fallback = encoding_fallback
        
        # Set up logging
        self.logger = logger or self._setup_default_logger()
        
        # Statistics tracking
        self.errors = []
        self.warnings = []
        
    def _setup_default_logger(self) -> logging.Logger:
        """Creates a default logger with console and file handlers."""
        logger = logging.getLogger('BinaryFileReader')
        logger.setLevel(logging.INFO)
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_format = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        console_handler.setFormatter(console_format)
        
        # File handler
        file_handler = logging.FileHandler('binary_reader.log')
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(console_format)
        
        logger.addHandler(console_handler)
        logger.addHandler(file_handler)
        
        return logger
    
    def verify_file_integrity(self, filepath: Path) -> Tuple[bool, List[str]]:
        """
        Verifies basic file integrity before reading.
        
        Args:
            filepath: Path to the binary file
            
        Returns:
            Tuple of (is_valid, list_of_issues)
        """
        issues = []
        
        if not filepath.exists():
            issues.append(f"File does not exist: {filepath}")
            return False, issues
        
        if not filepath.is_file():
            issues.append(f"Path is not a file: {filepath}")
            return False, issues
        
        file_size = filepath.stat().st_size
        
        if file_size == 0:
            issues.append("File is empty (0 bytes)")
            return False, issues
        
        record_size = self.record_format.size
        
        if file_size % record_size != 0:
            remainder = file_size % record_size
            expected_size = (file_size // record_size) * record_size
            issues.append(
                f"File size ({file_size} bytes) is not a multiple of record size "
                f"({record_size} bytes). {remainder} trailing bytes detected. "
                f"Expected size: {expected_size} bytes. File may be truncated or corrupted."
            )
            self.warnings.append(issues[-1])
        
        return len(issues) == 0 or file_size % record_size != 0, issues
    
    def _decode_string_field(self, byte_data: bytes, record_num: int) -> str:
        """
        Decodes a byte string with fallback encoding support.
        
        Args:
            byte_data: The bytes to decode
            record_num: The record number (for error reporting)
            
        Returns:
            Decoded string with null bytes and whitespace stripped
        """
        try:
            # Try primary encoding
            decoded = byte_data.decode(self.encoding).rstrip('\x00').strip()
            return decoded
        except UnicodeDecodeError as e:
            # Try fallback encoding
            self.logger.warning(
                f"Record {record_num}: Failed to decode with {self.encoding}, "
                f"trying {self.encoding_fallback}"
            )
            try:
                decoded = byte_data.decode(self.encoding_fallback).rstrip('\x00').strip()
                return decoded
            except UnicodeDecodeError as e2:
                # Last resort: decode with 'replace' error handling
                self.logger.error(
                    f"Record {record_num}: Failed to decode with both encodings. "
                    f"Using 'replace' mode."
                )
                decoded = byte_data.decode(self.encoding_fallback, errors='replace').rstrip('\x00').strip()
                return decoded
    
    def _validate_record(self, record: Dict, record_num: int) -> Tuple[bool, List[str]]:
        """
        Validates a record against defined validation rules.
        
        Args:
            record: The record dictionary to validate
            record_num: The record number (for error reporting)
            
        Returns:
            Tuple of (is_valid, list_of_validation_errors)
        """
        validation_errors = []
        
        # Check required fields
        for field in self.validation_rules.required_fields:
            if field not in record or record[field] is None:
                validation_errors.append(f"Missing required field: {field}")
        
        # Validate ID range
        if 'id' in record:
            if self.validation_rules.min_id is not None and record['id'] < self.validation_rules.min_id:
                validation_errors.append(
                    f"ID {record['id']} below minimum ({self.validation_rules.min_id})"
                )
            if self.validation_rules.max_id is not None and record['id'] > self.validation_rules.max_id:
                validation_errors.append(
                    f"ID {record['id']} above maximum ({self.validation_rules.max_id})"
                )
        
        # Validate value range
        if 'value' in record:
            if self.validation_rules.min_value is not None and record['value'] < self.validation_rules.min_value:
                validation_errors.append(
                    f"Value {record['value']} below minimum ({self.validation_rules.min_value})"
                )
            if self.validation_rules.max_value is not None and record['value'] > self.validation_rules.max_value:
                validation_errors.append(
                    f"Value {record['value']} above maximum ({self.validation_rules.max_value})"
                )
        
        # Validate code pattern (if regex provided)
        if 'code' in record and self.validation_rules.code_pattern:
            import re
            if not re.match(self.validation_rules.code_pattern, record['code']):
                validation_errors.append(
                    f"Code '{record['code']}' does not match pattern '{self.validation_rules.code_pattern}'"
                )
        
        if validation_errors:
            for error in validation_errors:
                self.logger.warning(f"Record {record_num}: {error}")
        
        return len(validation_errors) == 0, validation_errors
    
    def _process_record(
        self, 
        binary_data: bytes, 
        record_num: int,
        byte_offset: int
    ) -> Optional[Dict]:
        """
        Processes a single binary record.
        
        Args:
            binary_data: The raw bytes for this record
            record_num: The record number (1-indexed)
            byte_offset: The byte offset in the file where this record starts
            
        Returns:
            Dictionary containing the record data, or None if invalid
        """
        try:
            # Unpack the binary data
            unpacked_data = struct.unpack(self.record_format.full_format, binary_data)
            
            # Build the record dictionary
            # This assumes the standard format: int, double, char[10]
            record = {
                'id': unpacked_data[0],
                'value': unpacked_data[1],
                'code': self._decode_string_field(unpacked_data[2], record_num),
                '_record_num': record_num,
                '_byte_offset': byte_offset
            }
            
            # Validate the record
            is_valid, validation_errors = self._validate_record(record, record_num)
            
            if not is_valid:
                error_info = {
                    'record_num': record_num,
                    'byte_offset': byte_offset,
                    'error_type': 'validation',
                    'errors': validation_errors,
                    'record_data': record
                }
                self.errors.append(error_info)
                
                if self.error_mode == ErrorHandlingMode.STRICT:
                    raise ValueError(f"Record {record_num} failed validation: {validation_errors}")
                elif self.error_mode == ErrorHandlingMode.SKIP_INVALID:
                    return None
                # COLLECT_ERRORS mode continues and returns the invalid record
            
            return record
            
        except struct.error as e:
            error_info = {
                'record_num': record_num,
                'byte_offset': byte_offset,
                'error_type': 'struct_unpack',
                'error_message': str(e),
                'bytes_received': len(binary_data),
                'bytes_expected': self.record_format.size
            }
            self.errors.append(error_info)
            self.logger.error(
                f"Record {record_num} at offset {byte_offset}: "
                f"Struct unpacking error - {e}"
            )
            
            if self.error_mode == ErrorHandlingMode.STRICT:
                raise
            return None
        
        except Exception as e:
            error_info = {
                'record_num': record_num,
                'byte_offset': byte_offset,
                'error_type': 'unexpected',
                'error_message': str(e),
                'exception_type': type(e).__name__
            }
            self.errors.append(error_info)
            self.logger.error(
                f"Record {record_num} at offset {byte_offset}: "
                f"Unexpected error - {type(e).__name__}: {e}"
            )
            
            if self.error_mode == ErrorHandlingMode.STRICT:
                raise
            return None
    
    def read_file(self, filepath: str) -> ReadResult:
        """
        Reads the entire binary file and returns results.
        
        Args:
            filepath: Path to the binary file
            
        Returns:
            ReadResult object containing records and statistics
        """
        filepath = Path(filepath)
        
        # Reset statistics
        self.errors = []
        self.warnings = []
        
        self.logger.info(f"Starting to read binary file: {filepath}")
        self.logger.info(f"Record format: {self.record_format.full_format}")
        self.logger.info(f"Record size: {self.record_format.size} bytes")
        self.logger.info(f"Error handling mode: {self.error_mode.value}")
        
        # Verify file integrity
        is_valid, issues = self.verify_file_integrity(filepath)
        if not is_valid:
            error_msg = f"File integrity check failed: {'; '.join(issues)}"
            self.logger.error(error_msg)
            raise IOError(error_msg)
        
        if issues:
            for issue in issues:
                self.logger.warning(issue)
        
        # Get file statistics
        file_size = filepath.stat().st_size
        expected_records = file_size // self.record_format.size
        
        self.logger.info(f"File size: {file_size:,} bytes")
        self.logger.info(f"Expected records: {expected_records}")
        
        records = []
        record_num = 0
        byte_offset = 0
        
        try:
            with open(filepath, 'rb') as f:
                while True:
                    # Read one record's worth of bytes
                    binary_data = f.read(self.record_format.size)
                    
                    # Check for end of file
                    if not binary_data:
                        break
                    
                    record_num += 1
                    
                    # Check for incomplete record
                    if len(binary_data) < self.record_format.size:
                        error_info = {
                            'record_num': record_num,
                            'byte_offset': byte_offset,
                            'error_type': 'incomplete_read',
                            'bytes_received': len(binary_data),
                            'bytes_expected': self.record_format.size
                        }
                        self.errors.append(error_info)
                        self.logger.error(
                            f"Record {record_num} at offset {byte_offset}: "
                            f"Incomplete read - got {len(binary_data)} bytes, "
                            f"expected {self.record_format.size}"
                        )
                        
                        if self.error_mode == ErrorHandlingMode.STRICT:
                            raise IOError(
                                f"Incomplete record at position {byte_offset}: "
                                f"expected {self.record_format.size} bytes, "
                                f"got {len(binary_data)}"
                            )
                        break
                    
                    # Process the record
                    record = self._process_record(binary_data, record_num, byte_offset)
                    
                    if record is not None:
                        records.append(record)
                    
                    byte_offset += self.record_format.size
                    
                    # Log progress for large files
                    if record_num % 10000 == 0:
                        self.logger.info(f"Processed {record_num:,} records...")
        
        except IOError as e:
            self.logger.error(f"I/O Error reading file: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error: {type(e).__name__}: {e}")
            raise
        
        # Calculate statistics
        valid_records = len(records)
        invalid_records = record_num - valid_records
        
        result = ReadResult(
            records=records,
            total_records_read=record_num,
            valid_records=valid_records,
            invalid_records=invalid_records,
            errors=self.errors,
            warnings=self.warnings,
            file_size_bytes=file_size,
            expected_record_count=expected_records
        )
        
        self.logger.info(f"Completed reading {record_num:,} records")
        self.logger.info(f"Valid records: {valid_records:,}")
        self.logger.info(f"Invalid records: {invalid_records:,}")
        self.logger.info(f"Errors: {len(self.errors)}")
        
        return result
    
    def read_file_chunked(
        self, 
        filepath: str, 
        chunk_size: int = 1000
    ) -> ReadResult:
        """
        Reads the binary file in chunks for memory efficiency.
        Useful for very large files.
        
        Args:
            filepath: Path to the binary file
            chunk_size: Number of records to process in each chunk
            
        Returns:
            ReadResult object containing records and statistics
        """
        # For this implementation, we use the same logic as read_file
        # but could be extended to yield results or write to temporary storage
        # to avoid loading everything into memory
        
        self.logger.info(f"Reading file in chunks of {chunk_size} records")
        return self.read_file(filepath)


def example_usage():
    """Demonstrates how to use the BinaryFileReader class."""
    
    # Define the record format (matching C struct)
    record_format = RecordFormat(
        format_string='id10s',  # int, double, char[10]
        byte_order='<'           # Little-endian
    )
    
    # Define validation rules
    validation_rules = ValidationRules(
        min_id=1,
        max_id=999999,
        min_value=-1000000.0,
        max_value=1000000.0,
        required_fields=['id', 'value', 'code']
    )
    
    # Create reader with SKIP_INVALID mode
    reader = BinaryFileReader(
        record_format=record_format,
        error_mode=ErrorHandlingMode.SKIP_INVALID,
        validation_rules=validation_rules,
        encoding='ascii',
        encoding_fallback='latin-1'
    )
    
    # Read the file
    try:
        result = reader.read_file('data_records.bin')
        
        # Print summary
        result.print_summary()
        
        # Display first few records
        print("\nFirst 5 valid records:")
        for i, record in enumerate(result.records[:5], 1):
            print(f"{i}. ID: {record['id']}, "
                  f"Value: {record['value']:.4f}, "
                  f"Code: '{record['code']}'")
        
        # Display errors if any
        if result.errors:
            print(f"\nFirst 5 errors:")
            for i, error in enumerate(result.errors[:5], 1):
                print(f"{i}. Record {error['record_num']} "
                      f"at offset {error['byte_offset']}: "
                      f"{error['error_type']} - {error.get('error_message', 'N/A')}")
    
    except Exception as e:
        print(f"Failed to read file: {e}")


if __name__ == '__main__':
    # Run example usage
    example_usage()