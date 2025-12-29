/*
 * customer_convert_v2.c - Production Version
 * 
 * Purpose: Converts customer data from CSV format to binary format
 *          Production-ready with validation, error handling, and high-volume support
 * 
 * Input:  CSV file (default: data_full\customers.csv)
 *         Validation rules (optional: validation_rules.txt)
 * Output: Binary file (default: data\customers.binary)
 *         Error log (conversion_errors.log)
 *         Summary report (conversion_summary.txt)
 * 
 * Features:
 * - Configurable validation rules from external file
 * - Batch writing for high-volume datasets
 * - Comprehensive error handling and logging
 * - Buffer overflow protection
 * - Memory safety
 * - Resume capability from checkpoint
 * - Performance metrics
 * 
 * Author: Production Data Pipeline Team
 * Date: 2025
 * Version: 2.0
 * Platform: Windows
 * 
 * Compilation (Windows with MinGW/MSVC):
 *   gcc -O2 -Wall -Wextra customer_convert_v2.c -o customer_convert_v2.exe
 *   cl /O2 /W4 customer_convert_v2.c
 * 
 * Usage:
 *   customer_convert_v2.exe [input_csv] [output_binary] [validation_file]
 *   customer_convert_v2.exe
 *   customer_convert_v2.exe data_full\customers.csv data\customers.binary
 *   customer_convert_v2.exe input.csv output.bin validation_rules.txt
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <errno.h>
#include <ctype.h>
#include <sys/stat.h>
#include <stdarg.h>

/* Windows-specific includes */
#ifdef _WIN32
    #include <direct.h>
    #include <io.h>
    #include <windows.h>
#endif

/* Version information */
#define VERSION "2.0"
#define BUILD_DATE __DATE__

/* Configuration constants */
#define MAX_FIRST_NAME 50
#define MAX_LAST_NAME 50
#define MAX_EMAIL 100
#define MAX_PHONE 20
#define MAX_CITY 50
#define MAX_STATE 3
#define MAX_ZIP_CODE 10
#define MAX_DATE 11
#define MAX_LINE 2048
#define MAX_PATH_LEN 512

/* Performance tuning */
#define WRITE_BUFFER_SIZE 1000
#define PROGRESS_INTERVAL 1000
#define CHECKPOINT_INTERVAL 5000
#define FLUSH_INTERVAL 10000

/* Validation error codes */
#define VAL_OK                  0x0000
#define VAL_ERR_INVALID_ID      0x0001
#define VAL_ERR_INVALID_EMAIL   0x0002
#define VAL_ERR_INVALID_PHONE   0x0004
#define VAL_ERR_INVALID_DATE    0x0008
#define VAL_ERR_INVALID_STATE   0x0010
#define VAL_ERR_INVALID_ZIP     0x0020
#define VAL_ERR_EMPTY_FIELD     0x0040
#define VAL_ERR_FIELD_TOO_LONG  0x0080

/* Log levels */
typedef enum {
    LOG_ERROR,
    LOG_WARNING,
    LOG_INFO,
    LOG_DEBUG
} LogLevel;

/* Customer structure with explicit padding for consistency */
#pragma pack(push, 1)
typedef struct {
    int customer_id;
    char first_name[MAX_FIRST_NAME];
    char last_name[MAX_LAST_NAME];
    char email[MAX_EMAIL];
    char phone[MAX_PHONE];
    char city[MAX_CITY];
    char state[MAX_STATE];
    char zip_code[MAX_ZIP_CODE];
    char registration_date[MAX_DATE];
} Customer;
#pragma pack(pop)

/* Validation rules structure */
typedef struct {
    int validate_email;
    int validate_phone;
    int validate_date;
    int validate_state;
    int validate_zip;
    int allow_empty_fields;
    int strict_mode;
} ValidationRules;

/* Statistics structure */
typedef struct {
    int total_lines;
    int processed_records;
    int successful_records;
    int failed_records;
    int validation_warnings;
    int validation_errors;
    time_t start_time;
    time_t end_time;
    long long bytes_written;
} ConversionStats;

/* Global variables */
static FILE *error_log = NULL;
static FILE *debug_log = NULL;
static LogLevel current_log_level = LOG_INFO;
static ValidationRules validation_rules;
static ConversionStats stats;

/* Function prototypes */
void init_globals(void);
void cleanup_globals(void);
void log_message(LogLevel level, const char *format, ...);
void log_parse_error(int line_num, const char *line, const char *reason);
void log_validation_warning(int line_num, int error_code);
int create_directory(const char *path);
char* trim_whitespace(char *str);
char* secure_strncpy(char *dest, const char *src, size_t dest_size);
int safe_atoi(const char *str, int *value);
void sanitize_input(char *str);
int load_validation_rules(const char *filename, ValidationRules *rules);
int validate_customer_id(const char *str);
int validate_email(const char *email);
int validate_phone(const char *phone);
int validate_date(const char *date);
int validate_state(const char *state);
int validate_zip(const char *zip);
int validate_customer(Customer *customer, int line_num);
int parse_csv_line(char *line, Customer *customer, int line_num);
int write_batch(FILE *binary, Customer *buffer, int count);
void save_checkpoint(int records_processed);
int load_checkpoint(void);
void print_progress(int records, int total_estimate);
void print_summary_report(const char *input_file, const char *output_file);
int save_summary_report(const char *input_file, const char *output_file);

/*
 * Function: init_globals
 * Description: Initialize global variables and state
 */
void init_globals(void) {
    memset(&stats, 0, sizeof(ConversionStats));
    stats.start_time = time(NULL);
    
    /* Default validation rules */
    validation_rules.validate_email = 1;
    validation_rules.validate_phone = 1;
    validation_rules.validate_date = 1;
    validation_rules.validate_state = 1;
    validation_rules.validate_zip = 1;
    validation_rules.allow_empty_fields = 0;
    validation_rules.strict_mode = 1;
    
    /* Open log files */
    error_log = fopen("conversion_errors.log", "w");
    if (error_log == NULL) {
        fprintf(stderr, "WARNING: Could not create error log file\n");
    } else {
        fprintf(error_log, "Customer Conversion Error Log\n");
        fprintf(error_log, "Started: %s\n", ctime(&stats.start_time));
        fprintf(error_log, "========================================\n\n");
    }
    
    if (current_log_level >= LOG_DEBUG) {
        debug_log = fopen("conversion_debug.log", "w");
        if (debug_log) {
            fprintf(debug_log, "Customer Conversion Debug Log\n");
            fprintf(debug_log, "Version: %s\n", VERSION);
            fprintf(debug_log, "Built: %s\n\n", BUILD_DATE);
        }
    }
}

/*
 * Function: cleanup_globals
 * Description: Clean up global resources
 */
void cleanup_globals(void) {
    if (error_log != NULL) {
        fclose(error_log);
        error_log = NULL;
    }
    
    if (debug_log != NULL) {
        fclose(debug_log);
        debug_log = NULL;
    }
}

/*
 * Function: log_message
 * Description: Log a message with specified level
 */
void log_message(LogLevel level, const char *format, ...) {
    if (level > current_log_level) return;
    
    const char *level_str[] = {"ERROR", "WARNING", "INFO", "DEBUG"};
    FILE *output = (level == LOG_ERROR) ? stderr : stdout;
    
    va_list args;
    va_start(args, format);
    
    fprintf(output, "[%s] ", level_str[level]);
    vfprintf(output, format, args);
    fprintf(output, "\n");
    
    va_end(args);
    
    /* Also log to debug file if available */
    if (debug_log != NULL && level <= LOG_DEBUG) {
        va_start(args, format);
        fprintf(debug_log, "[%s] ", level_str[level]);
        vfprintf(debug_log, format, args);
        fprintf(debug_log, "\n");
        fflush(debug_log);
        va_end(args);
    }
}

/*
 * Function: log_parse_error
 * Description: Log a parsing error with context
 */
void log_parse_error(int line_num, const char *line, const char *reason) {
    if (error_log == NULL) return;
    
    time_t now = time(NULL);
    char time_str[26];
    ctime_s(time_str, sizeof(time_str), &now);
    
    /* Remove newline from ctime */
    time_str[24] = '\0';
    
    fprintf(error_log, "[%s] Line %d: %s\n", time_str, line_num, reason);
    
    /* Sanitize line before logging (remove control characters) */
    char sanitized[MAX_LINE];
    int j = 0;
    for (int i = 0; i < (int)strlen(line) && i < MAX_LINE - 1 && j < MAX_LINE - 1; i++) {
        if (isprint((unsigned char)line[i]) || line[i] == ' ') {
            sanitized[j++] = line[i];
        }
    }
    sanitized[j] = '\0';
    
    fprintf(error_log, "  Content: %s\n\n", sanitized);
    fflush(error_log);
}

/*
 * Function: log_validation_warning
 * Description: Log validation warnings
 */
void log_validation_warning(int line_num, int error_code) {
    if (error_log == NULL) return;
    
    fprintf(error_log, "Line %d - Validation warnings:\n", line_num);
    
    if (error_code & VAL_ERR_INVALID_ID)
        fprintf(error_log, "  - Invalid customer ID format\n");
    if (error_code & VAL_ERR_INVALID_EMAIL)
        fprintf(error_log, "  - Invalid email format\n");
    if (error_code & VAL_ERR_INVALID_PHONE)
        fprintf(error_log, "  - Invalid phone format\n");
    if (error_code & VAL_ERR_INVALID_DATE)
        fprintf(error_log, "  - Invalid date format\n");
    if (error_code & VAL_ERR_INVALID_STATE)
        fprintf(error_log, "  - Invalid state code\n");
    if (error_code & VAL_ERR_INVALID_ZIP)
        fprintf(error_log, "  - Invalid zip code\n");
    if (error_code & VAL_ERR_EMPTY_FIELD)
        fprintf(error_log, "  - Empty required field\n");
    if (error_code & VAL_ERR_FIELD_TOO_LONG)
        fprintf(error_log, "  - Field exceeds maximum length\n");
    
    fprintf(error_log, "\n");
    fflush(error_log);
}

/*
 * Function: create_directory
 * Description: Creates a directory if it doesn't exist (Windows)
 */
int create_directory(const char *path) {
    struct _stat st = {0};
    
    if (_stat(path, &st) == -1) {
        if (_mkdir(path) != 0) {
            log_message(LOG_ERROR, "Failed to create directory '%s': %s", 
                       path, strerror(errno));
            return -1;
        }
        log_message(LOG_INFO, "Created directory: %s", path);
    }
    
    return 0;
}

/*
 * Function: trim_whitespace
 * Description: Removes leading and trailing whitespace
 */
char* trim_whitespace(char *str) {
    char *end;
    
    /* Trim leading space */
    while (isspace((unsigned char)*str)) str++;
    
    if (*str == 0) return str;
    
    /* Trim trailing space */
    end = str + strlen(str) - 1;
    while (end > str && isspace((unsigned char)*end)) end--;
    
    *(end + 1) = '\0';
    
    return str;
}

/*
 * Function: secure_strncpy
 * Description: Safe string copy with guaranteed null termination
 */
char* secure_strncpy(char *dest, const char *src, size_t dest_size) {
    if (dest == NULL || src == NULL || dest_size == 0) {
        return dest;
    }
    
    size_t i;
    for (i = 0; i < dest_size - 1 && src[i] != '\0'; i++) {
        dest[i] = src[i];
    }
    dest[i] = '\0';
    
    return dest;
}

/*
 * Function: safe_atoi
 * Description: Safe string to integer conversion with validation
 */
int safe_atoi(const char *str, int *value) {
    char *endptr;
    long val;
    
    if (str == NULL || *str == '\0') {
        return 0;
    }
    
    errno = 0;
    val = strtol(str, &endptr, 10);
    
    /* Check for conversion errors */
    if (errno == ERANGE || val > INT_MAX || val < INT_MIN) {
        return 0;
    }
    
    /* Check if entire string was consumed */
    if (*endptr != '\0') {
        return 0;
    }
    
    *value = (int)val;
    return 1;
}

/*
 * Function: sanitize_input
 * Description: Remove potentially dangerous characters from input
 */
void sanitize_input(char *str) {
    if (str == NULL) return;
    
    for (int i = 0; str[i] != '\0'; i++) {
        /* Remove null bytes and other control characters except tab/newline */
        if (str[i] < 32 && str[i] != '\t' && str[i] != '\n' && str[i] != '\r') {
            str[i] = ' ';
        }
        /* Remove potential SQL injection characters (defense in depth) */
        if (str[i] == '\'' || str[i] == '"' || str[i] == ';' || str[i] == '\\') {
            /* Allow these in data but log if validation is strict */
            if (validation_rules.strict_mode) {
                log_message(LOG_DEBUG, "Special character found in input: %c", str[i]);
            }
        }
    }
}

/*
 * Function: load_validation_rules
 * Description: Load validation rules from configuration file
 */
int load_validation_rules(const char *filename, ValidationRules *rules) {
    FILE *f;
    char line[256];
    int line_num = 0;
    
    /* Use defaults if no file specified */
    if (filename == NULL || strlen(filename) == 0) {
        log_message(LOG_INFO, "Using default validation rules");
        return 1;
    }
    
    f = fopen(filename, "r");
    if (f == NULL) {
        log_message(LOG_WARNING, "Could not open validation rules file '%s', using defaults", 
                   filename);
        return 0;
    }
    
    log_message(LOG_INFO, "Loading validation rules from: %s", filename);
    
    while (fgets(line, sizeof(line), f) != NULL) {
        char *trimmed;
        char key[128], value[128];
        
        line_num++;
        trimmed = trim_whitespace(line);
        
        /* Skip empty lines and comments */
        if (strlen(trimmed) == 0 || trimmed[0] == '#' || trimmed[0] == ';') {
            continue;
        }
        
        /* Parse key=value format */
        if (sscanf(trimmed, "%127[^=]=%127s", key, value) == 2) {
            char *key_trimmed = trim_whitespace(key);
            char *value_trimmed = trim_whitespace(value);
            
            /* Convert value to boolean */
            int bool_value = (strcmp(value_trimmed, "1") == 0 || 
                            strcmp(value_trimmed, "true") == 0 ||
                            strcmp(value_trimmed, "TRUE") == 0 ||
                            strcmp(value_trimmed, "yes") == 0 ||
                            strcmp(value_trimmed, "YES") == 0);
            
            /* Set rule based on key */
            if (strcmp(key_trimmed, "validate_email") == 0) {
                rules->validate_email = bool_value;
            } else if (strcmp(key_trimmed, "validate_phone") == 0) {
                rules->validate_phone = bool_value;
            } else if (strcmp(key_trimmed, "validate_date") == 0) {
                rules->validate_date = bool_value;
            } else if (strcmp(key_trimmed, "validate_state") == 0) {
                rules->validate_state = bool_value;
            } else if (strcmp(key_trimmed, "validate_zip") == 0) {
                rules->validate_zip = bool_value;
            } else if (strcmp(key_trimmed, "allow_empty_fields") == 0) {
                rules->allow_empty_fields = bool_value;
            } else if (strcmp(key_trimmed, "strict_mode") == 0) {
                rules->strict_mode = bool_value;
            } else {
                log_message(LOG_WARNING, "Unknown validation rule at line %d: %s", 
                           line_num, key_trimmed);
            }
        }
    }
    
    fclose(f);
    
    /* Log loaded rules */
    log_message(LOG_INFO, "Validation rules loaded:");
    log_message(LOG_INFO, "  Email validation: %s", rules->validate_email ? "ON" : "OFF");
    log_message(LOG_INFO, "  Phone validation: %s", rules->validate_phone ? "ON" : "OFF");
    log_message(LOG_INFO, "  Date validation: %s", rules->validate_date ? "ON" : "OFF");
    log_message(LOG_INFO, "  State validation: %s", rules->validate_state ? "ON" : "OFF");
    log_message(LOG_INFO, "  Zip validation: %s", rules->validate_zip ? "ON" : "OFF");
    log_message(LOG_INFO, "  Allow empty fields: %s", rules->allow_empty_fields ? "YES" : "NO");
    log_message(LOG_INFO, "  Strict mode: %s", rules->strict_mode ? "ON" : "OFF");
    
    return 1;
}

/*
 * Function: validate_customer_id
 * Description: Validate customer ID is a positive integer
 */
int validate_customer_id(const char *str) {
    int value;
    
    if (!safe_atoi(str, &value)) {
        return 0;
    }
    
    return (value > 0);
}

/*
 * Function: validate_email
 * Description: Validate email format (basic check)
 */
int validate_email(const char *email) {
    char *at, *dot;
    size_t len;
    
    if (email == NULL || *email == '\0') {
        return !validation_rules.allow_empty_fields ? 0 : 1;
    }
    
    len = strlen(email);
    
    /* Check minimum length */
    if (len < 6) return 0;  /* Minimum: a@b.co */
    
    /* Must contain exactly one @ */
    at = strchr(email, '@');
    if (at == NULL || at == email || strchr(at + 1, '@') != NULL) {
        return 0;
    }
    
    /* Must contain at least one . after @ */
    dot = strrchr(email, '.');
    if (dot == NULL || dot < at || dot == email + len - 1) {
        return 0;
    }
    
    /* Check for valid characters */
    for (size_t i = 0; i < len; i++) {
        char c = email[i];
        if (!isalnum((unsigned char)c) && c != '@' && c != '.' && 
            c != '_' && c != '-' && c != '+') {
            return 0;
        }
    }
    
    return 1;
}

/*
 * Function: validate_phone
 * Description: Validate phone format (XXX-XXX-XXXX)
 */
int validate_phone(const char *phone) {
    size_t len;
    
    if (phone == NULL || *phone == '\0') {
        return !validation_rules.allow_empty_fields ? 0 : 1;
    }
    
    len = strlen(phone);
    
    /* Check for standard format: XXX-XXX-XXXX (12 characters) */
    if (len != 12) return 0;
    
    /* Verify format */
    for (int i = 0; i < 12; i++) {
        if (i == 3 || i == 7) {
            if (phone[i] != '-') return 0;
        } else {
            if (!isdigit((unsigned char)phone[i])) return 0;
        }
    }
    
    return 1;
}

/*
 * Function: validate_date
 * Description: Validate date format (YYYY-MM-DD)
 */
int validate_date(const char *date) {
    int year, month, day;
    
    if (date == NULL || *date == '\0') {
        return !validation_rules.allow_empty_fields ? 0 : 1;
    }
    
    /* Check format: YYYY-MM-DD (10 characters) */
    if (strlen(date) != 10) return 0;
    
    if (date[4] != '-' || date[7] != '-') return 0;
    
    /* Verify all other characters are digits */
    for (int i = 0; i < 10; i++) {
        if (i != 4 && i != 7) {
            if (!isdigit((unsigned char)date[i])) return 0;
        }
    }
    
    /* Parse and validate date components */
    year = atoi(date);
    month = atoi(date + 5);
    day = atoi(date + 8);
    
    if (year < 1900 || year > 2100) return 0;
    if (month < 1 || month > 12) return 0;
    if (day < 1 || day > 31) return 0;
    
    /* Basic day validation per month */
    if (month == 2 && day > 29) return 0;
    if ((month == 4 || month == 6 || month == 9 || month == 11) && day > 30) {
        return 0;
    }
    
    return 1;
}

/*
 * Function: validate_state
 * Description: Validate state code (2 uppercase letters)
 */
int validate_state(const char *state) {
    if (state == NULL || *state == '\0') {
        return !validation_rules.allow_empty_fields ? 0 : 1;
    }
    
    /* Should be exactly 2 characters */
    if (strlen(state) != 2) return 0;
    
    /* Both should be uppercase letters */
    if (!isupper((unsigned char)state[0]) || !isupper((unsigned char)state[1])) {
        return 0;
    }
    
    return 1;
}

/*
 * Function: validate_zip
 * Description: Validate ZIP code (5 digits)
 */
int validate_zip(const char *zip) {
    if (zip == NULL || *zip == '\0') {
        return !validation_rules.allow_empty_fields ? 0 : 1;
    }
    
    /* Should be exactly 5 characters */
    if (strlen(zip) != 5) return 0;
    
    /* All should be digits */
    for (int i = 0; i < 5; i++) {
        if (!isdigit((unsigned char)zip[i])) return 0;
    }
    
    return 1;
}

/*
 * Function: validate_customer
 * Description: Validate all customer fields based on rules
 */
int validate_customer(Customer *customer, int line_num) {
    int error_code = VAL_OK;
    int is_valid = 1;
    
    /* Validate customer ID (always required) */
    if (customer->customer_id <= 0) {
        error_code |= VAL_ERR_INVALID_ID;
        is_valid = 0;
    }
    
    /* Check for empty required fields */
    if (!validation_rules.allow_empty_fields) {
        if (strlen(customer->first_name) == 0 || 
            strlen(customer->last_name) == 0) {
            error_code |= VAL_ERR_EMPTY_FIELD;
            is_valid = 0;
        }
    }
    
    /* Validate email */
    if (validation_rules.validate_email && strlen(customer->email) > 0) {
        if (!validate_email(customer->email)) {
            error_code |= VAL_ERR_INVALID_EMAIL;
            if (validation_rules.strict_mode) {
                is_valid = 0;
            } else {
                stats.validation_warnings++;
            }
        }
    }
    
    /* Validate phone */
    if (validation_rules.validate_phone && strlen(customer->phone) > 0) {
        if (!validate_phone(customer->phone)) {
            error_code |= VAL_ERR_INVALID_PHONE;
            if (validation_rules.strict_mode) {
                is_valid = 0;
            } else {
                stats.validation_warnings++;
            }
        }
    }
    
    /* Validate date */
    if (validation_rules.validate_date && strlen(customer->registration_date) > 0) {
        if (!validate_date(customer->registration_date)) {
            error_code |= VAL_ERR_INVALID_DATE;
            if (validation_rules.strict_mode) {
                is_valid = 0;
            } else {
                stats.validation_warnings++;
            }
        }
    }
    
    /* Validate state */
    if (validation_rules.validate_state && strlen(customer->state) > 0) {
        if (!validate_state(customer->state)) {
            error_code |= VAL_ERR_INVALID_STATE;
            if (validation_rules.strict_mode) {
                is_valid = 0;
            } else {
                stats.validation_warnings++;
            }
        }
    }
    
    /* Validate ZIP */
    if (validation_rules.validate_zip && strlen(customer->zip_code) > 0) {
        if (!validate_zip(customer->zip_code)) {
            error_code |= VAL_ERR_INVALID_ZIP;
            if (validation_rules.strict_mode) {
                is_valid = 0;
            } else {
                stats.validation_warnings++;
            }
        }
    }
    
    /* Log validation issues */
    if (error_code != VAL_OK) {
        if (!is_valid) {
            stats.validation_errors++;
        }
        log_validation_warning(line_num, error_code);
    }
    
    return is_valid;
}

/*
 * Function: parse_csv_line
 * Description: Parse CSV line with robust error handling
 */
int parse_csv_line(char *line, Customer *customer, int line_num) {
    char *field_start = line;
    char *field_end;
    int field_count = 0;
    char field_buffer[MAX_LINE];
    int in_quotes = 0;
    
    /* Initialize customer structure */
    memset(customer, 0, sizeof(Customer));
    
    /* Sanitize input for security */
    sanitize_input(line);
    
    while (*field_start != '\0' && field_count < 9) {
        int buffer_pos = 0;
        
        /* Handle quoted fields (CSV standard) */
        if (*field_start == '"') {
            in_quotes = 1;
            field_start++;
        }
        
        /* Extract field */
        field_end = field_start;
        while (*field_end != '\0') {
            if (in_quotes) {
                if (*field_end == '"') {
                    /* Check for escaped quote */
                    if (*(field_end + 1) == '"') {
                        field_buffer[buffer_pos++] = '"';
                        field_end += 2;
                        continue;
                    } else {
                        in_quotes = 0;
                        field_end++;
                        break;
                    }
                }
            } else {
                if (*field_end == ',') {
                    break;
                }
            }
            
            if (buffer_pos < MAX_LINE - 1) {
                field_buffer[buffer_pos++] = *field_end;
            }
            field_end++;
        }
        
        field_buffer[buffer_pos] = '\0';
        
        /* Trim whitespace */
        char *trimmed = trim_whitespace(field_buffer);
        
        /* Assign to appropriate field */
        switch (field_count) {
            case 0: /* customer_id */
                if (!safe_atoi(trimmed, &customer->customer_id)) {
                    log_parse_error(line_num, line, "Invalid customer ID");
                    return 0;
                }
                break;
            case 1: /* first_name */
                if (strlen(trimmed) >= MAX_FIRST_NAME) {
                    log_message(LOG_WARNING, "Line %d: First name truncated", line_num);
                    stats.validation_warnings++;
                }
                secure_strncpy(customer->first_name, trimmed, MAX_FIRST_NAME);
                break;
            case 2: /* last_name */
                if (strlen(trimmed) >= MAX_LAST_NAME) {
                    log_message(LOG_WARNING, "Line %d: Last name truncated", line_num);
                    stats.validation_warnings++;
                }
                secure_strncpy(customer->last_name, trimmed, MAX_LAST_NAME);
                break;
            case 3: /* email */
                if (strlen(trimmed) >= MAX_EMAIL) {
                    log_message(LOG_WARNING, "Line %d: Email truncated", line_num);
                    stats.validation_warnings++;
                }
                secure_strncpy(customer->email, trimmed, MAX_EMAIL);
                break;
            case 4: /* phone */
                secure_strncpy(customer->phone, trimmed, MAX_PHONE);
                break;
            case 5: /* city */
                if (strlen(trimmed) >= MAX_CITY) {
                    log_message(LOG_WARNING, "Line %d: City name truncated", line_num);
                    stats.validation_warnings++;
                }
                secure_strncpy(customer->city, trimmed, MAX_CITY);
                break;
            case 6: /* state */
                secure_strncpy(customer->state, trimmed, MAX_STATE);
                break;
            case 7: /* zip_code */
                secure_strncpy(customer->zip_code, trimmed, MAX_ZIP_CODE);
                break;
            case 8: /* registration_date */
                secure_strncpy(customer->registration_date, trimmed, MAX_DATE);
                break;
        }
        
        field_count++;
        
        /* Move to next field */
        if (*field_end == ',') {
            field_start = field_end + 1;
        } else {
            field_start = field_end;
        }
    }
    
    /* Verify we got all required fields */
    if (field_count != 9) {
        log_parse_error(line_num, line, "Incomplete record - missing fields");
        return 0;
    }
    
    return 1;
}

/*
 * Function: write_batch
 * Description: Write a batch of records to binary file
 */
int write_batch(FILE *binary, Customer *buffer, int count) {
    size_t written;
    
    if (count == 0) return 1;
    
    written = fwrite(buffer, sizeof(Customer), count, binary);
    
    if (written != (size_t)count) {
        log_message(LOG_ERROR, "Batch write failed: expected %d, wrote %zu", 
                   count, written);
        return 0;
    }
    
    stats.bytes_written += (long long)(written * sizeof(Customer));
    
    return 1;
}

/*
 * Function: save_checkpoint
 * Description: Save processing checkpoint for resume capability
 */
void save_checkpoint(int records_processed) {
    FILE *checkpoint = fopen(".conversion_checkpoint", "w");
    if (checkpoint != NULL) {
        fprintf(checkpoint, "%d\n", records_processed);
        fclose(checkpoint);
    }
}

/*
 * Function: load_checkpoint
 * Description: Load checkpoint to resume interrupted conversion
 */
int load_checkpoint(void) {
    FILE *checkpoint = fopen(".conversion_checkpoint", "r");
    int records = 0;
    
    if (checkpoint != NULL) {
        if (fscanf(checkpoint, "%d", &records) == 1) {
            log_message(LOG_INFO, "Found checkpoint at record %d", records);
        }
        fclose(checkpoint);
    }
    
    return records;
}

/*
 * Function: print_progress
 * Description: Display progress information
 */
void print_progress(int records, int total_estimate) {
    double percentage;
    time_t current_time = time(NULL);
    double elapsed = difftime(current_time, stats.start_time);
    double rate;
    
    if (total_estimate > 0) {
        percentage = (double)records / total_estimate * 100.0;
    } else {
        percentage = 0.0;
    }
    
    rate = (elapsed > 0) ? (records / elapsed) : 0;
    
    printf("\rProcessed: %d records", records);
    
    if (total_estimate > 0) {
        printf(" (%.1f%%)", percentage);
    }
    
    if (rate > 0) {
        printf(" - Rate: %.0f rec/sec", rate);
    }
    
    fflush(stdout);
}

/*
 * Function: print_summary_report
 * Description: Print conversion summary to console
 */
void print_summary_report(const char *input_file, const char *output_file) {
    double elapsed;
    double rate;
    double success_rate;
    
    stats.end_time = time(NULL);
    elapsed = difftime(stats.end_time, stats.start_time);
    rate = (elapsed > 0) ? (stats.successful_records / elapsed) : 0;
    
    if (stats.processed_records > 0) {
        success_rate = (double)stats.successful_records / stats.processed_records * 100.0;
    } else {
        success_rate = 0.0;
    }
    
    printf("\n\n");
    printf("================================================================================\n");
    printf("                     CONVERSION SUMMARY REPORT                                  \n");
    printf("================================================================================\n");
    printf("\n");
    printf("Input File:              %s\n", input_file);
    printf("Output File:             %s\n", output_file);
    printf("\n");
    printf("--- Processing Statistics ---\n");
    printf("Total lines read:        %d\n", stats.total_lines);
    printf("Records processed:       %d\n", stats.processed_records);
    printf("Successfully converted:  %d\n", stats.successful_records);
    printf("Failed records:          %d\n", stats.failed_records);
    printf("Success rate:            %.2f%%\n", success_rate);
    printf("\n");
    printf("--- Validation Statistics ---\n");
    printf("Validation errors:       %d\n", stats.validation_errors);
    printf("Validation warnings:     %d\n", stats.validation_warnings);
    printf("\n");
    printf("--- Performance Metrics ---\n");
    printf("Elapsed time:            %.2f seconds\n", elapsed);
    printf("Processing rate:         %.0f records/second\n", rate);
    printf("Record size:             %zu bytes\n", sizeof(Customer));
    printf("Total bytes written:     %lld bytes (%.2f MB)\n", 
           stats.bytes_written, stats.bytes_written / 1048576.0);
    printf("\n");
    
    if (stats.failed_records > 0 || stats.validation_errors > 0) {
        printf("*** WARNINGS ***\n");
        if (stats.failed_records > 0) {
            printf("  %d records failed conversion\n", stats.failed_records);
        }
        if (stats.validation_errors > 0) {
            printf("  %d validation errors detected\n", stats.validation_errors);
        }
        printf("  Check conversion_errors.log for details\n");
        printf("\n");
    }
    
    printf("================================================================================\n");
    
    if (stats.successful_records > 0) {
        printf("Status: COMPLETED %s\n", 
               (stats.failed_records == 0) ? "SUCCESSFULLY" : "WITH ERRORS");
    } else {
        printf("Status: FAILED - No records converted\n");
    }
    
    printf("================================================================================\n");
}

/*
 * Function: save_summary_report
 * Description: Save summary report to file
 */
int save_summary_report(const char *input_file, const char *output_file) {
    FILE *report = fopen("conversion_summary.txt", "w");
    double elapsed, rate, success_rate;
    
    if (report == NULL) {
        log_message(LOG_WARNING, "Could not create summary report file");
        return 0;
    }
    
    elapsed = difftime(stats.end_time, stats.start_time);
    rate = (elapsed > 0) ? (stats.successful_records / elapsed) : 0;
    success_rate = (stats.processed_records > 0) ? 
                   (double)stats.successful_records / stats.processed_records * 100.0 : 0.0;
    
    fprintf(report, "Customer Data Conversion Summary Report\n");
    fprintf(report, "Generated: %s\n", ctime(&stats.end_time));
    fprintf(report, "Version: %s\n", VERSION);
    fprintf(report, "========================================\n\n");
    
    fprintf(report, "Input File:  %s\n", input_file);
    fprintf(report, "Output File: %s\n\n", output_file);
    
    fprintf(report, "Processing Statistics:\n");
    fprintf(report, "  Total lines read:       %d\n", stats.total_lines);
    fprintf(report, "  Records processed:      %d\n", stats.processed_records);
    fprintf(report, "  Successfully converted: %d\n", stats.successful_records);
    fprintf(report, "  Failed records:         %d\n", stats.failed_records);
    fprintf(report, "  Success rate:           %.2f%%\n\n", success_rate);
    
    fprintf(report, "Validation Statistics:\n");
    fprintf(report, "  Validation errors:      %d\n", stats.validation_errors);
    fprintf(report, "  Validation warnings:    %d\n\n", stats.validation_warnings);
    
    fprintf(report, "Performance Metrics:\n");
    fprintf(report, "  Elapsed time:           %.2f seconds\n", elapsed);
    fprintf(report, "  Processing rate:        %.0f records/second\n", rate);
    fprintf(report, "  Total bytes written:    %lld bytes\n\n", stats.bytes_written);
    
    fprintf(report, "Configuration:\n");
    fprintf(report, "  Email validation:       %s\n", 
            validation_rules.validate_email ? "Enabled" : "Disabled");
    fprintf(report, "  Phone validation:       %s\n", 
            validation_rules.validate_phone ? "Enabled" : "Disabled");
    fprintf(report, "  Date validation:        %s\n", 
            validation_rules.validate_date ? "Enabled" : "Disabled");
    fprintf(report, "  State validation:       %s\n", 
            validation_rules.validate_state ? "Enabled" : "Disabled");
    fprintf(report, "  Zip validation:         %s\n", 
            validation_rules.validate_zip ? "Enabled" : "Disabled");
    fprintf(report, "  Strict mode:            %s\n\n", 
            validation_rules.strict_mode ? "Enabled" : "Disabled");
    
    fclose(report);
    
    log_message(LOG_INFO, "Summary report saved to: conversion_summary.txt");
    return 1;
}

/*
 * Function: main
 * Description: Main program entry point
 */
int main(int argc, char *argv[]) {
    FILE *csv_file = NULL;
    FILE *binary_file = NULL;
    char line[MAX_LINE];
    Customer *write_buffer = NULL;
    int buffer_count = 0;
    int line_number = 0;
    int ret_code = 0;
    int checkpoint_records = 0;
    int total_estimate = 0;
    
    char input_file[MAX_PATH_LEN] = "data_full\\customers.csv";
    char output_file[MAX_PATH_LEN] = "data\\customers.binary";
    char validation_file[MAX_PATH_LEN] = "";
    char output_dir[MAX_PATH_LEN];
    
    /* Initialize */
    init_globals();
    
    /* Print header */
    printf("================================================================================\n");
    printf("        Customer CSV to Binary Converter - Production Version %s\n", VERSION);
    printf("================================================================================\n");
    printf("Built: %s\n", BUILD_DATE);
    printf("Platform: Windows\n");
    printf("\n");
    
    /* Parse command-line arguments */
    if (argc > 1) {
        secure_strncpy(input_file, argv[1], MAX_PATH_LEN);
    }
    if (argc > 2) {
        secure_strncpy(output_file, argv[2], MAX_PATH_LEN);
    }
    if (argc > 3) {
        secure_strncpy(validation_file, argv[3], MAX_PATH_LEN);
    }
    
    /* Load validation rules */
    if (strlen(validation_file) > 0) {
        load_validation_rules(validation_file, &validation_rules);
    } else {
        log_message(LOG_INFO, "No validation file specified, using defaults");
    }
    
    /* Extract output directory and create it */
    {
        char *last_slash = strrchr(output_file, '\\');
        if (last_slash != NULL) {
            size_t dir_len = last_slash - output_file;
            if (dir_len < MAX_PATH_LEN) {
                strncpy(output_dir, output_file, dir_len);
                output_dir[dir_len] = '\0';
                
                log_message(LOG_INFO, "Creating output directory: %s", output_dir);
                if (create_directory(output_dir) != 0 && errno != EEXIST) {
                    log_message(LOG_ERROR, "Failed to create output directory");
                    cleanup_globals();
                    return 1;
                }
            }
        }
    }
    
    /* Check for checkpoint */
    checkpoint_records = load_checkpoint();
    if (checkpoint_records > 0) {
        char response[10];
        printf("Resume from checkpoint at record %d? (y/n): ", checkpoint_records);
        if (fgets(response, sizeof(response), stdin) != NULL) {
            if (response[0] != 'y' && response[0] != 'Y') {
                checkpoint_records = 0;
                remove(".conversion_checkpoint");
            }
        }
    }
    
    /* Allocate write buffer */
    write_buffer = (Customer *)malloc(WRITE_BUFFER_SIZE * sizeof(Customer));
    if (write_buffer == NULL) {
        log_message(LOG_ERROR, "Failed to allocate write buffer");
        cleanup_globals();
        return 1;
    }
    
    log_message(LOG_INFO, "Opening input file: %s", input_file);
    
    /* Open input CSV file */
    csv_file = fopen(input_file, "r");
    if (csv_file == NULL) {
        log_message(LOG_ERROR, "Could not open input file '%s': %s", 
                   input_file, strerror(errno));
        free(write_buffer);
        cleanup_globals();
        return 1;
    }
    
    /* Estimate total records for progress reporting */
    {
        long file_size;
        fseek(csv_file, 0, SEEK_END);
        file_size = ftell(csv_file);
        fseek(csv_file, 0, SEEK_SET);
        
        /* Rough estimate: average line is ~150 bytes */
        total_estimate = (int)(file_size / 150);
        log_message(LOG_INFO, "Estimated records: ~%d", total_estimate);
    }
    
    log_message(LOG_INFO, "Creating output file: %s", output_file);
    
    /* Open output binary file */
    binary_file = fopen(output_file, "wb");
    if (binary_file == NULL) {
        log_message(LOG_ERROR, "Could not create output file '%s': %s", 
                   output_file, strerror(errno));
        fclose(csv_file);
        free(write_buffer);
        cleanup_globals();
        return 1;
    }
    
    printf("\n");
    log_message(LOG_INFO, "Starting conversion...");
    printf("\n");
    
    /* Read and process CSV file line by line */
    while (fgets(line, sizeof(line), csv_file) != NULL) {
        Customer customer;
        
        line_number++;
        stats.total_lines++;
        
        /* Check for line truncation */
        {
            size_t len = strlen(line);
            if (len == sizeof(line) - 1 && line[len - 1] != '\n') {
                log_message(LOG_WARNING, "Line %d exceeds maximum length, may be truncated", 
                           line_number);
                
                /* Skip rest of line */
                int c;
                while ((c = fgetc(csv_file)) != '\n' && c != EOF);
            }
        }
        
        /* Skip header line */
        if (line_number == 1 && strstr(line, "customer_id") != NULL) {
            log_message(LOG_INFO, "Skipping header line");
            continue;
        }
        
        /* Skip empty lines */
        if (strlen(trim_whitespace(line)) == 0) {
            continue;
        }
        
        /* Skip if resuming and not at checkpoint yet */
        if (checkpoint_records > 0 && stats.processed_records < checkpoint_records) {
            stats.processed_records++;
            continue;
        }
        
        stats.processed_records++;
        
        /* Parse CSV line */
        if (!parse_csv_line(line, &customer, line_number)) {
            stats.failed_records++;
            continue;
        }
        
        /* Validate customer data */
        if (!validate_customer(&customer, line_number)) {
            stats.failed_records++;
            if (validation_rules.strict_mode) {
                log_message(LOG_WARNING, "Line %d: Record failed validation (strict mode)", 
                           line_number);
                continue;
            }
        }
        
        /* Add to write buffer */
        write_buffer[buffer_count++] = customer;
        
        /* Flush buffer when full */
        if (buffer_count >= WRITE_BUFFER_SIZE) {
            if (!write_batch(binary_file, write_buffer, buffer_count)) {
                log_message(LOG_ERROR, "Failed to write batch at record %d", 
                           stats.successful_records);
                ret_code = 1;
                break;
            }
            
            stats.successful_records += buffer_count;
            buffer_count = 0;
            
            /* Periodic file flush for safety */
            if (stats.successful_records % FLUSH_INTERVAL == 0) {
                fflush(binary_file);
            }
            
            /* Save checkpoint */
            if (stats.successful_records % CHECKPOINT_INTERVAL == 0) {
                save_checkpoint(stats.successful_records);
            }
        }
        
        /* Display progress */
        if (stats.processed_records % PROGRESS_INTERVAL == 0) {
            print_progress(stats.processed_records, total_estimate);
        }
    }
    
    /* Write remaining records in buffer */
    if (buffer_count > 0 && ret_code == 0) {
        if (!write_batch(binary_file, write_buffer, buffer_count)) {
            log_message(LOG_ERROR, "Failed to write final batch");
            ret_code = 1;
        } else {
            stats.successful_records += buffer_count;
        }
    }
    
    /* Final progress update */
    print_progress(stats.processed_records, total_estimate);
    printf("\n");
    
    /* Close files */
    fclose(csv_file);
    fflush(binary_file);
    fclose(binary_file);
    
    /* Free resources */
    free(write_buffer);
    
    /* Remove checkpoint file on successful completion */
    if (ret_code == 0 && stats.failed_records == 0) {
        remove(".conversion_checkpoint");
    }
    
    /* Print and save summary */
    print_summary_report(input_file, output_file);
    save_summary_report(input_file, output_file);
    
    /* Cleanup */
    cleanup_globals();
    
    /* Return appropriate exit code */
    if (stats.successful_records == 0) {
        return 1;  /* Fatal error */
    } else if (stats.failed_records > 0 || stats.validation_errors > 0) {
        return 2;  /* Completed with errors */
    } else {
        return 0;  /* Success */
    }
}