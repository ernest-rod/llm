/*
 * customer_convert.c
 * 
 * Purpose: Converts customer data from CSV format to binary format
 * 
 * Input:  data_full\customers.csv (CSV format)
 * Output: data\customers.binary (Binary format)
 * 
 * Author: Generated for CSV to Binary Conversion
 * Date: 2025
 * 
 * Compilation (Windows with VS Code):
 *   gcc customer_convert.c -o customer_convert.exe
 * 
 * Usage:
 *   customer_convert.exe
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <errno.h>
#ifdef _WIN32
    #include <direct.h>
    #include <sys/types.h>
    #include <sys/stat.h>
    #include <io.h>
#endif

/* Maximum field lengths based on sample data */
#define MAX_FIRST_NAME 50
#define MAX_LAST_NAME 50
#define MAX_EMAIL 100
#define MAX_PHONE 20
#define MAX_CITY 50
#define MAX_STATE 3
#define MAX_ZIP_CODE 10
#define MAX_DATE 11  /* YYYY-MM-DD format */
#define MAX_LINE 1024

/* Customer structure matching the CSV format */
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

/*
 * Function: create_directory
 * Description: Creates a directory if it doesn't exist
 * Parameters: path - Directory path to create
 * Returns: 0 on success, -1 on failure
 */
int create_directory(const char *path) {
#ifdef _WIN32
    struct _stat st = {0};
    if (_stat(path, &st) == -1) {
        if (_mkdir(path) != 0) {
            return -1;
        }
    }
#else
    struct stat st = {0};
    if (stat(path, &st) == -1) {
        if (mkdir(path, 0700) != 0) {
            return -1;
        }
    }
#endif
    return 0;
}

/*
 * Function: trim_whitespace
 * Description: Removes leading and trailing whitespace from a string
 * Parameters: str - String to trim (modified in place)
 * Returns: Pointer to trimmed string
 */
char* trim_whitespace(char *str) {
    char *end;
    
    /* Trim leading space */
    while (*str == ' ' || *str == '\t' || *str == '\n' || *str == '\r') {
        str++;
    }
    
    if (*str == 0) {
        return str;
    }
    
    /* Trim trailing space */
    end = str + strlen(str) - 1;
    while (end > str && (*end == ' ' || *end == '\t' || *end == '\n' || *end == '\r')) {
        end--;
    }
    
    *(end + 1) = '\0';
    
    return str;
}

/*
 * Function: parse_csv_line
 * Description: Parses a CSV line and populates a Customer structure
 * Parameters: line - CSV line to parse
 *             customer - Pointer to Customer structure to populate
 * Returns: 1 on success, 0 on failure
 */
int parse_csv_line(char *line, Customer *customer) {
    char *token;
    char *saveptr;
    int field_count = 0;
    char line_copy[MAX_LINE];
    
    /* Make a copy of the line for parsing */
    strncpy(line_copy, line, MAX_LINE - 1);
    line_copy[MAX_LINE - 1] = '\0';
    
    /* Parse CSV fields */
    token = strtok_r(line_copy, ",", &saveptr);
    
    while (token != NULL && field_count < 9) {
        token = trim_whitespace(token);
        
        switch (field_count) {
            case 0: /* customer_id */
                customer->customer_id = atoi(token);
                break;
            case 1: /* first_name */
                strncpy(customer->first_name, token, MAX_FIRST_NAME - 1);
                customer->first_name[MAX_FIRST_NAME - 1] = '\0';
                break;
            case 2: /* last_name */
                strncpy(customer->last_name, token, MAX_LAST_NAME - 1);
                customer->last_name[MAX_LAST_NAME - 1] = '\0';
                break;
            case 3: /* email */
                strncpy(customer->email, token, MAX_EMAIL - 1);
                customer->email[MAX_EMAIL - 1] = '\0';
                break;
            case 4: /* phone */
                strncpy(customer->phone, token, MAX_PHONE - 1);
                customer->phone[MAX_PHONE - 1] = '\0';
                break;
            case 5: /* city */
                strncpy(customer->city, token, MAX_CITY - 1);
                customer->city[MAX_CITY - 1] = '\0';
                break;
            case 6: /* state */
                strncpy(customer->state, token, MAX_STATE - 1);
                customer->state[MAX_STATE - 1] = '\0';
                break;
            case 7: /* zip_code */
                strncpy(customer->zip_code, token, MAX_ZIP_CODE - 1);
                customer->zip_code[MAX_ZIP_CODE - 1] = '\0';
                break;
            case 8: /* registration_date */
                strncpy(customer->registration_date, token, MAX_DATE - 1);
                customer->registration_date[MAX_DATE - 1] = '\0';
                break;
        }
        
        field_count++;
        token = strtok_r(NULL, ",", &saveptr);
    }
    
    /* Verify we got all required fields */
    return (field_count == 9) ? 1 : 0;
}

/*
 * Function: main
 * Description: Main program entry point
 * Returns: 0 on success, 1 on failure
 */
int main(void) {
    FILE *csv_file = NULL;
    FILE *binary_file = NULL;
    char line[MAX_LINE];
    Customer customer;
    int record_count = 0;
    int line_number = 0;
    int ret_code = 0;
    
    printf("Customer CSV to Binary Converter\n");
    printf("=================================\n\n");
    
    /* Create output directory if it doesn't exist */
    printf("Creating output directory 'data'...\n");
    if (create_directory("data") != 0 && errno != EEXIST) {
        fprintf(stderr, "Error: Could not create 'data' directory: %s\n", strerror(errno));
        return 1;
    }
    
    /* Open input CSV file */
    printf("Opening input file: data_full\\customers.csv\n");
    csv_file = fopen("data_full\\customers.csv", "r");
    if (csv_file == NULL) {
        fprintf(stderr, "Error: Could not open input file 'data_full\\customers.csv': %s\n", 
                strerror(errno));
        return 1;
    }
    
    /* Open output binary file */
    printf("Creating output file: data\\customers.binary\n");
    binary_file = fopen("data\\customers.binary", "wb");
    if (binary_file == NULL) {
        fprintf(stderr, "Error: Could not create output file 'data\\customers.binary': %s\n", 
                strerror(errno));
        fclose(csv_file);
        return 1;
    }
    
    printf("\nProcessing records...\n");
    
    /* Read and process CSV file line by line */
    while (fgets(line, sizeof(line), csv_file) != NULL) {
        line_number++;
        
        /* Skip header line if present */
        if (line_number == 1 && strstr(line, "customer_id") != NULL) {
            printf("Skipping header line\n");
            continue;
        }
        
        /* Skip empty lines */
        if (strlen(trim_whitespace(line)) == 0) {
            continue;
        }
        
        /* Initialize customer structure */
        memset(&customer, 0, sizeof(Customer));
        
        /* Parse CSV line */
        if (parse_csv_line(line, &customer)) {
            /* Write binary record */
            if (fwrite(&customer, sizeof(Customer), 1, binary_file) != 1) {
                fprintf(stderr, "Error: Failed to write record %d to binary file\n", 
                        record_count + 1);
                ret_code = 1;
                break;
            }
            
            record_count++;
            
            /* Display progress every 100 records */
            if (record_count % 100 == 0) {
                printf("Processed %d records...\n", record_count);
            }
        } else {
            fprintf(stderr, "Warning: Failed to parse line %d (skipping)\n", line_number);
        }
    }
    
    /* Close files */
    fclose(csv_file);
    fclose(binary_file);
    
    /* Display summary */
    printf("\n=================================\n");
    printf("Conversion Complete!\n");
    printf("=================================\n");
    printf("Total records converted: %d\n", record_count);
    printf("Input file:  data_full\\customers.csv\n");
    printf("Output file: data\\customers.binary\n");
    printf("Record size: %zu bytes\n", sizeof(Customer));
    printf("Total size:  %zu bytes\n", record_count * sizeof(Customer));
    
    return ret_code;
}