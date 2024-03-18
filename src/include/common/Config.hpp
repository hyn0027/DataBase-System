#pragma once

typedef unsigned int uint;
typedef unsigned char uchar;
typedef unsigned int *BufType;

// Common
#define BIT_PER_BUF 32
#define LOG_BIT_PER_BUF 5    // 2 ^ 5 = 32
#define BIT_PER_BUF_MASK 31  // 32 - 1
#define BIT_PER_BYTE 8
#define LOG_BIT_PER_BYTE 3  // 2 ^ 3 = 8

#define BYTE_PER_BUF 4
#define LOG_BYTE_PER_BUF 2   // 2 ^ 2 = 4
#define BYTE_PER_BUF_MASK 3  // 4 - 1

#define FLOAT_MAX float(1e50)

// BitMap
#define BIT_MAP_BIAS 5

// File Manager
#define PAGE_SIZE 8192  // BYTE
#define BUF_PER_PAGE 2048
#define BIT_PER_PAGE 65536
#define PAGE_SIZE_IDX 13

// HashMod
#define HASH_MOD 6007
#define HASH_PRIME_1 53
#define HASH_PRIME_2 89

// Cache
#define CACHE_CAPACITY 6000

// Record Manager
#define RECORD_META_DATA_LENGTH 80  // BYTE
#define RECORD_META_DATA_HEAD 32    // BYTE
#define MAX_COLUMN_NUM 102
#define RECORD_PAGE_HEADER 64  // BYTE
#define MAX_ITEM_PER_PAGE 512

// Index Manager
#define INDEX_HEADER_BYTE_LEN 16         // BYTE
#define INDEX_BITMAP_PAGE_BYTE_LEN 8188  // BYTE

// Data Path
#define DATABASE_PATH "./data"
#define DATABASE_BASE_PATH "./data/base"
#define DATABASE_GLOBAL_PATH "./data/global"
#define DATABASE_GLOBAL_RECORD_PATH "./data/global/ALLDatabase"
#define DATABSE_FOLDER_PREFIX "DB"
#define TABLE_FOLDER_PREFIX "TB"
#define INDEX_FOLDER_NAME "IndexFiles"
#define INDEX_FILE_PREFIX "INDEX"
#define ALL_TABLE_FILE_NAME "ALLTable"
#define RECORD_FILE_NAME "Record"
#define PRIMARY_KEY_FILE_NAME "PrimaryKey"
#define FOREIGN_KEY_FILE_NAME "ForeignKey"
#define DOMINATE_FILE_NAME "Dominate"
#define INDEX_INFO_FILE_NAME "IndexInfo"

#define UNIQUE_SUFFIX "_UNIQUE_SUFFIX_"

#define FOREIGN_KEY_MAX_NUM 10
#define INDEX_KEY_MAX_NUM 10

#define JOIN_TABLE_ID_MULTIPLY 88

#define TMP_FILE_PREFIX "TMP"

#define BLOCK_PAGE_NUM 8

static int tmp_file_idx = 0;