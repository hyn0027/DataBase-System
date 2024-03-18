#pragma once

#include <fstream>
#include <set>
#include <vector>

#include "common/Config.hpp"
#include "fs/BufPageManager.hpp"
#include "fs/FileManager.hpp"
#include "index/IndexManager.hpp"
#include "record/DataType.hpp"
#include "record/RecordManager.hpp"
#include "system/SystemColumns.hpp"

namespace dbs {
namespace system {

class SystemManager {
   public:
    /**
     * @brief Construct a new System Manager object
     *
     * @param fm_ file manager
     * @param bpm_ buffer page manager
     * @param rm_ record manager
     * @param im_ index manager
     */
    SystemManager(fs::FileManager* fm_, record::RecordManager* rm_,
                  index::IndexManager* im_);

    /**
     * @brief Destroy the System Manager object
     */
    ~SystemManager();

    /**
     * @brief initialize the system (setup if no data, do nothing if data
     * exists)
     */
    void initializeSystem();

    /**
     * @brief clean the system (delete all data)
     */
    void cleanSystem();

    /**
     * @brief Create a Database object
     *
     * @param database_name
     * @return true if success
     * @return false if fail
     */
    bool createDatabase(const char* database_name);

    /**
     * @brief Drop a Database object
     *
     * @param database_name
     * @return true if success, false if fail
     */
    bool dropDatabase(const char* database_name);

    /**
     * @brief Get the Database Id
     *
     * @param database_name
     * @return int
     */
    int getDatabaseId(const char* database_name);

    /**
     * @brief Create a Table object
     *
     * @param table_name
     * @param column_types unique可能会改变
     * @param primary_keys
     * @param foreign_keys
     * @return true if success
     * @return false if fail
     */
    bool createTable(const char* table_name,
                     std::vector<record::ColumnType>& column_types,
                     const std::vector<std::string>& primary_keys,
                     const std::vector<ForeignKeyInputInfo>& foreign_keys);

    bool addForeignKey(const char* table_name,
                       const ForeignKeyInfo& new_foreign_key);

    bool deleteForeignKey(const char* table_name, std::string foreign_key_name);

    /**
     * @brief Get Table Column Types
     *
     * @param table_id
     * @param column_types
     * @return
     */
    bool getTableColumnTypes(int table_id,
                             std::vector<record::ColumnType>& column_types);

    /**
     * @brief get table primary keys
     *
     * @param table_id
     * @param primary_keys
     * @return
     */
    bool getTablePrimaryKeys(int table_id, std::set<int>& primary_keys);

    bool getTableForeignKeys(int table_id,
                             std::vector<ForeignKeyInfo>& foreign_keys);

    bool getTableDominate(int table_id, std::vector<int>& dominate_table_ids);

    bool addIndex(const char* table_name, const std::string& index_name,
                  const std::vector<int>& column_ids, bool check_unique);

    bool dropIndex(const char* table_name, const std::string& index_name);
    /**
     * @brief Drop a Table
     *
     * @param table_name
     * @return true if success
     * @return false if fail
     */
    bool dropTable(const char* table_name);

    /**
     * @brief Get the Table Id
     *
     * @param table_name
     * @return int, -1不存在
     */
    int getTableId(const char* table_name);

    /**
     * @brief Get the All Database
     *
     * @param database_names return all database names
     */
    void getAllDatabase(std::vector<record::DataItem>& database_names,
                        std::vector<record::ColumnType>& column_types);

    /**
     * @brief Get the All Table
     *
     * @param table_names return all table names
     */
    bool getAllTable(std::vector<record::DataItem>& table_names,
                     std::vector<record::ColumnType>& column_types);

    /**
     * @brief describe a table (Field,Type,Null,Default)
     *
     * @param table_name
     * @param column_names
     * @param column_types
     * @return true
     * @return false
     */
    bool describeTable(const char* table_name,
                       std::vector<record::DataItem>& table_info,
                       std::vector<record::ColumnType>& column_types,
                       std::vector<std::string>& primary_keys,
                       std::vector<ForeignKeyInputInfo>& foreign_keys,
                       std::vector<std::vector<std::string>>& index,
                       std::vector<std::vector<std::string>>& unique);

    bool insertIntoTable(const char* table_name,
                         std::vector<record::DataItem>& data_items);

    /**
     * @brief Get the Database Path object
     * e.g. /data/base/DB1
     * @param database_id
     * @param result
     */
    void getDatabasePath(int database_id, char** result);

    /**
     * @brief Get the Table Record Path
     * e.g. /data/base/DB1/TB1
     * @param database_id
     * @param table_id
     * @param result
     */
    void getTableRecordPath(int database_id, int table_id, char** result);

    /**
     * @brief Get the Index Record Path
     * e.g. /data/base/DB1/TB1/IndexFiles/INDEX1
     * @param database_id
     * @param table_id
     * @param index_id
     * @param result
     */
    void getIndexRecordPath(int database_id, int table_id, int index_id,
                            char** result);

    /**
     * @brief use database
     * @param database_name
     * @return
     */
    bool useDatabase(const char* database_name);

    /**
     * @brief load table from file
     *
     * @param table_name
     * @param file_path
     * @param delimeter
     * @return true
     * @return false
     */
    bool loadTableFromFile(const char* table_name, const char* file_path,
                           const char* delimeter);

    int getCurrentDatabaseId() const;

    bool deleteFromTable(int table_id,
                         std::vector<SearchConstraint>& constraints);

    bool dropPrimaryKey(int table_id);

    bool addPrimaryKey(int table_id, std::vector<int> column_ids);

    bool addUnique(const char* table_name, const std::string& unique_name,
                   const std::vector<int>& column_ids);

    bool search(int table_id, std::vector<SearchConstraint>& constraints,
                std::vector<record::DataItem>& result_datas,
                std::vector<record::ColumnType>& column_types,
                std::vector<record::RecordLocation>& record_location_results,
                int sort_by);

    bool searchAndSave(int table_id,
                       std::vector<record::ColumnType>& column_types,
                       std::vector<SearchConstraint>& constraints,
                       std::string& save_path, int& total_num);

    bool update(int table_id, std::vector<SearchConstraint>& constraints,
                const record::DataItem& update_data);

    void fillInDataTypeField(
        std::vector<std::tuple<std::string, record::DataValue>>& update_info,
        int table_id, std::vector<std::string>& column_names,
        record::DataItem& data_item);

    void fillInDataTypeField(std::vector<SearchConstraint>& constraints,
                             int table_id);

    std::string findTableNameOfColumnName(
        const std::string& column_name, std::vector<std::string>& table_names);

    bool getColumnID(int table_id, const std::vector<std::string>& column_names,
                     std::vector<int>& column_ids);

   private:
    int createIndex(const char* index_info_path, const char* index_folder_path,
                    std::vector<int> index_ids, int table_id, std::string name);

    /**
     * @brief Get the All Index object
     *
     * @param database_id
     * @param table_id
     * @param index_ids index_ids中的每个元素(std::pair<int,
     * std::vector<int>>)对应一个index file，pair中的第一个int代表index_id,
     * 第二个vector代表index的column_id
     * @return true
     * @return false
     */
    void getAllIndex(int database_id, int table_id,
                     std::vector<std::pair<int, std::vector<int>>>& index_ids,
                     std::vector<std::string>& index_names);

    fs::FileManager* fm;
    record::RecordManager* rm;
    index::IndexManager* im;

    int current_database_id;
};

}  // namespace system
}  // namespace dbs
