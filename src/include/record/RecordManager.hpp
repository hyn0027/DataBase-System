#pragma once

#include <fstream>
#include <map>
#include <vector>

#include "common/Config.hpp"
#include "fs/BufPageManager.hpp"
#include "fs/FileManager.hpp"
#include "record/DataType.hpp"
#include "system/SystemColumns.hpp"
#include "utils/BitOperations.hpp"
#include "utils/FilePath.hpp"

namespace dbs {
namespace record {

class RecordManager {
   public:
    /**
     * @brief Construct a new Record Manager object
     * @param fm_
     * @param bpm_
     */
    RecordManager(fs::FileManager* fm_, fs::BufPageManager* bpm_);
    /**
     * @brief Destroy the Record Manager object
     */
    ~RecordManager();
    /**
     * @brief 创建一个记录文件，如果之前文件存在则直接强行覆盖(扔掉原来的数据)
     *
     * @param file_path 文件路径，需要保证文件所在的文件夹是存在的
     * @param column_types 所有列的类型
     */
    void initializeRecordFile(const char* file_path,
                              const std::vector<ColumnType>& column_types);

    // upd 新接口↓
    void updateColumnUnique(const char* file_path, int column_id, bool unique);
    /**
     * @brief Get the Column Types object
     *
     * @param file_path 文件路径
     * @param column_types 返回的vector，存储所有列的类型,
     * 会清空vector原来的数据
     */
    void getColumnTypes(const char* file_path,
                        std::vector<ColumnType>& column_types);
    /**
     * @brief
     * 插入一条记录，会进行类型检查&varchar长度&null值检查，
     * 但不会检查主键外键unique约束，这几个应该由上层模块调用元数据record和索引去检查；如果插入失败不会对数据造成影响
     * 插入的条目必须所有值都有，default相关的问题由parser模块或系统管理模块调用getColumnType进行处理
     * insert顺序可以不按照column_types的顺序，只需要保证data_item的column_id和column_types的column_id一致
     *
     * @param file_path 文件路径
     * @param data_item 插入的数据，需要保证数据的合法性
     * @return RecordLocation, -1, -1表示插入失败
     */
    RecordLocation insertRecord(const char* file_path, DataItem data_item);

    /**
     * @brief 从零开始插入多条记录，会进行类型检查&varchar长度&null值检查，
     * 但不会检查主键外键unique约束，这几个应该由上层模块调用元数据record和索引去检查；如果插入失败不会对数据造成影响
     * 插入的条目必须所有值都有，default相关的问题由parser模块或系统管理模块调用getColumnType进行处理
     * insert顺序可以不按照column_types的顺序，只需要保证data_item的column_id和column_types的column_id一致
     *
     * @param file_path 文件路径
     * @param data_items 插入的数据，需要保证数据的合法性
     * @return RecordLocation, -1, -1表示插入失败
     */
    int insertRecordsToEmptyRecord(const char* file_path, const char* csv_path,
                                   const char* delimeter, bool output);

    int getTotalPageNum(const char* file_path);
    /**
     * @brief 删除一条记录
     *
     * @param file_path 文件路径
     * @param record_location 记录的位置
     * @return true 删除成功
     * @return false 删除失败
     */
    bool deleteRecord(const char* file_path,
                      const RecordLocation& record_location);
    /**
     * @brief 更新一条记录，不成功的话不会对原数据造成影响
     *
     * @param file_path  文件路径
     * @param record_location 记录的位置
     * @param data_item
     * 更改的数据，不一定所有列都需要有，可以通过DataItem的columnid指定更新哪些列
     * @return true 更新成功
     * @return false 更新失败
     */
    bool updateRecord(const char* file_path,
                      const RecordLocation& record_location,
                      const DataItem& data_item);
    /**
     * @brief Get the Record object （一个）
     *
     * @param file_path  文件路径
     * @param record_location  记录的位置
     * @param data_item  返回的数据
     * @return true 获取成功
     * @return false 获取失败 (不存在)
     */
    bool getRecord(const char* file_path, const RecordLocation& record_location,
                   DataItem& data_item);

    void getAllRecordWithConstraint(
        const char* file_path, std::vector<DataItem>& data_items,
        std::vector<RecordLocation>& record_locations,
        const std::vector<system::SearchConstraint>& constraints);

    /**
     * @brief Get the Records object （vector的形式，多个）
     *
     * @param file_path 文件路径
     * @param record_locations 记录的位置
     * @param data_items 返回的数据，会清空vector里原有的
     * @return true 获取成功
     */
    bool getRecords(const char* file_path,
                    const std::vector<RecordLocation>& record_locations,
                    std::vector<DataItem>& data_items);
    /**
     * @brief Get the All Records object
     *
     * @param file_path 文件路径
     * @param data_items 返回的数据
     * @param record_locations 返回的位置
     */
    void getAllRecords(const char* file_path, std::vector<DataItem>& data_items,
                       std::vector<RecordLocation>& record_locations);

    void getRecordsInPageRange(const char* file_path,
                               std::vector<DataItem>& data_items, int low_page,
                               int upper_page);

    /**
     * @brief delete the record file
     *
     * @param file_path
     * @return true delete success
     * @return false delete failed
     */
    bool deleteRecordFile(const char* file_path);

    /**
     * @brief close all current file
     */
    void closeAllCurrentFile();

    /**
     * @brief clean all current column types
     */
    void cleanAllCurrentColumnTypes();

    int getAllRecordWithConstraintSaveFile(
        const char* file_path, const char* file_path_save,
        const std::vector<system::SearchConstraint>& constraints);

   private:
    /**
     * @brief sort the data item according to column types
     * @param column_types
     * @param data_item
     */
    void sortDataItem(const std::vector<ColumnType>& column_types,
                      DataItem& data_item);
    /**
     * @brief set the slot item
     * @param b
     * @param slot_id
     * @param slot_length
     * @param record_id
     * @param null_bitmap_buf_size 以buf为单位
     * @param data_item
     */
    void setSlotItem(BufType b, int slot_id, int slot_length, int record_id,
                     int null_bitmap_buf_size, const DataItem& data_item,
                     const std::vector<ColumnType>& column_types);
    /**
     * @brief get the slot item
     * @param b
     * @param slot_id
     * @param slot_length
     * @param null_bitmap_buf_size
     * @param column_types
     * @return DataItem
     */
    DataItem getSlotItem(BufType b, int slot_id, int slot_length,
                         int null_bitmap_buf_size,
                         const std::vector<ColumnType>& column_types);
    /**
     * @brief get the data item length
     * @param column_types
     * @return int (Byte) 一定是4的倍数
     * @param null_bitmap_size 以byte为单位
     */
    int dataItemLength(const std::vector<ColumnType>& column_types,
                       int null_bitmap_size);

    void closeFirstFile();
    void closeFileIfExist(const char* file_path);

    void cleanFirstColumnTypes();
    void cleanColumnTypesIfExist(const char* file_path);

    int openFile(const char* file_path);

    fs::FileManager* fm;
    fs::BufPageManager* bpm;

    std::vector<char*> current_opening_file_paths;
    std::vector<int> current_opening_file_ids;
    const int opening_cache_capacity = 10;

    std::vector<char*> current_column_types_file_paths;
    std::vector<std::vector<ColumnType>> current_column_types;
    const int column_types_cache_capacity = 10;
};

}  // namespace record
}  // namespace dbs