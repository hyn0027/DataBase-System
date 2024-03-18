#pragma once

#include <climits>
#include <vector>

#include "common/Config.hpp"
#include "record/DataType.hpp"

namespace dbs {
namespace system {

// static 的columnType初始化放在SystemManager的构造函数中

static std::vector<record::ColumnType> GlobalDatabaseInfoColumnType;
static std::vector<record::ColumnType> DatabaseTableInfoColumnType;
static std::vector<record::ColumnType> TablePrimaryKeyInfoColumnType;
static std::vector<record::ColumnType> TableForeignKeyInfoColumnType;
static std::vector<record::ColumnType> TableDominateInfoColumnType;
static std::vector<record::ColumnType> TableIndexInfoColumnType;

struct ForeignKeyInputInfo {
    std::vector<std::string> foreign_key_column_names;
    std::string reference_table_name;
    std::vector<std::string> reference_column_names;

    ForeignKeyInputInfo();
    ForeignKeyInputInfo(
        const std::vector<std::string>& foreign_key_column_names_,
        const std::string& reference_table_name_,
        const std::vector<std::string>& reference_column_names_);
};

struct ForeignKeyInfo {
    std::vector<int> foreign_key_column_ids;
    int reference_table_id;
    std::vector<int> reference_column_ids;
    std::string name;
    ForeignKeyInfo();
    ForeignKeyInfo(const std::vector<int>& foreign_key_column_ids_,
                   int reference_table_id_,
                   const std::vector<int>& reference_column_ids_,
                   std::string name_);
    friend bool operator==(const ForeignKeyInfo& a, const ForeignKeyInfo& b) {
        if (a.reference_table_id != b.reference_table_id) {
            return false;
        }
        if (a.foreign_key_column_ids.size() !=
            b.foreign_key_column_ids.size()) {
            return false;
        }
        for (int i = 0; i < a.foreign_key_column_ids.size(); i++) {
            int found_idx = -1;
            for (int j = 0; j < b.foreign_key_column_ids.size(); j++) {
                if (a.foreign_key_column_ids[i] ==
                    b.foreign_key_column_ids[j]) {
                    found_idx = j;
                    break;
                }
            }
            if (found_idx == -1) {
                return false;
            }
            if (a.reference_column_ids[i] !=
                b.reference_column_ids[found_idx]) {
                return false;
            }
        }
        return true;
    }
};

enum ConstraintType { EQ, NEQ, GT, GEQ, LT, LEQ };

struct SearchConstraint {
    int column_id;  // 是对哪一列的约束
    record::DataTypeName
        data_type;  // 约束的数据类型 （也就是对应列的数据类型）
    std::vector<ConstraintType> constraint_types;  // vector是为了方便后续操作；使用的时候push一个元素就好，约束对应上述类型
                                                   // EQ, NEQ, GT, GEQ, LT, LEQ
    std::vector<record::DataValue>
        constraint_values;  // vector是为了方便后续操作；使用的时候push一个元素就好，约束对应的值
    void print() const;
};

bool mergeConstraints(std::vector<SearchConstraint>& constraints);

bool validConstraint(const SearchConstraint& constraint,
                     const record::DataItem& item);

void filterConstraints(
    const std::vector<SearchConstraint>& constraints,
    const std::vector<record::DataItem>& data_items,
    const std::vector<record::RecordLocation>& record_locations,
    std::vector<record::DataItem>& result,
    std::vector<record::RecordLocation>& record_location_results);

}  // namespace system
}  // namespace dbs