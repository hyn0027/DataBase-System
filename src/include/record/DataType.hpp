#pragma once

#include <cstring>
#include <iomanip>
#include <iostream>
#include <map>
#include <string>
#include <vector>

namespace dbs {
namespace record {

enum DataTypeName {
    INT,
    FLOAT,
    VARCHAR,
    DATE,
    ANY,
};

/**
 * @brief Get the Data Type Size object (Byte),
 * VARCHAR类型返回的是单个字符的大小
 * @param type_name
 * @return int
 */
int getDataTypeSize(DataTypeName type_name);

struct DateValue {
    int year, month, day;
    DateValue();
    DateValue(int year_, int month_, int day_);
};

struct DataValue {
    DataTypeName type_name;
    bool is_null;
    struct {
        int int_value;
        double float_value;
        std::string char_value;
        DateValue date_value;
    } value;
    void print() const;
    DataValue();
    DataValue(DataTypeName type_name_, bool is_null_);
    DataValue(DataTypeName type_name_, bool is_null_, int int_value_);
    DataValue(DataTypeName type_name_, bool is_null_, double float_value_);
    DataValue(DataTypeName type_name_, bool is_null_,
              const std::string& char_value_);
    DataValue(DataTypeName type_name_, bool is_null_, DateValue date_value_);
    bool same(const DataValue& data_value) const;
    std::string toString() const;
    friend bool operator<=(const DataValue& a, const DataValue& b) {
        return a < b || a == b;
    }
    friend bool operator>(const DataValue& a, const DataValue& b) {
        return !(a <= b);
    }
    friend bool operator>=(const DataValue& a, const DataValue& b) {
        return !(a < b);
    }
    friend bool operator<(const DataValue& a, const DataValue& b) {
        if (a.is_null) return true;
        switch (a.type_name) {
            case INT:
                return a.value.int_value < b.value.int_value;
            case FLOAT:
                return a.value.float_value < b.value.float_value;
            case VARCHAR:
                return a.value.char_value < b.value.char_value;
            case DATE:
                return a.value.date_value.year < b.value.date_value.year ||
                       (a.value.date_value.year == b.value.date_value.year &&
                        a.value.date_value.month < b.value.date_value.month) ||
                       (a.value.date_value.year == b.value.date_value.year &&
                        a.value.date_value.month == b.value.date_value.month &&
                        a.value.date_value.day < b.value.date_value.day);
        }
        return false;
    }
    friend bool operator==(const DataValue& a, const DataValue& b) {
        if (a.type_name != b.type_name) {
            return false;
        }
        if (a.is_null != b.is_null) {
            return false;
        }
        if (a.is_null && b.is_null) {
            return true;
        }
        switch (a.type_name) {
            case INT:
                return a.value.int_value == b.value.int_value;
            case FLOAT:
                return a.value.float_value == b.value.float_value;
            case VARCHAR:
                return a.value.char_value == b.value.char_value;
            case DATE:
                return a.value.date_value.year == b.value.date_value.year &&
                       a.value.date_value.month == b.value.date_value.month &&
                       a.value.date_value.day == b.value.date_value.day;
        }
        return false;
    }
};

struct DataItem {
    std::vector<DataValue> data_values;
    std::vector<int> column_ids;
    unsigned int data_id;
    bool same(const DataItem& data_item) const;

    friend bool operator<(const DataItem& a, const DataItem& b) {
        for (int i = 0; i < a.data_values.size(); i++) {
            if (a.data_values[i] < b.data_values[i]) {
                return true;
            } else if (a.data_values[i] > b.data_values[i]) {
                return false;
            }
        }
        return false;
    }
};

struct DefaultValue {
    bool has_default_value;
    DataValue value;
    DefaultValue();
    bool same(const DefaultValue& default_value) const;
};

struct ColumnType {
    DataTypeName type_name;
    int varchar_length;
    int varchar_space;  // byte为单位
    bool require_not_null;
    bool require_unique;
    DefaultValue default_value;
    std::string column_name;
    int column_id;
    ColumnType();
    ColumnType(DataTypeName type_name_, int varchar_length_, int varchar_space_,
               bool require_not_null_, bool require_unique_,
               DefaultValue default_value_, std::string column_name_);
    bool same(const ColumnType& column_type) const;
    std::string typeAsString() const;
};

struct RecordLocation {
    int page_id;
    int slot_id;
};

bool exactMatch(const std::vector<ColumnType>& column_types,
                const DataItem& data_item);

}  // namespace record
}  // namespace dbs