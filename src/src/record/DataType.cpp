#include "record/DataType.hpp"

namespace dbs {
namespace record {

void DataValue::print() const {
    if (is_null) {
        std::cout << "NULL";
    } else {
        switch (type_name) {
            case INT:
                std::cout << value.int_value;
                break;
            case FLOAT:
                // 保留小数点后两位
                std::cout << std::fixed << std::setprecision(2)
                          << value.float_value;
                break;
            case VARCHAR:
                std::cout << value.char_value;
                break;
            case DATE:
                std::cout << value.date_value.year << "-"
                          << value.date_value.month << "-"
                          << value.date_value.day;
                break;
        }
    }
}

int getDataTypeSize(DataTypeName type_name) {
    switch (type_name) {
        case DataTypeName::INT:
            return 4;
        case DataTypeName::FLOAT:
            return 8;
        case DataTypeName::VARCHAR:
            return 1;
        case DataTypeName::DATE:
            return 4;
        default:
            return 0;
    }
}

DateValue::DateValue() {
    year = 0;
    month = 0;
    day = 0;
}

DateValue::DateValue(int year_, int month_, int day_) {
    year = year_;
    month = month_;
    day = day_;
}

DataValue::DataValue() {
    is_null = true;
    type_name = DataTypeName::INT;
    value.int_value = 0;
}

DataValue::DataValue(DataTypeName type_name_, bool is_null_) {
    is_null = is_null_;
    type_name = type_name_;
    switch (type_name) {
        case DataTypeName::INT:
            value.int_value = 0;
            break;
        case DataTypeName::FLOAT:
            value.float_value = 0;
            break;
        case DataTypeName::VARCHAR:
            value.char_value = "";
            break;
        case DataTypeName::DATE:
            value.date_value = {0, 0, 0};
            break;
        default:
            break;
    }
}

DataValue::DataValue(DataTypeName type_name_, bool is_null_, int int_value_) {
    is_null = is_null_;
    type_name = type_name_;
    value.int_value = int_value_;
}

DataValue::DataValue(DataTypeName type_name_, bool is_null_,
                     double float_value_) {
    is_null = is_null_;
    type_name = type_name_;
    value.float_value = float_value_;
}

DataValue::DataValue(DataTypeName type_name_, bool is_null_,
                     const std::string& char_value_) {
    is_null = is_null_;
    type_name = type_name_;
    value.char_value = char_value_;
}

DataValue::DataValue(DataTypeName type_name_, bool is_null_,
                     DateValue date_value_) {
    is_null = is_null_;
    type_name = type_name_;
    value.date_value = date_value_;
}

DefaultValue::DefaultValue() { has_default_value = false; }

ColumnType::ColumnType() {
    type_name = DataTypeName::INT;
    varchar_length = 0;
    varchar_space = 0;
    require_not_null = false;
    require_unique = false;
    column_name = "";
    column_id = -1;
}

ColumnType::ColumnType(DataTypeName type_name_, int varchar_length_,
                       int varchar_space_, bool require_not_null_,
                       bool require_unique_, DefaultValue default_value_,
                       std::string column_name_) {
    type_name = type_name_;
    varchar_length = varchar_length_;
    varchar_space = varchar_space_;
    require_not_null = require_not_null_;
    require_unique = require_unique_;
    default_value = default_value_;
    column_name = column_name_;
    column_id = -1;
}

std::string ColumnType::typeAsString() const {
    switch (type_name) {
        case DataTypeName::INT:
            return "INT";
        case DataTypeName::FLOAT:
            return "FLOAT";
        case DataTypeName::VARCHAR:
            return "VARCHAR(" + std::to_string(varchar_length) + ")";
        case DataTypeName::DATE:
            return "DATE";
        default:
            return "";
    }
}

std::string DataValue::toString() const {
    if (is_null) {
        return "NULL";
    } else {
        std::ostringstream stream;
        switch (type_name) {
            case DataTypeName::INT:
                return std::to_string(value.int_value);
            case DataTypeName::FLOAT:
                stream << std::fixed << std::setprecision(2)
                       << value.float_value;
                return stream.str();
            case DataTypeName::VARCHAR:
                return value.char_value;
            case DataTypeName::DATE:
                return std::to_string(value.date_value.year) + "-" +
                       std::to_string(value.date_value.month) + "-" +
                       std::to_string(value.date_value.day);
            default:
                return "";
        }
    }
}

bool DataValue::same(const DataValue& data_value) const {
    if (type_name != data_value.type_name) return false;
    if (is_null != data_value.is_null) return false;
    if (!is_null) {
        switch (type_name) {
            case DataTypeName::INT:
                if (value.int_value != data_value.value.int_value) return false;
                break;
            case DataTypeName::FLOAT:
                if (value.float_value != data_value.value.float_value)
                    return false;
                break;
            case DataTypeName::VARCHAR:
                if (value.char_value != data_value.value.char_value)
                    return false;
                break;
            case DataTypeName::DATE:
                if (value.date_value.year != data_value.value.date_value.year)
                    return false;
                if (value.date_value.month != data_value.value.date_value.month)
                    return false;
                if (value.date_value.day != data_value.value.date_value.day)
                    return false;
                break;
            default:
                break;
        }
    }
    return true;
}

bool DefaultValue::same(const DefaultValue& default_value) const {
    if (has_default_value != default_value.has_default_value) return false;
    if (has_default_value) {
        return value.same(default_value.value);
    }
    return true;
}

bool ColumnType::same(const ColumnType& column_type) const {
    if (type_name != column_type.type_name) return false;
    if (type_name == VARCHAR && varchar_length != column_type.varchar_length)
        return false;
    if (require_not_null != column_type.require_not_null) return false;
    if (require_unique != column_type.require_unique) return false;
    if (!default_value.same(column_type.default_value)) return false;
    if (column_name != column_type.column_name) return false;
    return true;
}

bool exactMatch(const std::vector<ColumnType>& column_types,
                const DataItem& data_item) {
    if (column_types.size() != data_item.data_values.size()) {
        return false;
    }
    for (size_t i = 0; i < column_types.size(); i++) {
        if (column_types[i].type_name != data_item.data_values[i].type_name) {
            return false;
        }
        if (data_item.data_values[i].is_null) {
            if (column_types[i].require_not_null) {
                return false;
            }
        } else {
            switch (column_types[i].type_name) {
                case DataTypeName::VARCHAR:
                    if (data_item.data_values[i].value.char_value.size() >
                        (size_t)column_types[i].varchar_length) {
                        return false;
                    }
                    break;
                default:
                    break;
            }
        }
    }
    return true;
}

bool DataItem::same(const DataItem& data_item) const {
    if (data_values.size() != data_item.data_values.size()) return false;
    std::map<int, DataValue> data_value_map;
    for (size_t i = 0; i < data_values.size(); i++) {
        data_value_map[column_ids[i]] = data_values[i];
    }
    for (size_t i = 0; i < data_item.data_values.size(); i++) {
        if (!data_value_map.count(data_item.column_ids[i])) return false;
        if (!data_value_map[data_item.column_ids[i]].same(
                data_item.data_values[i]))
            return false;
    }
    return true;
}

}  // namespace record
}  // namespace dbs