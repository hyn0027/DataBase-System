#include "system/SystemColumns.hpp"

namespace dbs {
namespace system {

ForeignKeyInputInfo::ForeignKeyInputInfo() {
    foreign_key_column_names = std::vector<std::string>();
    reference_table_name = "";
    reference_column_names = std::vector<std::string>();
}

ForeignKeyInputInfo::ForeignKeyInputInfo(
    const std::vector<std::string>& foreign_key_column_names_,
    const std::string& reference_table_name_,
    const std::vector<std::string>& reference_column_names_) {
    foreign_key_column_names = foreign_key_column_names_;
    reference_table_name = reference_table_name_;
    reference_column_names = reference_column_names_;
}

ForeignKeyInfo::ForeignKeyInfo() {
    foreign_key_column_ids = std::vector<int>();
    reference_table_id = -1;
    reference_column_ids = std::vector<int>();
    name = "";
}

ForeignKeyInfo::ForeignKeyInfo(const std::vector<int>& foreign_key_column_ids_,
                               int reference_table_id_,
                               const std::vector<int>& reference_column_ids_,
                               std::string name_) {
    foreign_key_column_ids = foreign_key_column_ids_;
    reference_table_id = reference_table_id_;
    reference_column_ids = reference_column_ids_;
    name = name_;
}

bool mergeConstraints(std::vector<SearchConstraint>& constraints) {
    int num = constraints.size();
    for (int i = 0; i < num; i++) {
        for (int j = i + 1; j < num; j++) {
            if (constraints[i].column_id == constraints[j].column_id) {
                constraints[i].constraint_types.insert(
                    constraints[i].constraint_types.end(),
                    constraints[j].constraint_types.begin(),
                    constraints[j].constraint_types.end());
                constraints[i].constraint_values.insert(
                    constraints[i].constraint_values.end(),
                    constraints[j].constraint_values.begin(),
                    constraints[j].constraint_values.end());
                constraints.erase(constraints.begin() + j);
                num--;
                j--;
            }
        }
    }
    bool valid = true;
    for (auto& constraint : constraints) {
        if (constraint.data_type == record::DataTypeName::INT) {
            int num = constraint.constraint_types.size();
            record::DataValue lower_bound(record::DataTypeName::INT, true,
                                          INT_MIN);
            record::DataValue upper_bound(record::DataTypeName::INT, true,
                                          INT_MAX);
            std::vector<record::DataValue> neq_val;
            std::vector<record::DataValue> null_val;
            std::vector<ConstraintType> null_type;
            for (int i = 0; i < num; i++) {
                if (constraint.constraint_values[i].is_null) {
                    null_val.push_back(constraint.constraint_values[i]);
                    null_type.push_back(constraint.constraint_types[i]);
                    continue;
                }
                if (constraint.constraint_types[i] == ConstraintType::EQ) {
                    lower_bound =
                        std::max(lower_bound, constraint.constraint_values[i]);
                    upper_bound =
                        std::min(upper_bound, constraint.constraint_values[i]);
                } else if (constraint.constraint_types[i] ==
                           ConstraintType::NEQ) {
                    neq_val.push_back(constraint.constraint_values[i]);
                } else if (constraint.constraint_types[i] ==
                           ConstraintType::GT) {
                    lower_bound = std::max(
                        lower_bound,
                        record::DataValue(
                            record::DataTypeName::INT, false,
                            constraint.constraint_values[i].value.int_value +
                                1));
                } else if (constraint.constraint_types[i] ==
                           ConstraintType::GEQ) {
                    lower_bound =
                        std::max(lower_bound, constraint.constraint_values[i]);
                } else if (constraint.constraint_types[i] ==
                           ConstraintType::LT) {
                    upper_bound = std::min(
                        upper_bound,
                        record::DataValue(
                            record::DataTypeName::INT, false,
                            constraint.constraint_values[i].value.int_value -
                                1));
                } else if (constraint.constraint_types[i] ==
                           ConstraintType::LEQ) {
                    upper_bound =
                        std::min(upper_bound, constraint.constraint_values[i]);
                }
            }
            constraint.constraint_values.clear();
            constraint.constraint_types.clear();
            for (int i = 0; i < null_type.size(); i++) {
                constraint.constraint_values.push_back(null_val[i]);
                constraint.constraint_types.push_back(null_type[i]);
            }
            if (!lower_bound.is_null && !upper_bound.is_null &&
                upper_bound < lower_bound) {
                valid = false;
                break;
            } else {
                if (!lower_bound.is_null) {
                    constraint.constraint_values.push_back(lower_bound);
                    constraint.constraint_types.push_back(ConstraintType::GEQ);
                }
                if (!upper_bound.is_null) {
                    constraint.constraint_values.push_back(upper_bound);
                    constraint.constraint_types.push_back(ConstraintType::LEQ);
                }
                for (auto& val : neq_val) {
                    if (!lower_bound.is_null && val < lower_bound) {
                        continue;
                    }
                    if (!upper_bound.is_null && upper_bound < val) {
                        continue;
                    }
                    constraint.constraint_values.push_back(val);
                    constraint.constraint_types.push_back(ConstraintType::NEQ);
                }
            }
        } else {
            record::DataValue lower_bound(record::DataTypeName::FLOAT, true,
                                          -FLOAT_MAX);
            record::DataValue upper_bound(record::DataTypeName::FLOAT, true,
                                          FLOAT_MAX);
            if (constraint.data_type == record::DataTypeName::FLOAT) {
                lower_bound = record::DataValue(record::DataTypeName::FLOAT,
                                                true, -FLOAT_MAX);
                upper_bound = record::DataValue(record::DataTypeName::FLOAT,
                                                true, FLOAT_MAX);
            } else if (constraint.data_type == record::DataTypeName::VARCHAR) {
                lower_bound =
                    record::DataValue(record::DataTypeName::VARCHAR, true, "");
                upper_bound =
                    record::DataValue(record::DataTypeName::VARCHAR, true, "");
            } else if (constraint.data_type == record::DataTypeName::DATE) {
                lower_bound =
                    record::DataValue(record::DataTypeName::DATE, true,
                                      record::DateValue(0, 0, 0));
                upper_bound =
                    record::DataValue(record::DataTypeName::DATE, true,
                                      record::DateValue(9999, 99, 99));
            }
            ConstraintType lower_bound_type, upper_bound_type;
            std::vector<record::DataValue> neq_val;
            std::vector<record::DataValue> null_val;
            std::vector<ConstraintType> null_type;
            int num = constraint.constraint_types.size();
            for (int i = 0; i < num; i++) {
                if (constraint.constraint_values[i].is_null) {
                    null_val.push_back(constraint.constraint_values[i]);
                    null_type.push_back(constraint.constraint_types[i]);
                    continue;
                }
                if (constraint.constraint_types[i] == ConstraintType::EQ) {
                    if (lower_bound < constraint.constraint_values[i] ||
                        lower_bound.is_null) {
                        lower_bound = constraint.constraint_values[i];
                        lower_bound_type = ConstraintType::GEQ;
                    }
                    if (constraint.constraint_values[i] < upper_bound ||
                        upper_bound.is_null) {
                        upper_bound = constraint.constraint_values[i];
                        upper_bound_type = ConstraintType::LEQ;
                    }
                } else if (constraint.constraint_types[i] ==
                           ConstraintType::NEQ) {
                    neq_val.push_back(constraint.constraint_values[i]);
                } else if (constraint.constraint_types[i] ==
                           ConstraintType::GT) {
                    if (lower_bound <= constraint.constraint_values[i] ||
                        lower_bound.is_null) {
                        lower_bound = constraint.constraint_values[i];
                        lower_bound_type = ConstraintType::GT;
                    }
                } else if (constraint.constraint_types[i] ==
                           ConstraintType::GEQ) {
                    if (lower_bound < constraint.constraint_values[i] ||
                        lower_bound.is_null) {
                        lower_bound = constraint.constraint_values[i];
                        lower_bound_type = ConstraintType::GEQ;
                    }
                } else if (constraint.constraint_types[i] ==
                           ConstraintType::LT) {
                    if (constraint.constraint_values[i] <= upper_bound ||
                        upper_bound.is_null) {
                        upper_bound = constraint.constraint_values[i];
                        upper_bound_type = ConstraintType::LT;
                    }
                } else if (constraint.constraint_types[i] ==
                           ConstraintType::LEQ) {
                    if (constraint.constraint_values[i] < upper_bound ||
                        upper_bound.is_null) {
                        upper_bound = constraint.constraint_values[i];
                        upper_bound_type = ConstraintType::LEQ;
                    }
                }
            }
            constraint.constraint_values.clear();
            constraint.constraint_types.clear();
            for (int i = 0; i < null_type.size(); i++) {
                constraint.constraint_values.push_back(null_val[i]);
                constraint.constraint_types.push_back(null_type[i]);
            }
            if (!lower_bound.is_null && !upper_bound.is_null &&
                (upper_bound < lower_bound ||
                 (upper_bound == lower_bound &&
                  !(lower_bound_type == ConstraintType::GEQ &&
                    upper_bound_type == ConstraintType::LEQ)))) {
                valid = false;
                break;
            } else {
                if (!lower_bound.is_null) {
                    constraint.constraint_values.push_back(lower_bound);
                    constraint.constraint_types.push_back(lower_bound_type);
                }
                if (!upper_bound.is_null) {
                    constraint.constraint_values.push_back(upper_bound);
                    constraint.constraint_types.push_back(upper_bound_type);
                }
                for (auto& val : neq_val) {
                    if (!lower_bound.is_null && val < lower_bound ||
                        (val == lower_bound &&
                         lower_bound_type == ConstraintType::GT)) {
                        continue;
                    }
                    if (!upper_bound.is_null && upper_bound < val ||
                        (val == upper_bound &&
                         upper_bound_type == ConstraintType::LT)) {
                        continue;
                    }
                    constraint.constraint_values.push_back(val);
                    constraint.constraint_types.push_back(ConstraintType::NEQ);
                }
            }
        }
    }
    return valid;
}

bool validConstraint(const SearchConstraint& constraint,
                     const record::DataItem& item) {
    for (int i = 0; i < item.column_ids.size(); i++) {
        if (constraint.column_id == item.column_ids[i]) {
            for (int j = 0; j < constraint.constraint_types.size(); j++) {
                if (constraint.constraint_values[j].is_null) {
                    if (constraint.constraint_types[j] == ConstraintType::EQ) {
                        if (!item.data_values[i].is_null) {
                            return false;
                        }
                    } else if (constraint.constraint_types[j] ==
                               ConstraintType::NEQ) {
                        if (item.data_values[i].is_null) {
                            return false;
                        }
                    }
                    continue;
                }
                if (constraint.constraint_types[j] == ConstraintType::EQ) {
                    if (item.data_values[i] !=
                        constraint.constraint_values[j]) {
                        return false;
                    }
                } else if (constraint.constraint_types[j] ==
                           ConstraintType::NEQ) {
                    if (item.data_values[i] ==
                        constraint.constraint_values[j]) {
                        return false;
                    }
                } else if (constraint.constraint_types[j] ==
                           ConstraintType::GT) {
                    if (item.data_values[i] <=
                            constraint.constraint_values[j] ||
                        item.data_values[i].is_null) {
                        return false;
                    }
                } else if (constraint.constraint_types[j] ==
                           ConstraintType::GEQ) {
                    if (item.data_values[i] < constraint.constraint_values[j] ||
                        item.data_values[i].is_null) {
                        return false;
                    }
                } else if (constraint.constraint_types[j] ==
                           ConstraintType::LT) {
                    if (item.data_values[i] >=
                            constraint.constraint_values[j] ||
                        item.data_values[i].is_null) {
                        return false;
                    }
                } else if (constraint.constraint_types[j] ==
                           ConstraintType::LEQ) {
                    if (item.data_values[i] > constraint.constraint_values[j] ||
                        item.data_values[i].is_null) {
                        return false;
                    }
                }
            }
        }
    }
    return true;
}

void filterConstraints(
    const std::vector<SearchConstraint>& constraints,
    const std::vector<record::DataItem>& data_items,
    const std::vector<record::RecordLocation>& record_locations,
    std::vector<record::DataItem>& result,
    std::vector<record::RecordLocation>& record_location_results) {
    result.clear();
    record_location_results.clear();
    for (int i = 0; i < data_items.size(); i++) {
        auto& data_item = data_items[i];
        auto& location = record_locations[i];
        bool valid = true;
        for (auto& constraint : constraints) {
            if (!validConstraint(constraint, data_item)) {
                valid = false;
                break;
            }
        }
        if (valid) {
            result.push_back(data_item);
            record_location_results.push_back(location);
        }
    }
}

void SearchConstraint::print() const {
    std::cerr << "column_id: " << column_id << std::endl;
    std::cerr << "data_type: " << data_type << std::endl;
    std::cerr << "constraint_types: ";
    for (auto& constraint_type : constraint_types) {
        switch (constraint_type) {
            case ConstraintType::EQ:
                std::cerr << "EQ ";
                break;
            case ConstraintType::NEQ:
                std::cerr << "NEQ ";
                break;
            case ConstraintType::GT:
                std::cerr << "GT ";
                break;
            case ConstraintType::GEQ:
                std::cerr << "GEQ ";
                break;
            case ConstraintType::LT:
                std::cerr << "LT ";
                break;
            case ConstraintType::LEQ:
                std::cerr << "LEQ ";
                break;
        }
    }
    std::cerr << std::endl;
    std::cerr << "constraint_values: ";
    for (auto& constraint_value : constraint_values) {
        constraint_value.print();
        std::cerr << " ";
    }
    std::cerr << std::endl;
}

}  // namespace system
}  // namespace dbs