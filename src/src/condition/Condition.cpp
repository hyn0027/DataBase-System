#include "condition/Condition.hpp"

namespace dbs {
namespace condition {

/*
struct SearchConstraint {
    int column_id;
    record::DataTypeName data_type;
    std::vector<ConstraintType> constraint_types;
    std::vector<record::DataValue> constraint_values;
};
*/

system::SearchConstraint IndexCondition::to_search_constraint() const {
    system::SearchConstraint constraint;
    constraint.column_id = column_id;
    if (type == VariableType::INT) {
        constraint.data_type = record::DataTypeName::INT;
        if (op == Operator::EQ) {
            constraint.constraint_types.push_back(system::ConstraintType::EQ);
            constraint.constraint_values.push_back(
                record::DataValue(record::DataTypeName::INT, false, int_val));
        } else if (op == Operator::NEQ) {
            constraint.constraint_types.push_back(system::ConstraintType::NEQ);
            constraint.constraint_values.push_back(
                record::DataValue(record::DataTypeName::INT, false, int_val));
        } else {
            constraint.constraint_types.push_back(system::ConstraintType::LEQ);
            constraint.constraint_values.push_back(record::DataValue(
                record::DataTypeName::INT, false, int_upper_bound));
            constraint.constraint_types.push_back(system::ConstraintType::GEQ);
            constraint.constraint_values.push_back(record::DataValue(
                record::DataTypeName::INT, false, int_lower_bound));
        }
    } else if (type == VariableType::FLOAT) {
        constraint.data_type = record::DataTypeName::FLOAT;
        if (op == Operator::EQ) {
            constraint.constraint_types.push_back(system::ConstraintType::EQ);
            constraint.constraint_values.push_back(record::DataValue(
                record::DataTypeName::FLOAT, false, float_val));
        } else if (op == Operator::NEQ) {
            constraint.constraint_types.push_back(system::ConstraintType::NEQ);
            constraint.constraint_values.push_back(record::DataValue(
                record::DataTypeName::FLOAT, false, float_val));
        } else if (op == Operator::LT) {
            constraint.constraint_types.push_back(system::ConstraintType::LT);
            constraint.constraint_values.push_back(record::DataValue(
                record::DataTypeName::FLOAT, false, float_upper_bound));
        } else if (op == Operator::LEQ) {
            constraint.constraint_types.push_back(system::ConstraintType::LEQ);
            constraint.constraint_values.push_back(record::DataValue(
                record::DataTypeName::FLOAT, false, float_upper_bound));
        } else if (op == Operator::GT) {
            constraint.constraint_types.push_back(system::ConstraintType::GT);
            constraint.constraint_values.push_back(record::DataValue(
                record::DataTypeName::FLOAT, false, float_lower_bound));
        } else if (op == Operator::GEQ) {
            constraint.constraint_types.push_back(system::ConstraintType::GEQ);
            constraint.constraint_values.push_back(record::DataValue(
                record::DataTypeName::FLOAT, false, float_lower_bound));
        }
    } else if (type == VariableType::STRING) {
        constraint.data_type = record::DataTypeName::VARCHAR;
        if (op == Operator::EQ) {
            constraint.constraint_types.push_back(system::ConstraintType::EQ);
            constraint.constraint_values.push_back(record::DataValue(
                record::DataTypeName::VARCHAR, false, str_val));
        } else if (op == Operator::NEQ) {
            constraint.constraint_types.push_back(system::ConstraintType::NEQ);
            constraint.constraint_values.push_back(record::DataValue(
                record::DataTypeName::VARCHAR, false, str_val));
        } else if (op == Operator::LT) {
            constraint.constraint_types.push_back(system::ConstraintType::LT);
            constraint.constraint_values.push_back(record::DataValue(
                record::DataTypeName::VARCHAR, false, str_val));
        } else if (op == Operator::LEQ) {
            constraint.constraint_types.push_back(system::ConstraintType::LEQ);
            constraint.constraint_values.push_back(record::DataValue(
                record::DataTypeName::VARCHAR, false, str_val));
        } else if (op == Operator::GT) {
            constraint.constraint_types.push_back(system::ConstraintType::GT);
            constraint.constraint_values.push_back(record::DataValue(
                record::DataTypeName::VARCHAR, false, str_val));
        } else if (op == Operator::GEQ) {
            constraint.constraint_types.push_back(system::ConstraintType::GEQ);
            constraint.constraint_values.push_back(record::DataValue(
                record::DataTypeName::VARCHAR, false, str_val));
        }
    } else if (type == VariableType::NULL_OR_NOT) {
        constraint.data_type = record::DataTypeName::ANY;
        if (is_null) {
            constraint.constraint_types.push_back(system::ConstraintType::EQ);
            constraint.constraint_values.push_back(record::DataValue(
                record::DataTypeName::ANY, true, 0));
        } else {
            constraint.constraint_types.push_back(system::ConstraintType::NEQ);
            constraint.constraint_values.push_back(record::DataValue(
                record::DataTypeName::ANY, true, 0));
        }
    }
    return constraint;
}

Condition::Condition() {
    table_name = "";
    column_name = "";
    op = Operator::UNDEFINED;
    type = VariableType::INT;
    int_val = -1;
    float_val = -1;
    string_val = "";
    table_name_other = "";
    column_name_other = "";
    is_null = false;
}

IndexCondition::IndexCondition() {
    table_id = -1;
    column_id = -1;
    op = Operator::UNDEFINED;
    type = VariableType::INT;
    int_lower_bound = INT_MIN;
    int_upper_bound = INT_MAX;
    int_val = -1;
    float_lower_bound = std::numeric_limits<double>::min();
    float_upper_bound = std::numeric_limits<double>::max();
    float_val = -1;
    str_val = "";
    table_id_other = -1;
    column_id_other = -1;
    is_null = false;
}

IndexCondition::IndexCondition(int table_id_, int column_id_,
                               Condition& condition,
                               system::SystemManager* sm) {
    table_id = table_id_;
    column_id = column_id_;

    op = condition.op;
    type = condition.type;

    int_lower_bound = INT_MIN;
    int_upper_bound = INT_MAX;
    int_val = -1;
    float_lower_bound = std::numeric_limits<double>::min();
    float_upper_bound = std::numeric_limits<double>::max();
    float_val = -1;
    str_val = "";
    table_id_other = -1;
    column_id_other = -1;
    is_null = false;

    if (type == VariableType::INT) {
        switch (op) {
            case Operator::EQ:
                int_val = condition.int_val;
                break;
            case Operator::NEQ:
                int_val = condition.int_val;
                break;
            case Operator::LT:
                int_upper_bound = condition.int_val - 1;
                break;
            case Operator::LEQ:
                int_upper_bound = condition.int_val;
                break;
            case Operator::GT:
                int_lower_bound = condition.int_val + 1;
                break;
            case Operator::GEQ:
                int_lower_bound = condition.int_val;
                break;
            default:
                break;
        }
    } else if (type == VariableType::FLOAT) {
        switch (op) {
            case Operator::EQ:
                float_val = condition.float_val;
                break;
            case Operator::NEQ:
                float_val = condition.float_val;
                break;
            case Operator::LT:
                float_upper_bound = condition.float_val;
                break;
            case Operator::LEQ:
                float_upper_bound = condition.float_val;
                break;
            case Operator::GT:
                float_lower_bound = condition.float_val;
                break;
            case Operator::GEQ:
                float_lower_bound = condition.float_val;
                break;
            default:
                break;
        }
    } else if (type == VariableType::STRING) {
        str_val = condition.string_val;
    } else if (type == VariableType::TABLE_COLUMN) {
        table_id_other = sm->getTableId(condition.table_name_other.c_str());

        std::vector<record::ColumnType> column_types;
        if (table_id_other == -1) {
            std::cout << "!ERROR!" << std::endl;
            std::cout << "Table " << condition.table_name_other
                      << " does not exist" << std::endl;
        }
        // TODO: 似乎无法报错
        sm->getTableColumnTypes(table_id_other, column_types);
        for (int i = 0; i < column_types.size(); i++) {
            if (column_types[i].column_name == condition.column_name_other) {
                column_id_other = column_types[i].column_id;
                break;
            }
        }
        if (column_id_other == -1) {
            std::cout << "!ERROR!" << std::endl;
            std::cout << "Column " << condition.column_name_other
                      << " does not exist" << std::endl;
        }
        // TODO: 似乎无法报错
    } else if (type == VariableType::NULL_OR_NOT) {
        is_null = condition.is_null;
    }
}

bool IndexCondition::update(int table_id_, int column_id_,
                            Condition& condition) {
    if (table_id != table_id_ || column_id != column_id_) return false;
    if (type == VariableType::STRING || type == VariableType::TABLE_COLUMN ||
        type == VariableType::FLOAT || type == VariableType::NULL_OR_NOT ||
        type != condition.type)
        return false;

    // update if op in [LT, LEQ, GT, GEQ]
    switch (condition.op) {
        case Operator::LT:
            if (type == VariableType::INT) {
                if (condition.int_val - 1 < int_upper_bound) {
                    int_upper_bound = condition.int_val - 1;
                }
            } else if (type == VariableType::FLOAT) {
                if (condition.float_val < float_upper_bound) {
                    float_upper_bound = condition.float_val;
                }
            }
            break;
        case Operator::LEQ:
            if (type == VariableType::INT) {
                if (condition.int_val < int_upper_bound) {
                    int_upper_bound = condition.int_val;
                }
            } else if (type == VariableType::FLOAT) {
                if (condition.float_val < float_upper_bound) {
                    float_upper_bound = condition.float_val;
                }
            }
            break;
        case Operator::GT:
            if (type == VariableType::INT) {
                if (condition.int_val + 1 > int_lower_bound) {
                    int_lower_bound = condition.int_val + 1;
                }
            } else if (type == VariableType::FLOAT) {
                if (condition.float_val > float_lower_bound) {
                    float_lower_bound = condition.float_val;
                }
            }
            break;
        case Operator::GEQ:
            if (type == VariableType::INT) {
                if (condition.int_val > int_lower_bound) {
                    int_lower_bound = condition.int_val;
                }
            } else if (type == VariableType::FLOAT) {
                if (condition.float_val > float_lower_bound) {
                    float_lower_bound = condition.float_val;
                }
            }
            break;
    }
    return true;
}

bool update_all_index_condition(std::vector<IndexCondition>& index_conditions,
                                std::vector<Condition>& conditions,
                                system::SystemManager* sm,
                                std::vector<std::string> table_names) {
    bool updated = false;
    std::string selected_table_name = "";

    for (auto& condition : conditions) {
        // get table name
        if (condition.table_name == "") {
            if (table_names.size() == 1) {
                selected_table_name = table_names[0];
            } else {
                selected_table_name = sm->findTableNameOfColumnName(
                    condition.column_name, table_names);
            }
            if (selected_table_name == "") {
                std::cout << "!ERROR!" << std::endl;
                std::cout << "No table contains column "
                          << condition.column_name << std::endl;
                return false;
            }
        } else {
            selected_table_name = condition.table_name;
        }

        // get table id
        int table_id = sm->getTableId(selected_table_name.c_str());
        if (table_id == -1) {
            std::cout << "!ERROR!" << std::endl;
            std::cout << "Table " << selected_table_name
                      << " does not exist" << std::endl;
            return false;
        }

        // get column id
        int column_id;
        std::vector<record::ColumnType> column_types;
        sm->getTableColumnTypes(table_id, column_types);
        for (int i = 0; i < column_types.size(); i++) {
            if (column_types[i].column_name == condition.column_name) {
                column_id = column_types[i].column_id;
                break;
            }
        }
        if (column_id == -1) {
            std::cout << "!ERROR!" << std::endl;
            std::cout << "Column " << condition.column_name
                      << " does not exist" << std::endl;
            return false;
        }

        // update index condition
        for (auto& previous_condition : index_conditions) {
            if (previous_condition.table_id == table_id &&
                previous_condition.column_id == column_id) {
                if (previous_condition.update(table_id, column_id, condition)) {
                    updated = true;
                    break;
                }
            }
        }
        if (!updated)
            index_conditions.push_back(
                condition::IndexCondition(table_id, column_id, condition, sm));
        updated = false;
    }

    return true;
}

}  // namespace condition
}  // namespace dbs