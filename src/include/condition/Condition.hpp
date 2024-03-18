#pragma once

#include <iostream>
#include <limits>
#include <set>
#include <vector>

#include "common/Config.hpp"
#include "system/SystemColumns.hpp"
#include "system/SystemManager.hpp"

namespace dbs {
namespace condition {

enum class Operator { EQ, NEQ, LT, LEQ, GT, GEQ, IS, UNDEFINED };
enum class VariableType { INT, FLOAT, STRING, DATE, NULL_OR_NOT, TABLE_COLUMN };

struct NullType {};

struct Condition {
    std::string table_name;
    std::string column_name;
    Operator op;
    VariableType type;

    int int_val;
    double float_val;
    std::string string_val;
    std::string table_name_other, column_name_other;
    bool is_null;
    record::DateValue date_val;
    Condition();
};

struct IndexCondition {
    int table_id, column_id;
    int int_lower_bound, int_upper_bound, int_val;
    double float_lower_bound, float_upper_bound, float_val;
    std::string str_val;
    int table_id_other, column_id_other;
    bool is_null;

    Operator op;
    VariableType type;

    IndexCondition();
    IndexCondition(int table_id_, int column_id_, Condition& condition,
                   system::SystemManager* sm);
    /**
     * @brief update upper_bound and lower_bound of the current condition
     * @param table_id_     the table id of the new condition
     * @param column_id_    the column id of the new condition
     * @param condition     the new condition
     * @return true if successfully updated
     */
    bool update(int table_id_, int column_id_, Condition& condition);

    system::SearchConstraint to_search_constraint() const;
};

bool update_all_index_condition(std::vector<IndexCondition>& index_conditions,
                                std::vector<Condition>& conditions,
                                system::SystemManager* sm,
                                std::vector<std::string> table_namesq);

}  // namespace condition
}  // namespace dbs