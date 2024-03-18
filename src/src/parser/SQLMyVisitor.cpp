#include "parser/SQLMyVisitor.hpp"

namespace dbs {
namespace parser {

class NotImplementedError : public std::exception {
   private:
    std::string name;

   public:
    NotImplementedError(std::string name) : name(name) {}
    const char* what() const throw() {
        std::cerr << "Not implemented: " << name << std::endl;
        return ("Not implemented: " + name).c_str();
    }
};

struct JoinEdge {
    std::vector<int> start_pt_id;
    std::vector<int> end_pt_id;
    JoinEdge() {
        start_pt_id = std::vector<int>();
        end_pt_id = std::vector<int>();
    }
};

ParseResult::ParseResult() {
    columns.clear();
    data_items.clear();
}

ParseResult::ParseResult(const std::vector<record::ColumnType>& columns_,
                         const std::vector<record::DataItem>& data_items_) {
    columns = columns_;
    data_items = data_items_;
}

ParseResult::ParseResult(
    const std::vector<record::ColumnType>& columns_,
    const std::vector<record::DataItem>& data_items_,
    const std::vector<std::string>& primary_keys_,
    const std::vector<system::ForeignKeyInputInfo>& foreign_keys_) {
    columns = columns_;
    data_items = data_items_;
    primary_keys = primary_keys_;
    foreign_keys = foreign_keys_;
}
ParseResult::ParseResult(
    const std::vector<record::ColumnType>& columns_,
    const std::vector<record::DataItem>& data_items_,
    const std::vector<std::string>& primary_keys_,
    const std::vector<system::ForeignKeyInputInfo>& foreign_keys_,
    const std::vector<std::vector<std::string>>& indexes_,
    const std::vector<std::vector<std::string>>& unique_) {
    columns = columns_;
    data_items = data_items_;
    primary_keys = primary_keys_;
    foreign_keys = foreign_keys_;
    indexes = indexes_;
    unique = unique_;
}

void ParseResult::printTableRow(const std::vector<std::string>& values) {
    std::cout << "|";
    for (const auto& value : values) {
        std::cout << std::setw(11) << value << "|";
    }
    std::cout << std::endl;
}

void ParseResult::print(bool output_mode, int duration) {
    if (output_mode == false) {
        for (int i = 0; i < columns.size() - 1; i++) {
            std::cout << columns[i].column_name << ",";
        }
        if (columns.size() > 0)
            std::cout << columns[columns.size() - 1].column_name << std::endl;

        for (auto& item : data_items) {
            for (int i = 0; i < columns.size() - 1; i++) {
                for (int j = 0; j < item.column_ids.size(); j++) {
                    if (item.column_ids[j] == columns[i].column_id) {
                        item.data_values[j].print();
                        std::cout << ",";
                        break;
                    }
                }
            }
            for (int i = 0; i < item.column_ids.size(); i++) {
                if (columns.size() > 0 &&
                    item.column_ids[i] ==
                        columns[columns.size() - 1].column_id) {
                    item.data_values[i].print();
                    break;
                }
            }
            std::cout << std::endl;
        }

    } else {
        std::cout << "+";
        for (int i = 0; i < columns.size() * 12 - 1; i++) {
            std::cout << "-";
        }
        std::cout << "+" << std::endl;
        std::vector<std::string> columnNames;
        for (const auto& column : columns) {
            columnNames.push_back(column.column_name);
        }
        printTableRow(columnNames);

        std::cout << "+";
        for (int i = 0; i < columns.size() * 12 - 1; i++) {
            std::cout << "-";
        }
        std::cout << "+" << std::endl;
        for (const auto& item : data_items) {
            std::vector<std::string> rowData;
            for (const auto& column : columns) {
                auto it = std::find(item.column_ids.begin(),
                                    item.column_ids.end(), column.column_id);
                if (it != item.column_ids.end()) {
                    int index = std::distance(item.column_ids.begin(), it);
                    rowData.push_back(item.data_values[index].toString());
                } else {
                    rowData.push_back("");
                }
            }
            printTableRow(rowData);
        }

        std::cout << "+";
        for (int i = 0; i < columns.size() * 12 - 1; i++) {
            std::cout << "-";
        }
        std::cout << "+" << std::endl;
        if (data_items.size() < 2) {
            std::cout << data_items.size() << " row in set (" << duration
                      << " ms)" << std::endl;
        } else {
            std::cout << data_items.size() << " rows in set (" << duration
                      << " ms)" << std::endl;
        }
    }

    if (primary_keys.size() + foreign_keys.size() > 0) {
        std::cout << std::endl;

        if (primary_keys.size() > 0) {
            std::cout << "PRIMARY KEY (";
            for (int i = 0; i < primary_keys.size() - 1; i++) {
                std::cout << primary_keys[i] << ", ";
            }
            std::cout << primary_keys[primary_keys.size() - 1] << ");"
                      << std::endl;
        }

        for (auto& foreign_key : foreign_keys) {
            std::cout << "FOREIGN KEY (";
            for (int i = 0; i < foreign_key.foreign_key_column_names.size() - 1;
                 i++) {
                std::cout << foreign_key.foreign_key_column_names[i] << ", ";
            }
            std::cout << *std::next(
                             foreign_key.foreign_key_column_names.begin(),
                             foreign_key.foreign_key_column_names.size() - 1)
                      << ") REFERENCES " << foreign_key.reference_table_name
                      << "(";
            for (int i = 0; i < foreign_key.reference_column_names.size() - 1;
                 i++) {
                std::cout << foreign_key.reference_column_names[i] << ", ";
            }
            std::cout << *std::next(
                             foreign_key.reference_column_names.begin(),
                             foreign_key.reference_column_names.size() - 1)
                      << ");" << std::endl;
        }
    }
    if (indexes.size() > 0) {
        std::cout << std::endl;
        for (auto& index : indexes) {
            std::cout << "INDEX (";
            for (int i = 0; i < index.size() - 1; i++) {
                std::cout << index[i] << ", ";
            }
            std::cout << *std::next(index.begin(), index.size() - 1) << ");"
                      << std::endl;
        }
    }
    if (unique.size() > 0) {
        std::cout << std::endl;
        for (auto& u : unique) {
            std::cout << "UNIQUE (";
            for (int i = 0; i < u.size() - 1; i++) {
                std::cout << u[i] << ", ";
            }
            std::cout << *std::next(u.begin(), u.size() - 1) << ");"
                      << std::endl;
        }
    }
}

SQLMyVisitor::SQLMyVisitor(record::RecordManager* rm_, index::IndexManager* im_,
                           system::SystemManager* sm_, bool output_mode_) {
    rm = rm_;
    im = im_;
    sm = sm_;
    output_mode = output_mode_;
}

SQLMyVisitor::~SQLMyVisitor() {
    rm = nullptr;
    im = nullptr;
    sm = nullptr;
}

std::any SQLMyVisitor::visitProgram(antlr4::SQLParser::ProgramContext* ctx) {
    auto start_time = std::chrono::high_resolution_clock::now();
    auto res = visitChildren(ctx);
    if (res.type() == typeid(ParseResult)) {
        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
            end_time - start_time);
        std::any_cast<ParseResult>(res).print(output_mode, duration.count());
        return true;
    } else if (res.type() == typeid(bool)) {
        return std::any_cast<bool>(res);
    } else {
        return false;
    }
}

std::any SQLMyVisitor::aggregateResult(std::any result, std::any nextResult) {
    if (nextResult.type() == typeid(ParseResult)) {
        return std::any_cast<ParseResult>(nextResult);
    } else if (nextResult.type() == typeid(record::ColumnType)) {
        return std::any_cast<record::ColumnType>(nextResult);
    } else if (nextResult.type() == typeid(bool)) {
        return std::any_cast<bool>(nextResult);
    } else {
        return result;
    }
}

std::any SQLMyVisitor::visitStatement(
    antlr4::SQLParser::StatementContext* ctx) {
    return visitChildren(ctx);
}

std::any SQLMyVisitor::visitCreate_db(
    antlr4::SQLParser::Create_dbContext* ctx) {
    auto db_name = ctx->Identifier()->getText();
    return sm->createDatabase(db_name.c_str());
}

std::any SQLMyVisitor::visitDrop_db(antlr4::SQLParser::Drop_dbContext* ctx) {
    return sm->dropDatabase(ctx->Identifier()->getText().c_str());
}

std::any SQLMyVisitor::visitShow_dbs(antlr4::SQLParser::Show_dbsContext* ctx) {
    std::vector<record::DataItem> database_names;
    std::vector<record::ColumnType> column_types;
    sm->getAllDatabase(database_names, column_types);
    return ParseResult(column_types, database_names);
}

std::any SQLMyVisitor::visitUse_db(antlr4::SQLParser::Use_dbContext* ctx) {
    return sm->useDatabase(ctx->Identifier()->getText().c_str());
}

std::any SQLMyVisitor::visitShow_tables(
    antlr4::SQLParser::Show_tablesContext* ctx) {
    std::vector<record::DataItem> table_names;
    std::vector<record::ColumnType> column_types;
    if (!sm->getAllTable(table_names, column_types)) return false;
    return ParseResult(column_types, table_names);
}

std::any SQLMyVisitor::visitShow_indexes(
    antlr4::SQLParser::Show_indexesContext* ctx) {
    throw NotImplementedError("SQLMyVisitor::visitShow_indexes");
}

std::any SQLMyVisitor::visitCreate_table(
    antlr4::SQLParser::Create_tableContext* ctx) {
    std::vector<record::ColumnType> column_types;
    std::vector<std::string> primary_keys;
    std::vector<system::ForeignKeyInputInfo> foreign_keys;
    for (auto child : ctx->children) {
        std::any result = child->accept(this);
        if (result.type() ==
            typeid(std::tuple<std::vector<record::ColumnType>,
                              std::vector<std::string>,
                              std::vector<system::ForeignKeyInputInfo>>)) {
            auto result_tuple = std::any_cast<std::tuple<
                std::vector<record::ColumnType>, std::vector<std::string>,
                std::vector<system::ForeignKeyInputInfo>>>(result);
            column_types = std::get<0>(result_tuple);
            primary_keys = std::get<1>(result_tuple);
            foreign_keys = std::get<2>(result_tuple);
        }
    }

    // TODO unique, default value 未处理
    auto table_name = ctx->Identifier()->getText();
    return sm->createTable(table_name.c_str(), column_types, primary_keys,
                           foreign_keys);
}

std::any SQLMyVisitor::visitDrop_table(
    antlr4::SQLParser::Drop_tableContext* ctx) {
    return sm->dropTable(ctx->Identifier()->getText().c_str());
}

std::any SQLMyVisitor::visitDescribe_table(
    antlr4::SQLParser::Describe_tableContext* ctx) {
    std::vector<record::DataItem> table_names;
    std::vector<record::ColumnType> column_types;
    std::vector<std::string> primary_keys;
    std::vector<system::ForeignKeyInputInfo> foreign_keys;
    std::vector<std::vector<std::string>> index;
    std::vector<std::vector<std::string>> unique;
    if (!sm->describeTable(ctx->Identifier()->getText().c_str(), table_names,
                           column_types, primary_keys, foreign_keys, index,
                           unique))
        return false;
    return ParseResult(column_types, table_names, primary_keys, foreign_keys,
                       index, unique);
}

std::any SQLMyVisitor::visitLoad_table(
    antlr4::SQLParser::Load_tableContext* ctx) {
    std::string file_path = ctx->String()[0]->getText().substr(1);
    file_path.pop_back();

    std::string delimeter = ctx->String()[1]->getText().substr(1);
    delimeter.pop_back();
    return sm->loadTableFromFile(ctx->Identifier()->getText().c_str(),
                                 file_path.c_str(), delimeter.c_str());
}

std::any SQLMyVisitor::visitInsert_into_table(
    antlr4::SQLParser::Insert_into_tableContext* ctx) {
    std::vector<record::DataItem> result;
    for (auto child : ctx->children) {
        std::any res = child->accept(this);
        if (res.type() == typeid(std::vector<record::DataItem>)) {
            result = std::any_cast<std::vector<record::DataItem>>(res);
        }
    }
    return sm->insertIntoTable(ctx->Identifier()->getText().c_str(), result);
}

std::any SQLMyVisitor::visitDelete_from_table(
    antlr4::SQLParser::Delete_from_tableContext* ctx) {
    std::string table_name;
    std::vector<condition::Condition> conditions;

    enum Expecting {
        DELETE_,
        FROM_,
        FROM_TABLE,
        WHERE_,
        WHERE_CONDITION,
        FINISHED
    } expecting_target = Expecting::DELETE_;

    std::any result;
    for (auto child : ctx->children) {
        switch (expecting_target) {
            case Expecting::DELETE_:
                expecting_target = Expecting::FROM_;
                break;
            case Expecting::FROM_:
                expecting_target = Expecting::FROM_TABLE;
                break;
            case Expecting::FROM_TABLE:
                table_name = child->getText();
                expecting_target = Expecting::WHERE_;
                break;
            case Expecting::WHERE_:
                expecting_target = Expecting::WHERE_CONDITION;
                break;
            case Expecting::WHERE_CONDITION:
                result = child->accept(this);
                if (result.type() ==
                    typeid(std::vector<condition::Condition>)) {
                    conditions =
                        std::any_cast<std::vector<condition::Condition>>(
                            result);
                }
                expecting_target = Expecting::FINISHED;
                break;
            case Expecting::FINISHED:
                break;
        }
    }

    std::vector<condition::IndexCondition> index_conditions;
    condition::update_all_index_condition(index_conditions, conditions, sm,
                                          std::vector<std::string>{table_name});

    std::vector<system::SearchConstraint> constraints;
    for (auto& index_condition : index_conditions) {
        constraints.push_back(index_condition.to_search_constraint());
    }

    int table_id = sm->getTableId(table_name.c_str());
    if (table_id == -1) {
        std::cout << "!ERROR" << std::endl;
        std::cout << "Table " << table_name << " does not exist" << std::endl;
        return false;
    }

    return sm->deleteFromTable(table_id, constraints);
}

std::any SQLMyVisitor::visitUpdate_table(
    antlr4::SQLParser::Update_tableContext* ctx) {
    std::string table_name;
    std::vector<std::tuple<std::string, record::DataValue>> update_info;
    std::vector<condition::Condition> conditions;

    enum Expecting {
        UPDATE_,
        TABLE_NAME,
        SET_,
        UPDATE_INFO,
        WHERE_,
        WHERE_CONDITION,
        FINISHED
    } expecting_target = Expecting::UPDATE_;

    std::any result;
    for (auto child : ctx->children) {
        switch (expecting_target) {
            case Expecting::UPDATE_:
                expecting_target = Expecting::TABLE_NAME;
                break;
            case Expecting::TABLE_NAME:
                table_name = child->getText();
                expecting_target = Expecting::SET_;
                break;
            case Expecting::SET_:
                expecting_target = Expecting::UPDATE_INFO;
                break;
            case Expecting::UPDATE_INFO:
                result = child->accept(this);
                if (result.type() ==
                    typeid(std::vector<
                           std::tuple<std::string, record::DataValue>>)) {
                    update_info = std::any_cast<std::vector<
                        std::tuple<std::string, record::DataValue>>>(result);
                }
                expecting_target = Expecting::WHERE_;
                break;
            case Expecting::WHERE_:
                expecting_target = Expecting::WHERE_CONDITION;
                break;
            case Expecting::WHERE_CONDITION:
                result = child->accept(this);
                if (result.type() ==
                    typeid(std::vector<condition::Condition>)) {
                    conditions =
                        std::any_cast<std::vector<condition::Condition>>(
                            result);
                }
                expecting_target = Expecting::FINISHED;
                break;
            case Expecting::FINISHED:
                break;
        }
    }

    std::vector<condition::IndexCondition> index_conditions;
    condition::update_all_index_condition(index_conditions, conditions, sm,
                                          std::vector<std::string>{table_name});

    int table_id = sm->getTableId(table_name.c_str());
    if (table_id == -1) {
        std::cout << "!ERROR" << std::endl;
        std::cout << "Table " << table_name << " does not exist" << std::endl;
        return false;
    }

    std::vector<system::SearchConstraint> constraints;
    for (auto& index_condition : index_conditions) {
        constraints.push_back(index_condition.to_search_constraint());
    }
    sm->fillInDataTypeField(constraints, table_id);

    std::vector<std::string> column_names;
    record::DataItem data_item;
    sm->fillInDataTypeField(update_info, table_id, column_names, data_item);
    sm->getColumnID(table_id, column_names, data_item.column_ids);

    return sm->update(table_id, constraints, data_item);
}

std::any SQLMyVisitor::visitSelect_table_(
    antlr4::SQLParser::Select_table_Context* ctx) {
    return visitChildren(ctx);
}

std::any SQLMyVisitor::visitSelect_table(
    antlr4::SQLParser::Select_tableContext* ctx) {
    std::vector<std::string> table_names;
    std::vector<std::tuple<std::string, std::string>> column_names;
    std::vector<condition::Condition> conditions;
    std::string order_by_column_name = "";
    bool order_ascending = true;
    int limit_num = -1;
    int offset_num = -1;

    enum Expecting {
        SELECT_,
        SELECTED_COLUMNS,
        FROM_,
        FROM_TABLE,
        CONDITION_START,
        WHERE_CONDITION,
        GROUP_BY,
        HAVING,
        ORDER_BY_BY_,
        ORDER_BY_COLUMN,
        LIMIT_NUM,
        OFFSET_NUM
    } expecting_target = Expecting::SELECT_;

    std::any result;

    for (auto child : ctx->children) {
        switch (expecting_target) {
            case Expecting::SELECT_:
                expecting_target = Expecting::SELECTED_COLUMNS;
                break;
            case Expecting::SELECTED_COLUMNS:
                result = child->accept(this);
                column_names = std::any_cast<
                    std::vector<std::tuple<std::string, std::string>>>(result);
                expecting_target = Expecting::FROM_;
                break;
            case Expecting::FROM_:
                expecting_target = Expecting::FROM_TABLE;
                break;
            case Expecting::FROM_TABLE:
                result = child->accept(this);
                table_names = std::any_cast<std::vector<std::string>>(result);
                expecting_target = Expecting::CONDITION_START;
                break;
            case Expecting::CONDITION_START:
                if (child->getText() == "WHERE") {
                    expecting_target = Expecting::WHERE_CONDITION;
                } else if (child->getText() == "ORDER") {
                    expecting_target = Expecting::ORDER_BY_BY_;
                } else if (child->getText() == "LIMIT") {
                    expecting_target = Expecting::LIMIT_NUM;
                } else if (child->getText() == "OFFSET") {
                    expecting_target = Expecting::OFFSET_NUM;
                } else if (child->getText() == "DESC") {
                    order_ascending = false;
                    expecting_target = Expecting::CONDITION_START;
                } else {
                    throw NotImplementedError(
                        "SQLMyVisitor::visitSelect_table");
                }
                break;
            case Expecting::WHERE_CONDITION:
                result = child->accept(this);
                conditions =
                    std::any_cast<std::vector<condition::Condition>>(result);
                expecting_target = Expecting::CONDITION_START;
                break;
            case Expecting::ORDER_BY_BY_:
                expecting_target = Expecting::ORDER_BY_COLUMN;
                break;
            case Expecting::ORDER_BY_COLUMN:
                order_by_column_name = child->getText();
                expecting_target = Expecting::CONDITION_START;
                break;
            case Expecting::LIMIT_NUM:
                limit_num = std::stoi(child->getText());
                expecting_target = Expecting::CONDITION_START;
                break;
            case Expecting::OFFSET_NUM:
                offset_num = std::stoi(child->getText());
                expecting_target = Expecting::CONDITION_START;
                break;
            default:
                break;
        }
    }

    // TODO: Ambiguous column name
    for (auto& column_name : column_names) {
        if (std::get<0>(column_name) == "") {
            std::get<0>(column_name) = sm->findTableNameOfColumnName(
                std::get<1>(column_name), table_names);
            if (std::get<0>(column_name) == "") return false;
        }
    }

    std::vector<condition::IndexCondition> index_conditions;
    condition::update_all_index_condition(index_conditions, conditions, sm,
                                          table_names);

    if (sm->getCurrentDatabaseId() == -1) {
        std::cout << "ERROR!" << std::endl;
        std::cout << "no database selected" << std::endl;
        return false;
    }

    // *table_names*

    if (table_names.size() == 1) {
        std::vector<record::DataItem> result_datas;
        std::vector<record::ColumnType> column_types;
        int table_id = sm->getTableId(table_names[0].c_str());
        if (table_id == -1) {
            std::cout << "!ERROR" << std::endl;
            std::cout << "Table " << table_names[0] << " does not exist"
                      << std::endl;
            return false;
        }

        std::vector<system::SearchConstraint> constraints;
        for (auto& index_condition : index_conditions) {
            constraints.push_back(index_condition.to_search_constraint());
        }
        sm->fillInDataTypeField(constraints, table_id);

        std::vector<record::RecordLocation> record_locations;
        if (!sm->search(table_id, constraints, result_datas, column_types,
                        record_locations, -1)) {
            return false;
        }

        if (order_by_column_name != "") {
            int order_column_id = -1;
            for (auto& column_type : column_types) {
                if (column_type.column_name == order_by_column_name) {
                    order_column_id = column_type.column_id;
                    break;
                }
            }
            if (order_column_id == -1) {
                std::cout << "!ERROR" << std::endl;
                std::cout << "order column does not exist" << std::endl;
            }
            // sort result_datas accordto it's item's datavlue[order_column_id]
            std::sort(
                result_datas.begin(), result_datas.end(),
                [order_column_id, order_ascending](const record::DataItem& a,
                                                   const record::DataItem& b) {
                    if (order_ascending)
                        return a.data_values[order_column_id] <
                               b.data_values[order_column_id];
                    else
                        return a.data_values[order_column_id] >
                               b.data_values[order_column_id];
                });
        }
        if (offset_num != -1) {
            // skip the first offset_num item in result_data
            if (result_datas.size() >= offset_num)
                result_datas.erase(result_datas.begin(),
                                   result_datas.begin() + offset_num);
            else
                result_datas.clear();
        }
        if (limit_num != -1) {
            if (result_datas.size() > limit_num)
                result_datas.erase(result_datas.begin() + limit_num,
                                   result_datas.end());
        }

        std::vector<record::ColumnType> result_column_types;
        for (auto& column_tuple : column_names) {
            auto column_name = std::get<1>(column_tuple);
            if (column_name == "*")
                return ParseResult(column_types, result_datas);
            for (auto& column_type : column_types) {
                if (column_type.column_name == column_name) {
                    result_column_types.push_back(column_type);
                    break;
                }
            }
        }
        return ParseResult(result_column_types, result_datas);
    }

    int total_table_num = table_names.size();
    std::map<int, int> table_id2point_id;
    std::map<int, int> point_id2table_id;
    for (int i = 0; i < total_table_num; i++) {
        table_id2point_id[sm->getTableId(table_names[i].c_str())] = i;
        point_id2table_id[i] = sm->getTableId(table_names[i].c_str());
    }

    std::map<int, std::vector<system::SearchConstraint>>
        table_id2seaerch_constraint;
    std::vector<std::vector<JoinEdge>> join_edges =
        std::vector<std::vector<JoinEdge>>(
            total_table_num, std::vector<JoinEdge>(total_table_num));

    for (auto& index_condition : index_conditions) {
        if (index_condition.type == condition::VariableType::TABLE_COLUMN) {
            int point_id_1 = table_id2point_id[index_condition.table_id];
            int point_id_2 = table_id2point_id[index_condition.table_id_other];
            join_edges[point_id_1][point_id_2].start_pt_id.push_back(
                index_condition.column_id);
            join_edges[point_id_1][point_id_2].end_pt_id.push_back(
                index_condition.column_id_other);

            join_edges[point_id_2][point_id_1].start_pt_id.push_back(
                index_condition.column_id_other);
            join_edges[point_id_2][point_id_1].end_pt_id.push_back(
                index_condition.column_id);

        } else {
            auto& table_id = index_condition.table_id;
            table_id2seaerch_constraint[table_id].push_back(
                index_condition.to_search_constraint());
        }
    }

    // std::map<int, std::vector<record::DataItem>> table_id2result_datas;
    std::map<int, std::vector<record::ColumnType>> table_id2column_types;
    std::map<int, int> table_id2data_num;
    std::map<int, std::string> table_id2path;

    for (int point_id = 0; point_id < total_table_num; point_id++) {
        int table_id = point_id2table_id[point_id];

        std::vector<record::ColumnType> column_types;
        auto& constraint = table_id2seaerch_constraint[table_id];

        std::string path;
        int total_num;
        if (!sm->searchAndSave(table_id, column_types, constraint, path,
                               total_num))
            return false;

        table_id2data_num[table_id] = total_num;
        table_id2column_types[table_id] = column_types;
        table_id2path[table_id] = path;
    }

    std::map<int, bool> table_id2checked;

    std::vector<record::DataItem> result_datas;
    std::vector<record::ColumnType> column_types;

    std::map<std::pair<int, int>, int> original_table_column_id2new_column_id;
    std::map<int, std::pair<int, int>> new_column_id2original_table_column_id;
    int new_column_id_cnt = 0;

    for (int round = 0; round < total_table_num; round++) {
        int select_table_id = -1;
        int max_intersect_num = 0;
        int min_data_num = INT_MAX;
        for (auto& table_id : table_id2data_num) {
            if (table_id2checked[table_id.first]) continue;
            std::vector<JoinEdge>& edge_from_selected_table =
                join_edges[table_id2point_id[table_id.first]];
            int intersect_num = 0;
            for (int i = 0; i < total_table_num; i++)
                if (table_id2checked[point_id2table_id[i]])
                    intersect_num +=
                        edge_from_selected_table[i].start_pt_id.size();
            if (intersect_num > max_intersect_num ||
                (intersect_num == max_intersect_num &&
                 table_id.second < min_data_num)) {
                select_table_id = table_id.first;
                max_intersect_num = intersect_num;
                min_data_num = table_id.second;
            }
        }
        table_id2checked[select_table_id] = true;

        std::vector<record::ColumnType>& upp_column_types =
            table_id2column_types[select_table_id];
        int total_page =
            rm->getTotalPageNum(table_id2path[select_table_id].c_str());

        if (round == 0) {
            for (auto& column_type : upp_column_types) {
                original_table_column_id2new_column_id[std::make_pair(
                    select_table_id, column_type.column_id)] =
                    new_column_id_cnt;
                new_column_id2original_table_column_id[new_column_id_cnt] =
                    std::make_pair(select_table_id, column_type.column_id);
                column_type.column_id = new_column_id_cnt;
                new_column_id_cnt++;
                column_types.push_back(column_type);
            }
            int low_page = 1,
                high_page = std::min(low_page + BLOCK_PAGE_NUM, total_page + 1);
            while (low_page <= total_page) {
                std::vector<record::DataItem> upp_result_datas;
                rm->getRecordsInPageRange(
                    table_id2path[select_table_id].c_str(), upp_result_datas,
                    low_page, high_page);
                for (auto& data_item : upp_result_datas) {
                    for (int i = 0; i < data_item.column_ids.size(); i++) {
                        data_item.column_ids[i] =
                            original_table_column_id2new_column_id
                                [std::make_pair(select_table_id,
                                                data_item.column_ids[i])];
                    }
                    result_datas.push_back(data_item);
                }
                low_page += BLOCK_PAGE_NUM;
                high_page =
                    std::min(high_page + BLOCK_PAGE_NUM, total_page + 1);
            }
        } else {
            std::vector<record::DataItem> new_result_datas;
            std::vector<record::ColumnType> new_column_types;

            std::vector<JoinEdge>& edge_from_selected_table =
                join_edges[table_id2point_id[select_table_id]];
            std::vector<record::ColumnType>& select_column_types =
                upp_column_types;

            new_column_types = column_types;
            for (auto column_type : select_column_types) {
                original_table_column_id2new_column_id[std::make_pair(
                    select_table_id, column_type.column_id)] =
                    new_column_id_cnt;
                new_column_id2original_table_column_id[new_column_id_cnt] =
                    std::make_pair(select_table_id, column_type.column_id);
                column_type.column_id = new_column_id_cnt;
                new_column_id_cnt++;
                new_column_types.push_back(column_type);
            }

            int sort_column_id_for_selected_table = -1;
            int sort_table_id_for_previous_result = -1;
            int sort_original_column_id_for_previous_result = -1;
            std::vector<std::tuple<int, int, int>> joint_constraints;

            for (int edge_point_id = 0; edge_point_id < total_table_num;
                 edge_point_id++) {
                if (!table_id2checked[point_id2table_id[edge_point_id]])
                    continue;
                if (edge_from_selected_table[edge_point_id]
                        .start_pt_id.size() == 0)
                    continue;
                int table_id = point_id2table_id[edge_point_id];
                if (sort_column_id_for_selected_table == -1) {
                    sort_column_id_for_selected_table =
                        edge_from_selected_table[edge_point_id].start_pt_id[0];
                    sort_table_id_for_previous_result = table_id;
                    sort_original_column_id_for_previous_result =
                        edge_from_selected_table[edge_point_id].end_pt_id[0];
                }
                for (int i = 0;
                     i <
                     edge_from_selected_table[edge_point_id].start_pt_id.size();
                     i++) {
                    joint_constraints.push_back(std::make_tuple(
                        edge_from_selected_table[edge_point_id].start_pt_id[i],
                        table_id,
                        edge_from_selected_table[edge_point_id].end_pt_id[i]));
                }
            }

            if (sort_column_id_for_selected_table != -1) {
                int sort_column_idx_for_previous_result =
                    original_table_column_id2new_column_id[std::make_pair(
                        sort_table_id_for_previous_result,
                        sort_original_column_id_for_previous_result)];

                // sort result_datas by sort_column_idx_for_previous_result
                std::sort(
                    result_datas.begin(), result_datas.end(),
                    [sort_column_idx_for_previous_result](
                        const record::DataItem& a, const record::DataItem& b) {
                        return a.data_values
                                   [sort_column_idx_for_previous_result] <
                               b.data_values
                                   [sort_column_idx_for_previous_result];
                    });

                int sort_column_idx_for_selected_table = -1;
                for (int i = 0; i < select_column_types.size(); i++) {
                    if (select_column_types[i].column_id ==
                        sort_column_id_for_selected_table) {
                        sort_column_idx_for_selected_table = i;
                        break;
                    }
                }
                if (sort_column_idx_for_selected_table == -1) {
                    std::cout << "!ERROR" << std::endl;
                    std::cout << "Column " << sort_column_id_for_selected_table
                              << " does not exist" << std::endl;
                    return false;
                }

                int low_page = 1,
                    high_page =
                        std::min(low_page + BLOCK_PAGE_NUM, total_page + 1);
                while (low_page <= total_page) {
                    std::vector<record::DataItem> select_result_datas;
                    rm->getRecordsInPageRange(
                        table_id2path[select_table_id].c_str(),
                        select_result_datas, low_page, high_page);
                    low_page += BLOCK_PAGE_NUM;
                    high_page =
                        std::min(high_page + BLOCK_PAGE_NUM, total_page + 1);

                    // sort select_result_datas by
                    // sort_column_idx_for_selected_table
                    std::sort(
                        select_result_datas.begin(), select_result_datas.end(),
                        [sort_column_idx_for_selected_table](
                            const record::DataItem& a,
                            const record::DataItem& b) {
                            return a.data_values
                                       [sort_column_idx_for_selected_table] <
                                   b.data_values
                                       [sort_column_idx_for_selected_table];
                        });

                    // merge select_result_datas and result_datas
                    int select_result_datas_idx = 0;
                    int result_datas_idx = 0;
                    while (select_result_datas_idx <
                               select_result_datas.size() &&
                           result_datas_idx < result_datas.size()) {
                        while (
                            select_result_datas_idx <
                                select_result_datas.size() &&
                            select_result_datas[select_result_datas_idx]
                                    .data_values
                                        [sort_column_idx_for_selected_table] <
                                result_datas[result_datas_idx].data_values
                                    [sort_column_idx_for_previous_result])
                            select_result_datas_idx++;
                        if (select_result_datas_idx >=
                                select_result_datas.size() ||
                            result_datas_idx >= result_datas.size())
                            break;
                        int select_result_datas_idx_begin =
                            select_result_datas_idx;
                        int result_datas_idx_begin = result_datas_idx;
                        int select_result_datas_idx_end =
                            select_result_datas_idx;
                        int result_datas_idx_end = result_datas_idx;
                        while (
                            select_result_datas_idx_end <
                                select_result_datas.size() &&
                            select_result_datas[select_result_datas_idx_end]
                                    .data_values
                                        [sort_column_idx_for_selected_table] ==
                                select_result_datas
                                    [select_result_datas_idx_begin]
                                        .data_values
                                            [sort_column_idx_for_selected_table])
                            select_result_datas_idx_end++;
                        while (
                            result_datas_idx_end < result_datas.size() &&
                            result_datas[result_datas_idx_end].data_values
                                    [sort_column_idx_for_previous_result] ==
                                result_datas[result_datas_idx_begin].data_values
                                    [sort_column_idx_for_previous_result])
                            result_datas_idx_end++;
                        if (select_result_datas[select_result_datas_idx]
                                .data_values
                                    [sort_column_idx_for_selected_table] !=
                            result_datas[result_datas_idx].data_values
                                [sort_column_idx_for_previous_result]) {
                            result_datas_idx = result_datas_idx_end;
                        }
                        for (; select_result_datas_idx_begin <
                               select_result_datas_idx_end;
                             select_result_datas_idx_begin++) {
                            for (int i = result_datas_idx_begin;
                                 i < result_datas_idx_end; i++) {
                                auto& select_result_data = select_result_datas
                                    [select_result_datas_idx_begin];
                                auto& result_data = result_datas[i];
                                bool flag = true;
                                for (auto& joint_constraint :
                                     joint_constraints) {
                                    int start_pt_id =
                                        std::get<0>(joint_constraint);
                                    int table_id =
                                        std::get<1>(joint_constraint);
                                    int end_pt_id =
                                        std::get<2>(joint_constraint);
                                    end_pt_id =
                                        original_table_column_id2new_column_id
                                            [std::make_pair(table_id,
                                                            end_pt_id)];
                                    int start_pt_id_idx = -1;
                                    int end_pt_id_idx = -1;
                                    for (int j = 0;
                                         j <
                                         select_result_data.column_ids.size();
                                         j++) {
                                        if (select_result_data.column_ids[j] ==
                                            start_pt_id) {
                                            start_pt_id_idx = j;
                                            break;
                                        }
                                    }
                                    for (int j = 0;
                                         j < result_data.column_ids.size();
                                         j++) {
                                        if (result_data.column_ids[j] ==
                                            end_pt_id) {
                                            end_pt_id_idx = j;
                                            break;
                                        }
                                    }
                                    if (start_pt_id_idx == -1 ||
                                        end_pt_id_idx == -1) {
                                        std::cout << "!ERROR" << std::endl;
                                        std::cout << "Column " << start_pt_id
                                                  << " or " << end_pt_id
                                                  << " does not exist"
                                                  << std::endl;
                                        return false;
                                    }
                                    if (select_result_data
                                            .data_values[start_pt_id_idx] !=
                                        result_data
                                            .data_values[end_pt_id_idx]) {
                                        flag = false;
                                        break;
                                    }
                                }
                                if (flag) {
                                    // merge two data
                                    record::DataItem new_data = result_data;
                                    for (int j = 0;
                                         j <
                                         select_result_data.data_values.size();
                                         j++) {
                                        new_data.data_values.push_back(
                                            select_result_data.data_values[j]);
                                        new_data.column_ids.push_back(
                                            original_table_column_id2new_column_id
                                                [std::make_pair(
                                                    select_table_id,
                                                    select_result_data
                                                        .column_ids[j])]);
                                    }
                                    new_result_datas.push_back(new_data);
                                }
                            }
                        }
                        result_datas_idx = result_datas_idx_end;
                    }
                }

            } else {
                throw NotImplementedError("SQLMyVisitor::visitSelect_table");
            }

            std::swap(result_datas, new_result_datas);
            std::swap(column_types, new_column_types);
        }
    }

    std::vector<record::ColumnType> result_column_types;

    for (auto& item : column_names) {
        auto& table_name = std::get<0>(item);
        auto& column_name = std::get<1>(item);
        int table_id = sm->getTableId(table_name.c_str());
        std::vector<int> column_ids;
        sm->getColumnID(table_id, {column_name}, column_ids);
        int column_id = column_ids[0];

        int new_id = original_table_column_id2new_column_id[std::make_pair(
            table_id, column_id)];

        if (output_mode == true) {
            auto insert_column_type = column_types[new_id];
            insert_column_type.column_name = table_name + "." + column_name;
            result_column_types.push_back(insert_column_type);
        } else {
            result_column_types.push_back(column_types[new_id]);
        }
    }

    return ParseResult(result_column_types, result_datas);
}

std::any SQLMyVisitor::visitAlter_add_index(
    antlr4::SQLParser::Alter_add_indexContext* ctx) {
    enum Expecting {
        ALTER_,
        TABLE_,
        TABLE_NAME,
        ADD_,
        INDEX_,
        INDEX_NAME,
        LEFT_PARENTHESE_INDEX_,
        INDEX_COLUMNS,
        RIGHT_PARENTHESE_INDEX_,
        FINISHED
    };
    Expecting expecting_target = Expecting::ALTER_;

    std::string table_name;
    std::string index_name;
    std::vector<std::string> column_names;

    std::any result;
    for (auto& child : ctx->children) {
        switch (expecting_target) {
            case Expecting::ALTER_:
                expecting_target = Expecting::TABLE_;
                break;
            case Expecting::TABLE_:
                expecting_target = Expecting::TABLE_NAME;
                break;
            case Expecting::TABLE_NAME:
                table_name = child->getText();
                expecting_target = Expecting::ADD_;
                break;
            case Expecting::ADD_:
                expecting_target = Expecting::INDEX_;
                break;
            case Expecting::INDEX_:
                expecting_target = Expecting::INDEX_NAME;
                break;
            case Expecting::INDEX_NAME:
                index_name = child->getText();
                expecting_target = Expecting::LEFT_PARENTHESE_INDEX_;
                break;
            case Expecting::LEFT_PARENTHESE_INDEX_:
                expecting_target = Expecting::INDEX_COLUMNS;
                break;
            case Expecting::INDEX_COLUMNS:
                result = child->accept(this);
                column_names = std::any_cast<std::vector<std::string>>(result);
                expecting_target = Expecting::RIGHT_PARENTHESE_INDEX_;
                break;
            case Expecting::RIGHT_PARENTHESE_INDEX_:
                expecting_target = Expecting::FINISHED;
                break;
            case Expecting::FINISHED:
                break;
        }
    }

    int table_id = sm->getTableId(table_name.c_str());
    if (table_id == -1) {
        std::cout << "!ERROR" << std::endl;
        std::cout << "Table " << table_name << " does not exist" << std::endl;
        return false;
    }

    std::vector<int> column_ids;
    std::vector<record::ColumnType> column_types;
    sm->getTableColumnTypes(table_id, column_types);
    for (auto& column_name : column_names) {
        int column_id = -1;
        for (int i = 0; i < column_types.size(); i++) {
            if (column_types[i].column_name == column_name) {
                column_id = column_types[i].column_id;
                break;
            }
        }
        if (column_id == -1) {
            std::cout << "!ERROR" << std::endl;
            std::cout << "Column " << column_name << " does not exist"
                      << std::endl;
            return false;
        }
        column_ids.push_back(column_id);
    }

    return sm->addIndex(table_name.c_str(), index_name, column_ids, false);
}

std::any SQLMyVisitor::visitAlter_drop_index(
    antlr4::SQLParser::Alter_drop_indexContext* ctx) {
    enum Expecting {
        ALTER_,
        TABLE_,
        TABLE_NAME,
        DROP_,
        INDEX_,
        INDEX_NAME,
        FINISHED
    };
    Expecting expecting_target = Expecting::ALTER_;

    std::string table_name;
    std::string index_name;

    for (auto& child : ctx->children) {
        switch (expecting_target) {
            case Expecting::ALTER_:
                expecting_target = Expecting::TABLE_;
                break;
            case Expecting::TABLE_:
                expecting_target = Expecting::TABLE_NAME;
                break;
            case Expecting::TABLE_NAME:
                table_name = child->getText();
                expecting_target = Expecting::DROP_;
                break;
            case Expecting::DROP_:
                expecting_target = Expecting::INDEX_;
                break;
            case Expecting::INDEX_:
                expecting_target = Expecting::INDEX_NAME;
                break;
            case Expecting::INDEX_NAME:
                index_name = child->getText();
                expecting_target = Expecting::FINISHED;
                break;
            case Expecting::FINISHED:
                break;
        }
    }

    int table_id = sm->getTableId(table_name.c_str());
    if (table_id == -1) {
        std::cout << "!ERROR" << std::endl;
        std::cout << "Table " << table_name << " does not exist" << std::endl;
        return false;
    }

    return sm->dropIndex(table_name.c_str(), index_name);
}

std::any SQLMyVisitor::visitAlter_table_drop_pk(
    antlr4::SQLParser::Alter_table_drop_pkContext* ctx) {
    enum Expecting {
        ALTER_,
        TABLE_,
        TABLE_NAME,
        DROP_,
        PRIMARY_,
        KEY_,
        FINISHED
    };
    Expecting expecting_target = Expecting::ALTER_;

    std::string table_name;
    for (auto& child : ctx->children) {
        switch (expecting_target) {
            case Expecting::ALTER_:
                expecting_target = Expecting::TABLE_;
                break;
            case Expecting::TABLE_:
                expecting_target = Expecting::TABLE_NAME;
                break;
            case Expecting::TABLE_NAME:
                table_name = child->getText();
                expecting_target = Expecting::DROP_;
                break;
            case Expecting::DROP_:
                expecting_target = Expecting::PRIMARY_;
                break;
            case Expecting::PRIMARY_:
                expecting_target = Expecting::KEY_;
                break;
            case Expecting::KEY_:
                expecting_target = Expecting::FINISHED;
                break;
            case Expecting::FINISHED:
                break;
        }
    }

    int table_id = sm->getTableId(table_name.c_str());

    sm->dropPrimaryKey(table_id);
    return true;
}

std::any SQLMyVisitor::visitAlter_table_drop_foreign_key(
    antlr4::SQLParser::Alter_table_drop_foreign_keyContext* ctx) {
    enum Expecting {
        ALTER_,
        TABLE_,
        TABLE_NAME,
        DROP_,
        FOREIGN_,
        KEY_,
        KEY_NAME,
        FINISHED
    };
    Expecting expecting_target = Expecting::ALTER_;

    std::string table_name;
    std::string key_name;
    for (auto& child : ctx->children) {
        switch (expecting_target) {
            case Expecting::ALTER_:
                expecting_target = Expecting::TABLE_;
                break;
            case Expecting::TABLE_:
                expecting_target = Expecting::TABLE_NAME;
                break;
            case Expecting::TABLE_NAME:
                table_name = child->getText();
                expecting_target = Expecting::DROP_;
                break;
            case Expecting::DROP_:
                expecting_target = Expecting::FOREIGN_;
                break;
            case Expecting::FOREIGN_:
                expecting_target = Expecting::KEY_;
                break;
            case Expecting::KEY_:
                expecting_target = Expecting::KEY_NAME;
                break;
            case Expecting::KEY_NAME:
                key_name = child->getText();
                expecting_target = Expecting::FINISHED;
                break;
            case Expecting::FINISHED:
                break;
        }
    }

    int table_id = sm->getTableId(table_name.c_str());
    if (table_id == -1) {
        std::cout << "!ERROR" << std::endl;
        std::cout << "Table " << table_name << " does not exist" << std::endl;
        return false;
    }

    return sm->deleteForeignKey(table_name.c_str(), key_name);
}

std::any SQLMyVisitor::visitAlter_table_add_pk(
    antlr4::SQLParser::Alter_table_add_pkContext* ctx) {
    enum Expecting {
        ALTER_,
        TABLE_,
        TABLE_NAME,
        ADD_,
        CONSTRAINT_OR_PRIMARY_,
        CONSTRAINT_NAME,
        PRIMARY_,
        KEY_,
        LEFT_PARENTHESE_KEY_,
        KEY_NAMES,
        RIGHT_PARENTHESE_KEY_,
        FINISHED
    };
    Expecting expecting_target = Expecting::ALTER_;

    std::string table_name;
    std::vector<std::string> column_names;
    std::string constraint_name;

    std::any result;
    for (auto& child : ctx->children) {
        switch (expecting_target) {
            case Expecting::ALTER_:
                expecting_target = Expecting::TABLE_;
                break;
            case Expecting::TABLE_:
                expecting_target = Expecting::TABLE_NAME;
                break;
            case Expecting::TABLE_NAME:
                table_name = child->getText();
                expecting_target = Expecting::ADD_;
                break;
            case Expecting::ADD_:
                expecting_target = Expecting::CONSTRAINT_OR_PRIMARY_;
                break;
            case Expecting::CONSTRAINT_OR_PRIMARY_:
                if (child->getText() == "CONSTRAINT") {
                    expecting_target = Expecting::CONSTRAINT_NAME;
                } else if (child->getText() == "PRIMARY") {
                    constraint_name = "";
                    expecting_target = Expecting::KEY_;
                } else {
                    std::cout << "!ERROR" << std::endl;
                    std::cout << "Expecting CONSTRAINT or PRIMARY" << std::endl;
                    std::cout << "But get " << child->getText() << std::endl;
                    return false;
                }
                break;
            case Expecting::CONSTRAINT_NAME:
                constraint_name = child->getText();
                expecting_target = Expecting::PRIMARY_;
                break;
            case Expecting::PRIMARY_:
                expecting_target = Expecting::KEY_;
                break;
            case Expecting::KEY_:
                expecting_target = Expecting::LEFT_PARENTHESE_KEY_;
                break;
            case Expecting::LEFT_PARENTHESE_KEY_:
                expecting_target = Expecting::KEY_NAMES;
                break;
            case Expecting::KEY_NAMES:
                result = child->accept(this);
                column_names = std::any_cast<std::vector<std::string>>(result);
                expecting_target = Expecting::RIGHT_PARENTHESE_KEY_;
                break;
            case Expecting::RIGHT_PARENTHESE_KEY_:
                expecting_target = Expecting::FINISHED;
                break;
            case Expecting::FINISHED:
                break;
        }
    }

    int table_id = sm->getTableId(table_name.c_str());
    if (table_id == -1) {
        std::cout << "!ERROR" << std::endl;
        std::cout << "Table " << table_name << " does not exist" << std::endl;
        return false;
    }

    std::vector<int> column_ids;
    std::vector<record::ColumnType> column_types;
    sm->getTableColumnTypes(table_id, column_types);
    for (auto& column_name : column_names) {
        int column_id = -1;
        for (int i = 0; i < column_types.size(); i++) {
            if (column_types[i].column_name == column_name) {
                column_id = column_types[i].column_id;
                break;
            }
        }
        if (column_id == -1) {
            std::cout << "!ERROR" << std::endl;
            std::cout << "Column " << column_name << " does not exist"
                      << std::endl;
            return false;
        }
        column_ids.push_back(column_id);
    }

    sm->addPrimaryKey(table_id, column_ids);
    return true;
}

std::any SQLMyVisitor::visitAlter_table_add_foreign_key(
    antlr4::SQLParser::Alter_table_add_foreign_keyContext* ctx) {
    enum Expecting {
        ALTER_,
        TABLE_,
        TABLE_NAME,
        ADD_,
        CONSTRAINT_,
        CONSTRAINT_NAME,
        FOREIGN_,
        KEY_,
        LEFT_PARENTHESE_KEY_,
        KEY_NAMES,
        RIGHT_PARENTHESE_KEY_,
        REFERENCES_,
        LEFT_PARENTHESE_REF_,
        REFERENCE_TABLE_NAME,
        RIGHT_PARENTHESE_REF_,
        REFERENCE_COLUMN_NAMES,
        FINISHED
    };
    Expecting expecting_target = Expecting::ALTER_;

    std::string table_name;
    std::vector<std::string> column_names;
    std::string reference_table_name;
    std::vector<std::string> reference_column_names;
    std::string constraint_name;

    std::any result;
    for (auto& child : ctx->children) {
        switch (expecting_target) {
            case Expecting::ALTER_:
                expecting_target = Expecting::TABLE_;
                break;
            case Expecting::TABLE_:
                expecting_target = Expecting::TABLE_NAME;
                break;
            case Expecting::TABLE_NAME:
                table_name = child->getText();
                expecting_target = Expecting::ADD_;
                break;
            case Expecting::ADD_:
                expecting_target = Expecting::CONSTRAINT_;
                break;
            case Expecting::CONSTRAINT_:
                expecting_target = Expecting::CONSTRAINT_NAME;
                break;
            case Expecting::CONSTRAINT_NAME:
                constraint_name = child->getText();
                expecting_target = Expecting::FOREIGN_;
                break;
            case Expecting::FOREIGN_:
                expecting_target = Expecting::KEY_;
                break;
            case Expecting::KEY_:
                expecting_target = Expecting::LEFT_PARENTHESE_KEY_;
                break;
            case Expecting::LEFT_PARENTHESE_KEY_:
                expecting_target = Expecting::KEY_NAMES;
                break;
            case Expecting::KEY_NAMES:
                result = child->accept(this);
                column_names = std::any_cast<std::vector<std::string>>(result);
                expecting_target = Expecting::RIGHT_PARENTHESE_KEY_;
                break;
            case Expecting::RIGHT_PARENTHESE_KEY_:
                expecting_target = Expecting::REFERENCES_;
                break;
            case Expecting::REFERENCES_:
                expecting_target = Expecting::REFERENCE_TABLE_NAME;
                break;
            case Expecting::REFERENCE_TABLE_NAME:
                reference_table_name = child->getText();
                expecting_target = Expecting::LEFT_PARENTHESE_REF_;
                break;
            case Expecting::LEFT_PARENTHESE_REF_:
                expecting_target = Expecting::REFERENCE_COLUMN_NAMES;
                break;
            case Expecting::REFERENCE_COLUMN_NAMES:
                result = child->accept(this);
                reference_column_names =
                    std::any_cast<std::vector<std::string>>(result);
                expecting_target = Expecting::RIGHT_PARENTHESE_REF_;
                break;
            case Expecting::RIGHT_PARENTHESE_REF_:
                expecting_target = Expecting::FINISHED;
                break;
            case Expecting::FINISHED:
                break;
        }
    }

    int table_id = sm->getTableId(table_name.c_str());
    if (table_id == -1) {
        std::cout << "!ERROR" << std::endl;
        std::cout << "Table " << table_name << " does not exist" << std::endl;
        return false;
    }

    std::vector<int> column_ids;
    std::vector<record::ColumnType> column_types;
    sm->getTableColumnTypes(table_id, column_types);
    for (auto& column_name : column_names) {
        int column_id = -1;
        for (int i = 0; i < column_types.size(); i++) {
            if (column_types[i].column_name == column_name) {
                column_id = column_types[i].column_id;
                break;
            }
        }
        if (column_id == -1) {
            std::cout << "!ERROR" << std::endl;
            std::cout << "Column " << column_name << " does not exist"
                      << std::endl;
            return false;
        }
        column_ids.push_back(column_id);
    }

    int reference_table_id = sm->getTableId(reference_table_name.c_str());
    if (reference_table_id == -1) {
        std::cout << "!ERROR" << std::endl;
        std::cout << "Table " << reference_table_name << " does not exist"
                  << std::endl;
        return false;
    }

    std::vector<int> reference_column_ids;
    std::vector<record::ColumnType> reference_column_types;
    sm->getTableColumnTypes(reference_table_id, reference_column_types);
    for (auto& column_name : reference_column_names) {
        int column_id = -1;
        for (int i = 0; i < reference_column_types.size(); i++) {
            if (reference_column_types[i].column_name == column_name) {
                column_id = reference_column_types[i].column_id;
                break;
            }
        }
        if (column_id == -1) {
            std::cout << "!ERROR" << std::endl;
            std::cout << "Column " << column_name << " does not exist"
                      << std::endl;
            return false;
        }
        reference_column_ids.push_back(column_id);
    }

    system::ForeignKeyInfo fk_info(column_ids, reference_table_id,
                                   reference_column_ids, constraint_name);

    return sm->addForeignKey(table_name.c_str(), fk_info);
}

std::any SQLMyVisitor::visitAlter_table_add_unique(
    antlr4::SQLParser::Alter_table_add_uniqueContext* ctx) {
    enum Expecting {
        ALTER_,
        TABLE_,
        TABLE_NAME,
        ADD_,
        UNIQUE_,
        UNIQUE_CONSTRAINT_NAME,
        LEFT_PARENTHESE_UNIQUE_,
        UNIQUE_COLUMN,
        RIGHT_PARENTHESE_UNIQUE_,
        FINISHED
    };
    Expecting expecting_target = Expecting::ALTER_;

    std::string table_name;
    std::string unique_constraint_name;
    std::vector<std::string> unique_column_names;

    for (auto& child : ctx->children) {
        switch (expecting_target) {
            case Expecting::ALTER_:
                expecting_target = Expecting::TABLE_;
                break;
            case Expecting::TABLE_:
                expecting_target = Expecting::TABLE_NAME;
                break;
            case Expecting::TABLE_NAME:
                table_name = child->getText();
                expecting_target = Expecting::ADD_;
                break;
            case Expecting::ADD_:
                expecting_target = Expecting::UNIQUE_;
                break;
            case Expecting::UNIQUE_:
                expecting_target = Expecting::UNIQUE_CONSTRAINT_NAME;
                break;
            case Expecting::UNIQUE_CONSTRAINT_NAME:
                unique_constraint_name = child->getText();
                expecting_target = Expecting::LEFT_PARENTHESE_UNIQUE_;
                break;
            case Expecting::LEFT_PARENTHESE_UNIQUE_:
                expecting_target = Expecting::UNIQUE_COLUMN;
                break;
            case Expecting::UNIQUE_COLUMN:
                unique_column_names = std::any_cast<std::vector<std::string>>(
                    child->accept(this));
                expecting_target = Expecting::RIGHT_PARENTHESE_UNIQUE_;
                break;
            case Expecting::RIGHT_PARENTHESE_UNIQUE_:
                expecting_target = Expecting::FINISHED;
                break;
            case Expecting::FINISHED:
                break;
        }
    }

    int table_id = sm->getTableId(table_name.c_str());
    if (table_id == -1) {
        std::cout << "!ERROR" << std::endl;
        std::cout << "Table " << table_name << " does not exist" << std::endl;
        return false;
    }

    std::vector<record::ColumnType> column_types;
    sm->getTableColumnTypes(table_id, column_types);
    std::vector<int> unique_column_ids;
    for (auto& column_name : unique_column_names) {
        int column_id = -1;
        for (int i = 0; i < column_types.size(); i++) {
            if (column_types[i].column_name == column_name) {
                column_id = column_types[i].column_id;
                break;
            }
        }
        if (column_id == -1) {
            std::cout << "!ERROR" << std::endl;
            std::cout << "Column " << column_name << " does not exist"
                      << std::endl;
            return false;
        }
        unique_column_ids.push_back(column_id);
    }

    // TODO: add unique constraint to *table_id* - *unique_column_ids*
    return sm->addUnique(table_name.c_str(), unique_constraint_name,
                         unique_column_ids);

    throw NotImplementedError("SQLMyVisitor::visitAlter_table_add_unique");
}

std::any SQLMyVisitor::visitField_list(
    antlr4::SQLParser::Field_listContext* ctx) {
    std::vector<record::ColumnType> column_list;
    std::vector<std::string> primary_keys;
    std::vector<system::ForeignKeyInputInfo> foreign_keys;
    for (auto child : ctx->children) {
        std::any result = child->accept(this);
        if (result.type() == typeid(record::ColumnType)) {
            column_list.push_back(std::any_cast<record::ColumnType>(result));
        } else if (result.type() == typeid(std::vector<std::string>)) {
            primary_keys = std::any_cast<std::vector<std::string>>(result);
        } else if (result.type() == typeid(system::ForeignKeyInputInfo)) {
            foreign_keys.push_back(
                std::any_cast<system::ForeignKeyInputInfo>(result));
        }
    }

    std::tuple<std::vector<record::ColumnType>, std::vector<std::string>,
               std::vector<system::ForeignKeyInputInfo>>
        result_tuple(column_list, primary_keys, foreign_keys);
    return result_tuple;
}

std::any SQLMyVisitor::visitNormal_field(
    antlr4::SQLParser::Normal_fieldContext* ctx) {
    auto visitResult = visitChildren(ctx);

    if (visitResult.type() != typeid(record::ColumnType)) {
        return false;
    }
    auto columnType = std::any_cast<record::ColumnType>(visitResult);

    auto not_node = ctx->getToken(antlr4::SQLParser::T__42, 0);
    if (not_node != nullptr && ctx->Null() != nullptr) {
        columnType.require_not_null = true;
    }

    columnType.column_name = ctx->Identifier()->getText();

    // TODO: UNIQUE, default value 未处理
    return columnType;
}

std::any SQLMyVisitor::visitPrimary_key_field(
    antlr4::SQLParser::Primary_key_fieldContext* ctx) {
    std::vector<std::string> primary_keys;

    for (auto child : ctx->children) {
        std::any result = child->accept(this);
        if (result.type() == typeid(std::vector<std::string>)) {
            primary_keys = std::any_cast<std::vector<std::string>>(result);
        }
    }
    return primary_keys;
}

std::any SQLMyVisitor::visitForeign_key_field(
    antlr4::SQLParser::Foreign_key_fieldContext* ctx) {
    std::vector<std::string> foreign_key_column_names;
    std::string reference_table_name;
    std::vector<std::string> reference_column_names;

    enum class Expecting {
        FOREIGN_KEY_COLUMN_NAMES,
        REFERENCES,
        REFERENCE_TABLE_NAME,
        REFERENCE_COLUMN_NAMES,
        FINISHED
    } expecting_target = Expecting::FOREIGN_KEY_COLUMN_NAMES;

    for (auto child : ctx->children) {
        std::any result = child->accept(this);
        switch (expecting_target) {
            case Expecting::FOREIGN_KEY_COLUMN_NAMES:
                if (result.type() == typeid(std::vector<std::string>)) {
                    for (auto& res :
                         std::any_cast<std::vector<std::string>>(result)) {
                        foreign_key_column_names.push_back(res);
                    }
                    expecting_target = Expecting::REFERENCES;
                }
                break;
            case Expecting::REFERENCES:
                if (child->getText() == "REFERENCES") {
                    expecting_target = Expecting::REFERENCE_TABLE_NAME;
                }
                break;
            case Expecting::REFERENCE_TABLE_NAME:
                reference_table_name = child->getText();
                expecting_target = Expecting::REFERENCE_COLUMN_NAMES;
                break;
            case Expecting::REFERENCE_COLUMN_NAMES:
                if (result.type() == typeid(std::vector<std::string>)) {
                    for (auto& res :
                         std::any_cast<std::vector<std::string>>(result)) {
                        reference_column_names.push_back(res);
                    }
                    expecting_target = Expecting::FINISHED;
                }
                break;
            case Expecting::FINISHED:
                break;
        }
    }

    if (foreign_key_column_names.size() != reference_column_names.size()) {
        std::cout << "!ERROR" << std::endl;
        std::cout << "Foreign key column names and reference column names "
                     "should have the same size"
                  << std::endl;
        return false;
    }

    system::ForeignKeyInputInfo foreign_key;
    foreign_key.foreign_key_column_names = foreign_key_column_names;
    foreign_key.reference_table_name = reference_table_name;
    foreign_key.reference_column_names = reference_column_names;

    return foreign_key;
}

std::any SQLMyVisitor::visitType_(antlr4::SQLParser::Type_Context* ctx) {
    auto type = ctx->getText();
    std::transform(type.begin(), type.end(), type.begin(), ::toupper);

    record::ColumnType return_val;

    if (type == "INT") {
        return_val.type_name = record::DataTypeName::INT;
    } else if (type == "FLOAT") {
        return_val.type_name = record::DataTypeName::FLOAT;
    } else if (type == "DATE") {
        return_val.type_name = record::DataTypeName::DATE;
    } else if (type.find("VARCHAR") != std::string::npos) {
        return_val.type_name = record::DataTypeName::VARCHAR;
        return_val.varchar_length = std::stoi(type.substr(8, type.size() - 9));
    } else {
        std::cout << "!ERROR" << std::endl;
        std::cout << "Unknown type: " << type << std::endl;
        return false;
    }
    return return_val;
}

std::any SQLMyVisitor::visitOrder(antlr4::SQLParser::OrderContext* ctx) {
    throw NotImplementedError("SQLMyVisitor::visitOrder");
}

std::any SQLMyVisitor::visitValue_lists(
    antlr4::SQLParser::Value_listsContext* ctx) {
    std::vector<record::DataItem> result;
    for (auto child : ctx->children) {
        auto res = child->accept(this);
        if (res.type() == typeid(std::vector<record::DataValue>)) {
            record::DataItem data_item;
            data_item.data_values =
                std::any_cast<std::vector<record::DataValue>>(res);
            result.push_back(data_item);
        }
    }
    return result;
}

std::any SQLMyVisitor::visitValue_list(
    antlr4::SQLParser::Value_listContext* ctx) {
    std::vector<record::DataValue> result;
    for (auto child : ctx->children) {
        auto res = child->accept(this);
        if (res.type() == typeid(std::string)) {
            result.push_back(
                record::DataValue(record::DataTypeName::VARCHAR, false,
                                  std::any_cast<std::string>(res)));
        } else if (res.type() == typeid(int)) {
            result.push_back(record::DataValue(record::DataTypeName::INT, false,
                                               std::any_cast<int>(res)));
        } else if (res.type() == typeid(double)) {
            result.push_back(record::DataValue(record::DataTypeName::FLOAT,
                                               false,
                                               std::any_cast<double>(res)));
        } else if (res.type() == typeid(record::DateValue)) {
            result.push_back(
                record::DataValue(record::DataTypeName::DATE, false,
                                  std::any_cast<record::DateValue>(res)));
        } else if (res.type() == typeid(bool)) {
            result.push_back(
                record::DataValue(record::DataTypeName::INT, true));
        } else if (res.type() == typeid(condition::NullType)) {
            result.push_back(
                record::DataValue(record::DataTypeName::INT, true));
        }
    }
    return result;
}

std::any SQLMyVisitor::visitValue(antlr4::SQLParser::ValueContext* ctx) {
    auto str = ctx->getText();
    if (str[0] == '\'' && str[str.size() - 1] == '\'') {
        // check if str is of format 'YYYY-MM-DD'
        if (str.size() == 12 && str[5] == '-' && str[8] == '-') {
            int year = std::stoi(str.substr(1, 4));
            int month = std::stoi(str.substr(6, 2));
            int day = std::stoi(str.substr(9, 2));
            return record::DateValue(year, month, day);
        } else {
            return str.substr(1, str.size() - 2);
        }
    } else if (str.find('.') != std::string::npos) {
        std::istringstream iss(str);
        double floatValue;
        iss >> floatValue;
        return floatValue;
    } else if (str == "NULL") {
        return condition::NullType();
    } else {
        return std::stoi(str);
    }
}

std::any SQLMyVisitor::visitWhere_and_clause(
    antlr4::SQLParser::Where_and_clauseContext* ctx) {
    std::vector<condition::Condition> conditions;

    std::any result;
    condition::Condition new_condition;
    for (auto child : ctx->children) {
        result = child->accept(this);
        if (result.type() == typeid(condition::Condition)) {
            new_condition = std::any_cast<condition::Condition>(result);
            conditions.push_back(new_condition);
        }
    }

    return conditions;
}

std::any SQLMyVisitor::visitWhere_operator_expression(
    antlr4::SQLParser::Where_operator_expressionContext* ctx) {
    condition::Condition condition;
    std::any result;
    std::string value;

    enum Expecting {
        LEFT_OPERAND,
        OPERATOR,
        RIGHT_OPERAND,
        FINISHED
    } expecting_target = Expecting::LEFT_OPERAND;

    for (auto child : ctx->children) {
        switch (expecting_target) {
            case Expecting::LEFT_OPERAND:
                result = child->accept(this);
                if (result.type() == typeid(std::vector<std::string>)) {
                    if (std::any_cast<std::vector<std::string>>(result)
                            .size() == 2) {
                        condition.table_name =
                            std::any_cast<std::vector<std::string>>(result)[0];
                        condition.column_name =
                            std::any_cast<std::vector<std::string>>(result)[1];
                    } else {
                        condition.column_name =
                            std::any_cast<std::vector<std::string>>(result)[0];
                    }
                } else {
                    throw NotImplementedError(
                        "SQLMyVisitor::visitWhere_operator_expression "
                        "LEFT_OPERAND");
                }
                expecting_target = Expecting::OPERATOR;
                break;
            case Expecting::OPERATOR:
                result = child->accept(this);
                if (result.type() != typeid(condition::Operator)) {
                    std::cout << "!ERROR" << std::endl;
                    std::cout << "Unknown operator: " << child->getText()
                              << std::endl;
                    return false;
                }
                condition.op = std::any_cast<condition::Operator>(result);
                expecting_target = Expecting::RIGHT_OPERAND;
                break;
            case Expecting::RIGHT_OPERAND:
                result = child->accept(this);
                value = child->getText();
                if (result.type() == typeid(std::vector<std::string>)) {
                    if (std::any_cast<std::vector<std::string>>(result)
                            .size() == 2) {
                        condition.table_name_other =
                            std::any_cast<std::vector<std::string>>(result)[0];
                        condition.column_name_other =
                            std::any_cast<std::vector<std::string>>(result)[1];
                    } else {
                        condition.column_name_other =
                            std::any_cast<std::vector<std::string>>(result)[0];
                    }
                    condition.type = condition::VariableType::TABLE_COLUMN;
                } else if (result.type() == typeid(std::string)) {
                    if (value.size() == 12 && value[5] == '-' &&
                        value[8] == '-') {
                        int year = std::stoi(value.substr(1, 4));
                        int month = std::stoi(value.substr(6, 2));
                        int day = std::stoi(value.substr(9, 2));
                        condition.date_val =
                            record::DateValue(year, month, day);
                    } else {
                        condition.string_val =
                            value.substr(1, value.size() - 2);
                        condition.type = condition::VariableType::STRING;
                    }
                } else if (result.type() == typeid(double)) {
                    condition.float_val = std::stod(value);
                    condition.type = condition::VariableType::FLOAT;
                } else if (result.type() == typeid(int)) {
                    condition.int_val = std::stoi(value);
                    condition.type = condition::VariableType::INT;
                } else {
                    throw NotImplementedError(
                        "SQLMyVisitor::visitWhere_operator_expression "
                        "RIGHT_OPERAND");
                }
                expecting_target = Expecting::FINISHED;
                break;
            case Expecting::FINISHED:
                break;
        }
    }

    return condition;
}

std::any SQLMyVisitor::visitWhere_operator_select(
    antlr4::SQLParser::Where_operator_selectContext* ctx) {
    throw NotImplementedError("SQLMyVisitor::visitWhere_operator_select");
}

std::any SQLMyVisitor::visitWhere_null(
    antlr4::SQLParser::Where_nullContext* ctx) {
    enum Expecting {
        COLUMN_NAME,
        IS_,
        NOT_OR_NULL_,
        NULL_,
        FINISHED
    } expecting_target = Expecting::COLUMN_NAME;

    std::vector<std::string> table_column_name;
    std::string table_name;
    std::string column_name;
    bool is_null;

    for (auto child : ctx->children) {
        switch (expecting_target) {
            case Expecting::COLUMN_NAME:
                table_column_name = std::any_cast<std::vector<std::string>>(
                    child->accept(this));
                if (table_column_name.size() == 2) {
                    table_name = table_column_name[0];
                    column_name = table_column_name[1];
                } else {
                    column_name = table_column_name[0];
                }
                expecting_target = Expecting::IS_;
                break;
            case Expecting::IS_:
                expecting_target = Expecting::NOT_OR_NULL_;
                break;
            case Expecting::NOT_OR_NULL_:
                if (child->getText() == "NOT") {
                    is_null = false;
                    expecting_target = Expecting::NULL_;
                } else if (child->getText() == "NULL") {
                    is_null = true;
                    expecting_target = Expecting::FINISHED;
                } else {
                    throw NotImplementedError(
                        "SQLMyVisitor::visitWhere_null NOT_OR_NULL_");
                }
                break;
            case Expecting::NULL_:
                expecting_target = Expecting::FINISHED;
                break;
            case Expecting::FINISHED:
                break;
        }
    }

    condition::Condition condition;
    condition.table_name = table_name;
    condition.column_name = column_name;
    condition.op = condition::Operator::IS;
    condition.type = condition::VariableType::NULL_OR_NOT;
    condition.is_null = is_null;

    return condition;
}

std::any SQLMyVisitor::visitWhere_in_list(
    antlr4::SQLParser::Where_in_listContext* ctx) {
    throw NotImplementedError("SQLMyVisitor::visitWhere_in_list");
}

std::any SQLMyVisitor::visitWhere_in_select(
    antlr4::SQLParser::Where_in_selectContext* ctx) {
    throw NotImplementedError("SQLMyVisitor::visitWhere_in_select");
}

std::any SQLMyVisitor::visitWhere_like_string(
    antlr4::SQLParser::Where_like_stringContext* ctx) {
    throw NotImplementedError("SQLMyVisitor::visitWhere_like_string");
}

std::any SQLMyVisitor::visitColumn(antlr4::SQLParser::ColumnContext* ctx) {
    std::vector<std::string> table_column_name;
    std::string table_name;
    std::string column_name;

    if (ctx->children.size() == 1) {
        table_column_name.push_back(ctx->children[0]->getText());
    } else if (ctx->children.size() == 3) {
        table_column_name.push_back(ctx->children[0]->getText());
        table_column_name.push_back(ctx->children[2]->getText());
    } else {
        throw NotImplementedError("SQLMyVisitor::visitColumn Size Missmatch");
    }

    return table_column_name;
}

std::any SQLMyVisitor::visitExpression(
    antlr4::SQLParser::ExpressionContext* ctx) {
    std::string str = ctx->getText();

    // string, table_.column_, int, float
    if (str[0] == '\'' && str[str.size() - 1] == '\'') {
        return str.substr(1, str.size() - 2);
    } else if ((str[0] < '0' || str[0] > '9') && str[0] != '-') {
        std::vector<std::string> table_column_name;
        if (str.find('.') != std::string::npos) {
            table_column_name.push_back(str.substr(0, str.find('.')));
            table_column_name.push_back(str.substr(str.find('.') + 1));
        } else {
            table_column_name.push_back(str);
        }
        return table_column_name;
    } else if (str.find('.') != std::string::npos) {
        std::istringstream iss(str);
        double floatValue;
        iss >> floatValue;
        return floatValue;
    } else if (str == "NULL") {
        throw NotImplementedError("SQLMyVisitor::visitExpression NULL");
    } else {
        return std::stoi(str);
    }
    // TODO: DateType not implemented
}

std::any SQLMyVisitor::visitSet_clause(
    antlr4::SQLParser::Set_clauseContext* ctx) {
    std::vector<std::tuple<std::string, record::DataValue>> assignments;
    std::tuple<std::string, record::DataValue> assignment;
    std::any result;

    enum expecting_target {
        COLUMN_NAME,
        EQUAL_TO_SIGN_,
        EXPRESSION,
        COMA_SIGN_
    } expecting_target = COLUMN_NAME;

    for (auto child : ctx->children) {
        switch (expecting_target) {
            case COLUMN_NAME:
                assignment =
                    std::make_tuple(child->getText(), record::DataValue());
                expecting_target = EQUAL_TO_SIGN_;
                break;
            case EQUAL_TO_SIGN_:
                expecting_target = EXPRESSION;
                break;
            case EXPRESSION:
                result = child->accept(this);
                if (result.type() == typeid(std::string)) {
                    std::get<1>(assignment).type_name =
                        record::DataTypeName::VARCHAR;
                    std::get<1>(assignment).value.char_value =
                        std::any_cast<std::string>(result);
                    std::get<1>(assignment).is_null = false;
                } else if (result.type() == typeid(int)) {
                    std::get<1>(assignment).type_name =
                        record::DataTypeName::INT;
                    std::get<1>(assignment).value.int_value =
                        std::any_cast<int>(result);
                    std::get<1>(assignment).is_null = false;
                } else if (result.type() == typeid(double)) {
                    std::get<1>(assignment).type_name =
                        record::DataTypeName::FLOAT;
                    std::get<1>(assignment).value.float_value =
                        std::any_cast<double>(result);
                    std::get<1>(assignment).is_null = false;
                } else if (child->getText() == "NULL") {
                    // ! notice that NULL is of type ANY!
                    // ! becouse we don't know the type of the column
                    std::get<1>(assignment).type_name =
                        record::DataTypeName::ANY;
                    std::get<1>(assignment).is_null = true;
                } else {
                    // TODO: DateType
                    std::cout << "!ERROR" << std::endl;
                    std::cout << "Unknown type: " << result.type().name()
                              << std::endl;
                    return false;
                }
                assignments.push_back(assignment);
                expecting_target = COMA_SIGN_;
                break;
            case COMA_SIGN_:
                expecting_target = COLUMN_NAME;
                break;
        }
    }

    return assignments;
}

std::any SQLMyVisitor::visitSelectors(
    antlr4::SQLParser::SelectorsContext* ctx) {
    std::vector<std::tuple<std::string, std::string>> selectors;

    if (ctx->getText() == "*") {
        selectors.push_back(std::make_tuple("*", "*"));
    } else {
        for (auto child : ctx->children) {
            std::any result = child->accept(this);
            if (result.type() == typeid(std::tuple<std::string, std::string>)) {
                selectors.push_back(
                    std::any_cast<std::tuple<std::string, std::string>>(
                        result));
            }
        }
    }
    return selectors;
}

std::any SQLMyVisitor::visitSelector(antlr4::SQLParser::SelectorContext* ctx) {
    std::tuple<std::string, std::string> table_column_name;
    std::string str = ctx->getText();
    if (str.find('.') != std::string::npos) {
        std::string table_name = str.substr(0, str.find('.'));
        std::string column_name = str.substr(str.find('.') + 1);
        table_column_name = std::make_tuple(table_name, column_name);
    } else {
        table_column_name = std::make_tuple("", str);
    }
    return table_column_name;
}

std::any SQLMyVisitor::visitIdentifiers(
    antlr4::SQLParser::IdentifiersContext* ctx) {
    auto identifier_text = ctx->getText();
    std::vector<std::string> identifiers_list;
    std::string identifier;
    for (int i = 0; i < identifier_text.size(); i++) {
        if (identifier_text[i] == ',') {
            identifiers_list.push_back(identifier);
            identifier.clear();
        } else {
            identifier += identifier_text[i];
        }
    }
    identifiers_list.push_back(identifier);
    return identifiers_list;
}

std::any SQLMyVisitor::visitOperator_(
    antlr4::SQLParser::Operator_Context* ctx) {
    std::map<std::string, condition::Operator> op_map = {
        {"=", condition::Operator::EQ}, {"<>", condition::Operator::NEQ},
        {"<", condition::Operator::LT}, {"<=", condition::Operator::LEQ},
        {">", condition::Operator::GT}, {">=", condition::Operator::GEQ}};

    auto op = ctx->getText();
    if (op_map.find(op) != op_map.end()) {
        return op_map[op];
    } else {
        std::cout << "!ERROR" << std::endl;
        std::cout << "Unknown operator: " << op << std::endl;
        return false;
    }
}

std::any SQLMyVisitor::visitAggregator(
    antlr4::SQLParser::AggregatorContext* ctx) {
    throw NotImplementedError("SQLMyVisitor::visitAggregator");
}

}  // namespace parser
}  // namespace dbs