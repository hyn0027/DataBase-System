#pragma once

#include "antlr4-runtime.h"
#include "condition/Condition.hpp"
#include "index/IndexManager.hpp"
#include "parser/Parser.hpp"
#include "parser/SQLBaseVisitor.hpp"
#include "record/RecordManager.hpp"
#include "system/SystemManager.hpp"

namespace dbs {

namespace parser {

class ParseResult {
   public:
    std::vector<record::ColumnType> columns;
    std::vector<record::DataItem> data_items;
    std::vector<std::string> primary_keys;
    std::vector<system::ForeignKeyInputInfo> foreign_keys;
    std::vector<std::vector<std::string>> indexes;
    std::vector<std::vector<std::string>> unique;
    ParseResult();
    ParseResult(const std::vector<record::ColumnType> &columns_,
                const std::vector<record::DataItem> &data_itmes_);
    ParseResult(const std::vector<record::ColumnType> &columns_,
                const std::vector<record::DataItem> &data_itmes_,
                const std::vector<std::string> &primary_keys_,
                const std::vector<system::ForeignKeyInputInfo> &foreign_keys_);
    ParseResult(const std::vector<record::ColumnType> &columns_,
                const std::vector<record::DataItem> &data_itmes_,
                const std::vector<std::string> &primary_keys_,
                const std::vector<system::ForeignKeyInputInfo> &foreign_keys_,
                const std::vector<std::vector<std::string>> &indexes_,
                const std::vector<std::vector<std::string>> &unique_);

    void print(bool output_mode, int duration);
    void printTableRow(const std::vector<std::string> &values);
    // typename
};

class SQLMyVisitor : public antlr4::SQLBaseVisitor {
   public:
    bool output_mode;
    SQLMyVisitor(record::RecordManager *rm_, index::IndexManager *im_,
                 system::SystemManager *sm_, bool output_mode_);
    ~SQLMyVisitor();
    std::any aggregateResult(std::any result, std::any nextResult) override;
    std::any visitProgram(antlr4::SQLParser::ProgramContext *ctx) override;
    std::any visitStatement(antlr4::SQLParser::StatementContext *ctx) override;
    std::any visitCreate_db(antlr4::SQLParser::Create_dbContext *ctx) override;
    std::any visitDrop_db(antlr4::SQLParser::Drop_dbContext *ctx) override;
    std::any visitShow_dbs(antlr4::SQLParser::Show_dbsContext *ctx) override;
    std::any visitUse_db(antlr4::SQLParser::Use_dbContext *ctx) override;
    std::any visitShow_tables(
        antlr4::SQLParser::Show_tablesContext *ctx) override;
    std::any visitShow_indexes(
        antlr4::SQLParser::Show_indexesContext *ctx) override;
    std::any visitCreate_table(
        antlr4::SQLParser::Create_tableContext *ctx) override;
    std::any visitDrop_table(
        antlr4::SQLParser::Drop_tableContext *ctx) override;
    std::any visitDescribe_table(
        antlr4::SQLParser::Describe_tableContext *ctx) override;
    std::any visitLoad_table(
        antlr4::SQLParser::Load_tableContext *ctx) override;
    std::any visitInsert_into_table(
        antlr4::SQLParser::Insert_into_tableContext *ctx) override;
    std::any visitDelete_from_table(
        antlr4::SQLParser::Delete_from_tableContext *ctx) override;
    std::any visitUpdate_table(
        antlr4::SQLParser::Update_tableContext *ctx) override;
    std::any visitSelect_table_(
        antlr4::SQLParser::Select_table_Context *ctx) override;
    std::any visitSelect_table(
        antlr4::SQLParser::Select_tableContext *ctx) override;
    std::any visitAlter_add_index(
        antlr4::SQLParser::Alter_add_indexContext *ctx) override;
    std::any visitAlter_drop_index(
        antlr4::SQLParser::Alter_drop_indexContext *ctx) override;
    std::any visitAlter_table_drop_pk(
        antlr4::SQLParser::Alter_table_drop_pkContext *ctx) override;
    std::any visitAlter_table_drop_foreign_key(
        antlr4::SQLParser::Alter_table_drop_foreign_keyContext *ctx) override;
    std::any visitAlter_table_add_pk(
        antlr4::SQLParser::Alter_table_add_pkContext *ctx) override;
    std::any visitAlter_table_add_foreign_key(
        antlr4::SQLParser::Alter_table_add_foreign_keyContext *ctx) override;
    std::any visitAlter_table_add_unique(
        antlr4::SQLParser::Alter_table_add_uniqueContext *ctx) override;
    std::any visitField_list(
        antlr4::SQLParser::Field_listContext *ctx) override;
    std::any visitNormal_field(
        antlr4::SQLParser::Normal_fieldContext *ctx) override;
    std::any visitPrimary_key_field(
        antlr4::SQLParser::Primary_key_fieldContext *ctx) override;
    std::any visitForeign_key_field(
        antlr4::SQLParser::Foreign_key_fieldContext *ctx) override;
    std::any visitType_(antlr4::SQLParser::Type_Context *ctx) override;
    std::any visitOrder(antlr4::SQLParser::OrderContext *ctx) override;
    std::any visitValue_lists(
        antlr4::SQLParser::Value_listsContext *ctx) override;
    std::any visitValue_list(
        antlr4::SQLParser::Value_listContext *ctx) override;
    std::any visitValue(antlr4::SQLParser::ValueContext *ctx) override;
    std::any visitWhere_and_clause(
        antlr4::SQLParser::Where_and_clauseContext *ctx) override;
    std::any visitWhere_operator_expression(
        antlr4::SQLParser::Where_operator_expressionContext *ctx) override;
    std::any visitWhere_operator_select(
        antlr4::SQLParser::Where_operator_selectContext *ctx) override;
    std::any visitWhere_null(
        antlr4::SQLParser::Where_nullContext *ctx) override;
    std::any visitWhere_in_list(
        antlr4::SQLParser::Where_in_listContext *ctx) override;
    std::any visitWhere_in_select(
        antlr4::SQLParser::Where_in_selectContext *ctx) override;
    std::any visitWhere_like_string(
        antlr4::SQLParser::Where_like_stringContext *ctx) override;
    std::any visitColumn(antlr4::SQLParser::ColumnContext *ctx) override;
    std::any visitExpression(
        antlr4::SQLParser::ExpressionContext *ctx) override;
    std::any visitSet_clause(
        antlr4::SQLParser::Set_clauseContext *ctx) override;
    std::any visitSelectors(antlr4::SQLParser::SelectorsContext *ctx) override;
    std::any visitSelector(antlr4::SQLParser::SelectorContext *ctx) override;
    std::any visitIdentifiers(
        antlr4::SQLParser::IdentifiersContext *ctx) override;
    std::any visitOperator_(antlr4::SQLParser::Operator_Context *ctx) override;
    std::any visitAggregator(
        antlr4::SQLParser::AggregatorContext *ctx) override;

   private:
    record::RecordManager *rm;
    index::IndexManager *im;
    system::SystemManager *sm;
};

}  // namespace parser
}  // namespace dbs