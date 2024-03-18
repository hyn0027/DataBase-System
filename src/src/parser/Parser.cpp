#include "parser/Parser.hpp"

namespace dbs {
namespace parser {

bool Parser::parse(std::string sSQL) {
    // to input stream
    antlr4::ANTLRInputStream sInputStream(sSQL);
    // setup lexer
    antlr4::SQLLexer iLexer(&sInputStream);
    antlr4::CommonTokenStream sTokenStream(&iLexer);
    // setup parser
    antlr4::SQLParser iParser(&sTokenStream);
    auto iTree = iParser.program();
    // return iTree;

    // auto context = iTree->toStringTree(&iParser, true);
    // std::cout << context << std::endl;

    if (iParser.getNumberOfSyntaxErrors() > 0) {
        std::cout << "!ERROR" << std::endl;
        std::cout << "Syntax Error" << std::endl;
        return false;
    }

    // setup visitor
    SQLMyVisitor iVisitor{SQLMyVisitor(rm, im, sm, output_mode)};
    auto res = iVisitor.visit(iTree);
    return std::any_cast<bool>(res);
}

Parser::Parser(record::RecordManager* rm_, index::IndexManager* im_,
               system::SystemManager* sm_) {
    rm = rm_;
    im = im_;
    sm = sm_;
}

void Parser::setOutputMode(bool mode) { output_mode = mode; }

Parser::~Parser() {
    rm = nullptr;
    im = nullptr;
    sm = nullptr;
}

}  // namespace parser
}  // namespace dbs