#pragma once

#include <string>

#include "antlr4-runtime.h"
#include "index/IndexManager.hpp"
#include "parser/SQLLexer.hpp"
#include "parser/SQLMyVisitor.hpp"
#include "parser/SQLParser.hpp"
#include "record/RecordManager.hpp"
#include "system/SystemManager.hpp"

namespace dbs {
namespace parser {

class Parser {
   public:
    Parser(record::RecordManager* rm_, index::IndexManager* im_,
           system::SystemManager* sm_);
    ~Parser();
    bool parse(std::string sSQL);
    void setOutputMode(bool mode);  // false batch

   private:
    record::RecordManager* rm;
    index::IndexManager* im;
    system::SystemManager* sm;
    bool output_mode;
};

}  // namespace parser
}  // namespace dbs