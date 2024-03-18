#include <gtest/gtest.h>

#include "fs/BufPageManager.hpp"
#include "fs/FileManager.hpp"
#include "index/IndexManager.hpp"
#include "parser/Parser.hpp"
#include "record/RecordManager.hpp"
#include "system/SystemManager.hpp"

TEST(InterpreterTest, ParserFail) {
    dbs::fs::FileManager *fm = new dbs::fs::FileManager();
    dbs::fs::BufPageManager *bpm = new dbs::fs::BufPageManager(fm);
    dbs::record::RecordManager *rm = new dbs::record::RecordManager(fm, bpm);
    dbs::index::IndexManager *im = new dbs::index::IndexManager(fm, bpm);
    dbs::system::SystemManager *sm = new dbs::system::SystemManager(fm, rm, im);
    dbs::parser::Parser *parser = new dbs::parser::Parser(rm, im, sm);
    auto result = parser->parse("CREATE DATABASE DB;");

    assert(false);
}