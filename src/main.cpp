#include <cstring>
#include <fstream>
#include <iostream>

#include "fs/BufPageManager.hpp"
#include "fs/FileManager.hpp"
#include "index/IndexManager.hpp"
#include "parser/Parser.hpp"
#include "record/RecordManager.hpp"
#include "system/SystemManager.hpp"

int main(int argc, char *argv[]) {
    bool batch = false;
    bool init = false;
    std::string file_path = "";
    std::string table_name = "";
    std::string database_name = "";
    for (int i = 1; i < argc; i++) {
        auto param = std::string(argv[i]);
        if (param == "--init") {
            init = true;
        } else if (param == "--batch" || param == "-b") {
            batch = true;
        } else if (param == "--file" || param == "-f") {
            file_path = std::string(argv[++i]);
        } else if (param == "--table" || param == "-t") {
            table_name = std::string(argv[++i]);
        } else if (param == "--database" || param == "-d") {
            database_name = std::string(argv[++i]);
        } else {
            std::cout << "unknown param: " << param << std::endl;
            i++;
            return -1;
        }
    }

    if (init) {
        dbs::fs::FileManager *fm = new dbs::fs::FileManager();
        dbs::fs::BufPageManager *bpm = new dbs::fs::BufPageManager(fm);
        dbs::record::RecordManager *rm =
            new dbs::record::RecordManager(fm, bpm);
        dbs::index::IndexManager *im = new dbs::index::IndexManager(fm, bpm);
        dbs::system::SystemManager *sm =
            new dbs::system::SystemManager(fm, rm, im);
        sm->cleanSystem();
        delete sm;
        delete im;
        delete rm;
        delete bpm;
        delete fm;
        return 0;
    }
    dbs::fs::FileManager *fm = new dbs::fs::FileManager();
    dbs::fs::BufPageManager *bpm = new dbs::fs::BufPageManager(fm);
    dbs::record::RecordManager *rm = new dbs::record::RecordManager(fm, bpm);
    dbs::index::IndexManager *im = new dbs::index::IndexManager(fm, bpm);
    dbs::system::SystemManager *sm = new dbs::system::SystemManager(fm, rm, im);
    dbs::parser::Parser *parser = new dbs::parser::Parser(rm, im, sm);
    sm->initializeSystem();
    if (database_name != "") {
        sm->useDatabase(database_name.c_str());
    }
    if (table_name != "" && file_path != "") {
        std::string input = "LOAD DATA INFILE '" + file_path + "' INTO TABLE " +
                            table_name + " FIELDS TERMINATED BY ',';\n";
        auto result = parser->parse(input.c_str());
        std::cout << "@ " << (result ? "success" : "fail") << std::endl;
    }
    if (batch) {
        // while 输入一行
        parser->setOutputMode(false);
        char input[1000];
        while (std::cin.getline(input, 1000)) {
            if (strcmp(input, "exit") == 0) {
                break;
            }
            auto result = parser->parse(input);
            // print type of result
            std::cout << "@ " << (result ? "success" : "fail") << std::endl;
        }
    } else {
        // while 输入一行 string
        parser->setOutputMode(true);
        std::string input;
        std::cout << "mysql> " << std::flush;
        while (std::getline(std::cin, input, '\n')) {
            if (input == "exit") {
                break;
            }
            while (input.find(";") == std::string::npos) {
                std::cout << "    -> " << std::flush;
                std::string input_continue;
                std::getline(std::cin, input_continue, '\n');
                input = input + input_continue;
            }
            auto result = parser->parse(input);
            bpm->close();
            std::cout << "mysql> " << std::flush;
        }
    }
    delete parser;
    delete sm;
    delete im;
    delete rm;
    delete bpm;
    delete fm;
    return 0;
}