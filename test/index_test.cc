#include <gtest/gtest.h>

#include <fstream>
#include <map>
#include <vector>

#include "common/Config.hpp"
#include "fs/BufPageManager.hpp"
#include "fs/FileManager.hpp"
#include "index/IndexManager.hpp"
#include "index/IndexType.hpp"

TEST(IndexTest, Basics) {
    srand(time(0));
    dbs::fs::FileManager* fm = new dbs::fs::FileManager();
    dbs::fs::BufPageManager* bpm = new dbs::fs::BufPageManager(fm);
    dbs::index::IndexManager* im = new dbs::index::IndexManager(fm, bpm);
    if (!fm->existFolder("./data")) {
        fm->createFolder("./data");
    }
    std::vector<std::string> str_paths = {
        "./data/testfile1.txt",  "./data/testfile2.txt",
        "./data/testfile3.txt",  "./data/testfile4.txt",
        "./data/testfile5.txt",  "./data/testfile6.txt",
        "./data/testfile7.txt",  "./data/testfile8.txt",
        "./data/testfile9.txt",  "./data/testfile10.txt",
        "./data/testfile11.txt", "./data/testfile12.txt",
        "./data/testfile13.txt", "./data/testfile14.txt"};  // 14个
    std::vector<std::map<dbs::index::IndexValue, int>> tree_contents;
    std::vector<
        std::map<dbs::index::IndexValue, std::vector<dbs::index::IndexValue>>>
        tree_contents_detail;

    im->initializeIndexFile(str_paths[0].c_str(), 200);
    for (int path_i = 0; path_i < 14; path_i++) {
        im->initializeIndexFile(str_paths[path_i].c_str(), path_i + 1);
        tree_contents.push_back(std::map<dbs::index::IndexValue, int>());
        tree_contents_detail.push_back(
            std::map<dbs::index::IndexValue,
                     std::vector<dbs::index::IndexValue>>());
    }
    int total_insert = 2000;
    for (int i = 0; i < total_insert; i++) {
        int path_i = rand() % 14;
        int key_num = path_i + 1;
        dbs::index::IndexValue insert_value;
        insert_value.page_id = rand();
        insert_value.slot_id = rand();
        for (int j = 0; j < key_num; j++) {
            insert_value.key.push_back(rand() % 10);
        }
        if (tree_contents[path_i].find(insert_value) ==
            tree_contents[path_i].end()) {
            tree_contents[path_i][insert_value] = 1;
            tree_contents_detail[path_i][insert_value] = {insert_value};
        } else {
            tree_contents[path_i][insert_value]++;
            tree_contents_detail[path_i][insert_value].push_back(insert_value);
        }

        ASSERT_TRUE(im->insertIndex(str_paths[path_i].c_str(), insert_value));
    }
    int check_num = 1000;
    for (int i = 0; i < check_num; i++) {
        int path_i = rand() % 14;
        int key_num = path_i + 1;
        dbs::index::IndexValue check_value;
        check_value.page_id = rand();
        check_value.slot_id = rand();
        for (int j = 0; j < key_num; j++) {
            check_value.key.push_back(rand() % 10);
        }
        std::vector<dbs::index::IndexValue> result;
        ASSERT_TRUE(
            im->searchIndex(str_paths[path_i].c_str(), check_value, result));
        if (tree_contents[path_i].find(check_value) ==
            tree_contents[path_i].end()) {
            ASSERT_EQ(result.size(), 0);
        } else {
            ASSERT_EQ(result.size(), tree_contents[path_i][check_value]);
            int size = result.size();
            for (int j = 0; j < size; j++) {
                bool found = false;
                for (int k = 0; k < size; k++) {
                    if (result[j].slot_id ==
                            tree_contents_detail[path_i][check_value][k]
                                .slot_id &&
                        result[j].page_id ==
                            tree_contents_detail[path_i][check_value][k]
                                .page_id) {
                        ASSERT_EQ(
                            result[j].key,
                            tree_contents_detail[path_i][check_value][k].key);
                        found = true;
                    }
                }
                ASSERT_TRUE(found);
            }
        }
    }
    int check_num_2 = 200;
    for (int i = 0; i < check_num_2; i++) {
        int path_i = rand() % 5;
        int key_num = path_i + 1;
        dbs::index::IndexValue check_value_low;
        check_value_low.page_id = rand();
        check_value_low.slot_id = rand();
        for (int j = 0; j < key_num; j++) {
            check_value_low.key.push_back(rand() % 10);
        }
        dbs::index::IndexValue check_value_high;
        check_value_high.page_id = rand();
        check_value_high.slot_id = rand();
        for (int j = 0; j < key_num; j++) {
            check_value_high.key.push_back(rand() % 10);
        }
        if (check_value_high < check_value_low) {
            std::swap(check_value_low, check_value_high);
        }
        std::vector<dbs::index::IndexValue> result;
        ASSERT_TRUE(im->searchIndexInRanges(str_paths[path_i].c_str(),
                                            check_value_low, check_value_high,
                                            result));
        auto start = tree_contents[path_i].lower_bound(check_value_low);
        auto end = tree_contents[path_i].upper_bound(check_value_high);
        int size = 0;
        for (auto it = start; it != end; it++) {
            size += it->second;
            auto detail_start = tree_contents_detail[path_i][it->first].begin();
            auto detail_end = tree_contents_detail[path_i][it->first].end();
            for (auto detail_it = detail_start; detail_it != detail_end;
                 detail_it++) {
                bool found = false;
                for (size_t j = 0; j < result.size(); j++) {
                    if (result[j].slot_id == detail_it->slot_id &&
                        result[j].page_id == detail_it->page_id) {
                        ASSERT_EQ(result[j].key, detail_it->key);
                        found = true;
                    }
                }
                ASSERT_TRUE(found);
            }
        }
        ASSERT_EQ(result.size(), size);
    }
    for (int path_i = 0; path_i < 14; path_i++) {
        int key_num = path_i + 1;
        dbs::index::IndexValue check_value_low;
        check_value_low.page_id = rand();
        check_value_low.slot_id = rand();
        for (int j = 0; j < key_num; j++) {
            check_value_low.key.push_back(-1);
        }
        dbs::index::IndexValue check_value_high;
        check_value_high.page_id = rand();
        check_value_high.slot_id = rand();
        for (int j = 0; j < key_num; j++) {
            check_value_high.key.push_back(100);
        }
        std::vector<dbs::index::IndexValue> result;
        ASSERT_TRUE(im->searchIndexInRanges(str_paths[path_i].c_str(),
                                            check_value_low, check_value_high,
                                            result));
    }

    int delete_num = 800;
    for (int i = 0; i < delete_num; i++) {
        int path_i = rand() % 5;
        dbs::index::IndexValue delete_value;
        if (tree_contents[path_i].size() == 0) continue;

        int delete_index = rand() % tree_contents[path_i].size();
        delete_value =
            std::next(tree_contents[path_i].begin(), delete_index)->first;
        int delete_detail_index =
            rand() % tree_contents_detail[path_i][delete_value].size();
        delete_value =
            tree_contents_detail[path_i][delete_value][delete_detail_index];
        ASSERT_TRUE(
            im->deleteIndex(str_paths[path_i].c_str(), delete_value, true));
        tree_contents[path_i][delete_value]--;
        if (tree_contents[path_i][delete_value] == 0) {
            tree_contents[path_i].erase(delete_value);
            tree_contents_detail[path_i].erase(delete_value);
        } else {
            tree_contents_detail[path_i][delete_value].erase(
                tree_contents_detail[path_i][delete_value].begin() +
                delete_detail_index);
        }

        std::vector<dbs::index::IndexValue> result;
        ASSERT_TRUE(
            im->searchIndex(str_paths[path_i].c_str(), delete_value, result));
        if (tree_contents[path_i].find(delete_value) ==
            tree_contents[path_i].end()) {
            ASSERT_EQ(result.size(), 0);
        } else {
            ASSERT_EQ(result.size(), tree_contents[path_i][delete_value]);
            int size = result.size();
            for (int j = 0; j < size; j++) {
                bool found = false;
                for (int k = 0; k < size; k++) {
                    if (result[j].slot_id ==
                            tree_contents_detail[path_i][delete_value][k]
                                .slot_id &&
                        result[j].page_id ==
                            tree_contents_detail[path_i][delete_value][k]
                                .page_id) {
                        ASSERT_EQ(
                            result[j].key,
                            tree_contents_detail[path_i][delete_value][k].key);
                        found = true;
                    }
                }
                ASSERT_TRUE(found);
            }
        }

        int key_num = path_i + 1;
        dbs::index::IndexValue check_value_low;
        check_value_low.page_id = rand();
        check_value_low.slot_id = rand();
        for (int j = 0; j < key_num; j++) {
            check_value_low.key.push_back(-1);
        }
        dbs::index::IndexValue check_value_high;
        check_value_high.page_id = rand();
        check_value_high.slot_id = rand();
        for (int j = 0; j < key_num; j++) {
            check_value_high.key.push_back(100);
        }
        ASSERT_TRUE(im->searchIndexInRanges(str_paths[path_i].c_str(),
                                            check_value_low, check_value_high,
                                            result));
    }

    for (int i = 0; i < check_num; i++) {
        int path_i = rand() % 5;
        int key_num = path_i + 1;
        dbs::index::IndexValue check_value_low;
        check_value_low.page_id = rand();
        check_value_low.slot_id = rand();
        for (int j = 0; j < key_num; j++) {
            check_value_low.key.push_back(rand() % 10);
        }
        dbs::index::IndexValue check_value_high;
        check_value_high.page_id = rand();
        check_value_high.slot_id = rand();
        for (int j = 0; j < key_num; j++) {
            check_value_high.key.push_back(rand() % 10);
        }
        if (check_value_high < check_value_low) {
            std::swap(check_value_low, check_value_high);
        }
        std::vector<dbs::index::IndexValue> result;
        ASSERT_TRUE(im->searchIndexInRanges(str_paths[path_i].c_str(),
                                            check_value_low, check_value_high,
                                            result));
        auto start = tree_contents[path_i].lower_bound(check_value_low);
        auto end = tree_contents[path_i].upper_bound(check_value_high);
        int size = 0;
        for (auto it = start; it != end; it++) {
            size += it->second;
            auto detail_start = tree_contents_detail[path_i][it->first].begin();
            auto detail_end = tree_contents_detail[path_i][it->first].end();
            for (auto detail_it = detail_start; detail_it != detail_end;
                 detail_it++) {
                bool found = false;
                for (size_t j = 0; j < result.size(); j++) {
                    if (result[j].slot_id == detail_it->slot_id &&
                        result[j].page_id == detail_it->page_id) {
                        ASSERT_EQ(result[j].key, detail_it->key);
                        found = true;
                    }
                }
                ASSERT_TRUE(found);
            }
        }
        ASSERT_EQ(result.size(), size);
    }
    delete im;
    delete bpm;
    delete fm;
}

TEST(IndexTest, ExtraLargeFile) {
    dbs::fs::FileManager* fm = new dbs::fs::FileManager();
    dbs::fs::BufPageManager* bpm = new dbs::fs::BufPageManager(fm);
    dbs::index::IndexManager* im = new dbs::index::IndexManager(fm, bpm);
    if (!fm->existFolder("./cache_data")) {
        fm->createFolder("./cache_data");
    }
    int key_num = 300;
    int total_prev_insert = 131000;
    if (!fm->existFile("./cache_data/ELF.txt")) {
        std::string str_path = "./cache_data/ELF.txt";
        im->initializeIndexFile(str_path.c_str(), key_num);
        dbs::index::IndexValue insert_value;
        insert_value.page_id = rand();
        insert_value.slot_id = rand();
        for (int i = 0; i < key_num; i++) {
            insert_value.key.push_back(0);
        }
        for (int i = total_prev_insert; i >= 0; i--) {
            insert_value.key[0] = i;
            ASSERT_TRUE(im->insertIndex(str_path.c_str(), insert_value));
        }
    }

    bpm->close();

    if (!fm->existFolder("./data")) {
        fm->createFolder("./data");
    }
    if (fm->existFile("./data/test.txt")) {
        fm->deleteFile("./data/test.txt");
    }
    fm->createFile("./data/test.txt");
    std::ifstream sourceFile("./cache_data/ELF.txt");
    std::ofstream destinationFile("./data/test.txt");

    ASSERT_TRUE(sourceFile);
    ASSERT_TRUE(destinationFile);
    destinationFile << sourceFile.rdbuf();
    sourceFile.close();
    destinationFile.close();

    dbs::index::IndexValue insert_value;
    insert_value.page_id = rand();
    insert_value.slot_id = rand();
    for (int i = 0; i < key_num; i++) {
        insert_value.key.push_back(0);
    }

    int total_operation = 4000;
    int cnt = -1;
    for (int i = 0; i < total_operation; i++) {
        // insert
        insert_value.key[0] = cnt--;
        insert_value.page_id = rand();
        insert_value.slot_id = rand();
        ASSERT_TRUE(im->insertIndex("./data/test.txt", insert_value));
        std::vector<dbs::index::IndexValue> result;
        ASSERT_TRUE(im->searchIndex("./data/test.txt", insert_value, result));
        ASSERT_EQ(result.size(), 1);
    }

    for (int i = 0; i < total_operation; i++) {
        // delete
        insert_value.key[0] = ++cnt;
        insert_value.page_id = rand();
        insert_value.slot_id = rand();
        ASSERT_TRUE(im->deleteIndex("./data/test.txt", insert_value, false));
        std::vector<dbs::index::IndexValue> result;
        ASSERT_TRUE(im->searchIndex("./data/test.txt", insert_value, result));
        ASSERT_EQ(result.size(), 0);
    }

    ASSERT_TRUE(im->deleteIndexFile("./data/test.txt"));
    ASSERT_FALSE(im->deleteIndexFile("./data/test.txt"));

    delete im;
    delete bpm;
    delete fm;
}

TEST(IndexTest, ExtensiveTest) {
    srand(time(0));
    dbs::fs::FileManager* fm = new dbs::fs::FileManager();
    dbs::fs::BufPageManager* bpm = new dbs::fs::BufPageManager(fm);
    dbs::index::IndexManager* im = new dbs::index::IndexManager(fm, bpm);

    if (!fm->existFolder("./cache_data")) {
        fm->createFolder("./cache_data");
    }
    int key_num = 20;
    int total_prev_insert = 80000;
    int total_prev_delete = 60000;
    int range = 100;
    if (!fm->existFile("./cache_data/ET.txt")) {
        std::string str_path = "./cache_data/ET.txt";
        im->initializeIndexFile(str_path.c_str(), key_num);
        dbs::index::IndexValue insert_value;
        for (int i = 0; i < key_num; i++) {
            insert_value.key.push_back(0);
        }
        for (int i = total_prev_insert; i >= 0; i--) {
            insert_value.key[0] = i % range;
            insert_value.page_id = i + 1;
            insert_value.slot_id = i + 2;
            ASSERT_TRUE(im->insertIndex(str_path.c_str(), insert_value));
        }

        for (int i = 0; i < total_prev_delete; i++) {
            insert_value.key[0] = i % range;
            insert_value.page_id = i + 1;
            insert_value.slot_id = i + 2;
            ASSERT_TRUE(im->deleteIndex(str_path.c_str(), insert_value, true));
        }
    }

    bpm->close();

    std::string str_path = "./data/test_extensive.txt";
    if (!fm->existFolder("./data")) {
        fm->createFolder("./data");
    }
    if (fm->existFile("./data/test_extensive.txt"))
        fm->deleteFile("./data/test_extensive.txt");
    fm->createFile("./data/test_extensive.txt");
    std::ifstream sourceFile("./cache_data/ET.txt");
    std::ofstream destinationFile("./data/test_extensive.txt");

    ASSERT_TRUE(sourceFile);
    ASSERT_TRUE(destinationFile);
    destinationFile << sourceFile.rdbuf();
    sourceFile.close();
    destinationFile.close();

    std::map<dbs::index::IndexValue, std::vector<dbs::index::IndexValue>>
        tree_contents;
    dbs::index::IndexValue insert_value;
    for (int i = 0; i < key_num; i++) {
        insert_value.key.push_back(0);
    }
    for (int i = total_prev_insert; i >= total_prev_delete; i--) {
        insert_value.key[0] = i % range;
        insert_value.page_id = i + 1;
        insert_value.slot_id = i + 2;
        if (tree_contents.find(insert_value) == tree_contents.end()) {
            tree_contents[insert_value] = {insert_value};
        } else {
            tree_contents[insert_value].push_back(insert_value);
        }
    }

    int total_operation = 6000;

    while (total_operation--) {
        int op = rand() % 7;
        if (total_operation < 6000) op += 2;

        if (op < 3 || tree_contents.size() == 0) {
            // insert
            dbs::index::IndexValue insert_value;
            insert_value.page_id = rand();
            insert_value.slot_id = rand();
            for (int i = 0; i < key_num; i++) {
                insert_value.key.push_back(rand() % range);
            }
            if (tree_contents.find(insert_value) == tree_contents.end()) {
                tree_contents[insert_value] = {insert_value};
            } else {
                tree_contents[insert_value].push_back(insert_value);
            }
            ASSERT_TRUE(im->insertIndex(str_path.c_str(), insert_value));
        } else {
            // delete
            if (tree_contents.size() == 0) continue;
            int delete_index = rand() % tree_contents.size();
            auto delete_it = tree_contents.begin();
            std::advance(delete_it, delete_index);
            int delete_detail_index = rand() % delete_it->second.size();
            dbs::index::IndexValue delete_value =
                delete_it->second[delete_detail_index];
            ASSERT_TRUE(im->deleteIndex(str_path.c_str(), delete_value, true));
            delete_it->second.erase(delete_it->second.begin() +
                                    delete_detail_index);
            if (delete_it->second.size() == 0) {
                tree_contents.erase(delete_it);
            }
        }
    }

    dbs::index::IndexValue check_value_low(0, 0,
                                           std::vector<int>(key_num, INT_MIN));
    dbs::index::IndexValue check_value_high(0, 0,
                                            std::vector<int>(key_num, INT_MAX));
    std::vector<dbs::index::IndexValue> result;
    ASSERT_TRUE(im->searchIndexInRanges(str_path.c_str(), check_value_low,
                                        check_value_high, result));
    auto start = tree_contents.lower_bound(check_value_low);
    auto end = tree_contents.upper_bound(check_value_high);
    int size = 0;
    for (auto it = start; it != end; it++) {
        size += it->second.size();
        for (auto detail_it = it->second.begin(); detail_it != it->second.end();
             detail_it++) {
            bool found = false;
            for (int j = 0; j < (int)result.size(); j++) {
                if (result[j].slot_id == detail_it->slot_id &&
                    result[j].page_id == detail_it->page_id) {
                    ASSERT_EQ(result[j].key, detail_it->key);
                    found = true;
                }
            }
            ASSERT_TRUE(found);
        }
    }
    ASSERT_EQ(result.size(), size);

    int test_delete_fail_num = 10;
    for (int i = 0; i < test_delete_fail_num; i++) {
        dbs::index::IndexValue delete_value;
        delete_value.page_id = rand();
        delete_value.slot_id = rand();
        for (int j = 0; j < key_num; j++) {
            delete_value.key.push_back(rand() % range + range);
        }
        ASSERT_FALSE(im->deleteIndex(str_path.c_str(), delete_value, false));
    }
    for (int i = 0; i < test_delete_fail_num; i++) {
        dbs::index::IndexValue delete_value;
        delete_value.page_id = rand();
        delete_value.slot_id = rand();
        for (int j = 0; j < key_num; j++) {
            delete_value.key.push_back(rand() % range - range);
        }
        ASSERT_FALSE(im->deleteIndex(str_path.c_str(), delete_value, false));
    }
    for (int i = 0; i < test_delete_fail_num; i++) {
        int delete_index = tree_contents.size() - 1;
        auto delete_it = tree_contents.begin();
        std::advance(delete_it, delete_index);
        int delete_detail_index = rand() % delete_it->second.size();
        dbs::index::IndexValue delete_value =
            delete_it->second[delete_detail_index];
        delete_value.slot_id = -100;
        ASSERT_FALSE(im->deleteIndex(str_path.c_str(), delete_value, true));
    }

    while (tree_contents.size() > 2000) {
        int delete_index = tree_contents.size() - 1;
        auto delete_it = tree_contents.begin();
        std::advance(delete_it, delete_index);
        int delete_detail_index = rand() % delete_it->second.size();
        dbs::index::IndexValue delete_value =
            delete_it->second[delete_detail_index];
        ASSERT_TRUE(im->deleteIndex(str_path.c_str(), delete_value, true));
        delete_it->second.erase(delete_it->second.begin() +
                                delete_detail_index);
        if (delete_it->second.size() == 0) {
            tree_contents.erase(delete_it);
        }
    }
    while (tree_contents.size() > 0) {
        int delete_index = 0;
        auto delete_it = tree_contents.begin();
        std::advance(delete_it, delete_index);
        int delete_detail_index = rand() % delete_it->second.size();
        dbs::index::IndexValue delete_value =
            delete_it->second[delete_detail_index];
        ASSERT_TRUE(im->deleteIndex(str_path.c_str(), delete_value, true));
        delete_it->second.erase(delete_it->second.begin() +
                                delete_detail_index);
        if (delete_it->second.size() == 0) {
            tree_contents.erase(delete_it);
        }
    }

    check_value_low =
        dbs::index::IndexValue(0, 0, std::vector<int>(key_num, INT_MIN));
    check_value_high =
        dbs::index::IndexValue(0, 0, std::vector<int>(key_num, INT_MAX));
    ASSERT_TRUE(im->searchIndexInRanges(str_path.c_str(), check_value_low,
                                        check_value_high, result));
    ASSERT_EQ(result.size(), 0);

    delete im;
    delete bpm;
    delete fm;
}