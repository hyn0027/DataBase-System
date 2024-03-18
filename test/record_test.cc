#include <gtest/gtest.h>

#include "common/Config.hpp"
#include "fs/BufPageManager.hpp"
#include "fs/FileManager.hpp"
#include "record/RecordManager.hpp"

TEST(RecordTest, Basics) {
    srand(time(0));
    dbs::fs::FileManager* fm = new dbs::fs::FileManager();
    dbs::fs::BufPageManager* bpm = new dbs::fs::BufPageManager(fm);
    dbs::record::RecordManager* rm = new dbs::record::RecordManager(fm, bpm);

    if (fm->existFolder("./data")) {
        fm->deleteFolder("./data");
    }
    fm->createFolder("./data");
    std::vector<dbs::record::ColumnType> column_types_1;
    column_types_1.push_back(
        dbs::record::ColumnType(dbs::record::DataTypeName::INT, 0, 0, true,
                                true, dbs::record::DefaultValue(), "id"));
    column_types_1.push_back(dbs::record::ColumnType(
        dbs::record::DataTypeName::VARCHAR, 10, 0, false, false,
        dbs::record::DefaultValue(), "name"));
    column_types_1.push_back(
        dbs::record::ColumnType(dbs::record::DataTypeName::FLOAT, 0, 0, false,
                                false, dbs::record::DefaultValue(), "score"));
    column_types_1.push_back(dbs::record::ColumnType(
        dbs::record::DataTypeName::DATE, 0, 0, false, false,
        dbs::record::DefaultValue(), "birthday"));
    std::vector<std::string> str_paths = {
        "./data/testfile1.txt",  "./data/testfile2.txt",
        "./data/testfile3.txt",  "./data/testfile4.txt",
        "./data/testfile5.txt",  "./data/testfile6.txt",
        "./data/testfile7.txt",  "./data/testfile8.txt",
        "./data/testfile9.txt",  "./data/testfile10.txt",
        "./data/testfile11.txt", "./data/testfile12.txt",
        "./data/testfile13.txt", "./data/testfile14.txt"};  // 14个

    char* path = const_cast<char*>(str_paths[0].c_str());
    rm->initializeRecordFile(path, column_types_1);

    std::vector<dbs::record::ColumnType> column_types_2;
    dbs::record::DefaultValue default_value;
    default_value.has_default_value = true;
    default_value.value.is_null = false;
    default_value.value.type_name = dbs::record::DataTypeName::INT;
    default_value.value.value.int_value = 233;
    column_types_2.push_back(dbs::record::ColumnType(
        dbs::record::DataTypeName::INT, 0, 0, true, true, default_value, "id"));
    default_value.has_default_value = true;
    default_value.value.is_null = false;
    default_value.value.type_name = dbs::record::DataTypeName::VARCHAR;
    default_value.value.value.char_value = "default_name";
    column_types_2.push_back(
        dbs::record::ColumnType(dbs::record::DataTypeName::VARCHAR, 10, 0,
                                false, false, default_value, "name"));
    default_value.has_default_value = true;
    default_value.value.is_null = false;
    default_value.value.type_name = dbs::record::DataTypeName::FLOAT;
    default_value.value.value.float_value = 233.233;
    column_types_2.push_back(
        dbs::record::ColumnType(dbs::record::DataTypeName::FLOAT, 0, 0, false,
                                false, default_value, "score"));
    default_value.has_default_value = true;
    default_value.value.is_null = false;
    default_value.value.type_name = dbs::record::DataTypeName::DATE;
    default_value.value.value.date_value.year = 2024;
    default_value.value.value.date_value.month = 1;
    default_value.value.value.date_value.day = 2;
    column_types_2.push_back(
        dbs::record::ColumnType(dbs::record::DataTypeName::DATE, 0, 0, false,
                                false, default_value, "birthday"));

    path = const_cast<char*>(str_paths[1].c_str());
    rm->initializeRecordFile(path, column_types_2);

    std::vector<dbs::record::ColumnType> return_column_type;

    path = const_cast<char*>(str_paths[0].c_str());
    rm->getColumnTypes(path, return_column_type);
    ASSERT_EQ(return_column_type.size(), column_types_1.size());
    for (size_t i = 0; i < return_column_type.size(); i++) {
        ASSERT_TRUE(return_column_type[i].same(column_types_1[i]));
    }

    path = const_cast<char*>(str_paths[1].c_str());
    rm->getColumnTypes(path, return_column_type);
    rm->getColumnTypes(path, return_column_type);

    ASSERT_EQ(return_column_type.size(), column_types_2.size());
    for (size_t i = 0; i < return_column_type.size(); i++) {
        ASSERT_TRUE(return_column_type[i].same(column_types_2[i]));
    }

    default_value.has_default_value = true;
    default_value.value.is_null = true;
    default_value.value.type_name = dbs::record::DataTypeName::FLOAT;
    column_types_2[2].default_value = default_value;

    for (int i = 0; i < 14; i++) {
        path = const_cast<char*>(str_paths[i].c_str());
        rm->initializeRecordFile(path, column_types_2);
    }

    for (int i = 13; i >= 0; i--) {
        path = const_cast<char*>(str_paths[i].c_str());
        rm->getColumnTypes(path, return_column_type);
        ASSERT_EQ(return_column_type.size(), column_types_2.size());
        for (size_t j = 0; j < return_column_type.size(); j++) {
            ASSERT_TRUE(return_column_type[j].same(column_types_2[j]));
        }
    }

    std::vector<std::vector<dbs::record::DataItem>> data_items;
    std::vector<std::vector<dbs::record::RecordLocation>> record_locations;

    int data_item_num = 100;
    for (int path_i = 0; path_i < 14; path_i++) {
        path = const_cast<char*>(str_paths[path_i].c_str());
        data_items.push_back(std::vector<dbs::record::DataItem>());
        record_locations.push_back(std::vector<dbs::record::RecordLocation>());
        for (int i = 0; i < data_item_num; i++) {
            dbs::record::DataItem data_item;
            data_item.data_values.push_back(dbs::record::DataValue(
                dbs::record::DataTypeName::INT, false,
                rand() * rand() * (rand() % 2 ? 1 : -1)));
            data_item.column_ids.push_back(0);
            data_item.data_values.push_back(dbs::record::DataValue(
                dbs::record::DataTypeName::VARCHAR, false, "name"));
            data_item.column_ids.push_back(1);
            data_item.data_values.push_back(dbs::record::DataValue(
                dbs::record::DataTypeName::FLOAT, false,
                (float)((float)rand() * (rand() % 2 ? 1 : -1) / 100.0)));
            data_item.column_ids.push_back(2);
            data_item.data_values.push_back(
                dbs::record::DataValue(dbs::record::DataTypeName::DATE, false,
                                       dbs::record::DateValue{2020, 1, 1}));
            data_item.column_ids.push_back(3);
            data_items[path_i].push_back(data_item);
            auto location = rm->insertRecord(path, data_item);
            ASSERT_NE(location.page_id, -1);
            ASSERT_NE(location.slot_id, -1);
            record_locations[path_i].push_back(location);

            ASSERT_TRUE(rm->getRecord(path, location, data_item));
            ASSERT_TRUE(data_item.same(data_items[path_i][i]));
        }
    }

    for (int path_i = 13; path_i >= 0; path_i--) {
        path = const_cast<char*>(str_paths[path_i].c_str());
        for (int i = 0; i < data_item_num; i++) {
            dbs::record::DataItem data_item;
            data_item.data_values.push_back(dbs::record::DataValue(
                dbs::record::DataTypeName::VARCHAR, false, "name"));
            data_item.column_ids.push_back(1);
            data_item.data_values.push_back(dbs::record::DataValue(
                dbs::record::DataTypeName::INT, false,
                rand() * rand() * (rand() % 2 ? 1 : -1)));
            data_item.column_ids.push_back(0);
            data_item.data_values.push_back(dbs::record::DataValue(
                dbs::record::DataTypeName::FLOAT, false,
                (float)((float)rand() * (rand() % 2 ? 1 : -1) / 100.0)));
            data_item.column_ids.push_back(2);
            data_item.data_values.push_back(
                dbs::record::DataValue(dbs::record::DataTypeName::DATE, false,
                                       dbs::record::DateValue{2020, 1, 1}));
            data_item.column_ids.push_back(3);
            data_items[path_i].push_back(data_item);
            auto location = rm->insertRecord(path, data_item);
            ASSERT_NE(location.page_id, -1);
            ASSERT_NE(location.slot_id, -1);
            record_locations[path_i].push_back(location);

            ASSERT_TRUE(rm->getRecord(path, location, data_item));
            ASSERT_TRUE(data_item.same(data_items[path_i][i + data_item_num]));
        }
    }

    for (int path_i = 5; path_i < 14; path_i++) {
        path = const_cast<char*>(str_paths[path_i].c_str());
        for (int i = 0; i < data_item_num; i++) {
            dbs::record::DataItem data_item;
            data_item.data_values.push_back(dbs::record::DataValue(
                dbs::record::DataTypeName::VARCHAR, true, ""));
            data_item.column_ids.push_back(1);
            data_item.data_values.push_back(dbs::record::DataValue(
                dbs::record::DataTypeName::INT, false,
                rand() * rand() * (rand() % 2 ? 1 : -1)));
            data_item.column_ids.push_back(0);
            data_item.data_values.push_back(
                dbs::record::DataValue(dbs::record::DataTypeName::DATE, false,
                                       dbs::record::DateValue{2020, 1, 1}));
            data_item.column_ids.push_back(3);
            data_item.data_values.push_back(dbs::record::DataValue(
                dbs::record::DataTypeName::FLOAT, false,
                (float)((float)rand() * (rand() % 2 ? 1 : -1) / 100.0)));
            data_item.column_ids.push_back(2);
            data_items[path_i].push_back(data_item);
            auto location = rm->insertRecord(path, data_item);
            ASSERT_NE(location.page_id, -1);
            ASSERT_NE(location.slot_id, -1);
            record_locations[path_i].push_back(location);

            ASSERT_TRUE(rm->getRecord(path, location, data_item));
            ASSERT_TRUE(
                data_item.same(data_items[path_i][i + data_item_num * 2]));
        }
    }

    dbs::record::DataItem data_item;
    data_item.data_values.push_back(dbs::record::DataValue(
        dbs::record::DataTypeName::VARCHAR, false, "name"));
    data_item.column_ids.push_back(1);
    data_item.data_values.push_back(
        dbs::record::DataValue(dbs::record::DataTypeName::INT, false, rand()));
    data_item.column_ids.push_back(0);
    data_item.data_values.push_back(
        dbs::record::DataValue(dbs::record::DataTypeName::INT, false, rand()));
    data_item.column_ids.push_back(2);
    data_item.data_values.push_back(
        dbs::record::DataValue(dbs::record::DataTypeName::DATE, false,
                               dbs::record::DateValue{2020, 1, 1}));
    data_item.column_ids.push_back(3);
    path = const_cast<char*>(str_paths[0].c_str());
    auto location = rm->insertRecord(path, data_item);
    ASSERT_EQ(location.page_id, -1);
    ASSERT_EQ(location.slot_id, -1);

    data_item.data_values.push_back(dbs::record::DataValue(
        dbs::record::DataTypeName::VARCHAR, false, "name"));
    data_item.column_ids.push_back(1);
    data_item.data_values.push_back(
        dbs::record::DataValue(dbs::record::DataTypeName::INT, true, 0));
    data_item.column_ids.push_back(0);
    data_item.data_values.push_back(
        dbs::record::DataValue(dbs::record::DataTypeName::FLOAT, false,
                               (float)((float)rand() / 100.0)));
    data_item.column_ids.push_back(2);
    data_item.data_values.push_back(
        dbs::record::DataValue(dbs::record::DataTypeName::DATE, false,
                               dbs::record::DateValue{2020, 1, 1}));
    data_item.column_ids.push_back(3);
    location = rm->insertRecord(path, data_item);
    ASSERT_EQ(location.page_id, -1);
    ASSERT_EQ(location.slot_id, -1);

    // random delete record
    for (int i = 0; i < 100; i++) {
        int path_i = rand() % 14;
        int index = rand() % record_locations[path_i].size();
        path = const_cast<char*>(str_paths[path_i].c_str());
        ASSERT_TRUE(rm->deleteRecord(path, record_locations[path_i][index]));
        ASSERT_FALSE(
            rm->getRecord(path, record_locations[path_i][index], data_item));
        ASSERT_FALSE(rm->updateRecord(path, record_locations[path_i][index],
                                      data_items[path_i][index]));
        record_locations[path_i].erase(record_locations[path_i].begin() +
                                       index);
        data_items[path_i].erase(data_items[path_i].begin() + index);
    }

    // random get record
    for (int i = 0; i < 50; i++) {
        int path_i = rand() % 14;
        int index = rand() % record_locations[path_i].size();
        ASSERT_EQ(record_locations[path_i].size(), data_items[path_i].size());
        path = const_cast<char*>(str_paths[path_i].c_str());
        ASSERT_TRUE(
            rm->getRecord(path, record_locations[path_i][index], data_item));
        ASSERT_TRUE(data_item.same(data_items[path_i][index]));
    }

    // random update record
    for (int i = 0; i < 50; i++) {
        int index = rand() % record_locations.size();
        data_item = dbs::record::DataItem();
        int random_int = rand() * (rand() % 2 ? 1 : -1);
        data_item.data_values.push_back(dbs::record::DataValue(
            dbs::record::DataTypeName::INT, false, random_int));
        data_item.column_ids.push_back(0);
        int path_i = rand() % 14;
        path = const_cast<char*>(str_paths[path_i].c_str());
        for (int j = 0; j < 4; j++) {
            if (data_items[path_i][index].column_ids[j] == 0) {
                data_items[path_i][index].data_values[j].value.int_value =
                    random_int;
                break;
            }
        }
        ASSERT_TRUE(
            rm->updateRecord(path, record_locations[path_i][index], data_item));
        ASSERT_TRUE(
            rm->getRecord(path, record_locations[path_i][index], data_item));
        ASSERT_TRUE(data_item.same(data_items[path_i][index]));
    }

    for (int i = 0; i < 20; i++) {
        int index = rand() % record_locations.size();
        int path_i = rand() % 14;
        path = const_cast<char*>(str_paths[path_i].c_str());
        data_item = dbs::record::DataItem();
        data_item.data_values.push_back(
            dbs::record::DataValue(dbs::record::DataTypeName::INT, true, 0));
        data_item.column_ids.push_back(0);
        ASSERT_FALSE(
            rm->updateRecord(path, record_locations[path_i][index], data_item));
    }

    for (int i = 0; i < 20; i++) {
        int index = rand() % record_locations.size();
        data_item = dbs::record::DataItem();
        data_item.data_values.push_back(
            dbs::record::DataValue(dbs::record::DataTypeName::INT, true, 0));
        data_item.column_ids.push_back(4);
        int path_i = rand() % 14;
        path = const_cast<char*>(str_paths[path_i].c_str());
        ASSERT_FALSE(
            rm->updateRecord(path, record_locations[path_i][index], data_item));
    }

    // get all records
    for (int path_i = 0; path_i < 14; path_i++) {
        path = const_cast<char*>(str_paths[path_i].c_str());
        std::vector<dbs::record::DataItem> return_data_items;
        ASSERT_TRUE(
            rm->getRecords(path, record_locations[path_i], return_data_items));
        ASSERT_EQ(return_data_items.size(), data_items[path_i].size());
        for (size_t i = 0; i < return_data_items.size(); i++) {
            ASSERT_TRUE(return_data_items[i].same(data_items[path_i][i]));
        }
    }
    for (int path_i = 13; path_i >= 0; path_i--) {
        path = const_cast<char*>(str_paths[path_i].c_str());
        std::vector<dbs::record::DataItem> return_data_items;
        std::vector<dbs::record::RecordLocation> return_locations;
        rm->getAllRecords(path, return_data_items, return_locations);
        ASSERT_EQ(return_data_items.size(), data_items[path_i].size());
        for (size_t i = 0; i < return_data_items.size(); i++) {
            ASSERT_TRUE(return_data_items[i].same(data_items[path_i][i]));
        }
    }

    delete rm;
    delete bpm;
    delete fm;

    fm = new dbs::fs::FileManager();
    bpm = new dbs::fs::BufPageManager(fm);
    rm = new dbs::record::RecordManager(fm, bpm);

    for (int path_i = 0; path_i < 14; path_i++) {
        path = const_cast<char*>(str_paths[path_i].c_str());
        std::vector<dbs::record::DataItem> return_data_items;
        ASSERT_TRUE(
            rm->getRecords(path, record_locations[path_i], return_data_items));
        ASSERT_EQ(return_data_items.size(), data_items[path_i].size());
        for (size_t i = 0; i < return_data_items.size(); i++) {
            ASSERT_TRUE(return_data_items[i].same(data_items[path_i][i]));
        }
    }
    for (int path_i = 13; path_i >= 0; path_i--) {
        path = const_cast<char*>(str_paths[path_i].c_str());
        ASSERT_TRUE(rm->deleteRecordFile(path));
        ASSERT_FALSE(rm->deleteRecordFile(path));
    }

    delete rm;
    delete bpm;
    delete fm;
}