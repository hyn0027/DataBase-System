#include "system/SystemManager.hpp"

namespace dbs {
namespace system {

SystemManager::SystemManager(fs::FileManager* fm_, record::RecordManager* rm_,
                             index::IndexManager* im_)
    : fm(fm_), rm(rm_), im(im_) {
    current_database_id = -1;

    GlobalDatabaseInfoColumnType.clear();
    GlobalDatabaseInfoColumnType.push_back(
        record::ColumnType(record::DataTypeName::VARCHAR, 255, 0, true, true,
                           record::DefaultValue(), "DATABASES"));

    DatabaseTableInfoColumnType.clear();
    DatabaseTableInfoColumnType.push_back(
        record::ColumnType(record::DataTypeName::VARCHAR, 255, 0, true, true,
                           record::DefaultValue(), "TABLES"));

    TablePrimaryKeyInfoColumnType.clear();
    TablePrimaryKeyInfoColumnType.push_back(
        record::ColumnType(record::DataTypeName::INT, 0, 0, true, true,
                           record::DefaultValue(), "PRIMARY_KEY_IDS"));

    TableForeignKeyInfoColumnType.clear();
    TableForeignKeyInfoColumnType.push_back(record::ColumnType(
        record::DataTypeName::INT, 0, 0, true, false, record::DefaultValue(),
        "FOREIGN_KEY_REFERENCE_TABLE_ID"));
    for (int i = 0; i < FOREIGN_KEY_MAX_NUM; i++) {
        TableForeignKeyInfoColumnType.push_back(
            record::ColumnType(record::DataTypeName::INT, 0, 0, false, false,
                               record::DefaultValue(),
                               "FOREIGN_KEY_COLUMN_ID_" + std::to_string(i)));
    }
    for (int i = 0; i < FOREIGN_KEY_MAX_NUM; i++) {
        TableForeignKeyInfoColumnType.push_back(record::ColumnType(
            record::DataTypeName::INT, 0, 0, false, false,
            record::DefaultValue(),
            "FOREIGN_KEY_REFERENCE_COLUMN_ID_" + std::to_string(i)));
    }
    TableForeignKeyInfoColumnType.push_back(record::ColumnType(
        record::DataTypeName::VARCHAR, 255, 0, false, false,
        record::DefaultValue(), "FOREIGN_KEY_REFERENCE_TABLE_NAME"));

    TableDominateInfoColumnType.clear();
    TableDominateInfoColumnType.push_back(
        record::ColumnType(record::DataTypeName::INT, 0, 0, true, false,
                           record::DefaultValue(), "DOMINATE_TABLE_ID"));

    TableIndexInfoColumnType.clear();
    for (int i = 0; i < INDEX_KEY_MAX_NUM; i++) {
        TableIndexInfoColumnType.push_back(record::ColumnType(
            record::DataTypeName::INT, 0, 0, false, false,
            record::DefaultValue(), "INDEX_COLUMN_ID_" + std::to_string(i)));
    }
    TableIndexInfoColumnType.push_back(
        record::ColumnType(record::DataTypeName::VARCHAR, 255, 0, false, false,
                           record::DefaultValue(), "INDEX_NAME"));
}

SystemManager::~SystemManager() {
    fm = nullptr;
    rm = nullptr;
    im = nullptr;
}

void SystemManager::cleanSystem() {
    rm->cleanAllCurrentColumnTypes();
    rm->closeAllCurrentFile();
    im->closeAllCurrentFile();
    if (fm->existFolder(DATABASE_PATH)) {
        assert(fm->deleteFolder(DATABASE_PATH));
    }
    initializeSystem();
}

void SystemManager::initializeSystem() {
    rm->cleanAllCurrentColumnTypes();
    rm->closeAllCurrentFile();
    im->closeAllCurrentFile();
    if (!fm->existFolder(DATABASE_PATH)) {
        fm->createFolder(DATABASE_PATH);
        fm->createFolder(DATABASE_BASE_PATH);
        fm->createFolder(DATABASE_GLOBAL_PATH);
        rm->initializeRecordFile(DATABASE_GLOBAL_RECORD_PATH,
                                 GlobalDatabaseInfoColumnType);
    }
}

bool SystemManager::createDatabase(const char* database_name) {
    // check if database exists
    auto id = getDatabaseId(database_name);
    if (getDatabaseId(database_name) != -1) {
        return false;
        std::cout << "!ERROR" << std::endl;
        std::cout << "Database " << database_name << " already exists"
                  << std::endl;
    }

    // insert a new record into global database
    std::vector<record::ColumnType> column_types;
    rm->getColumnTypes(DATABASE_GLOBAL_RECORD_PATH, column_types);
    assert(column_types.size() == 1);

    record::DataItem data_item;
    data_item.data_values.push_back(
        record::DataValue(record::DataTypeName::VARCHAR, false, database_name));
    data_item.column_ids.push_back(column_types[0].column_id);
    auto location = rm->insertRecord(DATABASE_GLOBAL_RECORD_PATH, data_item);
    assert(location.page_id != -1 && location.slot_id != -1);

    // get record to updaate data_id
    assert(rm->getRecord(DATABASE_GLOBAL_RECORD_PATH, location, data_item));

    // create database folder
    char* database_path = nullptr;
    getDatabasePath(data_item.data_id, &database_path);
    assert(fm->createFolder(database_path));

    // create alltable.txt
    char* database_info_path = nullptr;
    utils::concatPath(database_path, ALL_TABLE_FILE_NAME, &database_info_path);
    rm->initializeRecordFile(database_info_path, DatabaseTableInfoColumnType);

    delete[] database_path;
    delete[] database_info_path;

    return true;
}

bool SystemManager::dropDatabase(const char* database_name) {
    // check if database exists
    int database_id = getDatabaseId(database_name);
    if (database_id == -1) {
        std::cout << "!ERROR" << std::endl;
        std::cout << "Database " << database_name << " does not exist"
                  << std::endl;
        return false;
    }

    if (database_id == current_database_id) {
        current_database_id = -1;
    }

    im->closeAllCurrentFile();
    rm->closeAllCurrentFile();
    rm->cleanAllCurrentColumnTypes();

    char* database_path = nullptr;
    getDatabasePath(database_id, &database_path);
    assert(fm->deleteFolder(database_path));

    delete[] database_path;

    // delete database record
    std::vector<record::DataItem> data_items;
    std::vector<record::RecordLocation> record_locations;
    rm->getAllRecords(DATABASE_GLOBAL_RECORD_PATH, data_items,
                      record_locations);

    for (int i = 0; i < data_items.size(); i++) {
        if (data_items[i].data_id == database_id) {
            rm->deleteRecord(DATABASE_GLOBAL_RECORD_PATH, record_locations[i]);
            return true;
        }
    }

    std::cout << "!ERROR" << std::endl;
    std::cout << "Database " << database_name << " does not exist" << std::endl;
    return false;
}

int SystemManager::getDatabaseId(const char* database_name) {
    std::vector<record::DataItem> data_items;
    std::vector<record::RecordLocation> record_locations;
    rm->getAllRecords(DATABASE_GLOBAL_RECORD_PATH, data_items,
                      record_locations);

    // search for the database name
    for (auto& item : data_items) {
        if (strcmp(item.data_values[0].value.char_value.c_str(),
                   database_name) == 0) {
            return item.data_id;
        }
    }
    return -1;
}

bool SystemManager::deleteForeignKey(const char* table_name,
                                     std::string foreign_key_name) {
    // check db id
    if (current_database_id == -1) {
        std::cout << "!ERROR" << std::endl;
        std::cout << "No database selected" << std::endl;
        return false;
    }

    int table_id = getTableId(table_name);

    // check if table exists
    if (table_id == -1) {
        std::cout << "!ERROR" << std::endl;
        std::cout << "Table " << table_name << " does not exist" << std::endl;
        return false;
    }

    // get fk key path
    char* table_path = nullptr;
    getTableRecordPath(current_database_id, table_id, &table_path);
    char* foreign_key_path = nullptr;
    utils::concatPath(table_path, FOREIGN_KEY_FILE_NAME, &foreign_key_path);
    // get all record from fk key file
    std::vector<record::DataItem> data_items;
    std::vector<record::RecordLocation> record_locations;
    rm->getAllRecords(foreign_key_path, data_items, record_locations);

    for (int i = 0; i < data_items.size(); i++) {
        auto& data_item = data_items[i];
        std::string name =
            data_item.data_values[FOREIGN_KEY_MAX_NUM * 2 + 1].value.char_value;
        if (name != foreign_key_name) {
            continue;
        }
        auto& record_location = record_locations[i];
        // delete record
        rm->deleteRecord(foreign_key_path, record_location);
        // delete dominates
        char* reference_table_path = nullptr;
        getTableRecordPath(current_database_id,
                           data_item.data_values[0].value.int_value,
                           &reference_table_path);
        char* reference_table_dominate_path = nullptr;
        utils::concatPath(reference_table_path, DOMINATE_FILE_NAME,
                          &reference_table_dominate_path);
        // get all record from reference dominate
        std::vector<record::DataItem> reference_table_data_items;
        std::vector<record::RecordLocation> reference_table_record_locations;
        rm->getAllRecords(reference_table_dominate_path,
                          reference_table_data_items,
                          reference_table_record_locations);
        for (int j = 0; j < reference_table_data_items.size(); j++) {
            auto& reference_table_data_item = reference_table_data_items[j];
            if (reference_table_data_item.data_values[0].value.int_value ==
                table_id) {
                rm->deleteRecord(reference_table_dominate_path,
                                 reference_table_record_locations[j]);
                break;
            }
        }
        delete[] reference_table_path;
        delete[] reference_table_dominate_path;
        delete[] table_path;
        delete[] foreign_key_path;

        return true;
    }
    std::cout << "!ERROR" << std::endl;
    std::cout << "Foreign key " << foreign_key_name << " does not exist"
              << std::endl;
    delete[] table_path;
    delete[] foreign_key_path;
    return false;
}

bool SystemManager::addForeignKey(const char* table_name,
                                  const ForeignKeyInfo& new_foreign_key) {
    // check db id
    if (current_database_id == -1) {
        std::cout << "!ERROR" << std::endl;
        std::cout << "No database selected" << std::endl;
        return false;
    }

    int table_id = getTableId(table_name);

    // check if table exists
    if (table_id == -1) {
        std::cout << "!ERROR" << std::endl;
        std::cout << "Table " << table_name << " does not exist" << std::endl;
        return false;
    }

    // get current fk key
    std::vector<ForeignKeyInfo> foreign_key_infos;
    getTableForeignKeys(table_id, foreign_key_infos);

    // check if already exist
    for (auto& fk_info : foreign_key_infos) {
        if (fk_info == new_foreign_key) {
            std::cout << "!ERROR" << std::endl;
            std::cout << "foreign" << std::endl;
            return false;
        }
    }

    // get all record
    char* table_path = nullptr;
    getTableRecordPath(current_database_id, table_id, &table_path);
    char* record_path = nullptr;
    utils::concatPath(table_path, RECORD_FILE_NAME, &record_path);
    std::vector<record::ColumnType> column_types;
    rm->getColumnTypes(record_path, column_types);
    std::vector<record::DataItem> data_items;
    std::vector<record::RecordLocation> record_locations;
    rm->getAllRecords(record_path, data_items, record_locations);

    delete[] record_path;

    // check all record satisfy new fk constraint
    for (auto& data_item : data_items) {
        std::vector<SearchConstraint> constraints;
        for (int fk_i = 0; fk_i < new_foreign_key.foreign_key_column_ids.size();
             fk_i++) {
            auto& foreign_key_column_id =
                new_foreign_key.foreign_key_column_ids[fk_i];
            auto& reference_column_id =
                new_foreign_key.reference_column_ids[fk_i];

            SearchConstraint constraint;
            constraint.column_id = reference_column_id;
            constraint.constraint_types.push_back(ConstraintType::EQ);
            constraint.data_type = record::DataTypeName::INT;
            for (int i = 0; i < data_item.data_values.size(); i++) {
                if (data_item.column_ids[i] == foreign_key_column_id) {
                    if (data_item.data_values[i].is_null) {
                        std::cout << "!ERROR" << std::endl;
                        std::cout << "foreign" << std::endl;
                        delete[] table_path;
                        return false;
                    }
                    if (data_item.data_values[i].type_name !=
                        record::DataTypeName::INT) {
                        std::cout << "!ERROR" << std::endl;
                        std::cout << "foreign" << std::endl;
                        delete[] table_path;
                        return false;
                    }
                    constraint.constraint_values.push_back(
                        data_item.data_values[i]);
                    break;
                }
            }
            constraints.push_back(constraint);
        }

        std::vector<record::DataItem> result_datas;
        std::vector<record::ColumnType> result_column_types;
        std::vector<record::RecordLocation> record_locations;
        search(new_foreign_key.reference_table_id, constraints, result_datas,
               result_column_types, record_locations, -1);
        if (result_datas.size() == 0) {
            std::cout << "!ERROR" << std::endl;
            std::cout << "foreign" << std::endl;
            delete[] table_path;
            return false;
        }
    }

    // insert a new record into foreign key
    char* foreign_key_path = nullptr;
    utils::concatPath(table_path, FOREIGN_KEY_FILE_NAME, &foreign_key_path);

    delete[] table_path;
    std::vector<record::ColumnType> foreign_key_column_types;
    rm->getColumnTypes(foreign_key_path, foreign_key_column_types);
    record::DataItem data_item;
    data_item.data_values.push_back(record::DataValue(
        record::DataTypeName::INT, false, new_foreign_key.reference_table_id));
    data_item.column_ids.push_back(foreign_key_column_types[0].column_id);
    for (int i = 0; i < FOREIGN_KEY_MAX_NUM; i++) {
        if (i < new_foreign_key.foreign_key_column_ids.size()) {
            data_item.data_values.push_back(
                record::DataValue(record::DataTypeName::INT, false,
                                  new_foreign_key.foreign_key_column_ids[i]));
            data_item.column_ids.push_back(
                foreign_key_column_types[i + 1].column_id);
        } else {
            data_item.data_values.push_back(
                record::DataValue(record::DataTypeName::INT, true));
            data_item.column_ids.push_back(
                foreign_key_column_types[i + 1].column_id);
        }
    }
    for (int i = 0; i < FOREIGN_KEY_MAX_NUM; i++) {
        if (i < new_foreign_key.reference_column_ids.size()) {
            data_item.data_values.push_back(
                record::DataValue(record::DataTypeName::INT, false,
                                  new_foreign_key.reference_column_ids[i]));
            data_item.column_ids.push_back(
                foreign_key_column_types[i + 1 + FOREIGN_KEY_MAX_NUM]
                    .column_id);
        } else {
            data_item.data_values.push_back(
                record::DataValue(record::DataTypeName::INT, true));
            data_item.column_ids.push_back(
                foreign_key_column_types[i + 1 + FOREIGN_KEY_MAX_NUM]
                    .column_id);
        }
    }
    data_item.data_values.push_back(record::DataValue(
        record::DataTypeName::VARCHAR, false, new_foreign_key.name));
    data_item.column_ids.push_back(
        foreign_key_column_types[FOREIGN_KEY_MAX_NUM * 2 + 1].column_id);
    auto location = rm->insertRecord(foreign_key_path, data_item);
    assert(location.page_id != -1 && location.slot_id != -1);

    delete[] foreign_key_path;

    //  add dominates for refernce table
    char* reference_table_path = nullptr;
    getTableRecordPath(current_database_id, new_foreign_key.reference_table_id,
                       &reference_table_path);
    char* reference_table_dominate_path = nullptr;
    utils::concatPath(reference_table_path, DOMINATE_FILE_NAME,
                      &reference_table_dominate_path);
    std::vector<record::ColumnType> reference_table_dominate_column_types;
    rm->getColumnTypes(reference_table_dominate_path,
                       reference_table_dominate_column_types);
    record::DataItem reference_table_data_item;
    reference_table_data_item.data_values.push_back(
        record::DataValue(record::DataTypeName::INT, false, table_id));
    reference_table_data_item.column_ids.push_back(
        reference_table_dominate_column_types[0].column_id);
    rm->insertRecord(reference_table_dominate_path, reference_table_data_item);

    delete[] reference_table_path;
    delete[] reference_table_dominate_path;

    return true;
}

bool SystemManager::createTable(
    const char* table_name, std::vector<record::ColumnType>& column_types,
    const std::vector<std::string>& primary_keys,
    const std::vector<ForeignKeyInputInfo>& foreign_keys) {
    // check db id
    if (current_database_id == -1) {
        std::cout << "!ERROR" << std::endl;
        std::cout << "No database selected" << std::endl;
        return false;
    }

    // check if table exists
    if (getTableId(table_name) != -1) {
        std::cout << "!ERROR" << std::endl;
        std::cout << "Table " << table_name << " already exists" << std::endl;
        return false;
    }

    // check column type has different name
    std::set<std::string> column_names;
    int column_names_idx = 0;
    for (auto& column_type : column_types) {
        if (column_names.find(column_type.column_name) != column_names.end()) {
            std::cout << "!ERROR" << std::endl;
            std::cout << "Multiple columns have the same name" << std::endl;
            return false;
        }
        column_names.insert(column_type.column_name);
        column_type.column_id = column_names_idx++;
    }

    // check primary key
    std::vector<int> primary_key_ids;
    for (auto& primary_key : primary_keys) {
        int find_idx = -1;
        for (int i = 0; i < column_types.size(); i++) {
            if (column_types[i].column_name == primary_key) {
                find_idx = i;
                break;
            }
        }
        if (find_idx == -1) {
            std::cout << "!ERROR" << std::endl;
            std::cout << "Primary key " << primary_key << " does not exist"
                      << std::endl;
            return false;
        }
        if (column_types[find_idx].type_name != record::DataTypeName::INT) {
            std::cout << "!ERROR" << std::endl;
            std::cout << "Primary key " << primary_key << " is not of type INT"
                      << std::endl;
            return false;
        }
        column_types[find_idx].require_not_null = true;
        primary_key_ids.push_back(find_idx);
    }

    // check foreign key
    std::vector<ForeignKeyInfo> foreign_key_infos;
    for (auto& foreign_key : foreign_keys) {
        int reference_table_id =
            getTableId(foreign_key.reference_table_name.c_str());
        if (reference_table_id == -1) {
            std::cout << "!ERROR" << std::endl;
            std::cout << "Foreign key reference table "
                      << foreign_key.reference_table_name << " does not exist"
                      << std::endl;
            return false;
        }

        std::vector<record::ColumnType> reference_table_column_types;
        getTableColumnTypes(reference_table_id, reference_table_column_types);

        std::vector<int> reference_column_ids;
        for (auto& column_name : foreign_key.reference_column_names) {
            int find_idx = -1;
            for (int i = 0; i < reference_table_column_types.size(); i++) {
                if (reference_table_column_types[i].column_name ==
                    column_name) {
                    find_idx = i;
                    break;
                }
            }
            if (find_idx == -1) {
                std::cout << "!ERROR" << std::endl;
                std::cout << "Foreign key reference column " << column_name
                          << " does not exist" << std::endl;
                return false;
            }
            reference_column_ids.push_back(
                reference_table_column_types[find_idx].column_id);
        }

        std::set<int> reference_table_primary_keys;
        getTablePrimaryKeys(reference_table_id, reference_table_primary_keys);

        // check if reference_table_primary_keys == refrence_column_ids
        if (reference_table_primary_keys.size() !=
            reference_column_ids.size()) {
            std::cout << "!ERROR" << std::endl;
            std::cout << "Foreign key reference column does not match"
                      << std::endl;
            return false;
        }
        for (auto& reference_table_primary_key : reference_table_primary_keys) {
            bool find = false;
            for (size_t i = 0; i < reference_column_ids.size(); i++) {
                if (reference_table_primary_key == reference_column_ids[i]) {
                    find = true;
                    break;
                }
            }
            if (!find) {
                std::cout << "!ERROR" << std::endl;
                std::cout << "Foreign key reference column does not "
                             "match"
                          << std::endl;
                return false;
            }
        }

        std::vector<int> foreign_key_column_ids;
        for (auto& column_name : foreign_key.foreign_key_column_names) {
            int find_idx = -1;
            for (int i = 0; i < column_types.size(); i++) {
                if (column_types[i].column_name == column_name) {
                    find_idx = i;
                    break;
                }
            }
            if (find_idx == -1) {
                std::cout << "!ERROR" << std::endl;
                std::cout << "Foreign key column " << column_name
                          << " does not exist" << std::endl;
                return false;
            }
            foreign_key_column_ids.push_back(find_idx);
        }

        if (foreign_key_column_ids.size() != reference_column_ids.size()) {
            std::cout << "!ERROR" << std::endl;
            std::cout << "Foreign key column does not match" << std::endl;
            return false;
        }

        foreign_key_infos.push_back(ForeignKeyInfo(foreign_key_column_ids,
                                                   reference_table_id,
                                                   reference_column_ids, ""));
    }

    // get all table path
    char* db_base_path = nullptr;
    getDatabasePath(current_database_id, &db_base_path);
    char* all_table_path = nullptr;
    utils::concatPath(db_base_path, ALL_TABLE_FILE_NAME, &all_table_path);

    delete[] db_base_path;

    // get column info from all table
    std::vector<record::ColumnType> all_table_column_types;
    rm->getColumnTypes(all_table_path, all_table_column_types);

    // insert a new record into all table
    record::DataItem data_item;
    data_item.data_values.push_back(
        record::DataValue(record::DataTypeName::VARCHAR, false, table_name));
    data_item.column_ids.push_back(all_table_column_types[0].column_id);
    auto location = rm->insertRecord(all_table_path, data_item);

    // get record to update data_id
    assert(rm->getRecord(all_table_path, location, data_item));
    int table_id = data_item.data_id;

    delete[] all_table_path;

    // create table folder
    char* table_path = nullptr;
    getTableRecordPath(current_database_id, table_id, &table_path);
    assert(fm->createFolder(table_path));

    // Record path
    char* record_path = nullptr;
    utils::concatPath(table_path, RECORD_FILE_NAME, &record_path);
    rm->initializeRecordFile(record_path, column_types);
    delete[] record_path;

    // primary key path
    char* primary_key_path = nullptr;
    utils::concatPath(table_path, PRIMARY_KEY_FILE_NAME, &primary_key_path);
    rm->initializeRecordFile(primary_key_path, TablePrimaryKeyInfoColumnType);
    std::vector<record::ColumnType> primary_key_table_column_types;
    rm->getColumnTypes(primary_key_path, primary_key_table_column_types);
    for (auto& primary_key_id : primary_key_ids) {
        record::DataItem data_item;
        data_item.data_values.push_back(record::DataValue(
            record::DataTypeName::INT, false, primary_key_id));
        data_item.column_ids.push_back(
            primary_key_table_column_types[0].column_id);
        rm->insertRecord(primary_key_path, data_item);
    }
    delete[] primary_key_path;

    // foreign key path
    char* foreign_key_path = nullptr;
    utils::concatPath(table_path, FOREIGN_KEY_FILE_NAME, &foreign_key_path);
    rm->initializeRecordFile(foreign_key_path, TableForeignKeyInfoColumnType);
    std::vector<record::ColumnType> foreign_key_table_column_types;
    rm->getColumnTypes(foreign_key_path, foreign_key_table_column_types);
    for (auto& foreign_key_info : foreign_key_infos) {
        record::DataItem data_item;
        data_item.data_values.push_back(
            record::DataValue(record::DataTypeName::INT, false,
                              foreign_key_info.reference_table_id));
        data_item.column_ids.push_back(
            foreign_key_table_column_types[0].column_id);
        for (int i = 0; i < FOREIGN_KEY_MAX_NUM; i++) {
            if (i < foreign_key_info.foreign_key_column_ids.size()) {
                data_item.data_values.push_back(record::DataValue(
                    record::DataTypeName::INT, false,
                    foreign_key_info.foreign_key_column_ids[i]));
                data_item.column_ids.push_back(
                    foreign_key_table_column_types[i + 1].column_id);
            } else {
                data_item.data_values.push_back(
                    record::DataValue(record::DataTypeName::INT, true));
                data_item.column_ids.push_back(
                    foreign_key_table_column_types[i + 1].column_id);
            }
        }
        for (int i = 0; i < FOREIGN_KEY_MAX_NUM; i++) {
            if (i < foreign_key_info.reference_column_ids.size()) {
                data_item.data_values.push_back(record::DataValue(
                    record::DataTypeName::INT, false,
                    foreign_key_info.reference_column_ids[i]));
                data_item.column_ids.push_back(
                    foreign_key_table_column_types[i + 1 + FOREIGN_KEY_MAX_NUM]
                        .column_id);
            } else {
                data_item.data_values.push_back(
                    record::DataValue(record::DataTypeName::INT, true));
                data_item.column_ids.push_back(
                    foreign_key_table_column_types[i + 1 + FOREIGN_KEY_MAX_NUM]
                        .column_id);
            }
        }
        data_item.data_values.push_back(record::DataValue(
            record::DataTypeName::VARCHAR, true, foreign_key_info.name));
        data_item.column_ids.push_back(
            foreign_key_table_column_types[FOREIGN_KEY_MAX_NUM * 2 + 1]
                .column_id);
        rm->insertRecord(foreign_key_path, data_item);
    }
    delete[] foreign_key_path;

    // dominate path
    char* dominate_path = nullptr;
    utils::concatPath(table_path, DOMINATE_FILE_NAME, &dominate_path);
    rm->initializeRecordFile(dominate_path, TableDominateInfoColumnType);
    delete[] dominate_path;

    // change dominates for other tables
    for (auto& foreign_key_info : foreign_key_infos) {
        char* reference_table_path = nullptr;
        getTableRecordPath(current_database_id,
                           foreign_key_info.reference_table_id,
                           &reference_table_path);
        char* reference_table_dominate_path = nullptr;
        utils::concatPath(reference_table_path, DOMINATE_FILE_NAME,
                          &reference_table_dominate_path);
        std::vector<record::ColumnType> reference_table_dominate_column_types;
        rm->getColumnTypes(reference_table_dominate_path,
                           reference_table_dominate_column_types);
        record::DataItem data_item;
        data_item.data_values.push_back(
            record::DataValue(record::DataTypeName::INT, false, table_id));
        data_item.column_ids.push_back(
            reference_table_dominate_column_types[0].column_id);
        rm->insertRecord(reference_table_dominate_path, data_item);
        delete[] reference_table_path;
        delete[] reference_table_dominate_path;
    }

    // index info path
    char* index_info_path = nullptr;
    utils::concatPath(table_path, INDEX_INFO_FILE_NAME, &index_info_path);
    rm->initializeRecordFile(index_info_path, TableIndexInfoColumnType);

    // index folder
    char* index_folder_path = nullptr;
    utils::concatPath(table_path, INDEX_FOLDER_NAME, &index_folder_path);
    fm->createFolder(index_folder_path);

    // create index
    // for (int i = 0; i < column_types.size(); i++) {
    //     if (column_types[i].type_name == record::INT)
    //         createIndex(index_info_path, index_folder_path, {i}, table_id,
    //         "");
    // }

    if (primary_key_ids.size() > 0)
        createIndex(index_info_path, index_folder_path, primary_key_ids,
                    table_id, "");
    for (auto foreign_key_info : foreign_key_infos) {
        // if (foreign_key_info.foreign_key_column_ids.size() > 1)
        createIndex(index_info_path, index_folder_path,
                    foreign_key_info.foreign_key_column_ids, table_id, "");
    }

    // delete path
    delete[] index_info_path;
    delete[] index_folder_path;
    delete[] table_path;

    return true;
}

bool SystemManager::addIndex(const char* table_name,
                             const std::string& index_name,
                             const std::vector<int>& column_ids,
                             bool check_unique) {
    if (current_database_id == -1) {
        std::cout << "!ERROR" << std::endl;
        std::cout << "No database selected" << std::endl;
    }

    int table_id = getTableId(table_name);
    if (table_id == -1) {
        std::cout << "!ERROR" << std::endl;
        std::cout << "Table " << table_name << " does not exist" << std::endl;
        return false;
    }

    // get index info path
    char* table_path = nullptr;
    getTableRecordPath(current_database_id, table_id, &table_path);
    char* index_info_path = nullptr;
    utils::concatPath(table_path, INDEX_INFO_FILE_NAME, &index_info_path);

    // read all from index info
    std::vector<record::DataItem> data_items;
    std::vector<record::RecordLocation> record_locations;
    rm->getAllRecords(index_info_path, data_items, record_locations);

    for (int i = 0; i < data_items.size(); i++) {
        auto& data_item = data_items[i];
        auto& origin_index_name = data_item.data_values[INDEX_KEY_MAX_NUM];
        if (!origin_index_name.is_null &&
            origin_index_name.value.char_value == index_name) {
            std::cout << "!ERROR" << std::endl;
            std::cout << "Index already exists" << std::endl;
            delete[] table_path;
            delete[] index_info_path;
            return false;
        }
    }

    if (!check_unique) {
        for (int i = 0; i < data_items.size(); i++) {
            auto& data_item = data_items[i];
            std::vector<int> index_id;
            for (int i = 0; i < INDEX_KEY_MAX_NUM; i++) {
                if (data_item.data_values[i].is_null == false) {
                    index_id.push_back(
                        data_item.data_values[i].value.int_value);
                }
            }
            auto& origin_index_name = data_item.data_values[INDEX_KEY_MAX_NUM];
            bool same = true;
            if (index_id.size() != column_ids.size()) {
                same = false;
            } else {
                for (int i = 0; i < index_id.size(); i++) {
                    if (index_id[i] != column_ids[i]) {
                        same = false;
                        break;
                    }
                }
            }
            if (same) {
                if (origin_index_name.is_null) {
                    data_item.data_values[INDEX_KEY_MAX_NUM].is_null = false;
                    data_item.data_values[INDEX_KEY_MAX_NUM].value.char_value =
                        index_name;
                    data_item.data_values[INDEX_KEY_MAX_NUM].type_name =
                        record::DataTypeName::VARCHAR;
                    rm->updateRecord(index_info_path, record_locations[i],
                                     data_item);
                    delete[] table_path;
                    delete[] index_info_path;
                    return true;
                } else {
                    std::cout << "!ERROR" << std::endl;
                    std::cout << "Index " << index_name << " already exists"
                              << std::endl;
                    delete[] table_path;
                    delete[] index_info_path;
                    return false;
                }
            }
        }
    }

    // get record path of table_id
    char* record_path = nullptr;
    utils::concatPath(table_path, RECORD_FILE_NAME, &record_path);
    std::vector<record::ColumnType> record_column_types;
    rm->getColumnTypes(record_path, record_column_types);
    // assert all column_ids are INT
    for (auto& column_id : column_ids) {
        bool found = false;
        for (auto& column_type : record_column_types) {
            if (column_type.column_id == column_id) {
                if (column_type.type_name != record::DataTypeName::INT) {
                    std::cout << "!ERROR" << std::endl;
                    std::cout << "Index column is not of type INT" << std::endl;
                    delete[] table_path;
                    delete[] index_info_path;
                    delete[] record_path;
                    return false;
                }
                found = true;
                break;
            }
        }
        if (!found) {
            std::cout << "!ERROR" << std::endl;
            std::cout << "Index column does not exist" << std::endl;
            delete[] table_path;
            delete[] index_info_path;
            delete[] record_path;
            return false;
        }
    }

    // create index
    int index_id = createIndex(index_info_path, table_path, column_ids,
                               table_id, index_name);

    // insert into index
    char* index_file_path = nullptr;
    getIndexRecordPath(current_database_id, table_id, index_id,
                       &index_file_path);
    std::vector<record::ColumnType> index_column_types;
    rm->getColumnTypes(index_file_path, index_column_types);

    // get all data from record file
    rm->getAllRecords(record_path, data_items, record_locations);

    // insert into index
    for (int i = 0; i < data_items.size(); i++) {
        auto& data_item = data_items[i];
        auto& location = record_locations[i];
        index::IndexValue insert_item(location.page_id, location.slot_id, 0);
        for (int j = 0; j < column_ids.size(); j++) {
            int column_id = column_ids[j];
            for (int k = 0; k < data_item.data_values.size(); k++) {
                if (data_item.column_ids[k] == column_id) {
                    insert_item.key.push_back(
                        data_item.data_values[k].value.int_value);
                    break;
                }
            }
        }
        if (check_unique) {
            std::vector<index::IndexValue> search_results;
            im->searchIndex(index_file_path, insert_item, search_results);
            if (search_results.size() > 0) {
                std::cout << "!ERROR" << std::endl;
                std::cout << "duplicate" << std::endl;
                dropIndex(table_name, index_name);
                delete[] table_path;
                delete[] index_info_path;
                delete[] record_path;
                delete[] index_file_path;
                return false;
            }
        }
        im->insertIndex(index_file_path, insert_item);
    }
    delete[] index_file_path;
    delete[] record_path;
    delete[] table_path;
    delete[] index_info_path;
    return true;
}

bool SystemManager::dropIndex(const char* table_name,
                              const std::string& index_name) {
    if (current_database_id == -1) {
        std::cout << "!ERROR" << std::endl;
        std::cout << "No database selected" << std::endl;
    }

    int table_id = getTableId(table_name);
    if (table_id == -1) {
        std::cout << "!ERROR" << std::endl;
        std::cout << "Table " << table_name << " does not exist" << std::endl;
        return false;
    }

    // get index info path
    char* table_path = nullptr;
    getTableRecordPath(current_database_id, table_id, &table_path);
    char* index_info_path = nullptr;
    utils::concatPath(table_path, INDEX_INFO_FILE_NAME, &index_info_path);

    // read all from index info
    std::vector<record::DataItem> data_items;
    std::vector<record::RecordLocation> record_locations;
    rm->getAllRecords(index_info_path, data_items, record_locations);

    for (int i = 0; i < data_items.size(); i++) {
        auto& data_item = data_items[i];
        auto& origin_index_name = data_item.data_values[INDEX_KEY_MAX_NUM];
        if (!origin_index_name.is_null &&
            origin_index_name.value.char_value == index_name) {
            // delete index
            rm->deleteRecord(index_info_path, record_locations[i]);
            // delete index file
            char* index_file_path = nullptr;
            getIndexRecordPath(current_database_id, table_id, data_item.data_id,
                               &index_file_path);
            im->deleteIndexFile(index_file_path);
            delete[] table_path;
            delete[] index_info_path;
            return true;
        }
    }
    std::string index_name_with_unique = index_name + UNIQUE_SUFFIX;
    for (int i = 0; i < data_items.size(); i++) {
        auto& data_item = data_items[i];
        auto& origin_index_name = data_item.data_values[INDEX_KEY_MAX_NUM];
        if (!origin_index_name.is_null &&
            origin_index_name.value.char_value == index_name_with_unique) {
            // delete index
            rm->deleteRecord(index_info_path, record_locations[i]);
            // delete index file
            char* index_file_path = nullptr;
            getIndexRecordPath(current_database_id, table_id, data_item.data_id,
                               &index_file_path);
            im->deleteIndexFile(index_file_path);
            delete[] table_path;
            delete[] index_info_path;
            return true;
        }
    }

    std::cout << "!ERROR" << std::endl;
    std::cout << "Index " << index_name << " does not exist" << std::endl;
    delete[] table_path;
    delete[] index_info_path;
    return false;
}

int SystemManager::createIndex(const char* index_info_path,
                               const char* index_folder_path,
                               std::vector<int> index_ids, int table_id,
                               std::string name) {
    std::vector<record::ColumnType> index_info_column_types;
    rm->getColumnTypes(index_info_path, index_info_column_types);
    record::DataItem data_item;
    for (int i = 0; i <= INDEX_KEY_MAX_NUM; i++) {
        data_item.data_values.push_back(
            record::DataValue(record::DataTypeName::INT, true));
        data_item.column_ids.push_back(index_info_column_types[i].column_id);
    }

    for (int i = 0; i < index_ids.size(); i++) {
        data_item.data_values[i].is_null = false;
        data_item.data_values[i].value.int_value = index_ids[i];
    }
    if (name == "") {
        // null
        data_item.data_values[INDEX_KEY_MAX_NUM].is_null = true;
        data_item.data_values[INDEX_KEY_MAX_NUM].type_name =
            record::DataTypeName::VARCHAR;
    } else {
        data_item.data_values[INDEX_KEY_MAX_NUM].is_null = false;
        data_item.data_values[INDEX_KEY_MAX_NUM].value.char_value = name;
        data_item.data_values[INDEX_KEY_MAX_NUM].type_name =
            record::DataTypeName::VARCHAR;
    }
    auto location = rm->insertRecord(index_info_path, data_item);
    assert(location.page_id != -1);
    // update data_item
    assert(rm->getRecord(index_info_path, location, data_item));

    // create index file
    char* index_file_path = nullptr;
    getIndexRecordPath(current_database_id, table_id, data_item.data_id,
                       &index_file_path);
    im->initializeIndexFile(index_file_path, index_ids.size());

    // delete path
    delete[] index_file_path;
    return data_item.data_id;
}

bool SystemManager::insertIntoTable(const char* table_name,
                                    std::vector<record::DataItem>& data_items) {
    // check db id
    if (current_database_id == -1) {
        std::cout << "!ERROR" << std::endl;
        std::cout << "No database selected" << std::endl;
        return false;
    }

    // get table id
    int table_id = getTableId(table_name);
    if (table_id == -1) {
        std::cout << "!ERROR" << std::endl;
        std::cout << "Table " << table_name << " does not exist" << std::endl;
        return false;
    }

    // get table path
    char* table_path = nullptr;
    getTableRecordPath(current_database_id, table_id, &table_path);

    // get record path
    char* record_path = nullptr;
    utils::concatPath(table_path, RECORD_FILE_NAME, &record_path);

    // get column types
    std::vector<record::ColumnType> column_types;
    rm->getColumnTypes(record_path, column_types);

    // get primary keys
    std::set<int> primary_keys;
    getTablePrimaryKeys(table_id, primary_keys);

    // get foreign keys
    std::vector<ForeignKeyInfo> foreign_key_infos;
    getTableForeignKeys(table_id, foreign_key_infos);

    // record insert val primary keys
    std::vector<std::vector<int>> insert_val_primary_keys;

    // check data_items
    for (auto& data_item : data_items) {
        if (data_item.data_values.size() != column_types.size()) {
            std::cout << "!ERROR" << std::endl;
            std::cout << "Data item size does not match column size"
                      << std::endl;
            return false;
        }
        data_item.column_ids.clear();
        for (int i = 0; i < data_item.data_values.size(); i++) {
            if (data_item.data_values[i].is_null) {
                if (column_types[i].require_not_null) {
                    std::cout << "!ERROR" << std::endl;
                    std::cout << "null" << std::endl;
                    return false;
                }
                data_item.data_values[i].type_name = column_types[i].type_name;
            }
            if (data_item.data_values[i].type_name !=
                column_types[i].type_name) {
                std::cout << "!ERROR" << std::endl;
                std::cout << "Data item type does not match column type"
                          << std::endl;
                return false;
            }
            data_item.column_ids.push_back(column_types[i].column_id);
        }

        // upd 以下检查主键是否唯一
        if (primary_keys.size() > 0) {
            // upd 设置查找主键的constraint
            std::vector<SearchConstraint> primary_constraints;
            std::vector<int> insert_val_primary_key;
            for (auto& primary_key : primary_keys) {
                SearchConstraint constraint;
                constraint.column_id = primary_key;
                constraint.constraint_types.push_back(ConstraintType::EQ);
                constraint.data_type = record::DataTypeName::INT;
                for (int i = 0; i < data_item.data_values.size(); i++) {
                    if (data_item.column_ids[i] == primary_key) {
                        constraint.constraint_values.push_back(
                            data_item.data_values[i]);
                        insert_val_primary_key.push_back(
                            data_item.data_values[i].value.int_value);
                        if (data_item.data_values[i].is_null) {
                            std::cout << "!ERROR" << std::endl;
                            std::cout << "null" << std::endl;
                            return false;
                        }
                    }
                }
                primary_constraints.push_back(constraint);
            }

            // upd 检查是否重复（insert val之间的primary key)
            for (int i = 0; i < insert_val_primary_keys.size(); i++) {
                bool same = true;
                for (int j = 0; j < insert_val_primary_keys[i].size(); j++) {
                    if (insert_val_primary_keys[i][j] !=
                        insert_val_primary_key[j]) {
                        same = false;
                        break;
                    }
                }
                if (same) {
                    std::cout << "!ERROR" << std::endl;
                    std::cout << "duplicate primary keys" << std::endl;
                    return false;
                }
            }
            insert_val_primary_keys.push_back(insert_val_primary_key);

            // upd 检查是否重复
            std::vector<record::DataItem> result_datas;
            std::vector<record::ColumnType> result_column_types;
            std::vector<record::RecordLocation> record_locations;
            search(table_id, primary_constraints, result_datas,
                   result_column_types, record_locations, -1);
            if (result_datas.size() > 0) {
                std::cout << "!ERROR" << std::endl;
                std::cout << "duplicate" << std::endl;
                return false;
            }
        }

        // upd 以下检查外键是否唯一
        for (auto& foreign_key_info : foreign_key_infos) {
            std::vector<record::DataValue> foreign_key_values;
            std::vector<SearchConstraint> constraints;
            for (int fk_i = 0;
                 fk_i < foreign_key_info.foreign_key_column_ids.size();
                 fk_i++) {
                auto& foreign_key_column_id =
                    foreign_key_info.foreign_key_column_ids[fk_i];
                auto& reference_column_id =
                    foreign_key_info.reference_column_ids[fk_i];

                SearchConstraint constraint;
                constraint.column_id = reference_column_id;
                constraint.constraint_types.push_back(ConstraintType::EQ);
                constraint.data_type = record::DataTypeName::INT;
                bool is_null = false;
                for (int i = 0; i < data_item.data_values.size(); i++) {
                    if (data_item.column_ids[i] == foreign_key_column_id) {
                        if (data_item.data_values[i].is_null) {
                            is_null = true;
                            break;
                        }
                        foreign_key_values.push_back(data_item.data_values[i]);
                        constraint.constraint_values.push_back(
                            data_item.data_values[i]);
                        break;
                    }
                }
                if (!is_null) constraints.push_back(constraint);
            }

            std::vector<record::DataItem> result_datas;
            std::vector<record::ColumnType> result_column_types;
            std::vector<record::RecordLocation> record_locations;
            search(foreign_key_info.reference_table_id, constraints,
                   result_datas, result_column_types, record_locations, -1);
            if (result_datas.size() == 0) {
                std::cout << "!ERROR" << std::endl;
                std::cout << "foreign key does not exist" << std::endl;
                return false;
            }
        }
    }

    // TODO unique 检查未实现
    std::vector<SearchConstraint> constraints;
    std::vector<record::DataItem> result_datas;
    std::vector<record::RecordLocation> record_locations;

    std::vector<std::pair<int, std::vector<int>>> all_index;
    std::vector<std::string> index_names;
    getAllIndex(current_database_id, table_id, all_index, index_names);

    rm->getColumnTypes(record_path, column_types);

    for (auto& data_item : data_items) {
        // check unique
        for (auto& column : column_types) {
            if (!column.require_unique) continue;
            std::vector<SearchConstraint> constraints;
            for (int i = 0; i < data_item.column_ids.size(); i++) {
                if (data_item.column_ids[i] == column.column_id) {
                    SearchConstraint constraint;
                    constraint.column_id = column.column_id;
                    constraint.constraint_types.push_back(ConstraintType::EQ);
                    constraint.data_type = column.type_name;
                    constraint.constraint_values.push_back(
                        data_item.data_values[i]);
                    constraints.push_back(constraint);
                    break;
                }
            }
            search(table_id, constraints, result_datas, column_types,
                   record_locations, -1);
            if (result_datas.size() > 0) {
                std::cout << "!ERROR" << std::endl;
                std::cout << "duplicate" << std::endl;
                return false;
            }
        }

        auto location = rm->insertRecord(record_path, data_item);

        for (auto& index : all_index) {
            char* index_file_path = nullptr;
            getIndexRecordPath(current_database_id, table_id, index.first,
                               &index_file_path);
            index::IndexValue index_value(location.page_id, location.slot_id,
                                          0);
            for (auto& column_id : index.second) {
                for (int i = 0; i < data_item.column_ids.size(); i++) {
                    if (data_item.column_ids[i] == column_id) {
                        if (data_item.data_values[i].is_null) {
                            index_value.key.push_back(INT_MIN);
                        } else {
                            index_value.key.push_back(
                                data_item.data_values[i].value.int_value);
                        }
                        break;
                    }
                }
            }
            assert(im->insertIndex(index_file_path, index_value));
            delete[] index_file_path;
        }
    }
    std::cout << "rows" << std::endl;
    std::cout << data_items.size() << std::endl;
    // delete path
    delete[] table_path;
    delete[] record_path;

    return true;
}

void SystemManager::fillInDataTypeField(
    std::vector<std::tuple<std::string, record::DataValue>>& update_info,
    int table_id, std::vector<std::string>& column_names,
    record::DataItem& data_item) {
    char* table_path = nullptr;
    getTableRecordPath(current_database_id, table_id, &table_path);
    char* record_path = nullptr;
    utils::concatPath(table_path, RECORD_FILE_NAME, &record_path);

    std::vector<record::ColumnType> column_types;
    rm->getColumnTypes(record_path, column_types);

    for (auto& info : update_info) {
        std::string& table_name = std::get<0>(info);
        record::DataValue& data_value = std::get<1>(info);

        if (data_value.type_name == record::DataTypeName::ANY) {
            for (auto& column_type : column_types) {
                if (column_type.column_name == table_name) {
                    data_value.type_name = column_type.type_name;
                    break;
                }
            }
        }
        assert(data_value.type_name != record::DataTypeName::ANY);

        column_names.push_back(std::get<0>(info));
        data_item.data_values.push_back(std::get<1>(info));
    }
}

void SystemManager::fillInDataTypeField(
    std::vector<SearchConstraint>& constraints, int table_id) {
    // find DataType of constraint columns
    // get table path
    char* table_path = nullptr;
    getTableRecordPath(current_database_id, table_id, &table_path);
    char* record_path = nullptr;
    utils::concatPath(table_path, RECORD_FILE_NAME, &record_path);

    std::vector<record::ColumnType> column_types;
    rm->getColumnTypes(record_path, column_types);

    for (auto& constraint : constraints) {
        if (constraint.data_type == record::DataTypeName::ANY) {
            for (auto& column_type : column_types) {
                if (column_type.column_id == constraint.column_id) {
                    constraint.data_type = column_type.type_name;
                    break;
                }
            }
        }
        assert(constraint.data_type != record::DataTypeName::ANY);
    }
}

std::string SystemManager::findTableNameOfColumnName(
    const std::string& column_name, std::vector<std::string>& table_names) {
    std::string selected_table_name = "";
    for (auto& table_name : table_names) {
        int table_id = getTableId(table_name.c_str());
        if (table_id == -1) {
            std::cout << "!ERROR" << std::endl;
            std::cout << "Table " << table_name << " does not exist"
                      << std::endl;
            return "";
        }

        std::vector<record::ColumnType> column_types;
        getTableColumnTypes(table_id, column_types);
        for (int i = 0; i < column_types.size(); i++) {
            if (column_types[i].column_name == column_name) {
                if (selected_table_name != "") {
                    std::cout << "!ERROR" << std::endl;
                    std::cout << "Ambiguous column name" << std::endl;
                    return "";
                }
                selected_table_name = table_name;
            }
        }
    }
    return selected_table_name;
}

bool SystemManager::getColumnID(int table_id,
                                const std::vector<std::string>& column_names,
                                std::vector<int>& column_ids) {
    // get table path
    char* table_path = nullptr;
    getTableRecordPath(current_database_id, table_id, &table_path);
    char* record_path = nullptr;
    utils::concatPath(table_path, RECORD_FILE_NAME, &record_path);

    // get column types
    std::vector<record::ColumnType> column_types;
    rm->getColumnTypes(record_path, column_types);

    // get column id
    for (auto& column_name : column_names) {
        bool find = false;
        for (auto& column_type : column_types) {
            if (column_type.column_name == column_name) {
                column_ids.push_back(column_type.column_id);
                find = true;
                break;
            }
        }
        if (!find) {
            std::cout << "!ERROR" << std::endl;
            std::cout << "Column " << column_name << " does not exist"
                      << std::endl;
            return false;
        }
    }

    delete[] table_path;
    delete[] record_path;
    return true;
}

bool SystemManager::update(int table_id,
                           std::vector<SearchConstraint>& constraints,
                           const record::DataItem& update_data) {
    // search those satisfy constraints
    std::vector<record::DataItem> result_datas;
    std::vector<record::ColumnType> column_types;
    std::vector<record::RecordLocation> record_location_results;
    if (!search(table_id, constraints, result_datas, column_types,
                record_location_results, -1)) {
        return false;
    }

    // get primary key id
    std::set<int> primary_keys;
    getTablePrimaryKeys(table_id, primary_keys);

    // get foriegn key id
    std::vector<ForeignKeyInfo> foreign_key_infos;
    getTableForeignKeys(table_id, foreign_key_infos);

    // get dominate table id
    std::vector<int> dominate_table_ids;
    getTableDominate(table_id, dominate_table_ids);

    std::vector<bool> change_foreign_key(foreign_key_infos.size(), false);
    for (int i = 0; i < foreign_key_infos.size(); i++) {
        auto& foreign_key_info = foreign_key_infos[i];
        for (auto& foreign_key_column_id :
             foreign_key_info.foreign_key_column_ids) {
            bool found = false;
            for (auto& data_column_id : update_data.column_ids) {
                if (foreign_key_column_id == data_column_id) {
                    change_foreign_key[i] = true;
                    found = true;
                    break;
                }
            }
            if (found) break;
        }
    }

    // get all index
    std::vector<std::pair<int, std::vector<int>>> all_index;
    std::vector<std::string> index_names;
    getAllIndex(current_database_id, table_id, all_index, index_names);

    std::vector<bool> change_index;
    for (int i = 0; i < all_index.size(); i++) {
        change_index.push_back(false);
        for (auto& index_id : all_index[i].second) {
            for (auto& data_column_id : update_data.column_ids) {
                if (index_id == data_column_id) {
                    change_index[i] = true;
                    break;
                }
            }
        }
    }

    // get record path
    char* table_path = nullptr;
    getTableRecordPath(current_database_id, table_id, &table_path);
    char* record_path = nullptr;
    utils::concatPath(table_path, RECORD_FILE_NAME, &record_path);

    for (int i = 0; i < result_datas.size(); i++) {
        auto& result_data = result_datas[i];
        auto& record_location_result = record_location_results[i];

        bool change_primary_key = false;
        dbs::record::DataItem new_data = result_data;
        for (int j = 0; j < update_data.column_ids.size(); j++) {
            bool find = false;
            for (int k = 0; k < new_data.column_ids.size(); k++) {
                if (update_data.column_ids[j] == new_data.column_ids[k]) {
                    if (primary_keys.find(update_data.column_ids[j]) !=
                            primary_keys.end() &&
                        !(new_data.data_values[k] ==
                          update_data.data_values[j])) {
                        change_primary_key = true;
                    }
                    new_data.data_values[k] = update_data.data_values[j];
                    find = true;
                    break;
                }
            }
            if (!find) {
                std::cout << "!ERROR" << std::endl;
                std::cout << "Update column does not exist" << std::endl;
                delete[] table_path;
                delete[] record_path;
                return false;
            }
        }

        // TODO
        // upd 检查new data的unique
        for (auto& column : column_types) {
            if (column.require_unique == false) continue;
            std::vector<SearchConstraint> constraints;
            bool equal_old = false;
            for (int ii = 0; ii < new_data.data_values.size(); ii++) {
                if (new_data.column_ids[ii] == column.column_id) {
                    SearchConstraint constraint;
                    constraint.column_id = column.column_id;
                    constraint.constraint_types.push_back(ConstraintType::EQ);
                    constraint.data_type = new_data.data_values[ii].type_name;
                    constraint.constraint_values.push_back(
                        new_data.data_values[ii]);
                    constraints.push_back(constraint);
                    if (new_data.data_values[ii] ==
                        result_data.data_values[ii]) {
                        equal_old = true;
                    }
                    break;
                }
            }
            if (equal_old) continue;
            std::vector<record::DataItem> result_datas;
            std::vector<record::ColumnType> result_column_types;
            std::vector<record::RecordLocation> record_locations;
            search(table_id, constraints, result_datas, result_column_types,
                   record_locations, -1);
            if (result_datas.size() > 0) {
                std::cout << "!ERROR" << std::endl;
                std::cout << "duplicate" << std::endl;
                delete[] table_path;
                delete[] record_path;
                return false;
            }
        }

        // upd 检查修改涉及到了主键 & 和该主键被其他表的外键引用的情况
        if (change_primary_key) {
            // 主键
            std::vector<SearchConstraint> primary_constraints;
            for (auto& primary_key : primary_keys) {
                SearchConstraint constraint;
                constraint.column_id = primary_key;
                constraint.constraint_types.push_back(ConstraintType::EQ);
                constraint.data_type = record::DataTypeName::INT;
                for (int ii = 0; ii < new_data.data_values.size(); ii++) {
                    if (new_data.column_ids[ii] == primary_key) {
                        constraint.constraint_values.push_back(
                            new_data.data_values[ii]);
                        if (new_data.data_values[ii].is_null) {
                            std::cout << "!ERROR" << std::endl;
                            std::cout << "null" << std::endl;
                            delete[] table_path;
                            delete[] record_path;
                            return false;
                        }
                    }
                }
                primary_constraints.push_back(constraint);
            }
            std::vector<record::DataItem> result_datas;
            std::vector<record::ColumnType> result_column_types;
            std::vector<record::RecordLocation> record_locations;
            search(table_id, primary_constraints, result_datas,
                   result_column_types, record_locations, -1);
            if (result_datas.size() > 0 &&
                !(result_datas.size() == 1 &&
                  result_datas[0].data_id == result_data.data_id)) {
                std::cout << "!ERROR" << std::endl;
                std::cout << "Primary key already exists" << std::endl;
                delete[] table_path;
                delete[] record_path;
                return false;
            }

            // 被外键
            for (auto& dominate_table_id : dominate_table_ids) {
                // get dominate table foreign key
                std::vector<ForeignKeyInfo> dominate_foreign_key_infos;
                getTableForeignKeys(dominate_table_id,
                                    dominate_foreign_key_infos);

                for (auto& dominate_foreign_key_info :
                     dominate_foreign_key_infos) {
                    if (dominate_foreign_key_info.reference_table_id ==
                        table_id) {
                        std::vector<SearchConstraint> dominate_constraints;
                        for (int dominate_i = 0;
                             dominate_i < dominate_foreign_key_info
                                              .foreign_key_column_ids.size();
                             dominate_i++) {
                            auto& dominate_foreign_key_column_id =
                                dominate_foreign_key_info
                                    .foreign_key_column_ids[dominate_i];
                            auto& dominate_reference_column_id =
                                dominate_foreign_key_info
                                    .reference_column_ids[dominate_i];

                            SearchConstraint constraint;
                            constraint.column_id =
                                dominate_foreign_key_column_id;
                            constraint.constraint_types.push_back(
                                ConstraintType::EQ);
                            constraint.data_type = record::DataTypeName::INT;
                            for (int ii = 0;
                                 ii < result_data.data_values.size(); ii++) {
                                if (result_data.column_ids[ii] ==
                                    dominate_reference_column_id) {
                                    constraint.constraint_values.push_back(
                                        result_data.data_values[ii]);
                                    break;
                                }
                            }
                            dominate_constraints.push_back(constraint);
                        }
                        std::vector<record::DataItem> result_datas;
                        std::vector<record::ColumnType> result_column_types;
                        std::vector<record::RecordLocation> record_locations;
                        search(dominate_table_id, dominate_constraints,
                               result_datas, result_column_types,
                               record_locations, -1);
                        if (result_datas.size() > 0) {
                            std::cout << "!ERROR" << std::endl;
                            std::cout << "break foreign key rules" << std::endl;
                            delete[] table_path;
                            delete[] record_path;
                            return false;
                        }
                    }
                }
            }
        }

        // upd 检查null 约束
        for (auto& column : column_types) {
            if (column.require_not_null) {
                for (int ii = 0; ii < new_data.data_values.size(); ii++) {
                    if (new_data.column_ids[ii] == column.column_id) {
                        if (new_data.data_values[ii].is_null) {
                            std::cout << "!ERROR" << std::endl;
                            std::cout << "null" << std::endl;
                            delete[] table_path;
                            delete[] record_path;
                            return false;
                        }
                        break;
                    }
                }
            }
        }

        // upd 检查外键
        for (int ii = 0; ii < foreign_key_infos.size(); ii++) {
            if (change_foreign_key[ii]) {
                auto& foreign_key_info = foreign_key_infos[ii];
                std::vector<SearchConstraint> foreign_key_constraints;
                for (int foreign_key_i = 0;
                     foreign_key_i <
                     foreign_key_info.foreign_key_column_ids.size();
                     foreign_key_i++) {
                    auto& foreign_key_column_id =
                        foreign_key_info.foreign_key_column_ids[foreign_key_i];
                    auto& reference_column_id =
                        foreign_key_info.reference_column_ids[foreign_key_i];

                    SearchConstraint constraint;
                    constraint.column_id = reference_column_id;
                    constraint.constraint_types.push_back(ConstraintType::EQ);
                    constraint.data_type = record::DataTypeName::INT;
                    bool foreign_key_is_null = false;
                    for (int ii = 0; ii < new_data.data_values.size(); ii++) {
                        if (new_data.column_ids[ii] == foreign_key_column_id) {
                            if (new_data.data_values[ii].is_null) {
                                foreign_key_is_null = true;
                            }
                            constraint.constraint_values.push_back(
                                new_data.data_values[ii]);
                            break;
                        }
                    }
                    if (!foreign_key_is_null)
                        foreign_key_constraints.push_back(constraint);
                }
                std::vector<record::DataItem> result_datas;
                std::vector<record::ColumnType> result_column_types;
                std::vector<record::RecordLocation> record_locations;
                search(foreign_key_info.reference_table_id,
                       foreign_key_constraints, result_datas,
                       result_column_types, record_locations, -1);
                if (result_datas.size() == 0) {
                    std::cout << "!ERROR" << std::endl;
                    std::cout << "foreign key does not exist" << std::endl;
                    delete[] table_path;
                    delete[] record_path;
                    return false;
                }
            }
        }

        // update record
        if (!rm->updateRecord(record_path, record_location_result, new_data)) {
            std::cout << "!ERROR" << std::endl;
            std::cout << "Update record failed" << std::endl;
            delete[] table_path;
            delete[] record_path;
            return false;
        }

        // delete index affected, insert again
        for (int i = 0; i < all_index.size(); i++) {
            if (!change_index[i]) continue;
            char* index_file_path = nullptr;
            getIndexRecordPath(current_database_id, table_id,
                               all_index[i].first, &index_file_path);

            index::IndexValue old_index_value(record_location_result.page_id,
                                              record_location_result.slot_id,
                                              0);
            for (auto& column_id : all_index[i].second) {
                for (int i = 0; i < result_data.column_ids.size(); i++) {
                    if (result_data.column_ids[i] == column_id) {
                        if (result_data.data_values[i].is_null) {
                            old_index_value.key.push_back(INT_MIN);
                        } else {
                            old_index_value.key.push_back(
                                result_data.data_values[i].value.int_value);
                        }
                        break;
                    }
                }
            }
            if (!im->deleteIndex(index_file_path, old_index_value, true)) {
                std::cout << "!ERROR" << std::endl;
                std::cout << "Delete index failed" << std::endl;
                delete[] table_path;
                delete[] record_path;
                delete[] index_file_path;
                return false;
            }

            index::IndexValue new_index_value(record_location_result.page_id,
                                              record_location_result.slot_id,
                                              0);
            for (auto& column_id : all_index[i].second) {
                for (int i = 0; i < new_data.column_ids.size(); i++) {
                    if (new_data.column_ids[i] == column_id) {
                        if (new_data.data_values[i].is_null) {
                            new_index_value.key.push_back(INT_MIN);
                        } else {
                            new_index_value.key.push_back(
                                new_data.data_values[i].value.int_value);
                        }
                        break;
                    }
                }
            }
            if (!im->insertIndex(index_file_path, new_index_value)) {
                std::cout << "!ERROR" << std::endl;
                std::cout << "Insert index failed" << std::endl;
                delete[] table_path;
                delete[] record_path;
                delete[] index_file_path;
                return false;
            }
            delete[] index_file_path;
        }
    }
    delete[] table_path;
    delete[] record_path;
    std::cout << "rows" << std::endl;
    std::cout << result_datas.size() << std::endl;
    return true;
}

bool SystemManager::deleteFromTable(
    int table_id, std::vector<SearchConstraint>& constraints) {
    // search those satisfy constraints
    std::vector<record::DataItem> result_datas;
    std::vector<record::ColumnType> column_types;
    std::vector<record::RecordLocation> record_location_results;
    if (!search(table_id, constraints, result_datas, column_types,
                record_location_results, -1)) {
        return false;
    }

    // get primary key id
    std::set<int> primary_keys;
    getTablePrimaryKeys(table_id, primary_keys);

    // get dominate table id
    std::vector<int> dominate_table_ids;
    getTableDominate(table_id, dominate_table_ids);

    // get all index
    std::vector<std::pair<int, std::vector<int>>> all_index;
    std::vector<std::string> index_names;
    getAllIndex(current_database_id, table_id, all_index, index_names);

    // get record path
    char* table_path = nullptr;
    getTableRecordPath(current_database_id, table_id, &table_path);
    char* record_path = nullptr;
    utils::concatPath(table_path, RECORD_FILE_NAME, &record_path);

    for (int i = 0; i < result_datas.size(); i++) {
        auto& result_data = result_datas[i];
        auto& record_location_result = record_location_results[i];

        // check 被外键
        for (auto& dominate_table_id : dominate_table_ids) {
            // get dominate table foreign key
            std::vector<ForeignKeyInfo> dominate_foreign_key_infos;
            getTableForeignKeys(dominate_table_id, dominate_foreign_key_infos);

            for (auto& dominate_foreign_key_info : dominate_foreign_key_infos) {
                if (dominate_foreign_key_info.reference_table_id == table_id) {
                    std::vector<SearchConstraint> dominate_constraints;
                    for (int dominate_i = 0;
                         dominate_i < dominate_foreign_key_info
                                          .foreign_key_column_ids.size();
                         dominate_i++) {
                        auto& dominate_foreign_key_column_id =
                            dominate_foreign_key_info
                                .foreign_key_column_ids[dominate_i];
                        auto& dominate_reference_column_id =
                            dominate_foreign_key_info
                                .reference_column_ids[dominate_i];

                        SearchConstraint constraint;
                        constraint.column_id = dominate_foreign_key_column_id;
                        constraint.data_type = record::DataTypeName::INT;
                        constraint.constraint_types.push_back(
                            ConstraintType::EQ);
                        for (int ii = 0; ii < result_data.data_values.size();
                             ii++) {
                            if (result_data.column_ids[ii] ==
                                dominate_reference_column_id) {
                                constraint.constraint_values.push_back(
                                    result_data.data_values[ii]);
                                break;
                            }
                        }
                        dominate_constraints.push_back(constraint);
                    }
                    std::vector<record::DataItem> result_datas;
                    std::vector<record::ColumnType> result_column_types;
                    std::vector<record::RecordLocation> record_locations;
                    search(dominate_table_id, dominate_constraints,
                           result_datas, result_column_types, record_locations,
                           -1);
                    if (result_datas.size() > 0) {
                        std::cout << "!ERROR" << std::endl;
                        std::cout << "break foreign key rules" << std::endl;
                        delete[] table_path;
                        delete[] record_path;
                        return false;
                    }
                }
            }
        }

        // delete record
        if (!rm->deleteRecord(record_path, record_location_result)) {
            std::cout << "!ERROR" << std::endl;
            std::cout << "Delete record failed" << std::endl;
            delete[] table_path;
            delete[] record_path;
            return false;
        }

        // delete all index
        for (int i = 0; i < all_index.size(); i++) {
            char* index_file_path = nullptr;
            getIndexRecordPath(current_database_id, table_id,
                               all_index[i].first, &index_file_path);

            index::IndexValue old_index_value(record_location_result.page_id,
                                              record_location_result.slot_id,
                                              0);
            for (auto& column_id : all_index[i].second) {
                for (int i = 0; i < result_data.column_ids.size(); i++) {
                    if (result_data.column_ids[i] == column_id) {
                        if (result_data.data_values[i].is_null) {
                            old_index_value.key.push_back(INT_MIN);
                        } else {
                            old_index_value.key.push_back(
                                result_data.data_values[i].value.int_value);
                        }
                        break;
                    }
                }
            }
            if (!im->deleteIndex(index_file_path, old_index_value, true)) {
                std::cout << "!ERROR" << std::endl;
                std::cout << "Delete index failed" << std::endl;
                delete[] table_path;
                delete[] record_path;
                delete[] index_file_path;
                return false;
            }
            delete[] index_file_path;
        }
    }

    delete[] table_path;
    delete[] record_path;
    std::cout << "rows" << std::endl;
    std::cout << result_datas.size() << std::endl;
    return true;
}

bool SystemManager::dropPrimaryKey(int table_id) {
    // get table path
    char* table_path = nullptr;
    getTableRecordPath(current_database_id, table_id, &table_path);

    // get table primary key path
    char* primary_key_path = nullptr;
    utils::concatPath(table_path, PRIMARY_KEY_FILE_NAME, &primary_key_path);

    // check if primary key exists in primary key file
    std::vector<record::RecordLocation> locations;
    std::vector<record::DataItem> primary_keys;
    rm->getAllRecords(primary_key_path, primary_keys, locations);

    if (primary_keys.size() == 0) {
        std::cout << "!ERROR" << std::endl;
        std::cout << "Table " << table_id << " does not have primary key"
                  << std::endl;
        return false;
    }

    for (auto& location : locations) {
        assert(rm->deleteRecord(primary_key_path, location));
    }

    return true;
}

bool SystemManager::addPrimaryKey(int table_id,
                                  std::vector<int> primary_key_ids) {
    // get table path
    char* table_path = nullptr;
    getTableRecordPath(current_database_id, table_id, &table_path);

    // get table primary key path
    char* primary_key_path = nullptr;
    utils::concatPath(table_path, PRIMARY_KEY_FILE_NAME, &primary_key_path);

    // check if primary key exists in primary key file
    std::vector<record::DataItem> data_items;
    std::vector<record::RecordLocation> record_locations;
    rm->getAllRecords(primary_key_path, data_items, record_locations);
    if (data_items.size() != 0) {
        std::cout << "!ERROR" << std::endl;
        std::cout << "Can only have one primary key" << std::endl;
        return false;
    }

    // get data items of private key
    std::vector<record::DataItem> table_data_items;
    char* record_path = nullptr;
    utils::concatPath(table_path, RECORD_FILE_NAME, &record_path);
    rm->getAllRecords(record_path, table_data_items, record_locations);

    std::vector<record::DataItem> primary_key_values;
    for (auto& table_data_item : table_data_items) {
        record::DataItem primary_key_value;
        for (int i = 0; i < table_data_item.data_values.size(); i++) {
            for (auto& primary_key_id : primary_key_ids) {
                if (table_data_item.column_ids[i] == primary_key_id) {
                    primary_key_value.data_values.push_back(
                        table_data_item.data_values[i]);
                    primary_key_value.column_ids.push_back(
                        table_data_item.column_ids[i]);
                    break;
                }
            }
        }
        primary_key_values.push_back(primary_key_value);
    }

    // check if primary key values are unique
    std::set<record::DataItem> primary_key_values_set(
        primary_key_values.begin(), primary_key_values.end());
    if (primary_key_values_set.size() != primary_key_values.size()) {
        std::cout << "!ERROR" << std::endl;
        std::cout << "duplicated value" << std::endl;
        return false;
    }

    // insert primary key
    std::vector<record::ColumnType> primary_key_table_column_types;
    rm->getColumnTypes(primary_key_path, primary_key_table_column_types);

    for (auto& primary_key_id : primary_key_ids) {
        record::DataItem data_item;
        data_item.data_values.push_back(record::DataValue(
            record::DataTypeName::INT, false, primary_key_id));
        data_item.column_ids.push_back(
            primary_key_table_column_types[0].column_id);
        record::RecordLocation location =
            rm->insertRecord(primary_key_path, data_item);
        if (location.page_id == -1) {
            throw std::runtime_error("insert primary key failed");
        }
    }

    delete[] table_path;
    delete[] primary_key_path;

    return true;
}

bool SystemManager::addUnique(const char* table_name,
                              const std::string& unique_name,
                              const std::vector<int>& column_ids) {
    if (current_database_id == -1) {
        std::cout << "!ERROR" << std::endl;
        std::cout << "No database selected" << std::endl;
        return false;
    }

    int table_id = getTableId(table_name);
    if (table_id == -1) {
        std::cout << "!ERROR" << std::endl;
        std::cout << "Table " << table_name << " does not exist" << std::endl;
        return false;
    }
    std::string index_name = unique_name + UNIQUE_SUFFIX;
    if (!addIndex(table_name, index_name, column_ids, true)) return false;

    // check if current item is unique
    char* table_path = nullptr;
    getTableRecordPath(current_database_id, table_id, &table_path);
    char* record_path = nullptr;
    utils::concatPath(table_path, RECORD_FILE_NAME, &record_path);

    if (column_ids.size() != 1) {
        std::cout << "!ERROR" << std::endl;
        std::cout << "Can only have one unique column" << std::endl;
        return false;
    }

    rm->updateColumnUnique(record_path, column_ids[0], true);

    std::vector<record::ColumnType> column_types;
    rm->getColumnTypes(record_path, column_types);

    assert(column_types[column_ids[0]].require_unique);

    return true;
}

bool SystemManager::searchAndSave(int table_id,
                                  std::vector<record::ColumnType>& column_types,
                                  std::vector<SearchConstraint>& constraints,
                                  std::string& save_path, int& total_num) {
    column_types.clear();
    if (current_database_id == -1) {
        std::cout << "!ERROR" << std::endl;
        std::cout << "No database selected" << std::endl;
        return false;
    }
    if (current_database_id == -1) {
        std::cout << "!ERROR" << std::endl;
        std::cout << "No database selected" << std::endl;
        return false;
    }

    std::string path = DATABASE_PATH;
    path.push_back('/');
    path += TMP_FILE_PREFIX;

    path += std::to_string(tmp_file_idx++);

    save_path = DATABASE_PATH;
    save_path.push_back('/');
    save_path += TMP_FILE_PREFIX;

    save_path += std::to_string(tmp_file_idx++);

    if (fm->existFile(path.c_str())) {
        fm->deleteFile(path.c_str());
    }
    fm->createFile(path.c_str());

    // get all column types
    getTableColumnTypes(table_id, column_types);
    bool has_item = mergeConstraints(constraints);
    rm->initializeRecordFile(save_path.c_str(), column_types);
    // check column type
    std::vector<int> int_constraint_with_range;
    for (auto& constraint : constraints) {
        bool find = false;
        for (auto& column_type : column_types) {
            if (constraint.column_id == column_type.column_id) {
                if (constraint.data_type != column_type.type_name) {
                    std::cout << "!ERROR" << std::endl;
                    std::cout << "Constraint data type does not match column "
                                 "type"
                              << std::endl;
                    std::cout
                        << "Constraint data type: " << int(constraint.data_type)
                        << " column type: " << int(column_type.type_name)
                        << std::endl;
                    return false;
                }
                find = true;
                break;
            }
        }
        if (!find) {
            std::cout << "!ERROR" << std::endl;
            std::cout << "Constraint column id does not exist" << std::endl;
            return false;
        }
        if (constraint.data_type == record::DataTypeName::INT &&
            constraint.constraint_types.size() > 0 &&
            constraint.constraint_types[0] != ConstraintType::NEQ) {
            int_constraint_with_range.push_back(constraint.column_id);
        }
    }

    if (!has_item) return true;

    // get all index
    std::vector<std::pair<int, std::vector<int>>> all_index;
    std::vector<std::string> index_names;
    getAllIndex(current_database_id, table_id, all_index, index_names);

    // get chosen index
    int chosen_index = -1, overlap_num = 0;
    std::vector<int> index_val;
    for (auto& index : all_index) {
        int index_id = index.first;
        auto& index_num = index.second;
        int overlap = 0;  // int_constraint_with_range
        for (auto& index_num_id : index_num) {
            bool found = false;
            for (auto& int_constraint_with_range_id :
                 int_constraint_with_range) {
                if (index_num_id == int_constraint_with_range_id) {
                    overlap++;
                    found = true;
                    break;
                }
            }
            if (!found) break;
        }
        if (overlap > overlap_num) {
            overlap_num = overlap;
            chosen_index = index_id;
            index_val = index_num;
        }
    }
    if (chosen_index == -1) {
        char* table_path = nullptr;
        getTableRecordPath(current_database_id, table_id, &table_path);
        char* record_path = nullptr;
        utils::concatPath(table_path, RECORD_FILE_NAME, &record_path);
        // std::vector<record::DataItem> data_items;
        // std::vector<record::RecordLocation> record_locations;
        // rm->getAllRecordWithConstraint(record_path, result_datas,
        //                                record_location_results, constraints);
        total_num = rm->getAllRecordWithConstraintSaveFile(
            record_path, path.c_str(), constraints);
        delete[] table_path;
        delete[] record_path;
        rm->insertRecordsToEmptyRecord(save_path.c_str(), path.c_str(), ",",
                                       false);
        return true;
    } else {
        index::IndexValue index_range_low, index_range_high;
        for (int i = 0; i < overlap_num; i++) {
            int low_range = INT_MIN, high_range = INT_MAX;
            for (auto& constraint : constraints) {
                if (constraint.column_id == index_val[i]) {
                    for (int j = 0; j < constraint.constraint_types.size();
                         j++) {
                        if (constraint.constraint_types[j] ==
                            ConstraintType::LEQ) {
                            high_range = std::min(
                                high_range, constraint.constraint_values[j]
                                                .value.int_value);

                        } else if (constraint.constraint_types[j] ==
                                   ConstraintType::GEQ) {
                            low_range = std::max(low_range,
                                                 constraint.constraint_values[j]
                                                     .value.int_value);
                        }
                    }
                }
            }
            index_range_low.key.push_back(low_range);
            index_range_high.key.push_back(high_range);
        }
        for (int i = overlap_num; i < index_val.size(); i++) {
            index_range_low.key.push_back(INT_MIN);
            index_range_high.key.push_back(INT_MAX);
        }
        // get index file path
        char* index_file_path = nullptr;
        getIndexRecordPath(current_database_id, table_id, chosen_index,
                           &index_file_path);
        // check index file
        std::vector<index::IndexValue> index_results;
        im->searchIndexInRanges(index_file_path, index_range_low,
                                index_range_high, index_results);
        delete[] index_file_path;

        // get record file path
        char* table_path = nullptr;
        getTableRecordPath(current_database_id, table_id, &table_path);
        char* record_path = nullptr;
        utils::concatPath(table_path, RECORD_FILE_NAME, &record_path);

        // get record
        std::vector<record::RecordLocation> record_locations;
        index::indexValueToRecordLocation(index_results, record_locations);
        std::vector<record::DataItem> data_items;
        rm->getRecords(record_path, record_locations, data_items);
        delete[] table_path;
        delete[] record_path;
        int cnt = 0;
        std::ofstream outputFile(path);
        if (!outputFile.is_open()) {
            std::cerr << "Error opening file!" << std::endl;
            return 0;
        }
        for (auto& data_item : data_items) {
            bool valid = true;
            for (auto& constraint : constraints) {
                if (!validConstraint(constraint, data_item)) {
                    valid = false;
                    break;
                }
            }
            if (valid) {
                for (int i = 0; i < data_item.data_values.size() - 1; i++) {
                    outputFile << data_item.data_values[i].toString() << ",";
                }
                outputFile << data_item.data_values.back().toString();
                outputFile << std::endl;
                cnt++;
            }
        }
        outputFile.close();
        total_num = cnt;
        rm->insertRecordsToEmptyRecord(save_path.c_str(), path.c_str(), ",",
                                       false);
        return true;
    }
}

bool SystemManager::search(
    int table_id, std::vector<SearchConstraint>& constraints,
    std::vector<record::DataItem>& result_datas,
    std::vector<record::ColumnType>& column_types,
    std::vector<record::RecordLocation>& record_location_results, int sort_by) {
    if (current_database_id == -1) {
        std::cout << "!ERROR" << std::endl;
        std::cout << "No database selected" << std::endl;
        return false;
    }
    // for (auto& constraint : constraints) {
    //     constraint.print();
    // }
    result_datas.clear();
    column_types.clear();
    record_location_results.clear();
    if (current_database_id == -1) {
        std::cout << "!ERROR" << std::endl;
        std::cout << "No database selected" << std::endl;
        return false;
    }

    // get all column types
    getTableColumnTypes(table_id, column_types);
    bool has_item = mergeConstraints(constraints);
    // check column type
    std::vector<int> int_constraint_with_range;
    for (auto& constraint : constraints) {
        bool find = false;
        for (auto& column_type : column_types) {
            if (constraint.column_id == column_type.column_id) {
                if (constraint.data_type != column_type.type_name) {
                    std::cout << "!ERROR" << std::endl;
                    std::cout << "Constraint data type does not match column "
                                 "type"
                              << std::endl;
                    std::cout
                        << "Constraint data type: " << int(constraint.data_type)
                        << " column type: " << int(column_type.type_name)
                        << std::endl;
                    return false;
                }
                find = true;
                break;
            }
        }
        if (!find) {
            std::cout << "!ERROR" << std::endl;
            std::cout << "Constraint column id does not exist" << std::endl;
            return false;
        }
        if (constraint.data_type == record::DataTypeName::INT &&
            constraint.constraint_types.size() > 0 &&
            constraint.constraint_types[0] != ConstraintType::NEQ) {
            int_constraint_with_range.push_back(constraint.column_id);
        }
    }

    if (!has_item) return true;

    // get all index
    std::vector<std::pair<int, std::vector<int>>> all_index;
    std::vector<std::string> index_names;
    getAllIndex(current_database_id, table_id, all_index, index_names);

    // get chosen index
    int chosen_index = -1, overlap_num = 0;
    std::vector<int> index_val;
    for (auto& index : all_index) {
        int index_id = index.first;
        auto& index_num = index.second;
        int overlap = 0;  // int_constraint_with_range
        for (auto& index_num_id : index_num) {
            bool found = false;
            for (auto& int_constraint_with_range_id :
                 int_constraint_with_range) {
                if (index_num_id == int_constraint_with_range_id) {
                    overlap++;
                    found = true;
                    break;
                }
            }
            if (!found) break;
        }
        if (overlap > overlap_num) {
            overlap_num = overlap;
            chosen_index = index_id;
            index_val = index_num;
        }
    }
    if (chosen_index == -1) {
        char* table_path = nullptr;
        getTableRecordPath(current_database_id, table_id, &table_path);
        char* record_path = nullptr;
        utils::concatPath(table_path, RECORD_FILE_NAME, &record_path);
        std::vector<record::DataItem> data_items;
        std::vector<record::RecordLocation> record_locations;
        rm->getAllRecordWithConstraint(record_path, result_datas,
                                       record_location_results, constraints);
        delete[] table_path;
        delete[] record_path;
        return true;
    } else {
        index::IndexValue index_range_low, index_range_high;
        for (int i = 0; i < overlap_num; i++) {
            int low_range = INT_MIN, high_range = INT_MAX;
            for (auto& constraint : constraints) {
                if (constraint.column_id == index_val[i]) {
                    for (int j = 0; j < constraint.constraint_types.size();
                         j++) {
                        if (constraint.constraint_types[j] ==
                            ConstraintType::LEQ) {
                            high_range = std::min(
                                high_range, constraint.constraint_values[j]
                                                .value.int_value);

                        } else if (constraint.constraint_types[j] ==
                                   ConstraintType::GEQ) {
                            low_range = std::max(low_range,
                                                 constraint.constraint_values[j]
                                                     .value.int_value);
                        }
                    }
                }
            }
            index_range_low.key.push_back(low_range);
            index_range_high.key.push_back(high_range);
        }
        for (int i = overlap_num; i < index_val.size(); i++) {
            index_range_low.key.push_back(INT_MIN);
            index_range_high.key.push_back(INT_MAX);
        }
        // get index file path
        char* index_file_path = nullptr;
        getIndexRecordPath(current_database_id, table_id, chosen_index,
                           &index_file_path);
        // check index file
        std::vector<index::IndexValue> index_results;
        im->searchIndexInRanges(index_file_path, index_range_low,
                                index_range_high, index_results);
        delete[] index_file_path;

        // get record file path
        char* table_path = nullptr;
        getTableRecordPath(current_database_id, table_id, &table_path);
        char* record_path = nullptr;
        utils::concatPath(table_path, RECORD_FILE_NAME, &record_path);

        // get record
        std::vector<record::RecordLocation> record_locations;
        index::indexValueToRecordLocation(index_results, record_locations);
        std::vector<record::DataItem> data_items;
        rm->getRecords(record_path, record_locations, data_items);
        delete[] table_path;
        delete[] record_path;
        filterConstraints(constraints, data_items, record_locations,
                          result_datas, record_location_results);
        return true;
    }
}

bool SystemManager::dropTable(const char* table_name) {
    // check db id
    if (current_database_id == -1) {
        std::cout << "!ERROR" << std::endl;
        std::cout << "No database selected" << std::endl;
        return false;
    }

    // get table id
    int table_id = getTableId(table_name);
    if (table_id == -1) {
        std::cout << "!ERROR" << std::endl;
        std::cout << "Table " << table_name << " does not exist" << std::endl;
        return false;
    }

    // get table path
    char* table_path = nullptr;
    getTableRecordPath(current_database_id, table_id, &table_path);

    // get dominate record
    char* dominate_path = nullptr;
    utils::concatPath(table_path, DOMINATE_FILE_NAME, &dominate_path);
    std::vector<record::DataItem> data_items;
    std::vector<record::RecordLocation> record_locations;
    rm->getAllRecords(dominate_path, data_items, record_locations);
    delete[] dominate_path;

    if (data_items.size() != 0) {
        std::cout << "!ERROR" << std::endl;
        std::cout << "Table " << table_name << " is referenced by other table"
                  << std::endl;
        delete[] table_path;
        return false;
    }

    std::vector<ForeignKeyInfo> foreign_key_infos;
    getTableForeignKeys(table_id, foreign_key_infos);

    // delete dominates for other tables
    for (auto& foreign_key_info : foreign_key_infos) {
        char* reference_table_path = nullptr;
        getTableRecordPath(current_database_id,
                           foreign_key_info.reference_table_id,
                           &reference_table_path);
        char* reference_table_dominate_path = nullptr;
        utils::concatPath(reference_table_path, DOMINATE_FILE_NAME,
                          &reference_table_dominate_path);
        // get all record in reference_table_dominate_path
        std::vector<record::DataItem> data_items;
        std::vector<record::RecordLocation> record_locations;
        rm->getAllRecords(reference_table_dominate_path, data_items,
                          record_locations);
        // delete record in reference_table_dominate_path
        for (int i = 0; i < data_items.size(); i++) {
            if (data_items[i].data_values[0].value.int_value == table_id) {
                rm->deleteRecord(reference_table_dominate_path,
                                 record_locations[i]);
            }
        }
        delete[] reference_table_path;
        delete[] reference_table_dominate_path;
    }

    im->closeAllCurrentFile();
    rm->closeAllCurrentFile();
    rm->cleanAllCurrentColumnTypes();

    // delete table folder
    assert(fm->deleteFolder(table_path));
    delete[] table_path;

    // delete table record
    char* db_base_path = nullptr;
    getDatabasePath(current_database_id, &db_base_path);
    char* all_table_path = nullptr;
    utils::concatPath(db_base_path, ALL_TABLE_FILE_NAME, &all_table_path);

    rm->getAllRecords(all_table_path, data_items, record_locations);
    for (int i = 0; i < data_items.size(); i++) {
        if (data_items[i].data_id == table_id) {
            rm->deleteRecord(all_table_path, record_locations[i]);
            break;
        }
    }

    delete[] db_base_path;
    delete[] all_table_path;

    return true;
}

bool SystemManager::getTableColumnTypes(
    int table_id, std::vector<record::ColumnType>& column_types) {
    char* table_path = nullptr;
    getTableRecordPath(current_database_id, table_id, &table_path);
    char* record_path = nullptr;
    utils::concatPath(table_path, RECORD_FILE_NAME, &record_path);
    rm->getColumnTypes(record_path, column_types);
    delete[] table_path;
    delete[] record_path;
    return true;
}

bool SystemManager::getTablePrimaryKeys(int table_id,
                                        std::set<int>& primary_keys) {
    char* table_path = nullptr;
    getTableRecordPath(current_database_id, table_id, &table_path);
    char* primary_key_path = nullptr;
    utils::concatPath(table_path, PRIMARY_KEY_FILE_NAME, &primary_key_path);
    std::vector<record::DataItem> data_items;
    std::vector<record::RecordLocation> record_locations;
    rm->getAllRecords(primary_key_path, data_items, record_locations);
    primary_keys.clear();
    for (auto& data_item : data_items) {
        primary_keys.insert(data_item.data_values[0].value.int_value);
    }
    delete[] table_path;
    delete[] primary_key_path;
    return true;
}

bool SystemManager::getTableDominate(int table_id,
                                     std::vector<int>& dominate_table_ids) {
    char* table_path = nullptr;
    getTableRecordPath(current_database_id, table_id, &table_path);
    char* dominate_path = nullptr;
    utils::concatPath(table_path, DOMINATE_FILE_NAME, &dominate_path);
    std::vector<record::DataItem> data_items;
    std::vector<record::RecordLocation> record_locations;
    rm->getAllRecords(dominate_path, data_items, record_locations);
    dominate_table_ids.clear();
    for (auto& data_item : data_items) {
        dominate_table_ids.push_back(data_item.data_values[0].value.int_value);
    }
    delete[] table_path;
    delete[] dominate_path;
    return true;
}

bool SystemManager::getTableForeignKeys(
    int table_id, std::vector<ForeignKeyInfo>& foreign_keys) {
    char* table_path = nullptr;
    getTableRecordPath(current_database_id, table_id, &table_path);
    char* foreign_key_path = nullptr;
    utils::concatPath(table_path, FOREIGN_KEY_FILE_NAME, &foreign_key_path);
    std::vector<record::DataItem> data_items;
    std::vector<record::RecordLocation> record_locations;
    rm->getAllRecords(foreign_key_path, data_items, record_locations);
    foreign_keys.clear();
    for (auto& data_item : data_items) {
        std::vector<int> foreign_key_column_ids;
        for (int i = 0; i < FOREIGN_KEY_MAX_NUM; i++) {
            if (data_item.data_values[i + 1].is_null == false) {
                foreign_key_column_ids.push_back(
                    data_item.data_values[i + 1].value.int_value);
            }
        }
        std::vector<int> reference_column_ids;
        for (int i = 0; i < FOREIGN_KEY_MAX_NUM; i++) {
            if (data_item.data_values[i + 1 + FOREIGN_KEY_MAX_NUM].is_null ==
                false) {
                reference_column_ids.push_back(
                    data_item.data_values[i + 1 + FOREIGN_KEY_MAX_NUM]
                        .value.int_value);
            }
        }
        foreign_keys.push_back(ForeignKeyInfo(
            foreign_key_column_ids, data_item.data_values[0].value.int_value,
            reference_column_ids,
            data_item.data_values[FOREIGN_KEY_MAX_NUM * 2 + 1]
                .value.char_value));
    }
    delete[] table_path;
    delete[] foreign_key_path;
    return true;
}

void SystemManager::getAllDatabase(
    std::vector<record::DataItem>& database_names,
    std::vector<record::ColumnType>& column_types) {
    database_names.clear();
    column_types.clear();

    std::vector<record::RecordLocation> record_locations;
    rm->getAllRecords(DATABASE_GLOBAL_RECORD_PATH, database_names,
                      record_locations);

    rm->getColumnTypes(DATABASE_GLOBAL_RECORD_PATH, column_types);
}

bool SystemManager::getAllTable(std::vector<record::DataItem>& table_names,
                                std::vector<record::ColumnType>& column_types) {
    table_names.clear();
    column_types.clear();

    // check db id
    if (current_database_id == -1) {
        std::cout << "!ERROR" << std::endl;
        std::cout << "No database selected" << std::endl;
        return false;
    }

    // get all table path
    char* db_base_path = nullptr;
    getDatabasePath(current_database_id, &db_base_path);
    char* all_table_path = nullptr;
    utils::concatPath(db_base_path, ALL_TABLE_FILE_NAME, &all_table_path);

    delete[] db_base_path;

    // get all table record
    table_names.clear();
    std::vector<record::RecordLocation> record_locations;
    rm->getAllRecords(all_table_path, table_names, record_locations);
    rm->getColumnTypes(all_table_path, column_types);

    delete[] all_table_path;

    return true;
}

bool SystemManager::describeTable(
    const char* table_name, std::vector<record::DataItem>& table_info,
    std::vector<record::ColumnType>& column_types,
    std::vector<std::string>& primary_keys,
    std::vector<ForeignKeyInputInfo>& foreign_keys,
    std::vector<std::vector<std::string>>& index,
    std::vector<std::vector<std::string>>& unique) {
    table_info.clear();
    column_types.clear();

    // check db id
    if (current_database_id == -1) {
        std::cout << "!ERROR" << std::endl;
        std::cout << "No database selected" << std::endl;
        return false;
    }

    // get table id
    int table_id = getTableId(table_name);
    if (table_id == -1) {
        std::cout << "!ERROR" << std::endl;
        std::cout << "Table " << table_name << " does not exist" << std::endl;
        return false;
    }

    // get table path
    char* table_path = nullptr;
    getTableRecordPath(current_database_id, table_id, &table_path);

    // get record path
    char* record_path = nullptr;
    utils::concatPath(table_path, RECORD_FILE_NAME, &record_path);

    // get record
    std::vector<record::ColumnType> all_column_types;
    rm->getColumnTypes(record_path, all_column_types);

    delete[] record_path;
    delete[] table_path;

    // set column_types
    column_types.push_back(record::ColumnType(record::DataTypeName::VARCHAR,
                                              255, 0, true, true,
                                              record::DefaultValue(), "Field"));
    column_types.push_back(record::ColumnType(record::DataTypeName::VARCHAR,
                                              255, 0, true, false,
                                              record::DefaultValue(), "Type"));
    column_types.push_back(record::ColumnType(record::DataTypeName::VARCHAR,
                                              255, 0, true, false,
                                              record::DefaultValue(), "Null"));
    column_types.push_back(
        record::ColumnType(record::DataTypeName::VARCHAR, 255, 0, false, false,
                           record::DefaultValue(), "Default"));
    for (int i = 0; i < 4; i++) column_types[i].column_id = i;

    // set column_values
    for (auto& all_column_type : all_column_types) {
        record::DataItem data_item;
        data_item.data_values.push_back(record::DataValue(
            record::DataTypeName::VARCHAR, false, all_column_type.column_name));
        data_item.column_ids.push_back(column_types[0].column_id);
        data_item.data_values.push_back(
            record::DataValue(record::DataTypeName::VARCHAR, false,
                              all_column_type.typeAsString()));
        data_item.column_ids.push_back(column_types[1].column_id);
        data_item.data_values.push_back(
            record::DataValue(record::DataTypeName::VARCHAR, false,
                              all_column_type.require_not_null ? "NO" : "YES"));
        data_item.column_ids.push_back(column_types[2].column_id);
        data_item.data_values.push_back(record::DataValue(
            record::DataTypeName::VARCHAR, false,
            all_column_type.default_value.has_default_value
                ? all_column_type.default_value.value.toString()
                : "NULL"));
        data_item.column_ids.push_back(column_types[3].column_id);
        table_info.push_back(data_item);
    }

    // set primary_keys
    std::set<int> primary_key_ids;
    getTablePrimaryKeys(table_id, primary_key_ids);
    for (auto& primary_key_id : primary_key_ids) {
        primary_keys.push_back(all_column_types[primary_key_id].column_name);
    }

    // set foreign_keys
    std::vector<ForeignKeyInfo> foreign_key_infos;
    getTableForeignKeys(table_id, foreign_key_infos);

    std::vector<ForeignKeyInputInfo> foreign_key_input_infos;
    for (auto& foreign_key_info : foreign_key_infos) {
        // get foreign key column names
        std::vector<std::string> foreign_key_column_names;
        for (auto& foreign_key_column_id :
             foreign_key_info.foreign_key_column_ids) {
            foreign_key_column_names.push_back(
                all_column_types[foreign_key_column_id].column_name);
        }

        // get reference table name
        char* db_base_path = nullptr;
        getDatabasePath(current_database_id, &db_base_path);
        char* all_table_path = nullptr;
        utils::concatPath(db_base_path, ALL_TABLE_FILE_NAME, &all_table_path);

        std::vector<record::DataItem> data_items;
        std::vector<record::RecordLocation> record_locations;
        rm->getAllRecords(all_table_path, data_items, record_locations);

        delete[] db_base_path;
        delete[] all_table_path;

        std::string reference_table_name;
        for (auto& data_item : data_items) {
            if (data_item.data_id == foreign_key_info.reference_table_id) {
                reference_table_name =
                    data_item.data_values[0].value.char_value;
                break;
            }
        }

        // get record of the reference table
        char* reference_table_path = nullptr;
        getTableRecordPath(current_database_id,
                           foreign_key_info.reference_table_id,
                           &reference_table_path);
        char* reference_table_record_path = nullptr;
        utils::concatPath(reference_table_path, RECORD_FILE_NAME,
                          &reference_table_record_path);

        std::vector<record::ColumnType> reference_table_column_types;
        rm->getColumnTypes(reference_table_record_path,
                           reference_table_column_types);

        delete[] reference_table_path;
        delete[] reference_table_record_path;

        // get reference column names
        std::vector<std::string> reference_column_names;
        for (auto& reference_column_id :
             foreign_key_info.reference_column_ids) {
            reference_column_names.push_back(
                reference_table_column_types[reference_column_id].column_name);
        }

        foreign_keys.push_back(ForeignKeyInputInfo(foreign_key_column_names,
                                                   reference_table_name,
                                                   reference_column_names));
    }

    // get all index and unique
    std::vector<std::pair<int, std::vector<int>>> all_index;
    std::vector<std::string> index_names;
    getAllIndex(current_database_id, table_id, all_index, index_names);

    index.clear();
    unique.clear();
    for (int i = 0; i < all_index.size(); i++) {
        auto& single_index = all_index[i];
        auto& index_name = index_names[i];
        if (index_name == "") continue;

        if (index_name.find(UNIQUE_SUFFIX) != std::string::npos) {
            std::vector<std::string> unique_column_names;
            for (auto& index_column_id : single_index.second) {
                for (auto& column_type : all_column_types) {
                    if (column_type.column_id == index_column_id) {
                        unique_column_names.push_back(column_type.column_name);
                        break;
                    }
                }
            }
            unique.push_back(unique_column_names);
            continue;
        } else {
            std::vector<std::string> index_column_names;
            for (auto& index_column_id : single_index.second) {
                for (auto& column_type : all_column_types) {
                    if (column_type.column_id == index_column_id) {
                        index_column_names.push_back(column_type.column_name);
                        break;
                    }
                }
            }
            index.push_back(index_column_names);
        }
    }
    return true;
}

bool SystemManager::loadTableFromFile(const char* table_name,
                                      const char* file_path,
                                      const char* delimeter) {
    if (current_database_id == -1) {
        std::cout << "!ERROR" << std::endl;
        std::cout << "No database selected" << std::endl;
        return false;
    }

    // get table id
    int table_id = getTableId(table_name);
    if (table_id == -1) {
        std::cout << "!ERROR" << std::endl;
        std::cout << "Table " << table_name << " does not exist" << std::endl;
        return false;
    }

    // get record path
    char* table_path = nullptr;
    getTableRecordPath(current_database_id, table_id, &table_path);
    char* record_path = nullptr;
    utils::concatPath(table_path, RECORD_FILE_NAME, &record_path);

    int data_item_per_page =
        rm->insertRecordsToEmptyRecord(record_path, file_path, delimeter, true);
    if (data_item_per_page == -1) {
        return false;
    }
    std::vector<record::ColumnType> column_types;
    rm->getColumnTypes(record_path, column_types);

    std::vector<std::pair<int, std::vector<int>>> all_index;
    std::vector<std::string> index_names;
    getAllIndex(current_database_id, table_id, all_index, index_names);

    std::ifstream csv_file(file_path);
    if (!csv_file.is_open()) {
        std::cerr << "Failed to open file " << file_path << std::endl;
        return false;
    }

    std::string line;
    int page_id = 1, slot_id = 0;
    while (getline(csv_file, line)) {
        if (slot_id == data_item_per_page) {
            page_id++;
            slot_id = 0;
        }
        std::vector<std::string> csv_row;
        std::string item;
        for (int i = 0; i < line.length(); i++) {
            if (line[i] == delimeter[0]) {
                csv_row.push_back(item);
                item = "";
            } else {
                item += line[i];
            }
        }
        csv_row.push_back(item);

        record::DataItem data_item;
        int csv_row_idx = 0;
        for (auto& column : column_types) {
            data_item.column_ids.push_back(column.column_id);
            record::DateValue date_value;
            std::istringstream iss;
            double floatValue;
            if (column.type_name == record::DataTypeName::FLOAT) {
                std::istringstream iss(csv_row[csv_row_idx++]);
                iss >> floatValue;
            }
            switch (column.type_name) {
                case record::DataTypeName::INT:
                    data_item.data_values.push_back(
                        record::DataValue(record::DataTypeName::INT, false,
                                          std::stoi(csv_row[csv_row_idx++])));
                    break;
                case record::DataTypeName::FLOAT:
                    data_item.data_values.push_back(record::DataValue(
                        record::DataTypeName::FLOAT, false, floatValue));
                    break;
                case record::DataTypeName::VARCHAR:
                    data_item.data_values.push_back(
                        record::DataValue(record::DataTypeName::VARCHAR, false,
                                          csv_row[csv_row_idx++]));
                    break;
                case record::DataTypeName::DATE:
                    date_value.year = std::stoi(csv_row[csv_row_idx].substr(
                        0, csv_row[csv_row_idx].find('-')));
                    date_value.month = std::stoi(csv_row[csv_row_idx].substr(
                        csv_row[csv_row_idx].find('-') + 1,
                        csv_row[csv_row_idx].rfind('-') -
                            csv_row[csv_row_idx].find('-') - 1));
                    date_value.day = std::stoi(csv_row[csv_row_idx].substr(
                        csv_row[csv_row_idx].rfind('-') + 1));
                    csv_row_idx++;
                    data_item.data_values.push_back(record::DataValue(
                        record::DataTypeName::DATE, false, date_value));
                    break;
            }
        }

        for (auto& index : all_index) {
            char* index_file_path = nullptr;
            getIndexRecordPath(current_database_id, table_id, index.first,
                               &index_file_path);
            index::IndexValue index_value(page_id, slot_id, 0);
            for (auto& index_id : index.second) {
                if (data_item.data_values[index_id].is_null == false)
                    index_value.key.push_back(
                        data_item.data_values[index_id].value.int_value);
                else
                    index_value.key.push_back(INT_MIN);
            }
            assert(im->insertIndex(index_file_path, index_value));
            delete[] index_file_path;
        }
        slot_id++;
    }

    delete[] table_path;
    delete[] record_path;
    return true;
}

void SystemManager::getAllIndex(
    int database_id, int table_id,
    std::vector<std::pair<int, std::vector<int>>>& index_ids,
    std::vector<std::string>& index_names) {
    index_ids.clear();
    // get index info path
    char* table_path = nullptr;
    getTableRecordPath(database_id, table_id, &table_path);
    char* index_info_path = nullptr;
    utils::concatPath(table_path, INDEX_INFO_FILE_NAME, &index_info_path);

    // read all from index info
    std::vector<record::DataItem> data_items;
    std::vector<record::RecordLocation> record_locations;
    rm->getAllRecords(index_info_path, data_items, record_locations);

    for (auto& data_item : data_items) {
        std::vector<int> index_id;
        for (int i = 0; i < INDEX_KEY_MAX_NUM; i++) {
            if (data_item.data_values[i].is_null == false) {
                index_id.push_back(data_item.data_values[i].value.int_value);
            }
        }
        index_ids.push_back(std::make_pair(data_item.data_id, index_id));
        if (data_item.data_values[INDEX_KEY_MAX_NUM].is_null == false)
            index_names.push_back(
                data_item.data_values[INDEX_KEY_MAX_NUM].value.char_value);
        else
            index_names.push_back("");
    }
    delete[] table_path;
    delete[] index_info_path;
}

bool SystemManager::useDatabase(const char* database_name) {
    // check if database exists
    int database_id = getDatabaseId(database_name);
    if (database_id == -1) {
        std::cout << "!ERROR" << std::endl;
        std::cout << "Database " << database_name << " does not exist"
                  << std::endl;
        return false;
    }

    current_database_id = database_id;
    return true;
}

int SystemManager::getTableId(const char* table_name) {
    // check db id
    if (current_database_id == -1) return -1;

    // get all table path
    char* db_base_path = nullptr;
    getDatabasePath(current_database_id, &db_base_path);
    char* all_table_path = nullptr;
    utils::concatPath(db_base_path, ALL_TABLE_FILE_NAME, &all_table_path);

    // get all table record
    std::vector<record::DataItem> data_items;
    std::vector<record::RecordLocation> record_locations;
    rm->getAllRecords(all_table_path, data_items, record_locations);

    delete[] db_base_path;
    delete[] all_table_path;

    // search for the table name
    for (int i = 0; i < data_items.size(); i++) {
        if (strcmp(data_items[i].data_values[0].value.char_value.c_str(),
                   table_name) == 0) {
            return data_items[i].data_id;
        }
    }
    return -1;
}

void SystemManager::getDatabasePath(int database_id, char** result) {
    char* folder_name = nullptr;
    char* database_id_str = nullptr;
    utils::int2str(database_id, &database_id_str);
    utils::concatStr(DATABSE_FOLDER_PREFIX, database_id_str, &folder_name);
    utils::concatPath(DATABASE_BASE_PATH, folder_name, result);
    delete[] folder_name;
    delete[] database_id_str;
}

void SystemManager::getTableRecordPath(int database_id, int table_id,
                                       char** result) {
    char* database_path = nullptr;
    getDatabasePath(database_id, &database_path);
    char* table_id_str = nullptr;
    utils::int2str(table_id, &table_id_str);
    char* folder_name = nullptr;
    utils::concatStr(TABLE_FOLDER_PREFIX, table_id_str, &folder_name);
    utils::concatPath(database_path, folder_name, result);
    delete[] database_path;
    delete[] table_id_str;
    delete[] folder_name;
}

void SystemManager::getIndexRecordPath(int database_id, int table_id,
                                       int index_id, char** result) {
    char* table_path = nullptr;
    getTableRecordPath(database_id, table_id, &table_path);
    char* index_id_str = nullptr;
    utils::int2str(index_id, &index_id_str);
    char* file_name = nullptr;
    utils::concatStr(INDEX_FILE_PREFIX, index_id_str, &file_name);
    char* folder_path = nullptr;
    utils::concatPath(table_path, INDEX_FOLDER_NAME, &folder_path);
    utils::concatPath(folder_path, file_name, result);
    delete[] table_path;
    delete[] index_id_str;
    delete[] file_name;
    delete[] folder_path;
}

int SystemManager::getCurrentDatabaseId() const { return current_database_id; }

}  // namespace system
}  // namespace dbs