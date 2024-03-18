#include "record/RecordManager.hpp"

namespace dbs {
namespace record {

RecordManager::RecordManager(fs::FileManager* fm_, fs::BufPageManager* bpm_) {
    fm = fm_;
    bpm = bpm_;
    current_opening_file_paths.clear();
    current_opening_file_ids.clear();
    current_column_types_file_paths.clear();
    current_column_types.clear();
}

RecordManager::~RecordManager() {
    cleanAllCurrentColumnTypes();
    closeAllCurrentFile();
    fm = nullptr;
    bpm = nullptr;
}

void RecordManager::closeAllCurrentFile() {
    bpm->close();
    for (auto& file_path : current_opening_file_paths) {
        delete[] file_path;
        file_path = nullptr;
    }
    current_opening_file_paths.clear();
    for (auto& file_id : current_opening_file_ids) {
        fm->closeFile(file_id);
    }
    current_opening_file_ids.clear();
}

void RecordManager::closeFileIfExist(const char* file_path) {
    int current_opening_file_num = current_opening_file_paths.size();
    for (int i = 0; i < current_opening_file_num; i++) {
        if (strcmp(current_opening_file_paths[i], file_path) == 0) {
            int file_id = current_opening_file_ids[i];
            bpm->close();
            fm->closeFile(file_id);
            delete[] current_opening_file_paths[i];
            current_opening_file_paths[i] = nullptr;
            current_opening_file_paths.erase(
                current_opening_file_paths.begin() + i);
            current_opening_file_ids.erase(current_opening_file_ids.begin() +
                                           i);
            return;
        }
    }
}

void RecordManager::closeFirstFile() {
    if (current_opening_file_ids.size() == 0) return;
    bpm->close();
    int file_id = current_opening_file_ids.front();
    current_opening_file_ids.erase(current_opening_file_ids.begin());
    fm->closeFile(file_id);
    delete[] current_opening_file_paths.front();
    current_opening_file_paths.front() = nullptr;
    current_opening_file_paths.erase(current_opening_file_paths.begin());
}

void RecordManager::cleanAllCurrentColumnTypes() {
    for (auto& file_path : current_column_types_file_paths) {
        delete[] file_path;
        file_path = nullptr;
    }
    current_column_types_file_paths.clear();
    current_column_types.clear();
}

void RecordManager::cleanFirstColumnTypes() {
    if (current_column_types_file_paths.size() == 0) return;
    delete[] current_column_types_file_paths.front();
    current_column_types_file_paths.erase(
        current_column_types_file_paths.begin());
    current_column_types.erase(current_column_types.begin());
}

void RecordManager::cleanColumnTypesIfExist(const char* file_path) {
    int current_column_types_num = current_column_types_file_paths.size();
    for (int i = 0; i < current_column_types_num; i++) {
        if (strcmp(current_column_types_file_paths[i], file_path) == 0) {
            delete[] current_column_types_file_paths[i];
            current_column_types_file_paths[i] = nullptr;
            current_column_types_file_paths.erase(
                current_column_types_file_paths.begin() + i);
            current_column_types.erase(current_column_types.begin() + i);
            return;
        }
    }
}

int RecordManager::openFile(const char* file_path) {
    int current_opening_file_num = current_opening_file_paths.size();
    for (int i = 0; i < current_opening_file_num; i++) {
        if (strcmp(current_opening_file_paths[i], file_path) == 0) {
            return current_opening_file_ids[i];
        }
    }
    if (current_opening_file_num == opening_cache_capacity) {
        closeFirstFile();
    }
    current_opening_file_paths.push_back(new char[strlen(file_path) + 1]);
    strcpy(current_opening_file_paths.back(), file_path);
    int file_id = fm->openFile(file_path);
    current_opening_file_ids.push_back(file_id);
    return file_id;
}

void RecordManager::initializeRecordFile(
    const char* file_path, const std::vector<ColumnType>& column_types) {
    closeFileIfExist(file_path);
    cleanColumnTypesIfExist(file_path);
    if (fm->existFile(file_path)) {
        assert(fm->deleteFile(file_path));
    }
    assert(fm->createFile(file_path));
    int file_id = openFile(file_path);
    assert(file_id != -1);
    int index;
    BufType b;
    b = bpm->getPage(file_id, 0, index);
    for (int i = 0; i < RECORD_META_DATA_HEAD / BYTE_PER_BUF; i++) b[i] = 0;
    b[7] = (column_types.size() + BIT_PER_BUF - 1) / BIT_PER_BUF + 1;
    for (auto& column : column_types) {
        utils::setBitFromBuf(b, b[4], true);
        unsigned int column_id = b[4]++;

        int start_buf_position =
            (column_id * RECORD_META_DATA_LENGTH + RECORD_META_DATA_HEAD) /
            BYTE_PER_BUF;
        int column_name_length = column.column_name.size();
        b[start_buf_position] = column_id;
        utils::set1Byte(b[start_buf_position + 1], 0, column.type_name);
        utils::set1Byte(b[start_buf_position + 1], 1, column_name_length);
        if (column.type_name == VARCHAR) {
            utils::set2Bytes(b[start_buf_position + 1], 1,
                             column.varchar_length);
            int varchar_space = column.varchar_length * 2;
            if (varchar_space % BYTE_PER_BUF == 0) {
                varchar_space += 2;
            }
            b[start_buf_position + 2] = varchar_space;
        }
        start_buf_position += 3;
        int buf_position = start_buf_position, byte_position = 0;
        for (int i = 0; i < column_name_length; i++) {
            if (byte_position == BYTE_PER_BUF) {
                buf_position++;
                byte_position = 0;
            }
            utils::set1Byte(b[buf_position], byte_position++,
                            column.column_name[i]);
        }
        start_buf_position += 8;
        auto& default_value = column.default_value.value;
        int default_value_varchar_len = 0;
        utils::setBitFromNum(b[start_buf_position], 0, column.require_not_null);
        utils::setBitFromNum(b[start_buf_position], 1,
                             column.default_value.has_default_value);
        utils::setBitFromNum(b[start_buf_position], 2, default_value.is_null);
        utils::setBitFromNum(b[start_buf_position], 3, column.require_unique);
        if (column.default_value.has_default_value && !default_value.is_null &&
            column.type_name == VARCHAR) {
            default_value_varchar_len = default_value.value.char_value.size();
            utils::set2Bytes(b[start_buf_position], 1,
                             default_value_varchar_len);
        }
        start_buf_position += 1;
        if (column.default_value.has_default_value && !default_value.is_null) {
            switch (column.type_name) {
                case INT:
                    b[start_buf_position] =
                        utils::int2bit32(default_value.value.int_value);
                    break;
                case FLOAT:
                    utils::float2bit32(default_value.value.float_value,
                                       b[start_buf_position],
                                       b[start_buf_position + 1]);
                    break;
                case VARCHAR:
                    buf_position = start_buf_position, byte_position = 0;
                    for (int i = 0; i < default_value_varchar_len; i++) {
                        if (byte_position == BYTE_PER_BUF) {
                            buf_position++;
                            byte_position = 0;
                        }
                        utils::set1Byte(b[buf_position], byte_position++,
                                        default_value.value.char_value[i]);
                    }
                    break;
                case DATE:
                    utils::set2Bytes(b[start_buf_position], 0,
                                     default_value.value.date_value.year);
                    utils::set1Byte(b[start_buf_position], 2,
                                    default_value.value.date_value.month);
                    utils::set1Byte(b[start_buf_position], 3,
                                    default_value.value.date_value.day);
                    break;
            }
        }
    }
    bpm->markDirty(index);
}

void RecordManager::updateColumnUnique(const char* file_path, int column_id,
                                       bool unique) {
    cleanAllCurrentColumnTypes();
    int file_id = openFile(file_path);
    assert(file_id != -1);
    int index;
    BufType b;
    b = bpm->getPage(file_id, 0, index);
    int column_num = b[4];
    bpm->access(index);
    if (column_id >= column_num) return;
    int start_buf_position =
        (column_id * RECORD_META_DATA_LENGTH + RECORD_META_DATA_HEAD) /
        BYTE_PER_BUF;
    utils::setBitFromNum(b[start_buf_position + 11], 3, unique);
    bpm->markDirty(index);
}

void RecordManager::getColumnTypes(const char* file_path,
                                   std::vector<ColumnType>& column_types) {
    column_types.clear();
    int current_column_types_num = current_column_types_file_paths.size();
    for (int i = 0; i < current_column_types_num; i++) {
        if (strcmp(current_column_types_file_paths[i], file_path) == 0) {
            for (auto& column_type : current_column_types[i]) {
                column_types.push_back(column_type);
            }
            return;
        }
    }
    if (current_column_types_num == column_types_cache_capacity) {
        cleanFirstColumnTypes();
    }
    current_column_types_file_paths.push_back(new char[strlen(file_path) + 1]);
    strcpy(current_column_types_file_paths.back(), file_path);
    int file_id = openFile(file_path);
    assert(file_id != -1);
    int index;
    BufType b;
    b = bpm->getPage(file_id, 0, index);
    for (int i = 0; i < MAX_COLUMN_NUM; i++) {
        if (!utils::getBitFromBuf(b, i)) continue;
        int start_buf_position =
            (i * RECORD_META_DATA_LENGTH + RECORD_META_DATA_HEAD) /
            BYTE_PER_BUF;
        ColumnType column_type;
        column_type.column_id = b[start_buf_position];
        column_type.type_name =
            (DataTypeName)utils::get1Byte(b[start_buf_position + 1], 0);
        int column_name_length = utils::get1Byte(b[start_buf_position + 1], 1);
        column_type.varchar_length =
            utils::get2Bytes(b[start_buf_position + 1], 1);
        column_type.varchar_space = b[start_buf_position + 2];
        start_buf_position += 3;
        int buf_position = start_buf_position, byte_position = 0;
        column_type.column_name = "";
        for (int i = 0; i < column_name_length; i++) {
            if (byte_position == BYTE_PER_BUF) {
                buf_position++;
                byte_position = 0;
            }
            column_type.column_name.push_back(
                utils::get1Byte(b[buf_position], byte_position++));
        }
        start_buf_position += 8;
        column_type.require_not_null =
            utils::getBitFromNum(b[start_buf_position], 0);
        column_type.default_value.has_default_value =
            utils::getBitFromNum(b[start_buf_position], 1);
        column_type.default_value.value.is_null =
            utils::getBitFromNum(b[start_buf_position], 2);
        column_type.require_unique =
            utils::getBitFromNum(b[start_buf_position], 3);
        int default_value_varchar_len =
            utils::get2Bytes(b[start_buf_position], 1);
        column_type.default_value.value.type_name = column_type.type_name;
        if (column_type.default_value.has_default_value &&
            !column_type.default_value.value.is_null) {
            start_buf_position++;
            switch (column_type.type_name) {
                case INT:
                    column_type.default_value.value.value.int_value =
                        utils::bit322int(b[start_buf_position]);
                    break;
                case FLOAT:
                    column_type.default_value.value.value.float_value =
                        utils::bit322float(b[start_buf_position],
                                           b[start_buf_position + 1]);
                    break;
                case VARCHAR:
                    buf_position = start_buf_position, byte_position = 0;
                    column_type.default_value.value.value.char_value = "";
                    for (int i = 0; i < default_value_varchar_len; i++) {
                        if (byte_position == BYTE_PER_BUF) {
                            buf_position++;
                            byte_position = 0;
                        }
                        column_type.default_value.value.value.char_value
                            .push_back(utils::get1Byte(b[buf_position],
                                                       byte_position++));
                    }
                    break;
                case DATE:
                    column_type.default_value.value.value.date_value.year =
                        utils::get2Bytes(b[start_buf_position], 0);

                    column_type.default_value.value.value.date_value.month =
                        utils::get1Byte(b[start_buf_position], 2);
                    //
                    column_type.default_value.value.value.date_value.day =
                        utils::get1Byte(b[start_buf_position], 3);
                    break;
            }
        }
        column_types.push_back(column_type);
    }
    bpm->access(index);
    current_column_types.push_back(std::vector<ColumnType>());
    for (auto& column_type : column_types) {
        current_column_types.back().push_back(column_type);
    }
}

int RecordManager::insertRecordsToEmptyRecord(const char* file_path,
                                              const char* csv_path,
                                              const char* delimeter,
                                              bool output) {
    std::ifstream csv_file(csv_path);
    if (!csv_file.is_open()) {
        std::cerr << "Failed to open file " << csv_path << std::endl;
        return -1;
    }

    int file_id = openFile(file_path);
    assert(file_id != -1);

    std::vector<ColumnType> column_types;
    getColumnTypes(file_path, column_types);

    BufType b;
    int index;
    b = bpm->getPage(file_id, 0, index);
    int null_bitmap_buf_size = b[7];
    bpm->markDirty(index);
    int data_item_length =
        dataItemLength(column_types, null_bitmap_buf_size * BYTE_PER_BUF);
    int data_item_per_page = std::min(
        (PAGE_SIZE - RECORD_PAGE_HEADER) / data_item_length, MAX_ITEM_PER_PAGE);

    int page_id = 1, slot_id = 0, record_id = 0;
    b = bpm->getPage(file_id, page_id, index);
    memset(b, 0, BUF_PER_PAGE * BYTE_PER_BUF);
    bpm->markDirty(index);

    std::string line;
    while (getline(csv_file, line)) {
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
        if (csv_row.size() != column_types.size()) {
            std::cout << "!ERROR" << std::endl;
            std::cout << "CSV file format error" << std::endl;
            return -1;
        }
        for (auto& column : column_types) {
            data_item.column_ids.push_back(column.column_id);
            record::DateValue date_value;
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

        if (slot_id == data_item_per_page) {
            page_id++;
            slot_id = 0;
            b = bpm->getPage(file_id, page_id, index);
            memset(b, 0, BUF_PER_PAGE * BYTE_PER_BUF);
            bpm->markDirty(index);
        }
        setSlotItem(b, slot_id, data_item_length, null_bitmap_buf_size,
                    record_id++, data_item, column_types);
        slot_id++;
    }
    csv_file.close();
    if (output) {
        std::cout << "rows" << std::endl;
        std::cout << record_id << std::endl;
    }

    b = bpm->getPage(file_id, 0, index);
    b[5] = page_id;
    b[6] = record_id;
    bpm->markDirty(index);
    return data_item_per_page;
}

RecordLocation RecordManager::insertRecord(const char* file_path,
                                           DataItem data_item) {
    int file_id = openFile(file_path);
    assert(file_id != -1);

    std::vector<ColumnType> column_types;
    getColumnTypes(file_path, column_types);
    sortDataItem(column_types, data_item);
    if (!exactMatch(column_types, data_item)) {
        return RecordLocation{-1, -1};
    }

    BufType b;
    int index;
    b = bpm->getPage(file_id, 0, index);
    int page_num = b[5];
    int record_id = b[6];
    int null_bitmap_buf_size = b[7];
    bpm->access(index);
    int data_item_length =
        dataItemLength(column_types, null_bitmap_buf_size * BYTE_PER_BUF);
    int data_item_per_page = std::min(
        (PAGE_SIZE - RECORD_PAGE_HEADER) / data_item_length, MAX_ITEM_PER_PAGE);

    for (int page_id = 1; page_id <= page_num; page_id++) {
        b = bpm->getPage(file_id, page_id, index);
        for (int slot_id = 0; slot_id < data_item_per_page; slot_id++) {
            if (!utils::getBitFromBuf(b, slot_id)) {
                setSlotItem(b, slot_id, data_item_length, null_bitmap_buf_size,
                            record_id, data_item, column_types);
                bpm->markDirty(index);
                b = bpm->getPage(file_id, 0, index);
                b[6]++;
                bpm->markDirty(index);
                return RecordLocation{page_id, slot_id};
            }
            bpm->access(index);
        }
    }
    b = bpm->getPage(file_id, page_num + 1, index);
    for (int i = 0; i < RECORD_PAGE_HEADER / BYTE_PER_BUF; i++) b[i] = 0;
    setSlotItem(b, 0, data_item_length, null_bitmap_buf_size, record_id,
                data_item, column_types);
    bpm->markDirty(index);
    b = bpm->getPage(file_id, 0, index);
    b[5]++;
    b[6]++;
    bpm->markDirty(index);
    return RecordLocation{page_num + 1, 0};
}

bool RecordManager::deleteRecord(const char* file_path,
                                 const RecordLocation& record_location) {
    int file_id = openFile(file_path);
    assert(file_id != -1);
    int index;
    BufType b;
    b = bpm->getPage(file_id, record_location.page_id, index);
    utils::setBitFromBuf(b, record_location.slot_id, false);
    bpm->markDirty(index);
    return true;
}

bool RecordManager::getRecord(const char* file_path,
                              const RecordLocation& record_location,
                              DataItem& data_item) {
    int file_id = openFile(file_path);
    assert(file_id != -1);
    std::vector<ColumnType> column_types;
    getColumnTypes(file_path, column_types);
    int index;
    BufType b;
    b = bpm->getPage(file_id, 0, index);
    int null_bitmap_buf_size = b[7];

    bpm->access(index);

    b = bpm->getPage(file_id, record_location.page_id, index);
    bpm->access(index);
    if (!utils::getBitFromBuf(b, record_location.slot_id)) return false;
    data_item = getSlotItem(
        b, record_location.slot_id,
        dataItemLength(column_types, null_bitmap_buf_size * BYTE_PER_BUF),
        null_bitmap_buf_size, column_types);
    return true;
}

bool RecordManager::getRecords(
    const char* file_path, const std::vector<RecordLocation>& record_locations,
    std::vector<DataItem>& data_items) {
    data_items.clear();
    int file_id = openFile(file_path);
    assert(file_id != -1);
    int index;
    BufType b;

    b = bpm->getPage(file_id, 0, index);
    int null_bitmap_buf_size = b[7];

    std::vector<ColumnType> column_types;
    getColumnTypes(file_path, column_types);

    for (auto& record_location : record_locations) {
        b = bpm->getPage(file_id, record_location.page_id, index);
        bpm->access(index);
        if (!utils::getBitFromBuf(b, record_location.slot_id)) return false;
        data_items.push_back(getSlotItem(
            b, record_location.slot_id,
            dataItemLength(column_types, null_bitmap_buf_size * BYTE_PER_BUF),
            null_bitmap_buf_size, column_types));
    }
    return true;
}

bool RecordManager::updateRecord(const char* file_path,
                                 const RecordLocation& record_location,
                                 const DataItem& data_item) {
    DataItem original_data_item, original_data_item_save;
    if (!getRecord(file_path, record_location, original_data_item)) {
        return false;
    }
    original_data_item_save = original_data_item;
    size_t update_column_num = data_item.data_values.size();
    for (size_t i = 0, j; i < update_column_num; i++) {
        auto& update_column_id = data_item.column_ids[i];
        auto& update_data_value = data_item.data_values[i];
        for (j = 0; j < original_data_item.column_ids.size(); j++) {
            if (original_data_item.column_ids[j] == update_column_id) {
                original_data_item.data_values[j] = update_data_value;
                break;
            }
        }
        if (j == original_data_item.column_ids.size()) {
            return false;
        }
    }
    if (!deleteRecord(file_path, record_location)) {
        return false;
    }
    int file_id = openFile(file_path);
    assert(file_id != -1);
    std::vector<ColumnType> column_types;
    getColumnTypes(file_path, column_types);

    BufType b;
    int index;
    b = bpm->getPage(file_id, 0, index);
    int null_bitmap_buf_size = b[7];
    bpm->access(index);
    int data_item_length =
        dataItemLength(column_types, null_bitmap_buf_size * BYTE_PER_BUF);

    b = bpm->getPage(file_id, record_location.page_id, index);

    sortDataItem(column_types, original_data_item);
    if (!exactMatch(column_types, original_data_item)) {
        setSlotItem(b, record_location.slot_id, data_item_length,
                    null_bitmap_buf_size, original_data_item.data_id,
                    original_data_item_save, column_types);
        bpm->markDirty(index);
        return false;
    }

    setSlotItem(b, record_location.slot_id, data_item_length,
                null_bitmap_buf_size, original_data_item.data_id,
                original_data_item, column_types);
    bpm->markDirty(index);
    return true;
}

void RecordManager::setSlotItem(BufType b, int slot_id, int slot_length,
                                int null_bitmap_buf_size, int record_id,
                                const DataItem& data_item,
                                const std::vector<ColumnType>& column_types) {
    utils::setBitFromBuf(b, slot_id, true);
    int start_buf_position =
        (RECORD_PAGE_HEADER + slot_id * slot_length) / BYTE_PER_BUF;
    b[start_buf_position] = record_id;
    int column_num = column_types.size(), buf_position = start_buf_position;
    int buf_offset = 0;
    start_buf_position++;
    for (int column_id = 0; column_id < column_num; column_id++) {
        if (column_id % BIT_PER_BUF == 0) buf_position++;
        utils::setBitFromNum(b[buf_position], column_id & BIT_PER_BUF_MASK,
                             data_item.data_values[column_id].is_null);
    }
    start_buf_position += null_bitmap_buf_size;
    for (int column_id = 0; column_id < column_num; column_id++) {
        auto& data_value = data_item.data_values[column_id];
        auto& column_type = column_types[column_id];
        int column_byte_width = 0;
        switch (column_type.type_name) {
            case INT:
            case FLOAT:
            case DATE:
                column_byte_width = getDataTypeSize(column_type.type_name);
                break;
            case VARCHAR:
                column_byte_width = column_type.varchar_space + 2;
                break;
        }
        int varchar_length = 0;
        if (!data_value.is_null) {
            switch (column_type.type_name) {
                case INT:
                    b[start_buf_position] =
                        utils::int2bit32(data_value.value.int_value);
                    break;
                case FLOAT:
                    utils::float2bit32(data_value.value.float_value,
                                       b[start_buf_position],
                                       b[start_buf_position + 1]);
                    break;
                case VARCHAR:
                    varchar_length = data_value.value.char_value.size();
                    utils::set2Bytes(b[start_buf_position], 0, varchar_length);
                    buf_position = start_buf_position, buf_offset = 2;
                    for (int i = 0; i < varchar_length; i++) {
                        if (buf_offset == BYTE_PER_BUF) {
                            buf_position++;
                            buf_offset = 0;
                        }
                        utils::set1Byte(b[buf_position], buf_offset++,
                                        data_value.value.char_value[i]);
                    }
                    break;
                case DATE:
                    utils::set2Bytes(b[start_buf_position], 0,
                                     data_value.value.date_value.year);
                    utils::set1Byte(b[start_buf_position], 2,
                                    data_value.value.date_value.month);
                    utils::set1Byte(b[start_buf_position], 3,
                                    data_value.value.date_value.day);
                    break;
            }
        }
        start_buf_position += column_byte_width / BYTE_PER_BUF;
    }
}

DataItem RecordManager::getSlotItem(
    BufType b, int slot_id, int slot_length, int null_bitmap_buf_size,
    const std::vector<ColumnType>& column_types) {
    DataItem data_item;
    int start_buf_position =
        (RECORD_PAGE_HEADER + slot_id * slot_length) / BYTE_PER_BUF;
    data_item.data_id = b[start_buf_position];
    int column_num = column_types.size(), buf_position = start_buf_position;
    int buf_offset = 0;
    start_buf_position++;
    for (int column_id = 0; column_id < column_num; column_id++) {
        if (column_id % BIT_PER_BUF == 0) buf_position++;
        bool is_null =
            utils::getBitFromNum(b[buf_position], column_id & BIT_PER_BUF_MASK);
        data_item.data_values.push_back(
            DataValue{column_types[column_id].type_name, is_null});
        data_item.column_ids.push_back(column_types[column_id].column_id);
    }
    start_buf_position += null_bitmap_buf_size;
    for (int column_id = 0; column_id < column_num; column_id++) {
        auto& column_type = column_types[column_id];
        int column_byte_width = 0;
        data_item.data_values[column_id].type_name = column_type.type_name;
        switch (column_type.type_name) {
            case INT:
            case FLOAT:
            case DATE:
                column_byte_width = getDataTypeSize(column_type.type_name);
                break;
            case VARCHAR:
                column_byte_width = column_type.varchar_space + 2;
                break;
        }
        if (!data_item.data_values[column_id].is_null) {
            int varchar_length = 0;
            switch (column_type.type_name) {
                case INT:
                    data_item.data_values[column_id].value.int_value =
                        utils::bit322int(b[start_buf_position]);
                    break;
                case FLOAT:
                    data_item.data_values[column_id].value.float_value =
                        utils::bit322float(b[start_buf_position],
                                           b[start_buf_position + 1]);
                    break;
                case VARCHAR:
                    varchar_length = utils::get2Bytes(b[start_buf_position], 0);
                    data_item.data_values[column_id].value.char_value = "";
                    buf_position = start_buf_position, buf_offset = 2;
                    for (int i = 0; i < varchar_length; i++) {
                        if (buf_offset == BYTE_PER_BUF) {
                            buf_position++;
                            buf_offset = 0;
                        }
                        data_item.data_values[column_id]
                            .value.char_value.push_back(
                                utils::get1Byte(b[buf_position], buf_offset++));
                    }
                    break;
                case DATE:
                    data_item.data_values[column_id].value.date_value.year =
                        utils::get2Bytes(b[start_buf_position], 0);

                    data_item.data_values[column_id].value.date_value.month =
                        utils::get1Byte(b[start_buf_position], 2);
                    data_item.data_values[column_id].value.date_value.day =
                        utils::get1Byte(b[start_buf_position], 3);
                    break;
            }
        }
        start_buf_position += column_byte_width / BYTE_PER_BUF;
    }
    return data_item;
}

int RecordManager::dataItemLength(const std::vector<ColumnType>& column_types,
                                  int null_bitmap_size) {
    // return byte length
    int length = 4 + null_bitmap_size, tmp_length;
    for (auto& column_type : column_types) {
        switch (column_type.type_name) {
            case INT:
            case FLOAT:
            case DATE:
                length += getDataTypeSize(column_type.type_name);
                break;
            case VARCHAR:
                tmp_length =
                    2 + column_type.varchar_space * getDataTypeSize(VARCHAR);
                tmp_length += tmp_length % 4 == 0 ? 0 : 4 - tmp_length % 4;
                length += tmp_length;
                break;
        }
    }
    length *= 2;
    return length;
}

bool RecordManager::deleteRecordFile(const char* file_path) {
    closeFileIfExist(file_path);
    cleanColumnTypesIfExist(file_path);
    if (!fm->existFile(file_path)) return false;
    return fm->deleteFile(file_path);
}

void RecordManager::sortDataItem(const std::vector<ColumnType>& column_types,
                                 DataItem& data_item) {
    std::vector<DataValue> sorted_data_values;
    std::vector<int> sorted_column_ids;
    for (auto& column_type : column_types) {
        for (size_t i = 0; i < data_item.column_ids.size(); i++) {
            if (data_item.column_ids[i] == column_type.column_id) {
                sorted_data_values.push_back(data_item.data_values[i]);
                sorted_column_ids.push_back(data_item.column_ids[i]);
                break;
            }
        }
    }
    data_item.data_values = sorted_data_values;
    data_item.column_ids = sorted_column_ids;
}

int RecordManager::getTotalPageNum(const char* file_path) {
    int file_id = openFile(file_path);
    assert(file_id != -1);
    BufType b;
    int index;
    b = bpm->getPage(file_id, 0, index);
    int page_num = b[5];
    bpm->access(index);
    return page_num;
}

void RecordManager::getRecordsInPageRange(const char* file_path,
                                          std::vector<DataItem>& data_items,
                                          int low_page, int upper_page) {
    data_items.clear();
    int file_id = openFile(file_path);
    assert(file_id != -1);

    std::vector<ColumnType> column_types;
    getColumnTypes(file_path, column_types);

    BufType b;
    int index;
    b = bpm->getPage(file_id, 0, index);
    int page_num = b[5];
    int null_bitmap_buf_size = b[7];
    bpm->access(index);
    int data_item_length =
        dataItemLength(column_types, null_bitmap_buf_size * BYTE_PER_BUF);
    int data_item_per_page = std::min(
        (PAGE_SIZE - RECORD_PAGE_HEADER) / data_item_length, MAX_ITEM_PER_PAGE);

    for (int page_id = low_page; page_id < upper_page; page_id++) {
        b = bpm->getPage(file_id, page_id, index);
        bpm->access(index);
        for (int slot_id = 0; slot_id < data_item_per_page; slot_id++) {
            if (!utils::getBitFromBuf(b, slot_id)) continue;
            data_items.push_back(
                getSlotItem(b, slot_id,
                            dataItemLength(column_types,
                                           null_bitmap_buf_size * BYTE_PER_BUF),
                            null_bitmap_buf_size, column_types));
        }
    }
}

void RecordManager::getAllRecords(
    const char* file_path, std::vector<DataItem>& data_items,
    std::vector<RecordLocation>& record_locations) {
    data_items.clear();
    record_locations.clear();
    int file_id = openFile(file_path);
    assert(file_id != -1);

    std::vector<ColumnType> column_types;
    getColumnTypes(file_path, column_types);

    BufType b;
    int index;
    b = bpm->getPage(file_id, 0, index);
    int page_num = b[5];
    int null_bitmap_buf_size = b[7];
    bpm->access(index);
    int data_item_length =
        dataItemLength(column_types, null_bitmap_buf_size * BYTE_PER_BUF);
    int data_item_per_page = std::min(
        (PAGE_SIZE - RECORD_PAGE_HEADER) / data_item_length, MAX_ITEM_PER_PAGE);

    for (int page_id = 1; page_id <= page_num; page_id++) {
        b = bpm->getPage(file_id, page_id, index);
        bpm->access(index);
        for (int slot_id = 0; slot_id < data_item_per_page; slot_id++) {
            if (!utils::getBitFromBuf(b, slot_id)) continue;
            data_items.push_back(
                getSlotItem(b, slot_id,
                            dataItemLength(column_types,
                                           null_bitmap_buf_size * BYTE_PER_BUF),
                            null_bitmap_buf_size, column_types));
            record_locations.push_back(RecordLocation{page_id, slot_id});
        }
    }
}

int RecordManager::getAllRecordWithConstraintSaveFile(
    const char* file_path, const char* file_path_save,
    const std::vector<system::SearchConstraint>& constraints) {
    int file_id = openFile(file_path);
    assert(file_id != -1);

    std::vector<ColumnType> column_types;
    getColumnTypes(file_path, column_types);

    std::ofstream outputFile(file_path_save);
    if (!outputFile.is_open()) {
        std::cerr << "Error opening file!" << std::endl;
        return 0;
    }

    int cnt = 0;
    BufType b;
    int index;
    b = bpm->getPage(file_id, 0, index);
    int page_num = b[5];
    int null_bitmap_buf_size = b[7];
    bpm->access(index);
    int data_item_length =
        dataItemLength(column_types, null_bitmap_buf_size * BYTE_PER_BUF);
    int data_item_per_page = std::min(
        (PAGE_SIZE - RECORD_PAGE_HEADER) / data_item_length, MAX_ITEM_PER_PAGE);

    for (int page_id = 1; page_id <= page_num; page_id++) {
        b = bpm->getPage(file_id, page_id, index);
        bpm->access(index);
        for (int slot_id = 0; slot_id < data_item_per_page; slot_id++) {
            if (!utils::getBitFromBuf(b, slot_id)) continue;
            auto data_item =
                getSlotItem(b, slot_id,
                            dataItemLength(column_types,
                                           null_bitmap_buf_size * BYTE_PER_BUF),
                            null_bitmap_buf_size, column_types);
            bool valid = true;
            for (auto& constraint : constraints) {
                if (!system::validConstraint(constraint, data_item)) {
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
    }
    outputFile.close();
    return cnt;
}

void RecordManager::getAllRecordWithConstraint(
    const char* file_path, std::vector<DataItem>& data_items,
    std::vector<RecordLocation>& record_locations,
    const std::vector<system::SearchConstraint>& constraints) {
    data_items.clear();
    record_locations.clear();
    int file_id = openFile(file_path);
    assert(file_id != -1);

    std::vector<ColumnType> column_types;
    getColumnTypes(file_path, column_types);

    BufType b;
    int index;
    b = bpm->getPage(file_id, 0, index);
    int page_num = b[5];
    int null_bitmap_buf_size = b[7];
    bpm->access(index);
    int data_item_length =
        dataItemLength(column_types, null_bitmap_buf_size * BYTE_PER_BUF);
    int data_item_per_page = std::min(
        (PAGE_SIZE - RECORD_PAGE_HEADER) / data_item_length, MAX_ITEM_PER_PAGE);

    for (int page_id = 1; page_id <= page_num; page_id++) {
        b = bpm->getPage(file_id, page_id, index);
        bpm->access(index);
        for (int slot_id = 0; slot_id < data_item_per_page; slot_id++) {
            if (!utils::getBitFromBuf(b, slot_id)) continue;
            auto data_item =
                getSlotItem(b, slot_id,
                            dataItemLength(column_types,
                                           null_bitmap_buf_size * BYTE_PER_BUF),
                            null_bitmap_buf_size, column_types);
            bool valid = true;
            for (auto& constraint : constraints) {
                if (!system::validConstraint(constraint, data_item)) {
                    valid = false;
                    break;
                }
            }
            if (valid) {
                data_items.push_back(data_item);
                record_locations.push_back(RecordLocation{page_id, slot_id});
            }
        }
    }
}
}  // namespace record
}  // namespace dbs