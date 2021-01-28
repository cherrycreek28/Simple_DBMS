#include "src/include/rm.h"

namespace PeterDB {
    RelationManager &RelationManager::instance() {
        static RelationManager _relation_manager = RelationManager();
        return _relation_manager;
    }

    RelationManager::RelationManager()
    {
        Attribute a1; 
        Attribute a2; 
        Attribute a3;

        a1.length = 4; 
        a1.name = "table-id"; 
        a1.type = AttrType::TypeInt; 

        a2.length = 50; 
        a2.name = "table-name"; 
        a2.type = AttrType::TypeVarChar; 

        a3.length = 50; 
        a3.name = "file-name"; 
        a3.type = AttrType::TypeVarChar; 

        record_descriptor_TABLE = std::vector<Attribute>{a1, a2, a3}; 

        Attribute b1; 
        Attribute b2; 
        Attribute b3;
        Attribute b4; 
        Attribute b5;

        b1.length = 4; 
        b1.name = "table-id"; 
        b1.type = AttrType::TypeInt; 

        b2.length = 50; 
        b2.name = "column-name"; 
        b2.type = AttrType::TypeVarChar; 

        b3.length = 4; 
        b3.name = "column-type"; 
        b3.type = AttrType::TypeInt; 

        b4.length = 4; 
        b4.name = "column-length"; 
        b4.type = AttrType::TypeInt; 

        b5.length = 4; 
        b5.name = "column-position"; 
        b5.type = AttrType::TypeInt; 

        record_descriptor_COL = std::vector<Attribute>{b1, b2, b3, b4, b5}; 

        Attribute c1;
        Attribute c2; 
        Attribute c3;

        c1.length = 50; 
        c1.name = "table-name"; 
        c1.type = AttrType::TypeVarChar; 

        c2.length = 50; 
        c2.name = "column-name"; 
        c2.type = AttrType::TypeVarChar; 

        c3.length = 50; 
        c3.name = "file-name"; 
        c3.type = AttrType::TypeVarChar; 

        record_descriptor_INDEX= std::vector<Attribute>{c1, c2, c3}; 

        FileHandle file_handle_TABLE;
        if (RecordBasedFileManager::instance().openFile("Tables", file_handle_TABLE) == -1)
        {
            num_of_tables = 0; 
            return; 
        }

        RID rid; 
        char* value = (char* )malloc(PAGE_SIZE);
        char* data = (char* )malloc(PAGE_SIZE);
        RBFM_ScanIterator it;
        RecordBasedFileManager::instance().scan(file_handle_TABLE, record_descriptor_TABLE, "", CompOp::NO_OP, value, std::vector<std::string>{"table-id"}, it); 

        while (it.getNextRecord(rid, data) == 0)
        {
            num_of_tables++; 
        }

        free(value); 
        free(data);
        
        RecordBasedFileManager::instance().closeFile(file_handle_TABLE);
    };

    RelationManager::~RelationManager() = default;

    RelationManager::RelationManager(const RelationManager &) = default;

    RelationManager &RelationManager::operator=(const RelationManager &) = default;

    RC RelationManager::createCatalog() {
        if (RecordBasedFileManager::instance().createFile("Tables") == -1)
            return -1; 
        if (RecordBasedFileManager::instance().createFile("Columns") == -1)
            return -1; 
        if (RecordBasedFileManager::instance().createFile("Indexes_Team17") == -1)
            return -1; 

        if (createTable("Tables", record_descriptor_TABLE) == -1)
            return -1;
        if (createTable("Columns", record_descriptor_COL) == -1)
            return -1;
        if (createTable("Indexes_Team17", record_descriptor_INDEX) == -1)
            return -1;

        return 0; 
    }

    RC RelationManager::deleteCatalog() {
        

        if (RecordBasedFileManager::instance().destroyFile("Tables") == -1)
            return -1;
        if (RecordBasedFileManager::instance().destroyFile("Columns") == -1)
            return -1;
        if (RecordBasedFileManager::instance().destroyFile("Indexes_Team17") == -1)
            return -1;
        

        //std::cout<<"called once"<<std::endl;
        return 0; 
    }

    RC RelationManager::createTable(const std::string &tableName, const std::vector<Attribute> &attrs) {
        FileHandle file_handle_COL;
        if (RecordBasedFileManager::instance().openFile("Columns", file_handle_COL) == -1)
            return -1; 
        FileHandle file_handle_TABLE;
        if (RecordBasedFileManager::instance().openFile("Tables", file_handle_TABLE) == -1)
            return -1;
        
        void* data = malloc(PAGE_SIZE);
        int offset = 0; 
        RID rid;
        num_of_tables++;

        //insert into Cols
        for (int i = 0; i < attrs.size(); i++)
        {
            memset(data, 0, PAGE_SIZE);
            offset = 1; 

            *(int* )(data + offset) = num_of_tables; 
            offset += 4;
            *(int* )(data + offset) = attrs[i].name.size(); 
            offset += 4; 
            memcpy(data + offset, attrs[i].name.c_str(), attrs[i].name.size()); 
            offset += attrs[i].name.size();
            *(int* )(data + offset) = attrs[i].type; 
            offset += 4;
            *(int* )(data + offset) = attrs[i].length; 
            offset += 4;
            *(int* )(data + offset) = i + 1; 
            offset += 4;
            if (RecordBasedFileManager::instance().insertRecord(file_handle_COL, record_descriptor_COL, data, rid) != 0)
            {
                free(data);
                return -1;
            }
        }

        //insert into Tabs
        memset(data, 0, PAGE_SIZE);
        offset = 1; 

        *(int* )(data + offset) = num_of_tables; 
        offset += 4;
        *(int* )(data + offset) = tableName.size(); 
        offset += 4; 
        memcpy(data + offset, tableName.c_str(), tableName.size()); 
        offset += tableName.size(); 
        *(int* )(data + offset) = tableName.size(); 
        offset += 4; 
        memcpy(data + offset, tableName.c_str(), tableName.size()); 
        offset += tableName.size(); 

        if (RecordBasedFileManager::instance().insertRecord(file_handle_TABLE, record_descriptor_TABLE, data, rid) != 0)
        {
            free(data);
            return -1;
        }
        free(data);

        if (tableName != "Tables" && tableName != "Columns" && tableName != "Indexes_Team17" && RecordBasedFileManager::instance().createFile(tableName) == -1)
            return -1;
        if (RecordBasedFileManager::instance().closeFile(file_handle_COL) == -1)
            return -1;
        if (RecordBasedFileManager::instance().closeFile(file_handle_TABLE) == -1)
            return -1;
        return 0;
    }

    RC RelationManager::deleteTable(const std::string &tableName) {
        if (tableName == "Tables" || tableName == "Columns" || tableName == "Indexes_Team17")
            return -1;

        if (RecordBasedFileManager::instance().destroyFile(tableName) == -1)
            return -1;

        FileHandle file_handle_COL;
        if (RecordBasedFileManager::instance().openFile("Columns", file_handle_COL) == -1)
            return -1; 
        FileHandle file_handle_TABLE;
        if (RecordBasedFileManager::instance().openFile("Tables", file_handle_TABLE) == -1)
            return -1;
        FileHandle file_handle_IDX;
        if (RecordBasedFileManager::instance().openFile("Indexes_Team17", file_handle_IDX) == -1)
            return -1;
        
        RID rid; 
        char* value = (char* )malloc(PAGE_SIZE);
        char* data = (char* )malloc(PAGE_SIZE);

        *(int* )value = tableName.size();
        memcpy(value + 4, tableName.c_str(), tableName.size());
        
        RBFM_ScanIterator it_TAB;
        RBFM_ScanIterator it_COL;
        RBFM_ScanIterator it_IDX;

        RecordBasedFileManager::instance().scan(file_handle_TABLE, record_descriptor_TABLE, "table-name", CompOp::EQ_OP, value, std::vector<std::string>{"table-id"}, it_TAB); 
        if (it_TAB.getNextRecord(rid, data) == -1)
        {
            free(value); 
            free(data); 
            return -1; 
        }
        if (deleteTuple("Tables", rid) == -1)
        {
            free(value); 
            free(data);
            return -1; 
        }

        int table_id = *(int* )((char* )data + 1); 
        RecordBasedFileManager::instance().scan(file_handle_COL, record_descriptor_COL, "table-id", CompOp::EQ_OP, &table_id, std::vector<std::string>{"column-name"}, it_COL); 

        while (it_COL.getNextRecord(rid, data) == 0)
        {
            if (deleteTuple("Columns", rid) == -1)
            {
                free(value); 
                free(data);
                return -1; 
            }
        }

        RecordBasedFileManager::instance().scan(file_handle_IDX, record_descriptor_INDEX, "table-name", CompOp::EQ_OP, value, std::vector<std::string>{"file-name"}, it_IDX); 
        while (it_IDX.getNextRecord(rid, data) == 0)
        {
            int offset = 0;
            //skip the null-indicator
            offset += 1; 
            int file_name_length = *(int* )(data + offset);
            offset += SIZE_OF_INT;
            std::string file_name;
            file_name.assign(data + offset, file_name_length);

            if (deleteTuple("Indexes_Team17", rid) == -1)
            {
                free(value); 
                free(data);
                return -1; 
            }
            if (RecordBasedFileManager::instance().destroyFile(file_name) == -1)
            {
                free(value); 
                free(data);
                return -1; 
            }
        }

        free(value); 
        free(data); 

        if (RecordBasedFileManager::instance().closeFile(file_handle_COL) == -1)
            return -1;
        if (RecordBasedFileManager::instance().closeFile(file_handle_TABLE) == -1)
            return -1;
        if (RecordBasedFileManager::instance().closeFile(file_handle_IDX) == -1)
            return -1;

        return 0;
    }

    RC RelationManager::getAttributes(const std::string &tableName, std::vector<Attribute> &attrs) {
        FileHandle file_handle_COL;
        if (RecordBasedFileManager::instance().openFile("Columns", file_handle_COL) == -1)
        {
            std::cout<<"Col not exist"<<std::endl;
            return -1; 
        }
        FileHandle file_handle_TABLE;
        if (RecordBasedFileManager::instance().openFile("Tables", file_handle_TABLE) == -1)
        {
            std::cout<<"Tab not exist"<<std::endl;
            return -1; 
        }

        RID rid; 
        char* value = (char* )malloc(PAGE_SIZE);
        char* data = (char* )malloc(PAGE_SIZE);

        /*
        *temp = 0; 
        *(int* )(temp + 1) = tableName.size(); 
        memcpy(temp + 5, tableName.c_str(), tableName.size());
        */
        //newly added
        *(int* )value = tableName.size();
        memcpy(value + 4, tableName.c_str(), tableName.size());
        
        RBFM_ScanIterator it_TAB;
        RBFM_ScanIterator it_COL;

        RecordBasedFileManager::instance().scan(file_handle_TABLE, record_descriptor_TABLE, "table-name", CompOp::EQ_OP, value, std::vector<std::string>{"table-id"}, it_TAB); 
        if (it_TAB.getNextRecord(rid, data) == -1)
        {
            std::cout<<"Wrong with getNextRecord"<<std::endl;
            free(value); 
            free(data); 
            return -1; 
        }

        int table_id = *(int* )((char* )data + 1); 
        RecordBasedFileManager::instance().scan(file_handle_COL, record_descriptor_COL, "table-id", CompOp::EQ_OP, &table_id, std::vector<std::string>{"column-name", "column-type", "column-length"}, it_COL); 

        while (it_COL.getNextRecord(rid, data) == 0)
        {
            Attribute a; 
            int data_offset = 1; 
            
            int length_of_name = *(int* )(data + data_offset); 
            data_offset += SIZE_OF_INT;

            std::string attribute_name;
            a.name = attribute_name.assign(data + data_offset, length_of_name); 
            data_offset += length_of_name;
            
            a.type = static_cast<AttrType>(*(int* )(data + data_offset));
            data_offset += SIZE_OF_INT;

            a.length = *(int* )(data + data_offset); 
            attrs.push_back(a); 
        }

        free(data); 
        free(value); 

        if (RecordBasedFileManager::instance().closeFile(file_handle_COL) == -1)
            return -1;
        if (RecordBasedFileManager::instance().closeFile(file_handle_TABLE) == -1)
            return -1;
        return 0;
    }

    RC RelationManager::insertTuple(const std::string &tableName, const void *data, RID &rid) {
        std::vector<Attribute> record_descriptor; 
        FileHandle file_handle; 
        if (RecordBasedFileManager::instance().openFile(tableName, file_handle) == -1)
            return -1; 
        if (getAttributes(tableName, record_descriptor) == -1)
            return -1; 
        if (RecordBasedFileManager::instance().insertRecord(file_handle, record_descriptor, data, rid) == -1)
            return -1;
        if (RecordBasedFileManager::instance().closeFile(file_handle) == -1)
            return -1;

        FileHandle file_handle_IDX;
        RBFM_ScanIterator it_IDX;

        RID rid_retrieve; 
        char* value = (char* )malloc(PAGE_SIZE);
        char* data_retrieve = (char* )malloc(PAGE_SIZE);
        
        *(int* )value = tableName.size();
        memcpy(value + SIZE_OF_INT, tableName.c_str(), tableName.size());

        if (RecordBasedFileManager::instance().openFile("Indexes_Team17", file_handle_IDX) == -1)
            return -1; 
        RecordBasedFileManager::instance().scan(file_handle_IDX, record_descriptor_INDEX, "table-name", CompOp::EQ_OP, value, std::vector<std::string>{"file-name", "column-name"}, it_IDX); 
        //RID rid_retrieve;
        while (it_IDX.getNextRecord(rid_retrieve, data_retrieve) == 0)
        {
            int offset = 0;
            //skip the null-indicator
            offset += 1; 
            int file_name_length = *(int* )(data_retrieve + offset);
            offset += SIZE_OF_INT;
            
            std::string file_name;
            file_name.assign(data_retrieve + offset, file_name_length);
            offset += file_name_length; 

            int attr_name_length = *(int* )(data_retrieve + offset);
            offset += SIZE_OF_INT;
            
            std::string attr_name;
            attr_name.assign(data_retrieve + offset, attr_name_length);
            offset += attr_name_length; 
            
            Attribute current_attr; 
            int flag = 0;

            for (const Attribute& a : record_descriptor)
            {
                if (a.name == attr_name)
                {
                    current_attr = a; 
                    flag = 1;
                    break;
                }
            }

            if (flag == 0)
                return -1;

            char* key = (char* )malloc(PAGE_SIZE); 
            if (readAttribute(tableName, rid, attr_name, key) == -1)
                return -1;
            memmove(key, key + 1, PAGE_SIZE - 1);
            IXFileHandle index_only_file_handle; 
            if (IndexManager::instance().openFile(file_name, index_only_file_handle) == -1)
                return -1;
            if (IndexManager::instance().insertEntry(index_only_file_handle, current_attr, key, rid) == -1)
                return -1;
            if (IndexManager::instance().closeFile(index_only_file_handle) == -1)
                return -1;
            free(key);
        }
        free(value); 
        free(data_retrieve);
        if (RecordBasedFileManager::instance().closeFile(file_handle_IDX) == -1)
            return -1; 
        
        return 0; 
    }

    RC RelationManager::deleteTuple(const std::string &tableName, const RID &rid) {
        std::vector<Attribute> record_descriptor; 
        FileHandle file_handle; 
        if (RecordBasedFileManager::instance().openFile(tableName, file_handle) == -1)
            return -1; 
        if (getAttributes(tableName, record_descriptor) == -1)
            return -1; 
        if (RecordBasedFileManager::instance().deleteRecord(file_handle, record_descriptor, rid) == -1)
            return -1;
        if (RecordBasedFileManager::instance().closeFile(file_handle) == -1)
            return -1;
        return 0; 
    }

    RC RelationManager::updateTuple(const std::string &tableName, const void *data, const RID &rid) {
        std::vector<Attribute> record_descriptor; 
        FileHandle file_handle; 
        if (RecordBasedFileManager::instance().openFile(tableName, file_handle) == -1)
            return -1; 
        if (getAttributes(tableName, record_descriptor) == -1)
            return -1; 
        if (RecordBasedFileManager::instance().updateRecord(file_handle, record_descriptor, data, rid) == -1)
            return -1;
        if (RecordBasedFileManager::instance().closeFile(file_handle) == -1)
            return -1;
        return 0; 
    }

    RC RelationManager::readTuple(const std::string &tableName, const RID &rid, void *data) {
        std::vector<Attribute> record_descriptor; 
        FileHandle file_handle; 
        if (RecordBasedFileManager::instance().openFile(tableName, file_handle) == -1)
            return -1; 
        if (getAttributes(tableName, record_descriptor) == -1)
            return -1; 
        if (RecordBasedFileManager::instance().readRecord(file_handle, record_descriptor, rid, data) == -1)
            return -1;
        if (RecordBasedFileManager::instance().closeFile(file_handle) == -1)
            return -1;
        return 0; 
    }

    RC RelationManager::printTuple(const std::vector<Attribute> &attrs, const void *data, std::ostream &out) {
        if (RecordBasedFileManager::instance().printRecord(attrs, data, out) == -1)
            return -1;
        return 0; 
    }

    RC RelationManager::readAttribute(const std::string &tableName, const RID &rid, const std::string &attributeName,
                                      void *data) {
        std::vector<Attribute> record_descriptor; 
        FileHandle file_handle; 
        if (RecordBasedFileManager::instance().openFile(tableName, file_handle) == -1)
            return -1; 
        if (getAttributes(tableName, record_descriptor) == -1)
            return -1; 
        if (RecordBasedFileManager::instance().readAttribute(file_handle, record_descriptor, rid, attributeName, data) == -1)
            return -1;
        if (RecordBasedFileManager::instance().closeFile(file_handle) == -1)
            return -1;
        return 0; 
    }

    RC RelationManager::scan(const std::string &tableName,
                             const std::string &conditionAttribute,
                             const CompOp compOp,
                             const void *value,
                             const std::vector<std::string> &attributeNames,
                             RM_ScanIterator &rm_ScanIterator) {
        std::vector<Attribute> record_descriptor; 
        FileHandle file_handle; 
        if (RecordBasedFileManager::instance().openFile(tableName, file_handle) == -1)
            return -1; 
        if (getAttributes(tableName, record_descriptor) == -1)
            return -1; 
        return RecordBasedFileManager::instance().scan(file_handle, record_descriptor, conditionAttribute, compOp, value, attributeNames, rm_ScanIterator.rbfm_scanner);
    }

    RM_ScanIterator::RM_ScanIterator() = default;

    RM_ScanIterator::~RM_ScanIterator() = default;

    RC RM_ScanIterator::getNextTuple(RID &rid, void *data) 
    { 
        return rbfm_scanner.getNextRecord(rid, data);
    }

    RC RM_ScanIterator::close() 
    {
        return rbfm_scanner.close();
    }

    // Extra credit work
    RC RelationManager::dropAttribute(const std::string &tableName, const std::string &attributeName) {
        return -1;
    }

    // Extra credit work
    RC RelationManager::addAttribute(const std::string &tableName, const Attribute &attr) {
        return -1;
    }

    // QE IX related
    RC RelationManager::createIndex(const std::string &tableName, const std::string &attributeName)
    {
        std::string index_file_name = tableName + "_" + attributeName + ".idx";
        if (IndexManager::instance().createFile(index_file_name) == -1)
            return -1;

        char* data = (char* )malloc(PAGE_SIZE);
        memset(data, 0, PAGE_SIZE);

        int offset = 0; 
        offset += 1;

        *(int* )(data + offset) = tableName.size(); 
        offset += SIZE_OF_INT;
        memcpy(data + offset, tableName.c_str(), tableName.size()); 
        offset += tableName.size(); 
        
        *(int* )(data + offset) = attributeName.size(); 
        offset += SIZE_OF_INT; 
        memcpy(data + offset, attributeName.c_str(), attributeName.size()); 
        offset += attributeName.size(); 

        *(int* )(data + offset) = index_file_name.size(); 
        offset += SIZE_OF_INT; 
        memcpy(data + offset, index_file_name.c_str(), index_file_name.size()); 
        offset += index_file_name.size(); 

        RID rid;
        if (insertTuple("Indexes_Team17", data, rid) == -1)
        {
            free(data); 
            return -1;
        }
        free(data);

        RM_ScanIterator it_records;
        //Notice attributeName isn't important here.
        if (scan(tableName, "", NO_OP, NULL, std::vector<std::string>{attributeName}, it_records) == -1)
            return -1;
        RID rid_retrieve; 
        std::vector<Attribute> record_descriptor;
        if (getAttributes(tableName, record_descriptor) == -1)
            return -1;
        char* data_retrieve = (char* )malloc(PAGE_SIZE);

        Attribute current_attr; 
        int flag = 0;
        for (const Attribute& a : record_descriptor)
        {
            if (a.name == attributeName)
            {
                current_attr = a; 
                flag = 1;
                break;
            }
        }

        if (flag == 0)
            return -1;

        while (it_records.rbfm_scanner.getNextRecord(rid_retrieve, data_retrieve) == 0)
        {
            //char* key = (char* )malloc(PAGE_SIZE); 
            /*
            if (readAttribute(tableName, rid, attributeName, key) == -1)
                return -1;
            */

            memmove(data_retrieve, data_retrieve + 1, PAGE_SIZE - 1);

            IXFileHandle index_only_file_handle; 
            if (IndexManager::instance().openFile(index_file_name, index_only_file_handle) == -1)
                return -1;
            if (IndexManager::instance().insertEntry(index_only_file_handle, current_attr, data_retrieve, rid_retrieve) == -1)
                return -1;
            if (IndexManager::instance().closeFile(index_only_file_handle) == -1)
                return -1;
            //free(key);
        }

        free(data_retrieve);
        return 0;
    }

    RC RelationManager::destroyIndex(const std::string &tableName, const std::string &attributeName)
    {
        return -1;
    }

    // indexScan returns an iterator to allow the caller to go through qualified entries in index
    RC RelationManager::indexScan(const std::string &tableName,
                    const std::string &attributeName,
                    const void *lowKey,
                    const void *highKey,
                    bool lowKeyInclusive,
                    bool highKeyInclusive,
                    RM_IndexScanIterator &rm_IndexScanIterator)
    {
        std::vector<Attribute> record_descriptor; 
        if (getAttributes(tableName, record_descriptor) == -1)
            return -1; 
        //return RecordBasedFileManager::instance().scan(file_handle, record_descriptor, conditionAttribute, compOp, value, attributeNames, rm_ScanIterator.rbfm_scanner);
        //Attribute attribute;

        Attribute current_attr; 
        int flag = 0;
        for (const Attribute& a : record_descriptor)
        {
            if (a.name == attributeName)
            {
                current_attr = a; 
                flag = 1;
                break;
            }
        }
        if (flag == 0)
            return -1;

        IXFileHandle index_only_file_handle;
        if (IndexManager::instance().openFile(tableName + "_" + attributeName + ".idx", index_only_file_handle) == -1)
            return -1;        
        return IndexManager::instance().scan(index_only_file_handle, current_attr, lowKey, highKey, lowKeyInclusive, highKeyInclusive, rm_IndexScanIterator.ix_scanner);
    }

    // "key" follows the same format as in IndexManager::insertEntry()
    RC RM_IndexScanIterator::getNextEntry(RID &rid, void *key)
    {
        return ix_scanner.getNextEntry(rid, key);
    }
    
    RC RM_IndexScanIterator::close()
    {
        return ix_scanner.close();
    }

    RM_IndexScanIterator::RM_IndexScanIterator(){;}    // Constructor
    RM_IndexScanIterator::~RM_IndexScanIterator(){;}    // Destructor

} // namespace PeterDB