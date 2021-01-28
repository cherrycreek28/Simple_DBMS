#include "src/include/rbfm.h"

namespace PeterDB {

    void attributes_to_data(const std::vector<int> &attributeNames, const std::vector<Attribute> &recordDescriptor, char *data, const char* record, int& data_length)
    {
        int data_offset = 0; 
        //int data_offset_back = 0;
        int record_offset_front = 0;
        int record_offset_back = 0;  

        int field_num = recordDescriptor.size();
        char* bitmap_ptr = nullptr; 

        int record_offset_back_origin = record_offset_back;
        int return_attributes_num = attributeNames.size();
        data_offset += ceil((double)(return_attributes_num) / (double)8); 
        
        for (int i = 0; i < return_attributes_num; i++)
        {
            int bit_offset = i % 8; 
            int byte_offset = i / 8; 
            bitmap_ptr = (char* )data + byte_offset; 

            if (bit_offset == 0)
                *(char* )bitmap_ptr = 0;

            Attribute current_attribute = recordDescriptor[attributeNames[i]]; 
            record_offset_front = (attributeNames[i] + 1) * SIZE_OF_SHORT;
            record_offset_back = *(short* )(record + record_offset_front); 

            if (*(short* )(record + record_offset_front) == -1)
            {
                *bitmap_ptr |= (1 << (7 - bit_offset)); 
            }
            else
            {
                *bitmap_ptr |= (0 << (7 - bit_offset)); 

                if (current_attribute.type == TypeInt || current_attribute.type == TypeReal)
                {
                    
                    *(int* )(data + data_offset) = *(int* )(record + record_offset_back - SIZE_OF_INT + 1);
                    data_offset += SIZE_OF_INT; 
                }   
            
                else if (current_attribute.type == TypeVarChar)
                {
                    int varchar_length = 0;
                    if (attributeNames[i] == 0)
                        //how did I calculate this??
                        varchar_length = record_offset_back + 1 - (*(short* )(record) + 1) * SIZE_OF_SHORT; 
                    else
                    {
                        int flag = 0;
                        //some problems here...
                        for (int i = 1; record_offset_front - SIZE_OF_SHORT * i >= SIZE_OF_SHORT; i++)
                        {
                            if (*(short* )(record + record_offset_front - SIZE_OF_SHORT * i) != -1)
                            {
                                varchar_length = *(short* )(record + record_offset_front) - *(short* )(record + record_offset_front - SIZE_OF_SHORT * i); 
                                flag = 1;
                                break;
                            }
                        }

                        if (flag == 0)
                            varchar_length = varchar_length = record_offset_back + 1 - (*(short* )(record) + 1) * SIZE_OF_SHORT; 
                    }  

                    *(int* )(data + data_offset) = varchar_length;
                    data_offset += SIZE_OF_INT; 
                    memcpy(data + data_offset, record + record_offset_back - varchar_length + 1, varchar_length); 
                    data_offset += varchar_length; 
                }

            }
            
        }
        data_length = data_offset;
        /*
        for (int i = 0; i < field_num; i++)
        {
            int bit_offset = i % 8; 
            int byte_offset = i / 8; 
            
            bitmap_ptr = (char* )data + byte_offset; 
            
            if (*(short* )(record + record_offset_front) == -1)
            {
                *bitmap_ptr |= (1 << (7 - bit_offset)); 
            }
            else
            {
                *bitmap_ptr |= (0 << (7 - bit_offset)); 

                Attribute curr_attribute = recordDescriptor[i];
                if (curr_attribute.type == TypeInt)
                {
                    *(int* )(data + data_offset) = *(int* )(record + record_offset_back);
                    record_offset_back += SIZE_OF_INT; 
                    data_offset += SIZE_OF_INT; 
                }     
                else if (curr_attribute.type == TypeReal)
                {
                    //float is same as int
                    *(int* )(data + data_offset) = *(int* )(record + record_offset_back);
                    record_offset_back += SIZE_OF_INT; 
                    data_offset += SIZE_OF_INT; 
                }
                else if (curr_attribute.type == TypeVarChar)
                {
                    int varchar_length = 0;
                    if (i == 0)
                        //how did I calculate this??
                        varchar_length = *(short* )(record + record_offset_front) - 1 - *(short* )(record) * SIZE_OF_SHORT;
                    
                    else
                    {
                        int flag = 0;
                        for (int i = 1; record_offset_front - SIZE_OF_SHORT * i >= SIZE_OF_SHORT; i++)
                        {
                            if (*(short* )(record + record_offset_front - SIZE_OF_SHORT * i) != -1)
                            {
                                varchar_length = *(short* )(record + record_offset_front) - *(short* )(record + record_offset_front - SIZE_OF_SHORT * i); 
                                flag = 1;
                                break;
                            }
                        }

                        if (flag == 0)
                            varchar_length = *(short* )(record + record_offset_front) - 1 - *(short* )(record) * SIZE_OF_SHORT; 
                    }  

                    *(short* )(data + data_offset) = varchar_length;
                    data_offset += SIZE_OF_INT; 
                    
                    memcpy(data + data_offset, record + record_offset_back, varchar_length); 
                    record_offset_back += varchar_length; 
                    data_offset += varchar_length; 
                }
            }
            record_offset_front += SIZE_OF_SHORT;
        }
        data_length = data_offset;
        */
    }

    void change_offset_for_slot_after_current(void* current_page, int current_length, Page_info* page_info, Record_info* record_info)
    {
        for (int i = 1; i <= page_info->number_of_slot; i++)
        {
            Record_info* current_record_info = (Record_info* )((char* )current_page + PAGE_SIZE - sizeof(Page_info) - i * sizeof(Record_info));
            if (current_record_info->offset_to_record > record_info->offset_to_record)
                current_record_info->offset_to_record += (current_length - record_info->record_length); 
        }
    }

    int attribute_to_data(const std::vector<Attribute> &recordDescriptor, char *data, const char* record, int& data_length, const std::string &attributeName)
    {
        int data_offset = 0; 
        int record_offset_front = 0;
        int record_offset_back = 0;  

        int field_num = recordDescriptor.size();
        void* bitmap_ptr = data;
        int varchar_length = -1; 

        //the null-fieid indicator will only occupy 1 byte
        data_offset = 1; 

        int index_of_attribute = -1; 
        for (int i = 0; i < field_num; i++)
        {
            if (recordDescriptor[i].name == attributeName)
            {
                index_of_attribute = i; 
                break; 
            }
        }
        if (index_of_attribute == -1)
        {
            return -1; 
        }
            
        Attribute curr_attribute = recordDescriptor[index_of_attribute]; 
        record_offset_front = SIZE_OF_SHORT * (1 + index_of_attribute); 
        record_offset_back = *(short* )(record + record_offset_front);

        if (*(short* )(record + record_offset_front) == -1)
        {
            *(char* )bitmap_ptr |= (1 << 7); 
        }
        else
        {
            *(char* )bitmap_ptr |= (0 << 7); 
            if (curr_attribute.type == TypeInt)
            {
                *(int* )(data + data_offset) = *(int* )(record + record_offset_back - SIZE_OF_INT + 1);
                data_offset += SIZE_OF_INT;
            }     
            else if (curr_attribute.type == TypeReal)
            {
                //float is same as int
                *(int* )(data + data_offset) = *(int* )(record + record_offset_back - SIZE_OF_INT + 1);
                data_offset += SIZE_OF_INT;
            }
            else if (curr_attribute.type == TypeVarChar)
            {
                int varchar_length = 0;
                if (index_of_attribute == 0)
                    //how did I calculate this??
                    varchar_length = *(short* )(record + record_offset_front) - 1 - *(short* )(record) * SIZE_OF_SHORT;
                
                else
                {
                    int flag = 0;
                    for (int i = 1; record_offset_front - SIZE_OF_SHORT * i >= SIZE_OF_SHORT; i++)
                    {
                        if (*(short* )(record + record_offset_front - SIZE_OF_SHORT * i) != -1)
                        {
                            varchar_length = *(short* )(record + record_offset_front) - *(short* )(record + record_offset_front - SIZE_OF_SHORT * i); 
                            flag = 1;
                            break;
                        }
                    }

                    if (flag == 0)
                        varchar_length = *(short* )(record + record_offset_front) - 1 - *(short* )(record) * SIZE_OF_SHORT; 
                }  

                *(int* )(data + data_offset) = varchar_length;
                data_offset += SIZE_OF_INT; 
                
                memcpy(data + data_offset, record + record_offset_back - varchar_length + 1, varchar_length); 
                data_offset += varchar_length; 
            }
        }
        data_length = data_offset;
        return 0;
    }
        
          

    void record_to_data(const std::vector<Attribute> &recordDescriptor, char *data, const char* record, int& data_length)
    {
        int data_offset = 0; 
        //int data_offset_back = 0;
        int record_offset_front = 0;
        int record_offset_back = 0;  

        int field_num = recordDescriptor.size();
        char* bitmap_ptr = nullptr; 

        record_offset_front += SIZE_OF_SHORT; 
        record_offset_back += (field_num + 1) * SIZE_OF_SHORT; 
        int record_offset_back_origin = record_offset_back;
        data_offset += ceil((double)field_num / (double)8); 

        for (int i = 0; i < field_num; i++)
        {
            int bit_offset = i % 8; 
            int byte_offset = i / 8; 
            
            bitmap_ptr = (char* )data + byte_offset; 
            
            if (*(short* )(record + record_offset_front) == -1)
            {
                *bitmap_ptr |= (1 << (7 - bit_offset)); 
            }
            else
            {
                *bitmap_ptr |= (0 << (7 - bit_offset)); 

                Attribute curr_attribute = recordDescriptor[i];
                if (curr_attribute.type == TypeInt)
                {
                    *(int* )(data + data_offset) = *(int* )(record + record_offset_back);
                    record_offset_back += SIZE_OF_INT; 
                    data_offset += SIZE_OF_INT; 
                }     
                else if (curr_attribute.type == TypeReal)
                {
                    //float is same as int
                    *(int* )(data + data_offset) = *(int* )(record + record_offset_back);
                    record_offset_back += SIZE_OF_INT; 
                    data_offset += SIZE_OF_INT; 
                }
                else if (curr_attribute.type == TypeVarChar)
                {
                    int varchar_length = 0;
                    if (i == 0)
                        //how did I calculate this??
                        varchar_length = *(short* )(record + record_offset_front) - 1 - *(short* )(record) * SIZE_OF_SHORT;
                    
                    else
                    {
                        int flag = 0;
                        for (int i = 1; record_offset_front - SIZE_OF_SHORT * i >= SIZE_OF_SHORT; i++)
                        {
                            if (*(short* )(record + record_offset_front - SIZE_OF_SHORT * i) != -1)
                            {
                                varchar_length = *(short* )(record + record_offset_front) - *(short* )(record + record_offset_front - SIZE_OF_SHORT * i); 
                                flag = 1;
                                break;
                            }
                        }

                        if (flag == 0)
                            varchar_length = *(short* )(record + record_offset_front) - 1 - *(short* )(record) * SIZE_OF_SHORT; 
                    }  

                    *(short* )(data + data_offset) = varchar_length;
                    data_offset += SIZE_OF_INT; 
                    
                    memcpy(data + data_offset, record + record_offset_back, varchar_length); 
                    record_offset_back += varchar_length; 
                    data_offset += varchar_length; 
                }
            }
            record_offset_front += SIZE_OF_SHORT;
        }
        data_length = data_offset;
    }
    
    void data_to_record(const std::vector<Attribute> &recordDescriptor, const char *data, char* record, int& record_length)
    {
        int data_offset = 0; 
        int record_offset_front = 0;
        int record_offset_back = 0;  

        int field_num = recordDescriptor.size();
        //data_offset = field_num; 
        char* bitmap_ptr = nullptr; 

        *(short* )record = field_num;
        record_offset_front += SIZE_OF_SHORT; 
        record_offset_back += (field_num + 1) * SIZE_OF_SHORT; 
        data_offset += ceil((double)field_num / (double)8); 

        for (int i = 0; i < field_num; i++)
        {
            int bit_offset = i % 8; 
            int byte_offset = i / 8; 

            bitmap_ptr = (char* )data + byte_offset; 
            char result = (*(bitmap_ptr) >> (7 - bit_offset)) & 1; 

            if (result == 1)
            {
                *(short* )(record + record_offset_front) = -1; 
            }
            else if (result == 0)
            {                
                Attribute curr_attribute = recordDescriptor[i];
                if (curr_attribute.type == TypeInt)
                {
                    *(int* )(record + record_offset_back) = *(int* )(data + data_offset); 
                    record_offset_back += SIZE_OF_INT; 
                    data_offset += SIZE_OF_INT; 
                }     
                else if (curr_attribute.type == TypeReal)
                {
                    //float is same as int
                    *(int* )(record + record_offset_back) = *(int* )(data + data_offset); 
                    record_offset_back += SIZE_OF_INT; 
                    data_offset += SIZE_OF_INT; 
                }
                else if (curr_attribute.type == TypeVarChar)
                {
                    int varchar_length = *(int* )(data + data_offset); 
                    data_offset += SIZE_OF_INT; 
                    
                    memcpy(record + record_offset_back, data + data_offset, varchar_length); 
                    record_offset_back += varchar_length; 
                    data_offset += varchar_length; 
                }
                *(short* )(record + record_offset_front) = record_offset_back - 1; //point to the last char of the previous record, not the first char of the current record
            }
            record_offset_front += SIZE_OF_SHORT; 
        }
        record_length = record_offset_back;
    }

    RecordBasedFileManager &RecordBasedFileManager::instance() {
        static RecordBasedFileManager _rbf_manager = RecordBasedFileManager();
        return _rbf_manager;
    }

    RecordBasedFileManager::RecordBasedFileManager() = default;
    
    RecordBasedFileManager::~RecordBasedFileManager() = default;

    RecordBasedFileManager::RecordBasedFileManager(const RecordBasedFileManager &) = default;

    RecordBasedFileManager &RecordBasedFileManager::operator=(const RecordBasedFileManager &) = default;

    RC RecordBasedFileManager::createFile(const std::string &fileName) {
        return PagedFileManager::instance().createFile(fileName); 
    }

    RC RecordBasedFileManager::destroyFile(const std::string &fileName) {
        return PagedFileManager::instance().destroyFile(fileName);
    }

    RC RecordBasedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
        return PagedFileManager::instance().openFile(fileName, fileHandle);
            

        /*
        void* current_page = malloc(PAGE_SIZE); 
        for (int i = 0; i < fileHandle.getNumberOfPages(); i++)
        {
            if (fileHandle.readPage(i, current_page) == -1)
            {
                free(current_page);
                return -1; 
            }
            else
            {
                Page_info* page_info = (Page_info* )((char* )current_page + PAGE_SIZE - sizeof(Page_info)); 
                
                //fileHandle.page_dir.push_back(PAGE_SIZE - page_info->offset_to_free - (page_info->number_of_slot + 1) * sizeof(Record_info));
                //fileHandle.page_dir_pq.push(std::make_pair(PAGE_SIZE - page_info->offset_to_free - (page_info->number_of_slot + 1) * sizeof(Record_info), i));

                fileHandle.available_slots.push_back(std::set<int>()); 
                for (int j = 1; j <= page_info->number_of_slot; j++)
                {
                    Record_info* record_info = (Record_info* )((char* )current_page + PAGE_SIZE - sizeof(Page_info) - j * sizeof(Record_info));
                    if (record_info->offset_to_record == -1)
                        fileHandle.available_slots[i].insert(j); 
                }
            }
        }
        free(current_page);
        */
    }

    RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
        return PagedFileManager::instance().closeFile(fileHandle); 
    }

    RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const void *data, RID &rid) {
        
        if (fileHandle.getNumberOfPages() == 0)
            fileHandle.appendPage(nullptr);

        int record_length = 0;
        char* record = (char* )malloc(SIZE_OF_PAGE); 
        data_to_record(recordDescriptor, (const char* )data, record, record_length); 
        int current_page_number = fileHandle.getNumberOfPages() - 1;
        char* current_page = (char* )malloc(SIZE_OF_PAGE); 
        //For now we don't check previous pages.

        //the record is too large to fit; abort
        if (record_length >= SIZE_OF_PAGE - sizeof(Page_info) - sizeof(Record_info))
        {
            free(record); 
            free(current_page); 
            return -1;
        }
        if (fileHandle.readPage(current_page_number, current_page) != 0)
        {
            free(record); 
            free(current_page); 
            return -1;
        }
        Page_info* page_info = (Page_info* )((char* )current_page + PAGE_SIZE -  static_cast<int>(sizeof(Record_info))); 



        if (PAGE_SIZE - page_info->offset_to_free - (page_info->number_of_slot + 1) * static_cast<int>(sizeof(Record_info)) >= record_length + sizeof(Record_info))
        //do nothing: current page can hold the record
            ;
        
        else
        {
            int flag = 0; 
            
            /*
            for (int i = 0; i < fileHandle.getNumberOfPages(); i++)
            {
                if (fileHandle.page_dir[i] >= record_length + static_cast<int>(sizeof(Record_info)))
                {
                    if (fileHandle.readPage(i, current_page) != 0)
                    {
                        free(record); 
                        free(current_page); 
                        return -1;
                    }
                    flag = 1; 
                    current_page_number = i;
                }
            }
            */

            //we don't use this method when the number of pages is one.
            /*
            if (fileHandle.page_dir_pq.size() > 1)
            {
                std::pair<short, int> top_element = fileHandle.page_dir_pq.top(); 
                //can hold the record
                if (top_element.first > record_length + static_cast<int>(sizeof(Record_info)))
                {
                    current_page_number = top_element.second;
                    if (fileHandle.readPage(current_page_number, current_page) != 0)
                    {
                        free(record); 
                        free(current_page); 
                        return -1;
                    }
                    flag = 1;
                }
            }
            */
                
            if (flag == 0)
            {
                fileHandle.appendPage(nullptr);
                current_page_number = fileHandle.getNumberOfPages() - 1;
                if (fileHandle.readPage(current_page_number, current_page) != 0)
                {
                    free(record); 
                    free(current_page); 
                    return -1;
                }
            }
        }

        memcpy(current_page + page_info->offset_to_free, record, record_length);
        Record_info* record_info = nullptr;

        page_info->number_of_slot += 1;
        record_info = (Record_info* )((char* )current_page + PAGE_SIZE - sizeof(Page_info) - page_info->number_of_slot * sizeof(Record_info));
        rid.slotNum = page_info->number_of_slot;

        /*
        if (fileHandle.available_slots[current_page_number].size() == 0)
        {
            page_info->number_of_slot += 1;
            record_info = (Record_info* )((char* )current_page + PAGE_SIZE - sizeof(Page_info) - page_info->number_of_slot * sizeof(Record_info));
            rid.slotNum = page_info->number_of_slot;
        }

        else 
        {
            auto next_available_slot_number_iterator = fileHandle.available_slots[current_page_number].begin(); 
            int next_available_slot_number = *next_available_slot_number_iterator;
            record_info = (Record_info* )((char* )current_page + PAGE_SIZE - sizeof(Page_info) - next_available_slot_number * sizeof(Record_info));
            fileHandle.available_slots[current_page_number].erase(next_available_slot_number_iterator); 
            rid.slotNum = next_available_slot_number;
        }
        */

        record_info->offset_to_record = page_info->offset_to_free;
        record_info->record_length = record_length;
        page_info->offset_to_free += record_length;
        rid.pageNum = current_page_number; 
        
        //fileHandle.page_dir[current_page_number] = PAGE_SIZE - page_info->offset_to_free - (page_info->number_of_slot + 1) * sizeof(Record_info);
        /*
        std::pair<short, int> top_element = fileHandle.page_dir_pq.top(); 
        fileHandle.page_dir_pq.pop(); 
        top_element.first = PAGE_SIZE - page_info->offset_to_free - (page_info->number_of_slot + 1) * sizeof(Record_info);
        fileHandle.page_dir_pq.push(top_element);
        */

        if (fileHandle.writePage(rid.pageNum, current_page) != 0)
        {
            free(record); 
            free(current_page); 
            return -1;
        }
        free(record); 
        free(current_page); 
        return 0;
    }

    RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                          const RID &rid, void *data) {
        int data_length = 0;
        
        void* temp = malloc(SIZE_OF_PAGE); 
        memset(temp, 0, SIZE_OF_PAGE); 

        char* current_page = (char* )malloc(SIZE_OF_PAGE); 
        if (fileHandle.readPage(rid.pageNum, current_page) != 0)
            return -1; 
        
        Record_info* record_info = (Record_info* )((char* )current_page + PAGE_SIZE - sizeof(Page_info) - rid.slotNum * sizeof(Record_info));
        if (record_info->offset_to_record == -1)
            return -1; 
        
        if (record_info->record_length == -1)
        {
            RID* new_addr = (RID* )(current_page + record_info->offset_to_record); 
            return readRecord(fileHandle, recordDescriptor, *new_addr, data); 
        }

        record_to_data(recordDescriptor, (char* )temp, (char* )current_page + record_info->offset_to_record, data_length); 
        memcpy(data, temp, data_length); 
        
        free(current_page);
        free(temp);
        return 0;
    }

    RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const RID &rid) {
        
        //For now we assume that the record isn't a tomb:
        //We can use [offset_to_record == -1] to determine if a slot is deleted,
        //and use [record_length == -1] to determine if a slot is moved to another page.

        char* current_page = (char* )malloc(SIZE_OF_PAGE); 
        if (fileHandle.readPage(rid.pageNum, current_page) != 0)
            return -1; 
        
        //check if the slot is available for deletion
        Page_info* page_info = (Page_info* )((char* )current_page + PAGE_SIZE - sizeof(Page_info)); 
        if (page_info->number_of_slot < rid.slotNum)
            return -1; 

        Record_info* record_info = (Record_info* )((char* )current_page + PAGE_SIZE - sizeof(Page_info) - rid.slotNum * sizeof(Record_info));
        if (record_info->offset_to_record == -1)
            return -1; 
        if (record_info->record_length == -1)
        {
            RID* new_addr = (RID* )(current_page + record_info->offset_to_record); 
            return deleteRecord(fileHandle, recordDescriptor, *new_addr); 
        }

        //delete the slot
        int record_length = record_info->record_length;
        void* current_record = current_page + record_info->offset_to_record; 
        void* next_record = current_page + record_info->offset_to_record + record_info->record_length; 
        int length_of_rest_records = page_info->offset_to_free - (record_info->offset_to_record + record_info->record_length); 
        memmove(current_record, next_record, length_of_rest_records); 
        int prev_offset_to_record = record_info->offset_to_record; 
        record_info->offset_to_record = -1; 
        //fileHandle.available_slots[rid.pageNum].insert(rid.slotNum);  
        
        //change the offset of all other slots
        for (int i = 1; i <= page_info->number_of_slot; i++)
        {
            Record_info* current_record_info = (Record_info* )((char* )current_page + PAGE_SIZE - sizeof(Page_info) - i * sizeof(Record_info));
            if (current_record_info->offset_to_record > prev_offset_to_record)
                current_record_info->offset_to_record -= record_length; 
        }

        if (fileHandle.writePage(rid.pageNum, current_page) != 0)
        {
            free(current_page); 
            return -1;
        }

        free(current_page); 
        return 0; 
    }

    RC RecordBasedFileManager::printRecord(const std::vector<Attribute> &recordDescriptor, const void *data,
                                           std::ostream &out) {

        int data_offset = 0; 
        int field_num = recordDescriptor.size();
        char* bitmap_ptr = nullptr; 
        data_offset += ceil((double)field_num / (double)8); 

        for (int i = 0; i < field_num; i++)
        {
            Attribute curr_attribute = recordDescriptor[i];
            out<<curr_attribute.name<<":"<<" "; 

            int bit_offset = i % 8; 
            int byte_offset = i / 8; 

            bitmap_ptr = (char* )data + byte_offset; 
            char result = (*(bitmap_ptr) >> (7 - bit_offset)) & 1; 

            if (result == 1)
            {
                out<<"NULL";
            }
            else if (result == 0)
            {                
                if (curr_attribute.type == TypeInt)
                {
                    out<<*(int* )((char* )data + data_offset);
                    data_offset += SIZE_OF_INT; 
                }     
                else if (curr_attribute.type == TypeReal)
                {
                    //float is same as int
                    out<<*(float* )((char* )data + data_offset);
                    data_offset += SIZE_OF_INT; 
                }
                else if (curr_attribute.type == TypeVarChar)
                {
                    int varchar_length = *(int* )((char* )data + data_offset); 
                    data_offset += SIZE_OF_INT; 
                    
                    std::string output((char* )data + data_offset, varchar_length);
                    out<<output;
                    data_offset += varchar_length; 
                }
            }
            if (i != field_num - 1)
                out<<","<<" ";
            else
                out<<" ";
        }
        return 0; 
    }

    RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const void *data, const RID &rid) {
        
        int record_length = 0;
        char* record = (char* )malloc(SIZE_OF_PAGE); 
        data_to_record(recordDescriptor, (const char* )data, record, record_length); 
        
        char* current_page = (char* )malloc(SIZE_OF_PAGE); 
        if (fileHandle.readPage(rid.pageNum, current_page) != 0)
            return -1;
        Page_info* page_info = (Page_info* )((char* )current_page + PAGE_SIZE - sizeof(Page_info)); 
        if (page_info->number_of_slot < rid.slotNum)
            return -1; 
        Record_info* record_info = (Record_info* )((char* )current_page + PAGE_SIZE - sizeof(Page_info) - rid.slotNum * sizeof(Record_info));
        if (record_info->offset_to_record == -1)
            return -1; 

        //if the current page can hold
        if (static_cast<int>(PAGE_SIZE - page_info->offset_to_free - (page_info->number_of_slot + 1) * sizeof(Record_info)) >= static_cast<int>(record_length - record_info->record_length))
        {
            //prepare for "move"
            void* current_record = current_page + record_info->offset_to_record; 
            void* next_record = current_page + record_info->offset_to_record + record_info->record_length; 
            void* new_place_for_next_record = next_record - (record_info->record_length - record_length);
            int length_of_rest_records = page_info->offset_to_free - (record_info->offset_to_record + record_info->record_length); 
            //move
            memmove(new_place_for_next_record, next_record, length_of_rest_records); 
            memcpy((char* )current_page + record_info->offset_to_record, record, record_length);
            //change the offset of all other slots
            change_offset_for_slot_after_current(current_page, record_length, page_info, record_info); 
            //change the slot & page descriptors
            page_info->offset_to_free -= (record_info->record_length - record_length); 
            record_info->record_length = record_length; 
        }
        //else
        else
        {
            void* next_record = current_page + record_info->offset_to_record + record_info->record_length; 
            //Use 8 bytes to hold pageNum and slotNum.
            void* new_place_for_next_record = current_page + record_info->offset_to_record + sizeof(RID);
            int length_of_rest_records = page_info->offset_to_free - (record_info->offset_to_record + record_info->record_length); 
            memmove(new_place_for_next_record, next_record, length_of_rest_records); 

            //change the offset of all other slots
            change_offset_for_slot_after_current(current_page, sizeof(RID), page_info, record_info); 
            page_info->offset_to_free -= (record_info->record_length - sizeof(RID)); 
            //Indicate we have a redirection.
            record_info->record_length = -1; 
            RID* new_addr = (RID* )(current_page + record_info->offset_to_record); 
            RID new_rid; 
            if (insertRecord(fileHandle, recordDescriptor, data, new_rid) != 0)
            {
                free(current_page); 
                free(record); 
                return -1; 
            }    
            new_addr->pageNum = new_rid.pageNum;
            new_addr->slotNum = new_rid.slotNum; 
        }
        
        return fileHandle.writePage(rid.pageNum, current_page); 
    }

    RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                             const RID &rid, const std::string &attributeName, void *data) {
        int data_length = 0;
        
        void* temp = malloc(SIZE_OF_PAGE); 
        memset(temp, 0, SIZE_OF_PAGE); 

        char* current_page = (char* )malloc(SIZE_OF_PAGE); 
        if (fileHandle.readPage(rid.pageNum, current_page) != 0)
            return -1;
        
        Record_info* record_info = (Record_info* )((char* )current_page + PAGE_SIZE - sizeof(Page_info) - rid.slotNum * sizeof(Record_info));
        if (record_info->offset_to_record == -1)
            return -1; 
        
        if (record_info->record_length == -1)
        {
            RID* new_addr = (RID* )(current_page + record_info->offset_to_record); 
            return readAttribute(fileHandle, recordDescriptor, *new_addr, attributeName, data); 
        }

        if (attribute_to_data(recordDescriptor, (char* )temp, (char* )current_page + record_info->offset_to_record, data_length, attributeName) == -1)
            return -1; 
        memcpy(data, temp, data_length); 
        
        free(current_page);
        free(temp);
        return 0; 
    }

    RC RecordBasedFileManager::scan(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                    const std::string &conditionAttribute, const CompOp compOp, const void *value,
                                    const std::vector<std::string> &attributeNames,
                                    RBFM_ScanIterator &rbfm_ScanIterator) {
        
        rbfm_ScanIterator.file_handle = fileHandle; 
        rbfm_ScanIterator.recordDescriptor = recordDescriptor; 
        rbfm_ScanIterator.conditionAttribute = conditionAttribute; 
        rbfm_ScanIterator.compOp = compOp; 
        rbfm_ScanIterator.value = (void* )value;
        rbfm_ScanIterator.attributeNames = attributeNames; 
        rbfm_ScanIterator._current_page = nullptr;

        RID first_rid;
        first_rid.pageNum = 0; 
        first_rid.slotNum = 0; 
        char* current_page = (char* )malloc(SIZE_OF_PAGE);
        Record_info* record_info = nullptr;
        Page_info* page_info = nullptr;
        rbfm_ScanIterator.current_rid = first_rid; 

        for (int i = 0; i < recordDescriptor.size(); i++)
        {
            if (recordDescriptor[i].name == conditionAttribute)
                rbfm_ScanIterator.conditionAttribute_full = recordDescriptor[i];
        }
        
        std::vector<int> index_of_attributes; 
        for (auto& i : attributeNames)
        {
            for (int j = 0; j < recordDescriptor.size(); j++)
            {
                if (i == recordDescriptor[j].name)
                    index_of_attributes.push_back(j); 
            }
        }

        if (index_of_attributes.size() != attributeNames.size())
            return -1;  
        rbfm_ScanIterator.index_of_attributes = index_of_attributes; 
        return 0; 
    }

    RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data) 
    { 
        //char* current_page = (char* )malloc(SIZE_OF_PAGE);
        if (_current_page == nullptr)
        {
            _current_page = malloc(PAGE_SIZE); 
            if (file_handle.readPage(0, _current_page) != 0)
            {
                //free(_current_page);
                return -1; 
            }
        }

        //void* data = malloc(PAGE_SIZE); 
        Record_info* record_info = nullptr;
        Page_info* page_info = nullptr;
        RID next_rid_bar;
        next_rid_bar.pageNum = current_rid.pageNum; 
        next_rid_bar.slotNum = current_rid.slotNum + 1; 
        int flag_terminate = 0; 
        int i = -1; 
        int j = -1; 
        
        for (i = next_rid_bar.pageNum; i < file_handle.getNumberOfPages(); i++)
        {
            /*
            if (file_handle.readPage(i, current_page) != 0)
            {
                free(current_page); 
                return -1;
            }  
            */  
            page_info = (Page_info* )((char* )_current_page + PAGE_SIZE - sizeof(Page_info)); 
            
            for (j = next_rid_bar.slotNum; j <= page_info->number_of_slot; j++)
            {   
                record_info = (Record_info* )((char* )_current_page + PAGE_SIZE - sizeof(Page_info) - j * sizeof(Record_info));
                
                if (record_info->offset_to_record != -1 && record_info->record_length != -1)
                {
                    //next_rid.pageNum = i; 
                    //next_rid.slotNum = j; 

                    if (compOp == CompOp::NO_OP)
                    {
                        flag_terminate = 1;
                        break;
                    }
                    
                    RID rid_tmp;
                    rid_tmp.pageNum = i;
                    rid_tmp.slotNum = j; 

                    if (RecordBasedFileManager::instance().readAttribute(file_handle, recordDescriptor, rid_tmp, conditionAttribute, data) == -1)
                    {
                        return -1; 
                    }
                    if (*(char*)data != 0)
                    {
                        continue;
                    }

                    if (conditionAttribute_full.type == TypeVarChar)
                    {
                        std::string first_string; 
                        std::string second_string;
        
                        //int first_string_length = *(int* )(value + 1);
                        int first_string_length = *(int* )value;
                        //first_string.assign(1 + SIZE_OF_INT + (char* )value, first_string_length); 
                        first_string.assign(SIZE_OF_INT + (char* )value, first_string_length); 
                        int second_string_length = *(int* )(data + 1);
                        second_string.assign(1 + SIZE_OF_INT + (char* )data, second_string_length);

                        if (compOp == CompOp::EQ_OP && second_string == first_string)
                            flag_terminate = 1; 
                        else if (compOp == CompOp::NE_OP && second_string != first_string)
                            flag_terminate = 1;
                        else if (compOp == CompOp::GE_OP && second_string >= first_string)
                            flag_terminate = 1; 
                        else if (compOp == CompOp::GT_OP && second_string > first_string)
                            flag_terminate = 1; 
                        else if (compOp == CompOp::LE_OP && second_string <= first_string)
                            flag_terminate = 1; 
                        else if (compOp == CompOp::LT_OP && second_string < first_string)
                            flag_terminate = 1; 
                    }
                    else
                    {
                        int first_int = *(int* )value; 
                        int second_int = *(int* )(data + 1); 
                        
                        if (compOp == CompOp::EQ_OP && second_int == first_int)
                            flag_terminate = 1; 
                        else if (compOp == CompOp::NE_OP && second_int != first_int)
                            flag_terminate = 1;
                        else if (compOp == CompOp::GE_OP && second_int >= first_int)
                            flag_terminate = 1; 
                        else if (compOp == CompOp::GT_OP && second_int > first_int)
                            flag_terminate = 1; 
                        else if (compOp == CompOp::LE_OP && second_int <= first_int)
                            flag_terminate = 1; 
                        else if (compOp == CompOp::LT_OP && second_int < first_int)
                            flag_terminate = 1; 
                        /*
                        else if (compOp == CompOp::NO_OP)
                            flag_terminate = 1;
                        */
                    }
                    if (flag_terminate == 1)
                        break; 
                }
            }
            if (flag_terminate == 1)
                break; 
            next_rid_bar.pageNum += 1; 
            next_rid_bar.slotNum = 1;
            if (file_handle.readPage(next_rid_bar.pageNum, _current_page) != 0)
            {
                //free(_current_page);
                return -1; 
            }
        }
        //if (next_rid.pageNum == -1 || next_rid.slotNum == -1)
        if (flag_terminate == 0)
        {
            //free(_current_page);
            return -1; 
        }

        current_rid.pageNum = i; 
        current_rid.slotNum = j;
        
        record_info = (Record_info* )((char* )_current_page + PAGE_SIZE - sizeof(Page_info) - current_rid.slotNum * sizeof(Record_info));
        int data_length; 
        attributes_to_data(index_of_attributes, recordDescriptor, (char* )data, (char* )_current_page + record_info->offset_to_record, data_length);

        rid = current_rid;
        //free(_current_page);
        return 0; 
    }

} // namespace PeterDB

