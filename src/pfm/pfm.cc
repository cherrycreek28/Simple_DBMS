#include "src/include/pfm.h"

namespace PeterDB {
    PagedFileManager &PagedFileManager::instance() {
        static PagedFileManager _pf_manager = PagedFileManager();
        return _pf_manager;
    }

    PagedFileManager::PagedFileManager() = default;

    PagedFileManager::~PagedFileManager() = default;

    PagedFileManager::PagedFileManager(const PagedFileManager &) = default;

    PagedFileManager &PagedFileManager::operator=(const PagedFileManager &) = default;

    RC PagedFileManager::createFile(const std::string &fileName) {
        //std::cout<<"inside createFile\n\n\n\n\n";
        FILE* f = nullptr; 
        if (f = fopen(fileName.c_str(), "r"))
        {
            fclose(f); 
            return -1; 
        }
        else
        {
            f = fopen(fileName.c_str(), "w+"); 
            
            //initialize the first page for counters;
            void* prev = malloc(PAGE_SIZE); 
            signed* first_page = (signed* )prev; 

            *first_page = 0; 
            *(++first_page) = 0; 
            *(++first_page) = 0; 
            //*(++first_page) = 0; 
            
            fseek(f, 0, SEEK_SET); 

            if (fwrite(prev, PAGE_SIZE, 1, f) != 1)
                return -1;
            fclose(f);
            free(prev);
            return 0; 
        }
    }

    RC PagedFileManager::destroyFile(const std::string &fileName) {
        FILE* f = nullptr; 
        if (!(f = fopen(fileName.c_str(), "r")))
        {
            return -1; 
        }
        else
        {
            remove(fileName.c_str()); 
            return 0; 
        }
    }

    RC PagedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
        //check if the file handle is already in use
        FILE* f = nullptr;
        if (fileHandle.show_file_name() != "")
            return -1; 
        //check if the file already exists
        if (!(f = fopen(fileName.c_str(), "r+")))
            return -1;
        
        fileHandle.assign_file(f, fileName); 

        void* prev = malloc(PAGE_SIZE); 
        signed* first_page = (signed* )prev; 

        if (fileHandle.readPage(-1, prev) == -1)
            return -1; 
        
        //'1' here represents the previous 'readPage' function.
        fileHandle.readPageCounter = 1 + *first_page;
        fileHandle.writePageCounter = *(++first_page);
        fileHandle.appendPageCounter = *(++first_page);

        free(prev);

        /*
        //to pass all pfm tests, you have to comment out these code
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
                fileHandle.page_dir.push_back(PAGE_SIZE - page_info->offset_to_free - (page_info->number_of_slot + 1) * sizeof(Record_info));
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

        return 0; 
    }

    RC PagedFileManager::closeFile(FileHandle &fileHandle) {
        FILE* f = fileHandle.show_file_ptr(); 
        
        void* prev = malloc(PAGE_SIZE); 
        signed* first_page = (signed* )prev; 
        
        *first_page = fileHandle.readPageCounter; 
        *(++first_page) = fileHandle.writePageCounter; 
        *(++first_page) = fileHandle.appendPageCounter; 
        //*(++first_page) = fileHandle._total_page_num; 
        
        fseek(f, 0, SEEK_SET); 
        
        if (fwrite(prev, PAGE_SIZE, 1, f) != 1)
            return -1; 

        free(prev); 
        return fclose(f); 
    }

    FileHandle::FileHandle() {
        readPageCounter = 0;
        writePageCounter = 0;
        appendPageCounter = 0;
    }

    FileHandle::~FileHandle() = default;

    RC FileHandle::readPage(PageNum pageNum, void *data) {
        if (pageNum + 1 > appendPageCounter)
            return -1;

        int rc = 0; 
        rc = fseek(_f, (pageNum + 1) * PAGE_SIZE, SEEK_SET);
        if (rc != 0)
            return -1; 
        if (fread(data, PAGE_SIZE, 1, _f) != 1)
            return -1;
        readPageCounter++;
        return 0; 
    }

    RC FileHandle::writePage(PageNum pageNum, const void *data) {
        //if (pageNum + 1 > _total_page_num)
        if (pageNum + 1 > appendPageCounter)
            return -1;

        int rc = 0; 
        rc = fseek(_f, (pageNum + 1) * PAGE_SIZE, SEEK_SET);
        if (rc != 0)
            return -1; 
        if (fwrite(data, PAGE_SIZE, 1, _f) != 1)
            return -1;
        writePageCounter++;
        return 0; 
    }

    RC FileHandle::appendPage(const void *data) {
        int rc = 0; 
        rc = fseek(_f, (appendPageCounter + 1) * PAGE_SIZE, SEEK_SET);
        if (rc != 0)
            return -1; 
        if (data == nullptr)
        {
            void* temp = malloc(PAGE_SIZE); 
            memset(temp, 0, PAGE_SIZE);
            if (fwrite(temp, PAGE_SIZE, 1, _f) != 1)
                return -1;
            free(temp);
        }
        else
        {
            if (fwrite(data, PAGE_SIZE, 1, _f) != 1)
                return -1;
        }
        appendPageCounter++;
        //page_dir.push_back(0);
        //page_dir_pq.push(std::make_pair(0, getNumberOfPages() - 1)); 

        //available_slots.push_back(std::set<int>()); 
        //_total_page_num++;
        return 0; 
    }

    unsigned FileHandle::getNumberOfPages() {
        return appendPageCounter; 
    }

    RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
        readPageCount = readPageCounter;
        writePageCount = writePageCounter;
        appendPageCount = appendPageCounter;

        return 0; 
    }

    std::string FileHandle::show_file_name()
    {
        return _file_name;
    }

    void FileHandle::assign_file(FILE* f, std::string file_name)
    {
        _f = f; 
        _file_name = file_name; 
    }

    FILE* FileHandle::show_file_ptr()
    {
        return _f;
    }
} // namespace PeterDB