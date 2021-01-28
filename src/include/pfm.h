#ifndef _pfm_h_
#define _pfm_h_

#define PAGE_SIZE 4096

#include <vector>
#include <errno.h>
#include <string>
#include <stdio.h>
#include <string.h>
#include <iostream>
#include <stack>
#include <set>
#include <queue>  

typedef struct
{
    short number_of_slot;       
    short offset_to_free; 
}Page_info; 

typedef struct 
{
    short offset_to_record; 
    short record_length; 
}Record_info;


namespace PeterDB {


    typedef unsigned PageNum;
    typedef int RC;

    class FileHandle;

    class PagedFileManager {
    public:
        static PagedFileManager &instance();                                // Access to the singleton instance

        RC createFile(const std::string &fileName);                         // Create a new file
        RC destroyFile(const std::string &fileName);                        // Destroy a file
        RC openFile(const std::string &fileName, FileHandle &fileHandle);   // Open a file
        RC closeFile(FileHandle &fileHandle);                               // Close a file

    protected:
        PagedFileManager();                                                 // Prevent construction
        ~PagedFileManager();                                                // Prevent unwanted destruction
        PagedFileManager(const PagedFileManager &);                         // Prevent construction by copying
        PagedFileManager &operator=(const PagedFileManager &);              // Prevent assignment

    };

    class FileHandle {
    public:
        // variables to keep the counter for each operation
        unsigned readPageCounter;
        unsigned writePageCounter;
        unsigned appendPageCounter;
        unsigned _total_page_num; 
        //std::vector<short> page_dir; 
        
        //avaiable bytes: short type
        //page_num: int type
        //std::priority_queue<std::pair<short, int>> page_dir_pq; 
        //std::vector<std::set<int>> available_slots; 

        FileHandle();                                                       // Default constructor
        ~FileHandle();                                                      // Destructor

        RC readPage(PageNum pageNum, void *data);                           // Get a specific page
        RC writePage(PageNum pageNum, const void *data);                    // Write a specific page
        RC appendPage(const void *data);                                    // Append a specific page
        unsigned getNumberOfPages();                                        // Get the number of pages in the file
        RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount,
                                unsigned &appendPageCount);                 // Put current counter values into variables
        
        
        FILE* show_file_ptr(); 
        std::string show_file_name(); 
        void assign_file(FILE* f, std::string file_name); 

    private:
        std::string _file_name; 
        FILE* _f; 
    };

} // namespace PeterDB

#endif // _pfm_h_