#include "src/include/pfm.h"
#include "test/utils/pfm_test_utils.h"

namespace PeterDBTesting {
    TEST_F (PFM_Private_Test, check_page_num_after_appending) {
        // Test case procedure:
        // 1. Append 39 Pages
        // 2. Check Page Number after each append
        // 3. Keep the file for the next test case

        size_t currentFileSize = getFileSize(fileName);
        inBuffer = malloc(PAGE_SIZE);
        int numPages = 39;
        for (int i = 1; i <= numPages; i++) {
            generateData(inBuffer, PAGE_SIZE, 53 + i, 47 - i);
            ASSERT_EQ(fileHandle.appendPage(inBuffer), success) << "Appending a page should succeed.";
            ASSERT_TRUE(getFileSize(fileName) % PAGE_SIZE == 0) << "File should be based on PAGE_SIZE.";
            ASSERT_GT(getFileSize(fileName), currentFileSize) << "File size should have been increased";
            currentFileSize = getFileSize(fileName);
            ASSERT_EQ(fileHandle.getNumberOfPages(), i)
                                        << "The page count should be " << i << " at this moment";
        }
        destroyFile = false;

    }

    TEST_F (PFM_Private_Test, check_page_num_after_writing) {
        // Test case procedure:
        // 1. Overwrite the 39 Pages from the previous test case
        // 2. Check Page Number after each write
        // 3. Keep the file for the next test case

        inBuffer = malloc(PAGE_SIZE);
        int numPages = 39;
        size_t fileSizeAfterAppend = getFileSize(fileName);
        for (int i = 0; i < numPages; i++) {
            std::cout<<i<<std::endl;
            generateData(inBuffer, PAGE_SIZE, 47 + i, 53 - i);
            ASSERT_EQ(fileHandle.writePage(i, inBuffer), success) << "Writing a page should succeed.";
            ASSERT_TRUE(getFileSize(fileName) % PAGE_SIZE == 0) << "File should be based on PAGE_SIZE.";
            ASSERT_EQ(getFileSize(fileName), fileSizeAfterAppend) << "File size should not have been increased";
            ASSERT_EQ(fileHandle.getNumberOfPages(), numPages) << "The page count should not have been increased";
        }
        destroyFile = false;
    }

    TEST_F (PFM_Private_Test, check_page_num_after_reading) {
        // Test case procedure:
        // 1. Read the 39 Pages from the previous test case
        // 2. Check Page Number after each read

        inBuffer = malloc(PAGE_SIZE);
        outBuffer = malloc(PAGE_SIZE);
        int numPages = 39;
        size_t fileSizeAfterAppend = getFileSize(fileName);
        for (int i = 0; i < numPages; i++) {
            generateData(inBuffer, PAGE_SIZE, 47 + i, 53 - i);
            ASSERT_EQ(fileHandle.readPage(i, outBuffer), success) << "Reading a page should succeed.";
            ASSERT_TRUE(getFileSize(fileName) % PAGE_SIZE == 0) << "File should be based on PAGE_SIZE.";
            ASSERT_EQ(fileHandle.getNumberOfPages(), numPages) << "The page count should not have been increased.";
            ASSERT_EQ(getFileSize(fileName), fileSizeAfterAppend) << "File size should not have been increased.";
            ASSERT_EQ(memcmp(inBuffer, outBuffer, PAGE_SIZE), 0)
                                        << "Checking the integrity of the page should succeed.";
        }
    }

    TEST_F (PFM_Private_Test, check_counters) {
        // Functions Tested:
        // 1. Open File
        // 2. Append Page
        // 3. Close File
        // 4. Open File again
        // 5. Get Number Of Pages
        // 6. Get Counter Values
        // 7. Close File

        unsigned readPageCount = 0;
        unsigned writePageCount = 0;
        unsigned appendPageCount = 0;
        unsigned readPageCount1 = 0;
        unsigned writePageCount1 = 0;
        unsigned appendPageCount1 = 0;


        // Collect before counters
        ASSERT_EQ(fileHandle.collectCounterValues(readPageCount, writePageCount, appendPageCount), success)
                                    << "Collecting counters should succeed.";

        // Append the first page read the first page write the first page append the second page
        inBuffer = malloc(PAGE_SIZE);
        outBuffer = malloc(PAGE_SIZE);
        generateData(inBuffer, 23, 15);
        ASSERT_EQ(fileHandle.appendPage(inBuffer), success) << "Appending a page should succeed.";
        ASSERT_EQ(fileHandle.readPage(0, outBuffer), success) << "Reading a page should succeed.";
        generateData(inBuffer, 27, 30);
        ASSERT_EQ(fileHandle.writePage(0, inBuffer), success) << "Writing a page should succeed.";
        generateData(inBuffer, 30, 26);
        ASSERT_EQ(fileHandle.appendPage(inBuffer), success) << "Appending a page should succeed.";

        // Get the number of pages
        ASSERT_EQ(fileHandle.getNumberOfPages(), 2) << "There should be 2 pages at this moment.";

        // Get number of hidden pages used
        ASSERT_TRUE(getFileSize(fileName) % PAGE_SIZE == 0) << "File should be based on PAGE_SIZE.";
        unsigned numOfHiddenPage = getFileSize(fileName) / PAGE_SIZE - fileHandle.getNumberOfPages();

        // collect after counters
        ASSERT_EQ(fileHandle.collectCounterValues(readPageCount1, writePageCount1, appendPageCount1), success)
                                    << "Collecting counters should succeed.";
        ASSERT_TRUE(readPageCount1 - readPageCount >= 1 && readPageCount1 - readPageCount <= 1 + 4 * numOfHiddenPage) <<
                                                                                                                      "Read counter should be correct.";
        ASSERT_TRUE(
                writePageCount1 - writePageCount >= 1 && writePageCount1 - writePageCount <= 1 + 4 * numOfHiddenPage)
                                    << "Write counter should be correct.";
        ASSERT_TRUE(
                appendPageCount1 - appendPageCount >= 2 && appendPageCount1 - appendPageCount <= 2 + numOfHiddenPage) <<
                                                                                                                      "Append counter should be correct.";

        reopenFile();

        // collect after counters
        ASSERT_EQ(fileHandle.collectCounterValues(readPageCount, writePageCount, appendPageCount), success)
                                    << "Collecting counters should succeed.";
        
        std::cout<<readPageCount - readPageCount1<<std::endl;
        std::cout<<numOfHiddenPage<<std::endl;

        ASSERT_TRUE(readPageCount - readPageCount1 >= 1 && readPageCount - readPageCount1 <= numOfHiddenPage)
                                    << "Read counter should be correct.";
        ASSERT_TRUE(writePageCount - writePageCount1 >= 0 && writePageCount - writePageCount1 <= numOfHiddenPage)
                                    << "Write counter should be correct.";
        ASSERT_EQ(appendPageCount, appendPageCount1) << "Append counter should be correct.";

        ASSERT_GT(getFileSize(fileName), 0) << "File Size should not be zero at this moment.";
    }

}
