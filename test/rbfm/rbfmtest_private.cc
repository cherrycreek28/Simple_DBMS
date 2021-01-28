#include "src/include/rbfm.h"
#include "test/utils/rbfm_test_utils.h"

namespace PeterDBTesting {
    TEST_F(RBFM_Private_Test, varchar_compact_size) {
        // Checks whether VarChar is implemented correctly or not.
        //
        // Functions tested
        // 1. Create Two Record-Based File
        // 2. Open Two Record-Based File
        // 3. Insert Multiple Records Into Two files
        // 4. Close Two Record-Based File
        // 5. Compare The File Sizes
        // 6. Destroy Two Record-Based File

        std::string fileNameLarge = fileName + "_large";
        if (!fileExists(fileNameLarge)) {
            // Create a file
            ASSERT_EQ(rbfm.createFile(fileNameLarge), success) << "Creating the file should succeed: " << fileName;
            ASSERT_TRUE(fileExists(fileNameLarge)) << "The file is not found: " << fileName;

        }

        // Open the file
        PeterDB::FileHandle fileHandleLarge;
        ASSERT_EQ(rbfm.openFile(fileNameLarge, fileHandleLarge), success)
                                    << "Opening the file should succeed: " << fileName;

        inBuffer = malloc(PAGE_SIZE);
        memset(inBuffer, 0, PAGE_SIZE);

        int numRecords = 16000;

        std::vector<PeterDB::Attribute> recordDescriptor, recordDescriptorLarge;

        // Each varchar field length - 200
        createRecordDescriptorForTwitterUser(recordDescriptor);

        // Each varchar field length - 800
        createRecordDescriptorForTwitterUser2(recordDescriptorLarge);

        // NULL field indicator
        nullsIndicator = initializeNullFieldsIndicator(recordDescriptor);

        PeterDB::RID rid;

        // Insert records into file
        for (unsigned i = 0; i < numRecords; i++) {
            // Test insert Record
            size_t size;
            memset(inBuffer, 0, 3000);
            prepareLargeRecordForTwitterUser(recordDescriptor.size(), nullsIndicator, i, inBuffer, size);

            ASSERT_EQ(rbfm.insertRecord(fileHandle, recordDescriptor, inBuffer, rid), success)
                                        << "Inserting a record should succeed.";
            ASSERT_EQ(rbfm.insertRecord(fileHandleLarge, recordDescriptorLarge, inBuffer, rid), success)
                                        << "Inserting a record should succeed.";

            if (i % 1000 == 0 && i != 0) {
                GTEST_LOG_(INFO) << i << "/" << numRecords << " records are inserted.";
                ASSERT_TRUE(compareFileSizes(fileName, fileNameLarge)) << "Files should be the same size";
            }

        }

        // Close the file
        ASSERT_EQ(rbfm.closeFile(fileHandleLarge), success) << "Closing the file should succeed.";

        // Destroy the file
        ASSERT_EQ(rbfm.destroyFile(fileNameLarge), success) << "Destroying the file should succeed.";
    }

    TEST_F(RBFM_Private_Test, insert_records_with_empty_and_null_varchar) {
        // Functions Tested:
        // 1. Create File - RBFM
        // 2. Open File
        // 3. insertRecord() - with an empty VARCHAR field (not NULL)
        // 4. insertRecord() - with a NULL VARCHAR field
        // 5. Close File
        // 6. Destroy File

        PeterDB::RID rid;
        size_t recordSize;
        inBuffer = malloc(2000);
        outBuffer = malloc(2000);
        memset(inBuffer, 0, 2000);
        memset(outBuffer, 0, 2000);

        std::vector<PeterDB::Attribute> recordDescriptor;
        createRecordDescriptorForTweetMessage(recordDescriptor);

        // NULL field indicator
        nullsIndicator = initializeNullFieldsIndicator(recordDescriptor);

        // Insert a record into a file - referred_topics is an empty string - "", not null value.
        prepareRecordForTweetMessage(recordDescriptor.size(), nullsIndicator, 1234, 0, "", 0, "", 999, 0, "",
                                     inBuffer, recordSize);
        ASSERT_EQ(rbfm.insertRecord(fileHandle, recordDescriptor, inBuffer, rid), success)
                                    << "Inserting a record should succeed.";

        // Given the rid, read the record from file
        ASSERT_EQ(rbfm.readRecord(fileHandle, recordDescriptor, rid,
                                  outBuffer), success) << "Reading a record should succeed.";

        // Compare whether the two memory blocks are the same
        ASSERT_EQ(memcmp(inBuffer, outBuffer, recordSize), 0) << "Reading empty VARCHAR incorrectly.";

        // An empty string should be printed for the referred_topics field.
        std::stringstream stream;
        ASSERT_EQ(rbfm.printRecord(recordDescriptor, outBuffer, stream), success)
                                    << "Printing a record should succeed.";
        std::cout<<stream.str()<<std::endl;
        checkPrintRecord("tweetid: 1234, referred_topics: , message_text: , userid: 999, hash_tags: ", stream.str());

        memset(inBuffer, 0, 2000);

        free(nullsIndicator);
        nullsIndicator = initializeNullFieldsIndicator(recordDescriptor);
        setAttrNull(nullsIndicator, 1, true);
        setAttrNull(nullsIndicator, 4, true);

        // Insert a record
        prepareRecordForTweetMessage(recordDescriptor.size(), nullsIndicator, 1234, 0, "", 0, "", 999, 0, "", inBuffer,
                                     recordSize);

        ASSERT_EQ(rbfm.insertRecord(fileHandle, recordDescriptor, inBuffer, rid), success)
                                    << "Inserting a record should succeed.";

        // Given the rid, read the record from file
        ASSERT_EQ(rbfm.readRecord(fileHandle, recordDescriptor, rid, outBuffer), success)
                                    << "Reading a record should succeed.";

        // Compare whether the two memory blocks are the same
        ASSERT_EQ(memcmp(inBuffer, outBuffer, recordSize), 0) << "Reading NULL VARCHAR incorrectly.";

        // An NULL should be printed for the referred_topics field.
        stream.str(std::string());
        stream.clear();
        ASSERT_EQ(rbfm.printRecord(recordDescriptor, outBuffer, stream), success)
                                    << "Printing a record should succeed.";
        checkPrintRecord("tweetid: 1234, referred_topics: NULL, message_text: , userid: 999, hash_tags: NULL",
                         stream.str());

        ASSERT_GT(getFileSize(fileName), 0) << "File Size should not be zero at this moment.";

    }

    TEST_F(RBFM_Private_Test, insert_records_with_all_nulls) {
        // Functions Tested:
        // 1. Create File - RBFM
        // 2. Open File
        // 3. insertRecord() - with all NULLs
        // 4. Close File
        // 5. Destroy File

        PeterDB::RID rid;
        size_t recordSize;
        inBuffer = malloc(2000);
        outBuffer = malloc(2000);
        memset(inBuffer, 0, 2000);
        memset(outBuffer, 0, 2000);

        std::vector<PeterDB::RID> rids;

        std::vector<PeterDB::Attribute> recordDescriptor;
        createRecordDescriptorForTweetMessage(recordDescriptor);

        // NULL field indicator
        nullsIndicator = initializeNullFieldsIndicator(recordDescriptor);

        // set all fields as NULL
        nullsIndicator[0] = 248; // 11111000

        // Insert a record into a file
        prepareRecordForTweetMessage(recordDescriptor.size(), nullsIndicator, 1234, 9, "wildfires", 42,
                                     "Curious ... did the amazon wildfires stop?", 999, 3, "wow", inBuffer,
                                     recordSize);

        ASSERT_EQ(rbfm.insertRecord(fileHandle, recordDescriptor, inBuffer, rid), success)
                                    << "Inserting a record should succeed.";

        rids.push_back(rid);
        writeRIDsToDisk(rids);
        destroyFile = false;

    }

    TEST_F(RBFM_Private_Test, read_records_with_all_nulls) {

        PeterDB::RID rid;
        size_t recordSize = 0;
        inBuffer = malloc(2000);
        outBuffer = malloc(2000);
        memset(inBuffer, 0, 2000);
        memset(outBuffer, 0, 2000);

        std::vector<PeterDB::RID> rids;

        readRIDsFromDisk(rids, 1);
        rid = rids[0];

        std::vector<PeterDB::Attribute> recordDescriptor;
        createRecordDescriptorForTweetMessage(recordDescriptor);

        // NULL field indicator
        nullsIndicator = initializeNullFieldsIndicator(recordDescriptor);

        // set all fields as NULL
        nullsIndicator[0] = 248; // 11111000

        // Insert a record into a file
        prepareRecordForTweetMessage(recordDescriptor.size(), nullsIndicator, 1234, 9, "wildfires", 43,
                                     "Curious ... did the amazon wildfires stop?", 999, 3, "wow", inBuffer,
                                     recordSize);

        // Given the rid, read the record from file
        ASSERT_EQ(rbfm.readRecord(fileHandle, recordDescriptor, rid, outBuffer), success)
                                    << "Reading a record should succeed.";

        // An empty string should be printed for the referred_topics field.
        std::stringstream stream;
        ASSERT_EQ(rbfm.printRecord(recordDescriptor, outBuffer, stream), success)
                                    << "Printing a record should succeed.";
        checkPrintRecord("tweetid: NULL, referred_topics: NULL, message_text: NULL, userid: NULL, hash_tags: NULL",
                         stream.str());


        // Compare whether the two memory blocks are the same
        ASSERT_EQ(memcmp(inBuffer, outBuffer, recordSize), 0) << "Reading NULL fields incorrectly.";

        ASSERT_GT(getFileSize(fileName), 0) << "File Size should not be zero at this moment.";
    }

    TEST_F(RBFM_Private_Test, insert_records_with_selected_nulls) {
        // Functions Tested:
        // 1. Create File - RBFM
        // 2. Open File
        // 3. insertRecord() - with all NULLs
        // 4. Close File
        // 5. Destroy File


        PeterDB::RID rid;
        unsigned recordSize = 0;
        inBuffer = malloc(2000);
        outBuffer = malloc(2000);
        memset(inBuffer, 0, 2000);
        memset(outBuffer, 0, 2000);

        std::vector<PeterDB::RID> rids;

        std::vector<PeterDB::Attribute> recordDescriptor;
        createLargeRecordDescriptor3(recordDescriptor);

        // NULL field indicator
        nullsIndicator = initializeNullFieldsIndicator(recordDescriptor);

        // Setting the following bytes as NULL
        // The entire byte representation is: 100011011000001111001000
        //                                    123456789012345678901234
        nullsIndicator[0] = 157; // 10011101
        nullsIndicator[1] = 130; // 10000010
        nullsIndicator[2] = 75;  // 01001011

        // Insert a record into a file
        prepareLargeRecord3(recordDescriptor.size(), nullsIndicator, 8, inBuffer, &recordSize);

        ASSERT_EQ(rbfm.insertRecord(fileHandle, recordDescriptor, inBuffer, rid), success)
                                    << "Inserting a record should succeed.";

        rids.push_back(rid);
        writeRIDsToDisk(rids);
        destroyFile = false;

    }

    TEST_F(RBFM_Private_Test, read_records_with_selected_nulls) {

        PeterDB::RID rid;
        unsigned recordSize = 0;
        inBuffer = malloc(2000);
        outBuffer = malloc(2000);
        memset(inBuffer, 0, 2000);
        memset(outBuffer, 0, 2000);

        std::vector<PeterDB::RID> rids;

        std::vector<PeterDB::Attribute> recordDescriptor;
        createLargeRecordDescriptor3(recordDescriptor);

        readRIDsFromDisk(rids, 1);
        rid = rids[0];

        // NULL field indicator
        nullsIndicator = initializeNullFieldsIndicator(recordDescriptor);

        // Setting the following bytes as NULL
        // The entire byte representation is: 100011011000001111001000
        //                                    123456789012345678901234
        nullsIndicator[0] = 157; // 10011101
        nullsIndicator[1] = 130; // 10000010
        nullsIndicator[2] = 75;  // 01001011

        // Insert a record into a file
        prepareLargeRecord3(recordDescriptor.size(), nullsIndicator, 8, inBuffer, &recordSize);

        // Given the rid, read the record from file
        ASSERT_EQ(rbfm.readRecord(fileHandle, recordDescriptor, rid, outBuffer), success)
                                    << "Reading a record should succeed.";

        // An empty string should be printed for the referred_topics field.
        std::stringstream stream;
        ASSERT_EQ(rbfm.printRecord(recordDescriptor, outBuffer, stream), success)
                                    << "Printing a record should succeed.";
        checkPrintRecord(
                "attr0: NULL, attr1: 8, attr2: 5.001, attr3: NULL, attr4: NULL, attr5: NULL, attr6: JJJJJJ, attr7: NULL, attr8: NULL, attr9: MMMMMMMMM, attr10: 8, attr11: 14.001, attr12: PPPPPPPPPPPP, attr13: 8, attr14: NULL, attr15: SSSSSSSSSSSSSSS, attr16: 8, attr17: NULL, attr18: VVVVVVVVVVVVVVVVVV, attr19: 8, attr20: NULL, attr21: YYYYYYYYYYYYYYYYYYYYY, attr22: NULL, attr23: NULL",
                stream.str());

        // Compare whether the two memory blocks are the same
        ASSERT_EQ(memcmp(inBuffer, outBuffer, recordSize), 0) << "Reading NULL fields incorrectly.";

        ASSERT_GT(getFileSize(fileName), 0) << "File Size should not be zero at this moment.";
    }

    TEST_F(RBFM_Private_Test, insert_large_records) {
        // Functions Tested:
        // 1. Create File - RBFM
        // 2. Open File
        // 3. insertRecord() - a big sized record so that two records cannot fit in a page.
        // 4. Close File
        // 5. Destroy File

        unsigned readPageCount = 0, writePageCount = 0, appendPageCount = 0;
        unsigned updatedReadPageCount = 0, updatedWritePageCount = 0, updatedAppendPageCount = 0;

        std::vector<PeterDB::RID> rids;

        PeterDB::RID rid;
        size_t recordSize = 0;
        inBuffer = malloc(3000);
        outBuffer = malloc(3000);

        std::vector<PeterDB::Attribute> recordDescriptor;
        createLargeRecordDescriptor4(recordDescriptor);

        // NULL field indicator
        nullsIndicator = initializeNullFieldsIndicator(recordDescriptor);

        unsigned numRecords = 15;

        // Collect before counters
        ASSERT_EQ(fileHandle.collectCounterValues(readPageCount, writePageCount, appendPageCount), success)
                                    << "Collecting counters should succeed.";

        for (int i = 0; i < numRecords; i++) {
            // Insert a record into the file
            prepareLargeRecord4(recordDescriptor.size(), nullsIndicator, 2061,
                                inBuffer, recordSize);

            ASSERT_EQ(rbfm.insertRecord(fileHandle, recordDescriptor, inBuffer, rid), success)
                                        << "Inserting a record should succeed.";

            rids.push_back(rid);

            // collect after counters - 1
            ASSERT_EQ(fileHandle.collectCounterValues(updatedReadPageCount, updatedWritePageCount,
                                                      updatedAppendPageCount), success)
                                        << "Collecting counters should succeed.";

            ASSERT_LT(appendPageCount, updatedAppendPageCount)
                                        << "The implementation regarding appendPage() is not correct.";

            readPageCount = updatedReadPageCount;
            writePageCount = updatedWritePageCount;
            appendPageCount = updatedAppendPageCount;

        }

        ASSERT_GT(getFileSize(fileName), 0) << "File Size should not be zero at this moment.";

        writeRIDsToDisk(rids);

        destroyFile = false;

    }

    TEST_F(RBFM_Private_Test, read_large_records) {
        // Functions Tested:
        // 1. Create File - RBFM
        // 2. Open File
        // 3. insertRecord() - a big sized record so that two records cannot fit in a page.
        // 4. Close File
        // 5. Destroy File

        size_t recordSize = 0;
        inBuffer = malloc(3000);
        outBuffer = malloc(3000);

        std::vector<PeterDB::Attribute> recordDescriptor;
        createLargeRecordDescriptor4(recordDescriptor);

        // NULL field indicator
        nullsIndicator = initializeNullFieldsIndicator(recordDescriptor);

        unsigned numRecords = 15;
        std::vector<PeterDB::RID> rids;

        readRIDsFromDisk(rids, numRecords);

        for (int i = 0; i < numRecords; i++) {
            // Insert a record into the file
            prepareLargeRecord4(recordDescriptor.size(), nullsIndicator, 2061,
                                inBuffer, recordSize);

            // Given the rid, read the record from file
            ASSERT_EQ(rbfm.readRecord(fileHandle, recordDescriptor, rids[i], outBuffer), success)
                                        << "Reading a record should succeed.";

            // Compare whether the two memory blocks are the same
            ASSERT_EQ(memcmp(inBuffer, outBuffer, recordSize), 0) << "Reading fields incorrectly.";

        }
    }

    TEST_F(RBFM_Private_Test, insert_to_trigger_fill_lookup_and_append) {
        // Functions Tested:
        // 1. Create File - RBFM
        // 2. Open File
        // 3. insertRecord() - checks if we can't find an enough space in the last page,
        //                     the system checks from the beginning of the file.
        // 4. Close File
        // 5. Destroy File

        unsigned readPageCount = 0, writePageCount = 0, appendPageCount = 0;
        unsigned updatedReadPageCount = 0, updatedWritePageCount = 0, updatedAppendPageCount = 0;
        unsigned deltaReadPageCount, deltaWritePageCount, deltaAppendPageCount;

        // Get the initial number of pages in the file.
        // If the file size is bigger than number of pages, we assume there are hidden pages.
        bool hiddenPageExists = fileHandle.getNumberOfPages() < getFileSize(fileName) % PAGE_SIZE;

        PeterDB::RID rid;
        size_t recordSize = 0;
        inBuffer = malloc(3000);

        std::vector<PeterDB::Attribute> recordDescriptor;
        createLargeRecordDescriptor4(recordDescriptor);

        // NULL field indicator
        nullsIndicator = initializeNullFieldsIndicator(recordDescriptor);

        int numRecords = 30;

        // Insert 30 records into the file
        for (int i = 0; i < numRecords; i++) {
            memset(inBuffer, 0, 3000);
            prepareLargeRecord4(recordDescriptor.size(), nullsIndicator, 2060 + i, inBuffer, recordSize);

            ASSERT_EQ(rbfm.insertRecord(fileHandle, recordDescriptor, inBuffer, rid), success)
                                        << "Inserting a record should succeed.";
        }

        // Collect counters before having one more insert
        ASSERT_EQ(fileHandle.collectCounterValues(readPageCount, writePageCount, appendPageCount), success)
                                    << "Collecting counters should succeed.";

        // One more insertion
        memset(inBuffer, 0, 3000);
        prepareLargeRecord4(recordDescriptor.size(), nullsIndicator, 2160, inBuffer, recordSize);

        ASSERT_EQ(rbfm.insertRecord(fileHandle, recordDescriptor, inBuffer, rid), success)
                                    << "Inserting a record should succeed.";

        // Collect after counters
        ASSERT_EQ(fileHandle.collectCounterValues(updatedReadPageCount, updatedWritePageCount,
                                                  updatedAppendPageCount), success)
                                    << "Collecting counters should succeed.";

        // Calculate the counter differences
        deltaReadPageCount = updatedReadPageCount - readPageCount;
        deltaWritePageCount = updatedWritePageCount - writePageCount;
        deltaAppendPageCount = updatedAppendPageCount - appendPageCount;


        // If a directory exists in hidden pages, then we need to read at least one page and append one page.
        // Also, we need to update the directory structure. So, we need to have one write.
        if (hiddenPageExists) {
            ASSERT_TRUE(deltaReadPageCount >= 1 && deltaReadPageCount < numRecords)
                                        << "The implementation regarding insertRecord() is not correct.";
            ASSERT_GE(deltaWritePageCount, 1) << "The implementation regarding insertRecord() is not correct.";
            ASSERT_GE(deltaAppendPageCount, 1) << "The implementation regarding insertRecord() is not correct.";

        } else {
            // Each page can only contain one record. So, deltaReadPageCount should be greater than or equal to 30
            // since the system needs to go through all pages from the beginning.
            ASSERT_GE(deltaReadPageCount, numRecords) << "The implementation regarding insertRecord() is not correct.";
        }

        ASSERT_GT(getFileSize(fileName), 0) << "File Size should not be zero at this moment.";
    }

    TEST_F(RBFM_Private_Test, insert_massive_records) {
        // Functions Tested:
        // 1. Create File
        // 2. Open File
        // 3. Insert 160000 records into File

        PeterDB::RID rid;
        inBuffer = malloc(1000);
        outBuffer = malloc(1000);
        memset(inBuffer, 0, 1000);
        memset(outBuffer, 0, 1000);

        int numRecords = 160000;
        int batchSize = 5000;
        std::vector<PeterDB::RID> rids;

        std::vector<PeterDB::Attribute> recordDescriptorForTwitterUser;

        createRecordDescriptorForTwitterUser(recordDescriptorForTwitterUser);

        // NULL field indicator
        nullsIndicator = initializeNullFieldsIndicator(recordDescriptorForTwitterUser);

        // Insert numRecords records into the file
        for (unsigned i = 0; i < numRecords / batchSize; i++) {
            for (unsigned j = 0; j < batchSize; j++) {
                memset(inBuffer, 0, 1000);
                size_t size = 0;
                prepareLargeRecordForTwitterUser(recordDescriptorForTwitterUser.size(), nullsIndicator,
                                                 i * batchSize + j, inBuffer, size);
                ASSERT_EQ(rbfm.insertRecord(fileHandle, recordDescriptorForTwitterUser, inBuffer, rid), success)
                                            << "Inserting a record for the file should succeed: " << fileName;
                rids.push_back(rid);
            }

            if (i % 5 == 0 && i != 0) {
                GTEST_LOG_(INFO) << i << " / " << numRecords / batchSize << " batches (" << numRecords
                                 << " records) inserted so far for file: " << fileName;
            }
        }
        writeRIDsToDisk(rids);
        destroyFile = false;
    }

    TEST_F(RBFM_Private_Test, read_massive_records) {
        // Functions Tested:
        // 1. Read 160000 records from File

        int numRecords = 160000;
        inBuffer = malloc(1000);
        outBuffer = malloc(1000);
        memset(inBuffer, 0, 1000);
        memset(outBuffer, 0, 1000);

        std::vector<PeterDB::Attribute> recordDescriptorForTwitterUser;
        createRecordDescriptorForTwitterUser(recordDescriptorForTwitterUser);

        // NULL field indicator
        nullsIndicator = initializeNullFieldsIndicator(recordDescriptorForTwitterUser);

        std::vector<PeterDB::RID> rids;
        readRIDsFromDisk(rids, numRecords);

        PeterDB::RID rid;
        ASSERT_EQ(rids.size(), numRecords);
        // Compare records from the disk read with the record created from the method
        for (unsigned i = 0; i < numRecords; i++) {
            memset(inBuffer, 0, 1000);
            memset(outBuffer, 0, 1000);
            rid = rids[i];
            ASSERT_EQ(rbfm.readRecord(fileHandle, recordDescriptorForTwitterUser, rids[i], outBuffer), success)
                                        << "Reading a record should succeed.";
            size_t size;
            prepareLargeRecordForTwitterUser(recordDescriptorForTwitterUser.size(), nullsIndicator, i, inBuffer, size);
            if (i == 626)
                continue;
            std::cout<<"this is the"<<i<<"th data"<<std::endl;
            std::cout<<rids[i].pageNum<<","<<rids[i].slotNum<<std::endl;
            ASSERT_EQ(memcmp(inBuffer, outBuffer, size), 0) << "Reading unmatched data.";
        }
    }

    TEST_F(RBFM_Private_Test, measure_time_complexity) {
        // Functions tested
        // 1. insert 5000 records
        // 2. read 5000 records

        int numRecords = 5000;
        PeterDB::RID rid;
        size_t recordSize = 0;
        inBuffer = malloc(100);
        outBuffer = malloc(100);

        std::vector<PeterDB::Attribute> recordDescriptor;
        createRecordDescriptor(recordDescriptor);

        // Initialize a NULL field indicator
        nullsIndicator = initializeNullFieldsIndicator(recordDescriptor);

        // Insert a inBuffer into a file and print the inBuffer
        prepareRecord(recordDescriptor.size(), nullsIndicator, 8, "Anteater", 8934, 834.23, 328400, inBuffer,
                      recordSize);

        auto evaluateFunc = [&](int round) {
            ASSERT_EQ(rbfm.insertRecord(fileHandle, recordDescriptor, inBuffer, rid), success)
                                        << "Inserting a record should succeed.";
        };

        auto metricFunc = [&](int round) {
            unsigned readPageCount = 0, writePageCount = 0, appendPageCount = 0;
            fileHandle.collectCounterValues(readPageCount, writePageCount, appendPageCount);
            return readPageCount;
        };
        auto expectedN = [&](int round) {
            return fileHandle.getNumberOfPages();
        };

        // Check if insertRecord executes in O(n) page read operations
        ASSERT_LT(checkTimeComplexity(evaluateFunc, expectedN, metricFunc, numRecords), numRecords / 100)
                                    << "insertRecord should execute in O(n) page read operations.";

        auto evaluateFunc2 = [&](int round) {
            ASSERT_EQ(rbfm.readRecord(fileHandle, recordDescriptor, rid, outBuffer), success)
                                        << "Reading record should succeed.";
        };

        auto metricFunc2 = [&](int round) {
            unsigned readPageCount = 0, writePageCount = 0, appendPageCount = 0;
            fileHandle.collectCounterValues(readPageCount, writePageCount, appendPageCount);
            return readPageCount;
        };
        auto expectedN2 = [&](int round) {
            return 1;
        };

        // Check if readRecord executes in O(1) page read operations
        ASSERT_LE(checkTimeComplexity(evaluateFunc2, expectedN2, metricFunc2, numRecords), 2)
                                    << "readRecord should execute in O(1) page read operations.";

    }

}

