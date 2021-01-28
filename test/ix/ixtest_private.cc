#include "test/utils/ix_test_utils.h"
#include "random"

namespace PeterDBTesting {
    TEST_F(IX_Private_Test, multiple_indexes_at_the_same_time) {
        // Check whether multiple indexes can be used at the same time.

        unsigned numOfTuples = 2000;
        float key;
        float key2;
        float compVal = 6500.0;
        unsigned inRidPageNumSum = 0;
        unsigned outRidPageNumSum = 0;

        // insert entry
        for (unsigned i = 1; i <= numOfTuples; i++) {
            key = (float) i + 87.6;
            rid.pageNum = i;
            rid.slotNum = i;

            ASSERT_EQ(ix.insertEntry(ixFileHandle, heightAttr, &key, rid), success)
                                        << "indexManager::insertEntry() should succeed.";

            ASSERT_EQ(ix.insertEntry(ixFileHandle2, heightAttr, &key, rid), success)
                                        << "indexManager::insertEntry() should succeed.";

            if (key < compVal) {
                inRidPageNumSum += rid.pageNum;
            }
        }

        // insert more entries
        for (unsigned i = 6000; i <= numOfTuples + 6000; i++) {
            key = (float) i + 87.6;
            rid.pageNum = i;
            rid.slotNum = i - (unsigned) 500;

            // insert entry
            ASSERT_EQ(ix.insertEntry(ixFileHandle, heightAttr, &key, rid), success)
                                        << "indexManager::insertEntry() should succeed.";

            ASSERT_EQ(ix.insertEntry(ixFileHandle2, heightAttr, &key, rid), success)
                                        << "indexManager::insertEntry() should succeed.";

            if (key < compVal) {
                inRidPageNumSum += rid.pageNum;
            }
        }

        // Conduct a scan
        ASSERT_EQ(ix.scan(ixFileHandle, heightAttr, NULL, &compVal, true, false, ix_ScanIterator), success)
                                    << "indexManager::scan() should succeed.";
        ASSERT_EQ(ix.scan(ixFileHandle2, heightAttr, NULL, &compVal, true, false, ix_ScanIterator2), success)
                                    << "indexManager::scan() should succeed.";

        unsigned returnedCount = 0;
        while (ix_ScanIterator.getNextEntry(rid, &key) != IX_EOF) {
            returnedCount++;

            ASSERT_EQ (ix_ScanIterator2.getNextEntry(rid2, &key2), success) << "Scan outputs should match.";

            ASSERT_EQ (rid.pageNum, rid2.pageNum) << "Scan outputs (PageNum) should match.";

            if (rid.pageNum % 1000 == 0) {
                GTEST_LOG_(INFO) << returnedCount << " - returned entries: " << rid.pageNum << " " << rid.slotNum;
            }
            outRidPageNumSum += rid.pageNum;
        }

        ASSERT_EQ (inRidPageNumSum, outRidPageNumSum) << "Scan outputs (PageNum) should match.";

        // Close Scan
        ASSERT_EQ(ix_ScanIterator.close(), success) << "IX_ScanIterator::close() should succeed.";
        ASSERT_EQ(ix_ScanIterator2.close(), success) << "IX_ScanIterator::close() should succeed.";

    }

    TEST_F(IX_Private_Test, scan_and_delete_and_reinsert) {
        // insert 30,000 entries to two indexes
        // scan and delete
        // insert 20,000 entries to two indexes
        // scan

        unsigned numOfTuples = 20000;
        unsigned numOfMoreTuples = 30000;
        float key;
        float key2;
        float compVal = 6500;
        int A[20000];
        int B[30000];

        // Prepare key entries
        for (int i = 0; i < numOfTuples; i++) {
            A[i] = i;
        }

        // Randomly shuffle the entries
        std::shuffle(A, A + numOfTuples, std::mt19937(std::random_device()()));

        // Insert entries
        for (int i = 0; i < numOfTuples; i++) {
            key = A[i];
            rid.pageNum = i + 1;
            rid.slotNum = i + 1;

            ASSERT_EQ(ix.insertEntry(ixFileHandle, ageAttr, &key, rid), success)
                                        << "indexManager::insertEntry() should succeed.";

            ASSERT_EQ(ix.insertEntry(ixFileHandle2, ageAttr, &key, rid), success)
                                        << "indexManager::insertEntry() should succeed.";
        }

        compVal = 5000;

        // Conduct a scan
        ASSERT_EQ(ix.scan(ixFileHandle, ageAttr, NULL, &compVal, true, true, ix_ScanIterator), success)
                                    << "indexManager::scan() should succeed.";
        ASSERT_EQ(ix.scan(ixFileHandle2, ageAttr, NULL, &compVal, true, true, ix_ScanIterator2), success)
                                    << "indexManager::scan() should succeed.";

        // scan & delete
        int returnedCount = 0;
        while (ix_ScanIterator.getNextEntry(rid, &key) != IX_EOF) {
            ASSERT_EQ (ix_ScanIterator2.getNextEntry(rid2, &key2), success) << "Scan outputs should match.";
            ASSERT_EQ (rid.pageNum, rid2.pageNum) << "Scan outputs (PageNum) should match.";


            // delete entry
            ASSERT_EQ(ix.deleteEntry(ixFileHandle, ageAttr, &key, rid), success)
                                        << "indexManager::deleteEntry() should succeed.";
            ASSERT_EQ(ix.deleteEntry(ixFileHandle2, ageAttr, &key2, rid2), success)
                                        << "indexManager::deleteEntry() should succeed.";

            returnedCount++;
        }
        ASSERT_EQ (returnedCount, 5001) << "Returned count should match inserted.";


        // Close Scan
        ASSERT_EQ(ix_ScanIterator.close(), success) << "IX_ScanIterator::close() should succeed.";
        ASSERT_EQ(ix_ScanIterator2.close(), success) << "IX_ScanIterator::close() should succeed.";


        // insert more entries Again
        for (int i = 0; i < numOfMoreTuples; i++) {
            B[i] = 20000 + i;
        }
        std::shuffle(B, B + numOfMoreTuples, std::mt19937(std::random_device()()));

        for (int i = 0; i < numOfMoreTuples; i++) {
            key = B[i];
            rid.pageNum = i + 20001;
            rid.slotNum = i + 20001;

            ASSERT_EQ(ix.insertEntry(ixFileHandle, ageAttr, &key, rid), success)
                                        << "indexManager::insertEntry() should succeed.";

            ASSERT_EQ(ix.insertEntry(ixFileHandle2, ageAttr, &key, rid), success)
                                        << "indexManager::insertEntry() should succeed.";
        }

        // scan
        compVal = 35000;

        ASSERT_EQ(ix.scan(ixFileHandle, ageAttr, NULL, &compVal, true, true, ix_ScanIterator), success)
                                    << "indexManager::scan() should succeed.";
        ASSERT_EQ(ix.scan(ixFileHandle2, ageAttr, NULL, &compVal, true, true, ix_ScanIterator2), success)
                                    << "indexManager::scan() should succeed.";

        returnedCount = 0;
        while (ix_ScanIterator.getNextEntry(rid, &key) != IX_EOF) {

            ASSERT_EQ (ix_ScanIterator2.getNextEntry(rid2, &key2), success) << "Scan outputs should match.";
            ASSERT_EQ (rid.pageNum, rid2.pageNum) << "Scan outputs (PageNum) should match.";
            ASSERT_FALSE(rid.pageNum > 20000 && B[rid.pageNum - 20001] > 35000)
                                        << "Scan outputs (PageNum) should match.";
            returnedCount++;
        }
        ASSERT_EQ (returnedCount, numOfMoreTuples) << "Returned count should match inserted.";

        // Close Scan
        ASSERT_EQ(ix_ScanIterator.close(), success) << "IX_ScanIterator::close() should succeed.";
        ASSERT_EQ(ix_ScanIterator2.close(), success) << "IX_ScanIterator::close() should succeed.";

    }

    TEST_F(IX_Private_Test, varchar_scan) {
        // Varchar index handling check

        char key[100];
        int numOfTuples = 200000;
        *(int *) key = 6;
        int count;
        char compVal[100];
        char highKey[100];

        // insert entry
        for (unsigned i = 1; i <= numOfTuples; i++) {
            sprintf(key + 4, "%06d", i);
            rid.pageNum = i;
            rid.slotNum = i % PAGE_SIZE;

            ASSERT_EQ(ix.insertEntry(ixFileHandle, shortEmpNameAttr, &key, rid), success)
                                        << "indexManager::insertEntry() should succeed.";
        }

        *(int *) compVal = 6;
        sprintf(compVal + 4, "%06d", 90000);


        // Conduct a scan
        ASSERT_EQ(ix.scan(ixFileHandle, shortEmpNameAttr, compVal, compVal, true, true, ix_ScanIterator), success)
                                    << "indexManager::scan() should succeed.";

        //iterate
        count = 0;
        unsigned expectedValue = 90000;
        while (ix_ScanIterator.getNextEntry(rid, &key) != IX_EOF) {
            key[10] = '\0';
            EXPECT_EQ(std::stoi(std::string(key + 4)), expectedValue++)
                                << "Scan output (value) should match inserted.";
            count++;
        }

        ASSERT_EQ(count, 1) << "Scan outputs should match.";

        // Close Scan
        ASSERT_EQ(ix_ScanIterator.close(), success) << "IX_ScanIterator::close() should succeed.";

    }

    TEST_F(IX_Private_Test, varchar_scan_range) {
        // Varchar index handling check

        char key[100];
        int numOfTuples = 200000;
        *(int *) key = 6;
        int count;
        char lowKey[100];
        char highKey[100];

        // insert entry
        for (unsigned i = 1; i <= numOfTuples; i++) {
            sprintf(key + 4, "%06d", i);
            rid.pageNum = i;
            rid.slotNum = i % PAGE_SIZE;

            ASSERT_EQ(ix.insertEntry(ixFileHandle, shortEmpNameAttr, &key, rid), success)
                                        << "indexManager::insertEntry() should succeed.";
        }

        *(int *) lowKey = 6;
        sprintf(lowKey + 4, "%06d", 90000);
        *(int *) highKey = 6;
        sprintf(highKey + 4, "%06d", 100000);

        // Conduct a scan
        ASSERT_EQ(ix.scan(ixFileHandle, shortEmpNameAttr, lowKey, highKey, false, false, ix_ScanIterator), success)
                                    << "indexManager::scan() should succeed.";

        //iterate
        count = 0;
        unsigned expectedValue = 90001;
        while (ix_ScanIterator.getNextEntry(rid, &key) != IX_EOF) {
            key[10] = '\0';
            EXPECT_EQ(std::stoi(std::string(key + 4)), expectedValue++)
                                << "Scan output (value) should match inserted.";
            count++;
        }

        ASSERT_EQ(count, 9999) << "Scan outputs should match.";

        // Close Scan
        ASSERT_EQ(ix_ScanIterator.close(), success) << "IX_ScanIterator::close() should succeed.";

    }

    TEST_F(IX_Private_Test, varchar_compact) {
        // Checks whether varchar key is handled properly.

        char key[100];
        char key2[100];
        int numOfTuples = 50000;
        *(int *) key = 5;
        int count;

        char lowKey[100];
        char highKey[100];


        // insert entries
        for (unsigned i = 1; i <= numOfTuples; i++) {
            sprintf(key + 4, "%05d", i);
            rid.pageNum = i;
            rid.slotNum = i % PAGE_SIZE;

            ASSERT_EQ(ix.insertEntry(ixFileHandle, shortEmpNameAttr, &key, rid), success)
                                        << "indexManager::insertEntry() should succeed.";

            ASSERT_EQ(ix.insertEntry(ixFileHandle2, longEmpNameAttr, &key, rid), success)
                                        << "indexManager::insertEntry() should succeed.";

        }

        // collect counters
        ASSERT_EQ(ixFileHandle.collectCounterValues(rc, wc, ac), success)
                                    << "indexManager::collectCounterValues() should succeed.";

        ASSERT_EQ(ixFileHandle.collectCounterValues(rc2, wc2, ac2), success)
                                    << "indexManager::collectCounterValues() should succeed.";

        EXPECT_GE(wc, 1);

        // Actually, there should be no difference.
        ASSERT_EQ(wc2 + ac2, wc + ac) << "VARCHAR length should be compacted, thus IO should be the same.";
        ASSERT_EQ(getFileSize(indexFileName), getFileSize(indexFileName2))
                                    << "VARCHAR length should be compacted, thus the file size should be the same.";

        *(int *) lowKey = 5;
        sprintf(lowKey + 4, "%05d", 30801);
        *(int *) highKey = 5;
        sprintf(highKey + 4, "%05d", 30900);
        ASSERT_EQ(ix.scan(ixFileHandle, shortEmpNameAttr, lowKey, highKey, true, true, ix_ScanIterator), success)
                                    << "indexManager::scan() should succeed.";
        ASSERT_EQ(ix.scan(ixFileHandle2, longEmpNameAttr, lowKey, highKey, true, true, ix_ScanIterator2), success)
                                    << "indexManager::scan() should succeed.";
        //iterate
        count = 0;
        while (ix_ScanIterator.getNextEntry(rid, &key) != IX_EOF) {

            ASSERT_EQ (ix_ScanIterator2.getNextEntry(rid2, &key2), success) << "Scan outputs should match.";
            ASSERT_EQ(std::stoi(key + 4), std::stoi(key2 + 4)) << "Scan outputs (value) should match.";
            count++;
        }
        ASSERT_EQ(count, 100) << "Scan outputs should match.";

        // Close Scan
        ASSERT_EQ(ix_ScanIterator.close(), success) << "IX_ScanIterator::close() should succeed.";
        ASSERT_EQ(ix_ScanIterator2.close(), success) << "IX_ScanIterator::close() should succeed.";
    }

    TEST_F(IX_Private_Test, duplicate_varchar_in_one_page) {
        // Checks whether duplicated entries in a page are handled properly.

        unsigned numOfTuples = 180;
        char key[100];
        *(unsigned *) key = 5;
        unsigned count;

        char lowKey[100];
        char highKey[100];

        unsigned inRidPageNumSum = 0;
        unsigned outRidPageNumSum = 0;

        // insert entries
        for (unsigned i = 0; i < numOfTuples; i++) {
            sprintf(key + 4, "%05d", i % 3);

            rid.pageNum = i;
            rid.slotNum = i % 3;
            ASSERT_EQ(ix.insertEntry(ixFileHandle, shortEmpNameAttr, &key, rid), success)
                                        << "indexManager::insertEntry() should succeed.";

            if (i % 3 == 1) {
                inRidPageNumSum += rid.pageNum;
            }
        }

        // Actually, this should print out only one page.
        std::stringstream stream;
        ASSERT_EQ(ix.printBTree(ixFileHandle, shortEmpNameAttr, stream), success)
                                    << "indexManager.printBTree() should succeed.";
        validateTree(stream, 3, 180, 0, PAGE_SIZE / 30, true);
        *(unsigned *) lowKey = 5;
        sprintf(lowKey + 4, "%05d", 1);
        *(unsigned *) highKey = 5;
        sprintf(highKey + 4, "%05d", 1);

        // scan
        ASSERT_EQ(ix.scan(ixFileHandle, shortEmpNameAttr, lowKey, highKey, true, true, ix_ScanIterator), success)
                                    << "indexManager::scan() should succeed.";

        //iterate
        count = 0;
        while (ix_ScanIterator.getNextEntry(rid, &key) != IX_EOF) {
            ASSERT_EQ(rid.slotNum, 1) << "Scan outputs (slotNum) should match inserted.";
            outRidPageNumSum += rid.pageNum;
            count++;
        }
        ASSERT_EQ(count, numOfTuples / 3) << "Scan outputs should match inserted.";
        ASSERT_EQ(outRidPageNumSum, inRidPageNumSum) << "Scan outputs (pageNumSum) should match inserted.";

        // Close Scan
        ASSERT_EQ(ix_ScanIterator.close(), success) << "IX_ScanIterator::close() should succeed.";

    }

    TEST_F(IX_Private_Test, extra_duplicate_keys_span_multiple_pages) {
        // Checks whether duplicated entries spanning multiple page are handled properly or not.

        unsigned numOfTuples = 10000;
        unsigned numExtra = 5000;
        unsigned key;

        int compVal1 = 9, compVal2 = 15;
        int count;

        // insert entry
        for (unsigned i = 1; i <= numOfTuples; i++) {
            key = i % 10;
            rid.pageNum = i;
            rid.slotNum = i;
            ASSERT_EQ(ix.insertEntry(ixFileHandle, ageAttr, &key, rid), success)
                                        << "indexManager::insertEntry() should succeed.";
        }

        for (unsigned i = numOfTuples; i < numOfTuples + numExtra; i++) {
            key = i % 10 + 10;
            rid.pageNum = i;
            rid.slotNum = i + 10;
            ASSERT_EQ(ix.insertEntry(ixFileHandle, ageAttr, &key, rid), success)
                                        << "indexManager::insertEntry() should succeed.";
        }

        // scan
        ASSERT_EQ(ix.scan(ixFileHandle, ageAttr, &compVal1, &compVal1, true, true, ix_ScanIterator), success)
                                    << "indexManager::scan() should succeed.";

        // iterate
        count = 0;
        while (ix_ScanIterator.getNextEntry(rid, &key) == success) {
            count++;

            ASSERT_EQ(rid.pageNum, rid.slotNum) << "Scan outputs (PageNum) should match.";
            ASSERT_EQ(key, compVal1) << "Scan outputs (value) should match.";

            if (count % 100 == 0) {
                GTEST_LOG_(INFO) << count << " - Returned rid: " << rid.pageNum << " " << rid.slotNum;
            }
        }

        ASSERT_EQ(count, 1000) << "Scan outputs should match inserted";

        // Close Scan
        ASSERT_EQ(ix_ScanIterator.close(), success) << "IX_ScanIterator::close() should succeed.";

        // scan
        ASSERT_EQ(ix.scan(ixFileHandle, ageAttr, &compVal2, &compVal2, true, true, ix_ScanIterator), success)
                                    << "indexManager::scan() should succeed.";

        count = 0;
        while (ix_ScanIterator.getNextEntry(rid, &key) == success) {
            count++;

            ASSERT_EQ(rid.pageNum, rid.slotNum - 10) << "Scan outputs (PageNum) should match.";
            ASSERT_EQ(key, compVal2) << "Scan outputs (value) should match.";

            if (count % 100 == 0) {
                GTEST_LOG_(INFO) << count << " - Returned rid: " << rid.pageNum << " " << rid.slotNum;
            }
        }

        ASSERT_EQ(count, 500) << "Scan outputs should match inserted";

        // Close Scan
        ASSERT_EQ(ix_ScanIterator.close(), success) << "IX_ScanIterator::close() should succeed.";

    }

    TEST_F(IX_Private_Test, extra_merge_on_deletion) {
        // Checks whether the deletion is properly managed (non-lazy deletion)
        // Functions tested
        // 1. Insert entries to make a height 2 tree
        // 2. Print BTree
        // 3. Delete all entries
        // 4. Print BTree

        unsigned numOfEntries = 19;
        char key[PAGE_SIZE];

        empNameAttr.length = PAGE_SIZE / 5;  // Each node could only have 4 children

        // insert entries
        unsigned i = 1;
        for (; i <= numOfEntries; i++) {
            prepareKeyAndRid(i, key, rid, empNameAttr.length);

            ASSERT_EQ(ix.insertEntry(ixFileHandle, empNameAttr, &key, rid), success)
                                        << "indexManager::insertEntry() should succeed.";
        }

        // print BTree, by this time the BTree should have height of 2
        std::stringstream stream;
        ASSERT_EQ(ix.printBTree(ixFileHandle, empNameAttr, stream), success)
                                    << "indexManager::printBTree() should succeed";

        validateTree(stream, numOfEntries, numOfEntries, 2, 2);

        // Conduct a scan
        ASSERT_EQ(ix.scan(ixFileHandle, empNameAttr, NULL, NULL, true, true, ix_ScanIterator), success)
                                    << "indexManager::scan() should succeed.";

        // scan & delete
        unsigned count = 0;
        while (ix_ScanIterator.getNextEntry(rid, &key) != IX_EOF) {
            if (count++ == numOfEntries - 1) {
                // leave only the last entry not deleted
                break;
            }
            // delete entry
            ASSERT_EQ(ix.deleteEntry(ixFileHandle, empNameAttr, &key, rid), success)
                                        << "indexManager::deleteEntry() should succeed.";
        }

        // print BTree, by this time the BTree should have height of 0, 1 entry
        stream.str(std::string());
        stream.clear();
        ASSERT_EQ(ix.printBTree(ixFileHandle, empNameAttr, stream), success)
                                    << "indexManager::printBTree() should succeed";

        validateTree(stream, 1, 1, 0, 2, true);
    }
}