#include "test.h"
#include <cmath>

using namespace std;

// Helper functions
// Clear SST data
void clearSST(string step) {
    if (step == "1") {
        system("rm -f -r ./SSTs/database_step1/*");
    } else if (step == "2") {
        system("rm -f -r ./SSTs/database_step2/*");
    } else if (step == "3") {
        system("rm -f -r ./SSTs/database_step3/*");
    }
}


// Step 1 test functions
// Check if two vectors are equal
bool vectorsEqual(vector<KV_Pair *> actual, vector<KV_Pair*> expected) {
    if (actual.size() != expected.size()) {
        return false;
    }
    for (size_t i = 0; i < actual.size(); i++) {
        if (actual[i]->key != expected[i]->key || actual[i]->val != expected[i]->val) {
            return false;
        }
    }
    return true;
}

// Check if two KV pairs are equal
bool kvpairsEqual(KV_Pair actual, KV_Pair expected) {
    return actual.key == expected.key && actual.val == expected.val;
}

void errorMessageScan(vector<KV_Pair *> actual, vector<KV_Pair*> expected, string testname) {
    cerr << "Test Failed: " << testname << endl;
    cerr << "Actual: ";
    for (const auto kvpair : actual) {
        cerr << "(" << kvpair->key << ", " << kvpair->val << ") ";
    }
    cerr << "!= Expected: ";
    for (const auto kvpair : expected) {
        cerr << "(" << kvpair->key << ", " << kvpair->val << ") ";
    }
    cerr << endl;
}

// Unit tests for get
// Test if key is in memtable
void test_get_memtable(Database *database) {
    int result = database->get(5);
    if (result != 50) {
        cerr << "Test Failed: Simple get from memtable" << endl;
        cerr << "Actual: " << result << " != Expected: " << 50 << endl;
    }
}

// Test if key is in SST
void test_get_SST(Database *database) {
    int result = database->get(10);
    if (result != 100) {
        cerr << "Test Failed: Simple get from SST" << endl;
        cerr << "Actual: " << result << " != Expected: " << 100 << endl;
    }
}

// Test if key is not in both memtable and SST
void test_get_not_found(Database *database) {
    int result = database->get(10);
    if (result != std::numeric_limits<int>::min()) {
        cerr << "Test Failed: Test key is not found" << endl;
        cerr << "A value should not exist with associated key but found: " << result << endl;
    }
}

// Unit tests for Scan
// Test functionality for memtable with no SSTs
void test_scan_memtable(Database *database) {
    vector <KV_Pair *> result;
    // Execute test
    result = database->scan(4, 6);
    vector <KV_Pair *> expected;
    for (int j = 4; j < 7; j++) {
        expected.push_back(new KV_Pair(j, j * 10));
    }
    if (!vectorsEqual(result, expected)) {
        errorMessageScan(result, expected, "Simple memtable scan");
    }
}

// Test for memtable if scan out of range
void test_scan_memtable_out_of_range(Database *database) {
    vector <KV_Pair *> result;
    // Execute test
    result = database->scan(7, 10);
    vector <KV_Pair *> expected;
    if (!vectorsEqual(result, expected)) {
        errorMessageScan(result, expected, "Memtable scan out of range");
    }
}

// Test memtable if all keys are includsive but edge keys are not in the table
void test_scan_memtable_all_includsive(Database *database) {
    vector <KV_Pair *> result;
    // Execute test
    result = database->scan(0, 10);
    vector <KV_Pair *> expected;
    for (int j = 4; j < 7; j++) {
        expected.push_back(new KV_Pair(j, j * 10));
    }
    if (!vectorsEqual(result, expected)) {
        errorMessageScan(result, expected, "Memtable scan lower and upper bound outside range");
    }
}

// Test duplicate put in memtable
void test_put_duplicate_memtable(Database *database) {
    database->put(4, 50);
    int result = database->get(4);
    if (result != 50) {
        cerr << "Test Failed: Duplicate puts on memtable" << endl;
        cerr << "Actual: " << result << " != Expected: " << 50 << endl;
    }
}

// Test functionality for SST scan
void test_scan_simeple_SST(Database *database) {
    vector<KV_Pair *> result;
    result = database->scan(5, 10);
    vector<KV_Pair *> expected;
    for (int k = 5; k <= 10; k++) {
        expected.push_back(new KV_Pair(k, k * 10));
    }
    if (!vectorsEqual(result, expected)) {
        errorMessageScan(result, expected, "Simple scan SST");
    }
}

// Test out of bound scan on SST
void test_scan_SST_out_of_range(Database *database) {
    vector <KV_Pair *> result;
    // Execute test
    result = database->scan(0, 3);
    vector <KV_Pair *> expected;
    if (!vectorsEqual(result, expected)) {
        errorMessageScan(result, expected, "Memtable scan out of range");
    }
}

// Test duplicate put in SST
void test_put_duplicate_SST(Database *database) {
    database->put(4, 40);
    int result = database->get(4);
    if (result != 40) {
        cerr << "Test Failed: Duplicate puts on SST" << endl;
        cerr << "Actual: " << result << " != Expected: " << 40 << endl;
    }
}

// Test SST if all keys are invludsive but lower and upperbound are not in SST
void test_scan_SST_all_includsive(Database *database) {
    vector <KV_Pair *> result;
    // Execute test
    result = database->scan(0, 20);
    vector <KV_Pair *> expected;
    for (int j = 4; j < 14; j++) {
        expected.push_back(new KV_Pair(j, j * 10));
    }
    if (!vectorsEqual(result, expected)) {
        errorMessageScan(result, expected, "SST scan lower and upper bound outside range");
    }
}

// Test if there are more than one SSTs and duplicate puts
void test_scan_multiple_SSTs(Database * database) {
    vector<KV_Pair *> result;
    // From 4 to 13 are already in SST, others are new inserts
    for (int i = 0; i < 20; i++) {
        database->put(i, i * 10);
    }
    // Execute test
    result = database->scan(0, 19);
    vector<KV_Pair *> expected;
    for (int k = 0; k < 20; k++) {
        expected.push_back(new KV_Pair(k, k * 10));
    }
    // Print error message if test failed
    if (!vectorsEqual(result, expected)) {
        errorMessageScan(result, expected, "Multiple SSTs scan");
    }
}


// Step 2 test functions
// Test the clock and reference in buffer pool system is work properly
void test_buffer_pool(Database *database) {
    // Put data with 4 page size into the database
    for (int i = 0; i < (4 * PAGE_SIZE) / KV_PAIR_SIZE; i++) {
        database->put(i, i * 10);
    }
    // L1 SST is created, keep access the first page in SST
    for (int j = 0; j < 5; j++) {
        database->get(j);
    } // Access the first page 5 times
    // Check if the associate hash key is created and page in buffer pool
    int page_index;
    if (!database->getBufferPool()->getDictionary().get(1, 0, page_index)) {
        cerr << "Test failed: page index should store in hashmap but not" << endl;
    }
    if (database->getBufferPool()->getReference()[page_index] != 1) {
        cerr << "Test failed: page should store in buffer pool but not" << endl;
    }
}

// Test the eviction policy in buffer pool system is work properly
void test_eviction_policy(Database *database) {
    // Put data with 1028 - 4 page size into the database
    for (int i = (4 * PAGE_SIZE) / KV_PAIR_SIZE; i < (1028 * PAGE_SIZE) / KV_PAIR_SIZE; i++) {
        database->put(i, i * 10);
    }
    // 1028 pages fit into SST with 4 extra pages than the size of buffer pool
    for (int i = 0; i < (1028 * PAGE_SIZE) / KV_PAIR_SIZE; i += PAGE_SIZE / KV_PAIR_SIZE) {
        // Perform query in each page so first 1024 pages fit into buffer pool and when fetch
        // last 4 pages, we should kick 4 pages out
        database->get(i);
    }
    // Verify if eviction policy correctly activated
    bitset<BUFFER_SIZE> reference = database->getBufferPool()->getReference();
    // Counter the referenced page
    int num_referenced = reference.count();
    if (num_referenced != 4) {
        cerr << "Test Failed: eviction policy did not correctly kick pages" << endl;
    }
}

void test_for_self_made_hash_table() {
    HashTable hashTable;

    for (int level = 1; level <= 15; level++) {
        for (int page = 0; page < pow(2, level); page++) {
            hashTable.insert(level, page, level+page);
        }
    }

    for (int level = 1; level <= 15; level++) {
        // std::cout << "level " << level << endl;
        for (int page = 0; page < pow(2, level); page++) {
            int value;
            if (hashTable.get(level, page, value) && value == level + page) {
                // std::cout << "Value found: " << value << std::endl;
            } else {
                cerr << "Value not found." << endl;
            }
        }
        // std::cout << endl;
    }

    for (int level = 1; level <= 15; level++) {
        // std::cout << "level " << level << endl;
        for (int page = 0; page < pow(2, level); page++) {
            int value;
            hashTable.remove(level, page);
            if (hashTable.get(level, page, value) && value == level + page) {
                std::cerr << "Found Value that should be removed: " << value << endl;
            } else {
                // cout << "Value not found." << endl;
            }
        }
        // std::cout << endl;
    }
}


// Test function for step 3
// Test the bloom_filter work properly for a Level 1 SST
void test_sst_bloom_filter_for_level1(Database *database) {
    // Memtable size is 1 PAGE_SIZE, so put 1 PAGE_SIZE of data to build an SST 
    for (int i = 0; i < PAGE_SIZE / KV_PAIR_SIZE; i++) {
        database->put(i, i * 10);
    }
    // Check if L1 SST is created with correct data
    int fd = open(string("./SSTs/database_step3/L1").c_str(), O_RDONLY);
    if (fd == -1) {
        cerr << "Test Failed: failed to read file in level 1" << endl;
    }
    close(fd);

    // Get the SST
    SST* level1sst = database->getsstManager()->getSST(1);
    // Check all the keys in the SST, none of them should fail
    for (int i = 0; i < PAGE_SIZE / KV_PAIR_SIZE; i++) {
        if (!level1sst->bloomFilterCheck(i)) {
            cerr << "Test Failed: key " << i << " exist in L1 sst but failed bloom filter test" << endl;
            return;
        }
    }
}

// Test the ability of merging 2 Level1 SSTs into a Level2 SST
void test_lsm_simple_merge(Database *database) {
    // Memtable size is 1 PAGE_SIZE, so put 1 PAGE_SIZE of data to fullfill the level
    // in order to merge to L2.  Since we already created L1 in previous test
    for (int i = PAGE_SIZE / KV_PAIR_SIZE; i < (2 * PAGE_SIZE) / KV_PAIR_SIZE; i++) {
        database->put(i, i * 10);
    }
    // Check if L2 is created with correct data
    int fd = open(string("./SSTs/database_step3/L2").c_str(), O_RDONLY);
    if (fd == -1) {
        cerr << "Test Failed: failed to create file in next level" << endl;
    }
    // Check if file has correct filesize
    int filesize = lseek(fd, 0, SEEK_END);
    if (filesize != 2 * PAGE_SIZE) {
        cerr << "Test Failed: merged file does not have correct filesize" << endl;
    }
    // Check if file contains correct data
    for (int j = 0; j < (2 * PAGE_SIZE) / KV_PAIR_SIZE; j += PAGE_SIZE / KV_PAIR_SIZE) {
        int value = database->get(j);
        if (value != j * 10) {
            cerr << "Test Failed: merged file does not contain correct data";
            return;
        }
    }
    close(fd);
}

// Test tombstones merge correctly into sst
void test_tombstone_merge_to_sst(Database *database) {
    // Now we have a L2 sst, test if tombstone value get converted into sst after memtable is full
    for (int i = 0; i < PAGE_SIZE / KV_PAIR_SIZE; i++) {
        database->delete_(i);
    }
    // L1 SST should be created with all tombstone value
    int fd = open(string("./SSTs/database_step3/L1").c_str(), O_RDONLY);
    if (fd == -1) {
        cerr << "Test Failed: failed to convert tombstone value into sst" << endl;
    }
    // Check the first KV Pair to see if the value is tombstone
    int value;
    pread(fd, &value, sizeof(int), sizeof(int));
    if (value != numeric_limits<int>::min()) {
        cerr << "Test Failed: failed to convert tombstone value into sst" << endl;
    }
    close(fd);
}

// Test the get result after delete operation is correct
void test_delete_after_merge(Database *database) {
    // Test delete API after merge
    // Since we performed merge and delete in previous test, we can just get the value
    // to check whether it's deleted
    for (int i = 0; i < PAGE_SIZE / KV_PAIR_SIZE; i++) {
        if (database->get(i) != numeric_limits<int>::min()) {
            cerr << "Test Failed: delete after merge SSTs" << endl;
            cerr << "database->get(" << i << ") = " << database->get(i) << endl;
            return;
        }
    }

    // Also we may check the value for another half is store properly in the databse.
    for (int i = PAGE_SIZE / KV_PAIR_SIZE; i < (2 * PAGE_SIZE) / KV_PAIR_SIZE; i++) {
        if (database->get(i) != i * 10) {
            cerr << "Test Failed: delete after merge SSTs" << endl;
            cerr << "database->get(" << i << ") = " << database->get(i) << endl;
            return;
        }
    }
}

// Test the ability of merging 2 Level2 SSTs into a Level3 SST
void test_lsm_merge_twice(Database *database) {
    // From previous tests, we had one L1 sst and one L2 sst, add 1 more PAGE_SIZE data
    // to fullfill L1 in order to merge to L3
    for (int i = (2 * PAGE_SIZE) / KV_PAIR_SIZE; i < (3 * PAGE_SIZE) / KV_PAIR_SIZE; i++) {
        database->put(i, i * 10);
    }
    // Check if L3 is created with correct data
    int fd = open(string("./SSTs/database_step3/L3").c_str(), O_RDONLY);
    if (fd == -1) {
        cerr << "Test Failed: failed to create file in next level" << endl;
    }
    // Check if file contains correct data
    for (int j = PAGE_SIZE / KV_PAIR_SIZE; j < (3 * PAGE_SIZE) / KV_PAIR_SIZE; j += PAGE_SIZE / KV_PAIR_SIZE) {
        int value = database->get(j);
        if (value != j * 10) {
            cerr << "Test Failed: merged file does not contain correct data" << endl;
        }
    }
    close(fd);
}

void test_delete_at_max_level(Database *database) {
    // Test if tombstone are cleared at the max level of LSM Tree
    int fd = open(string("./SSTs/database_step3/L3").c_str(), O_RDONLY);
    if (fd == -1) {
        cerr << "Test Failed: failed to create file in next level" << endl;
    }
    int filesize = lseek(fd, 0, SEEK_END);
    if (filesize != 8192) {
        cerr << "Test Failed: Tombstone are not cleared at the max level" << endl;
    }
    close(fd);
}

void test_sst_bloom_filter_for_merged(Database *database) {
    // Get the SST
    SST* sst = database->getsstManager()->getSST(3);
    // Check all the keys in the SST, none of them should fail
    for (int i = PAGE_SIZE / KV_PAIR_SIZE; i < (3 * PAGE_SIZE) / KV_PAIR_SIZE; i++) {
        if (!sst->bloomFilterCheck(i)) {
            cerr << "Test Failed: key " << i << " exist in L3 sst but failed bloom filter test" << endl;
            return;
        }
    }

    float falsePositiveNum = 0.0;
    // Check number of keys in the SST that has been deleted, only a little portion should pass due to false postive
    for (int i = 0; i < PAGE_SIZE / KV_PAIR_SIZE; i++) {
        if (sst->bloomFilterCheck(i)) {
            falsePositiveNum++;
        }
    }
    if (falsePositiveNum / (PAGE_SIZE / KV_PAIR_SIZE) > 0.1) {
        cerr << "Test Failed: The false postive rate: is over 10%" << endl;
    }
}


int main(int argc, char* argv[]) {
    // By performing the unittest, we will open the database and operate
    // a series of API command. In this way we can prevent collisions when
    // previous test creates SST files and later testcases failed because of
    // the SSTs created. The purpose of the test will be explain in comment
    if (argc != 2) {
        cerr << "Please execute ./test {step number}. E.g. ./test 1 for execute the unittests for step 1" << endl;
        return 0;
    }

    string step_num = argv[1];
    // Clear SST data
    clearSST(step_num);
    if (step_num == "1") {
        // Open database
        Database *database_step1 = new Database("database_step1", 80);
        database_step1->open("database_step1");

        // Test put by add 3 KV Pairs into the table
        for (int i = 4; i < 7; i++) {
            database_step1->put(i, i * 10);
        }
        // Test simple get in memtable
        test_get_memtable(database_step1);
        // Test simple scan in memtable
        test_scan_memtable(database_step1);
        // Test if scan range is greater than min max range of memtable
        test_scan_memtable_all_includsive(database_step1);
        // Test if scan range are not in the memtable
        test_scan_memtable_out_of_range(database_step1);
        // Test get for key not exists in memtable
        test_get_not_found(database_step1);
        // Test put with duplicate key in memtable
        test_put_duplicate_memtable(database_step1);
        // Test put to fullfill the memtable and check if it transfrom to create SST
        for (int j = 7; j < 14; j++) {
            database_step1->put(j, j * 10);
        }
        // Test open and close API make sure data would not get lost
        database_step1->close();
        database_step1->open("database_step1");

        // Test simple get on SST
        test_get_SST(database_step1);
        // Test simple scan on SST
        test_scan_simeple_SST(database_step1);
        // Test scan range is not in SST
        test_scan_SST_out_of_range(database_step1);
        // Test put with duplicate key in SST
        test_put_duplicate_SST(database_step1);
        // Test scan range is greater than min max range of SST
        test_scan_SST_all_includsive(database_step1);
        // Test scan with multiple SSTs and duplicate puts
        test_scan_multiple_SSTs(database_step1);

        // Close the database
        database_step1->close();
    } else if (step_num == "2") {
        // Test for step 2
        Database *database_step2 = new Database("database_step2", 4 * PAGE_SIZE);
        database_step2->open("database_step2");

        // Test the hash table
        test_for_self_made_hash_table();
        // Test buffer pool functionality
        test_buffer_pool(database_step2);
        // Test eviction policy
        test_eviction_policy(database_step2);

        // Close database
        database_step2->close();
    } else if (step_num == "3") {
        // Test for step 3
        Database *database_step3 = new Database("database_step3", PAGE_SIZE);
        database_step3->open("database_step3");

        // Test bloom filters are work correctly
        test_sst_bloom_filter_for_level1(database_step3);
        // Test LSM Tree simple merge
        test_lsm_simple_merge(database_step3);
        // test tombstone is merged to sst from memtable
        test_tombstone_merge_to_sst(database_step3);
        // Test delete API after merge ssts
        test_delete_after_merge(database_step3);
        // Test LSM Tree to be merged twice
        test_lsm_merge_twice(database_step3);
        // Test tombstone are cleared at max level of LSM Tree
        test_delete_at_max_level(database_step3);
        // // Test bloom filters when two sst were merged
        test_sst_bloom_filter_for_merged(database_step3);

        // CLose the database
        database_step3->close();
    } else {
        cerr << "Please enter a valid step number from 1 to 3" << endl;
        return 1;
    }
    return 0;
}