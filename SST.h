#ifndef SST_H
#define SST_H

#include <iostream>
#include <vector>
#include <unordered_map>
#include <sys/stat.h>
#include <fstream>
#include <unistd.h>
#include <fcntl.h>
#include <map>
#include <functional>
#include "memtable.h"
#include <cstdlib>

using namespace std;
#define PAGE_SIZE 4096
#define KV_PAIR_SIZE 8
#define GET 1
#define LOWER 2
#define UPPER 3
#define BITS_PER_ENTRY 5
#define HASH_FUNCTION_NUM 3

class SST {
public:
    // Constructor
    SST(int levelnum, string &prefix, bool istemp, vector<function<int(int)>> *hashFunctions);
    // Destructor
    ~SST();

    // Public variables
    string filepath;
    int levelnum;
    int filesize = 0;
    bool istemp;
    int hashFunctionNum;

    // Build key array of SST for binary search
    void buildKeyArray();
    // Set key array
    vector<int> getKeyArray();
    // Generate file size
    void generateFileSize();
    // Build bloom filter of a SST
    void buildBloomFilter();
    // Set bloom filter
    void setBloomFilter(vector<bool>& bloomFilter);
    // Helper function for binary search the potential page
    int binarySearchPage(int key);
    // Get the potential page according to it's type
    // GET = int 1 for get operation
    // LOWER = int 2 for lowerbound in scan operation
    // UPPER = int 3 for upperbound in scan operation
    int getPotentialPageNumberOfASST(int key, int type);
    bool bloomFilterCheck(int key); 
    // Print sst for testing puropse
    void printSST();
    void printKeyArray();
    void printBloomFilter();
    void printBuffer(KV_Pair* buffer);

private:
    vector<int> keyArray;
    vector<bool> bloomFilter;
    vector<function<int(int)>> *hashFunctions;
};

#endif  // SST_H
