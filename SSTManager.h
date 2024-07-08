#ifndef SST_MANAGER_H
#define SST_MANAGER_H

#include <iostream>
#include <unordered_map>
#include <vector>
#include "SST.h"
#include "memtable.h"
#include "bufferpool.h"

using namespace std;

class SSTManager {
public:
    // Keep track of max level of LSM Tree
    int max_level = 0;

    // Constructor
    SSTManager();

    // Destructor
    ~SSTManager();

    // Convert memtable to new SST, merge if needed
    void createSST(Memtable *memtable, string& prefix, BufferPool *bufferpool);

    // Delete SST metadata in a specific level
    void deleteSST(int levelnum, BufferPool *bufferpool);

    // Get SST metadata for a specific level
    SST *getSST(int levelnum);

    // Merge two SST
    SST *mergeSST(SST *sst1, SST *sst2, int levelnum, string& prefix);

private:
    // A hash map that manage all metadata of all SSTs
    unordered_map<int, SST*> sstTable;
    // A List of all hash functions that bloom filters will be used
    vector<function<int(int)>> hashFunctions;

    size_t hashWithSeed(int key, size_t seed);
    void buildAllHashFunctions();

};

#endif  // SST_MANAGER_H
