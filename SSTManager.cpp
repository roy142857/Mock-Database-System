#include "SSTManager.h"

SSTManager::SSTManager() {
    this->buildAllHashFunctions();
}

SSTManager::~SSTManager() {}

void SSTManager::createSST(Memtable *memtable, string& prefix, BufferPool *bufferpool) {
    auto it = this->sstTable.find(1);
    // If L1 is not empty, convert memtable to L1Temp file for merge
    if (it != this->sstTable.end()) {
        SST *sst = new SST(1, prefix, true, &hashFunctions);
        ofstream sstFile(sst->filepath);
        memtable->scanToFile(memtable->root, &sstFile);
        sstFile.close();
        sst->generateFileSize();
        // Iterating through the level
        int level = 1;
        bool stop = false;
        while (!stop) {
            // Merge current level
            SST *nextsst = mergeSST(this->getSST(level), sst, level, prefix);
            // Erase curr level SSTs
            this->deleteSST(level, bufferpool);
            sst->~SST();
            // If next level is empty
            if (!nextsst->istemp) {
                // Put the merged sst in next level
                nextsst->buildKeyArray();
                nextsst->buildBloomFilter();
                this->sstTable[level + 1] = nextsst;
                // Update max level
                if (max_level < level + 1) {
                    max_level = level + 1;
                }
                // Terminate merge algorithm
                stop = true;
            } else {
                sst = nextsst;
                sst->generateFileSize();
                level++;
            }  
        }
    } else {
        // Create SST if first level is empty
        SST *sst = new SST(1, prefix, false, &hashFunctions);
        ofstream sstFile(sst->filepath);
        memtable->scanToFile(memtable->root, &sstFile);
        sstFile.close();
        this->sstTable[1] = sst;
        sst->generateFileSize();
        sst->buildKeyArray();
        sst->buildBloomFilter();
        // Set max level of LSM Tree
        if (this->max_level == 0) {
            this->max_level = 1;
        }
    }
}

void SSTManager::deleteSST(int levelnum, BufferPool *bufferpool) {
    // Evict all the pages in the buffer pool
    SST *sst = getSST(levelnum);
    int numPages = sst->filesize / PAGE_SIZE + (sst->filesize % PAGE_SIZE != 0);
    for (int page = 0; page < numPages; page++) {
        bufferpool->evictPages(sst, page);
    }
    // Deconstruct the SST and erase it from SST Table
    this->sstTable[levelnum]->~SST();
    this->sstTable.erase(levelnum);
}

SST *SSTManager::getSST(int levelnum) {
    // Retrieve the vector for the specified level
    auto it = this->sstTable.find(levelnum);
    if (it != this->sstTable.end()) {
        return it->second;
    } else {
        return NULL;  // Level not found
    }
}

SST *SSTManager::mergeSST(SST *sst1, SST *sst2, int levelnum, string& prefix) {
    bool nextLevelExist = sstTable.find(levelnum + 1) != sstTable.end();
    SST* mergedSST = new SST(levelnum + 1, prefix, nextLevelExist, &hashFunctions);
    // Open files for read and write
    int sst1_fd = open(sst1->filepath.c_str(), O_RDONLY);
    int sst2_fd = open(sst2->filepath.c_str(), O_RDONLY);
    int merged_fd = open(mergedSST->filepath.c_str(), O_WRONLY);
    // Allocate buffer
    char* buffer1 = new char[PAGE_SIZE];
    char* buffer2 = new char[PAGE_SIZE];
    char* mergedBuffer = new char[PAGE_SIZE];
    // Keep track the page index
    int page_idx1 = 0;
    int page_idx2 = 0;
    int merge_page = 0; 
    // Keep track the total index of the file
    int total_idx1 = 0;
    int total_idx2 = 0;
    // Keep track the buffer size
    int buffer1size = 0;
    int buffer2size = 0;
    int mergedIdx = 0;

    // Perform merge operation
    while (total_idx1 < (sst1->filesize / KV_PAIR_SIZE) && total_idx2 < (sst2->filesize / KV_PAIR_SIZE)) { // as long as both file are not ended
        // Check if we need to swap in a new page for buffer1
        if (buffer1size == 0) {
            // Buffer1 is used up, read a new page
            buffer1size = PAGE_SIZE;
            pread(sst1_fd, buffer1, PAGE_SIZE, page_idx1 * PAGE_SIZE);
            page_idx1++;
        }
        // Check if we need to swap in a new page for buffer2
        if (buffer2size == 0) {
            // Buffer2 is used up, read a new page
            buffer2size = PAGE_SIZE;
            pread(sst2_fd, buffer2, PAGE_SIZE, page_idx2 * PAGE_SIZE);
            page_idx2++;
        }
        // Get key from both sst and compare
        int key1, key2, val1, val2;
        int idx1 = total_idx1 % (PAGE_SIZE / KV_PAIR_SIZE);
        int idx2 = total_idx2 % (PAGE_SIZE / KV_PAIR_SIZE);
        memcpy(&key1, buffer1 + idx1 * KV_PAIR_SIZE, sizeof(int));
        memcpy(&val1, buffer1 + idx1 * KV_PAIR_SIZE + sizeof(int), sizeof(int));
        memcpy(&key2, buffer2 + idx2 * KV_PAIR_SIZE, sizeof(int));
        memcpy(&val2, buffer2 + idx2 * KV_PAIR_SIZE + sizeof(int), sizeof(int));
        if (key1 < key2) {
            // Check if value is a tombstone at max level
            if (val1 != numeric_limits<int>::min() || levelnum != this->max_level) {
                // Append pair from sst1 to merged buffer
                memcpy(mergedBuffer + mergedIdx * KV_PAIR_SIZE, &key1, sizeof(int));
                memcpy(mergedBuffer + mergedIdx * KV_PAIR_SIZE + sizeof(int), &val1, sizeof(int));
                mergedIdx++;
            }
            // If tombstone at max level, discard it and update status
            total_idx1++;
            buffer1size -= KV_PAIR_SIZE;
        } else if (key1 > key2) {
            // Check if value is a tombstone at max level
            if (val2 != numeric_limits<int>::min() || levelnum != this->max_level) {
                // Append pair from sst2 to merged buffer
                memcpy(mergedBuffer + mergedIdx * KV_PAIR_SIZE, &key2, sizeof(int));
                memcpy(mergedBuffer + mergedIdx * KV_PAIR_SIZE + sizeof(int), &val2, sizeof(int));
                mergedIdx++;
            }
            // If tombstone at max level, discard it and update status
            total_idx2++;
            buffer2size -= KV_PAIR_SIZE;
        } else { // when key1 == key 2
            // Check if value is a tombstone at max level
            if (val2 != numeric_limits<int>::min() || levelnum != this->max_level) {
                // Key is present in both pages, use the value from buffer2
                memcpy(mergedBuffer + mergedIdx * KV_PAIR_SIZE, &key2, sizeof(int));
                memcpy(mergedBuffer + mergedIdx * KV_PAIR_SIZE + sizeof(int), &val2, sizeof(int));
                mergedIdx++;
            }
            // If tombstone at max level, discard it and update status
            total_idx1++;
            total_idx2++;
            buffer1size -= KV_PAIR_SIZE;
            buffer2size -= KV_PAIR_SIZE;
        }
        // Write/append the whole mergedBuffer to mergedFile
        if (mergedIdx != 0 && mergedIdx % (PAGE_SIZE / KV_PAIR_SIZE) == 0) {
            pwrite(merged_fd, mergedBuffer, PAGE_SIZE, merge_page * PAGE_SIZE);
            mergedIdx = 0;
            merge_page++;

            // update the file size of merged SST
            mergedSST->filesize += PAGE_SIZE;
            // TODO: KEY ARRAY
            // TODO: BLOOM FILTER

        }
    }
    // If file1 has some leftovers
    while (total_idx1 < (sst1->filesize / KV_PAIR_SIZE)) {
        // Check if we need to swap in a new page for buffer1
        if (buffer1size == 0) {
            // Buffer1 is used up, read a new page
            buffer1size = PAGE_SIZE;
            pread(sst1_fd, buffer1, PAGE_SIZE, page_idx1 * PAGE_SIZE);
            page_idx1++;
        }
        // Get key from sst1
        int key1, val1;
        int idx1 = total_idx1 % (PAGE_SIZE / KV_PAIR_SIZE);
        memcpy(&key1, buffer1 + idx1 * KV_PAIR_SIZE, sizeof(int));
        memcpy(&val1, buffer1 + idx1 * KV_PAIR_SIZE + sizeof(int), sizeof(int));
        // Check if value is a tombstone at max level
        if (val1 != numeric_limits<int>::min() || levelnum != this->max_level) {
            // Append pair from sst1 to merged buffer
            memcpy(mergedBuffer + mergedIdx * KV_PAIR_SIZE, &key1, sizeof(int));
            memcpy(mergedBuffer + mergedIdx * KV_PAIR_SIZE + sizeof(int), &val1, sizeof(int));
            mergedIdx++;
        }
        // If tombstone at max level, discard it and update status
        total_idx1++;
        buffer1size -= KV_PAIR_SIZE;
        // Write/append the whole mergedBuffer to mergedFile
        if (mergedIdx != 0 && mergedIdx % (PAGE_SIZE / KV_PAIR_SIZE) == 0) {
            pwrite(merged_fd, mergedBuffer, PAGE_SIZE, merge_page * PAGE_SIZE);
            mergedIdx = 0;
            merge_page++;
            
            // update the file size of merged SST
            mergedSST->filesize += PAGE_SIZE;
            // TODO: KEY ARRAY
            // TODO: BLOOM FILTER
        }
    }
    // If file2 has some leftovers
    while (total_idx2 < (sst2->filesize / KV_PAIR_SIZE)) {
        // Check if we need to swap in a new page for buffer2
        if (buffer2size == 0) {
            // Buffer2 is used up, read a new page
            buffer2size = PAGE_SIZE;
            pread(sst2_fd, buffer2, PAGE_SIZE, page_idx2 * PAGE_SIZE);
            page_idx2++;
        }
        // Get key from sst2
        int key2, val2;
        int idx2 = total_idx2 % (PAGE_SIZE / KV_PAIR_SIZE);
        memcpy(&key2, buffer2 + idx2 * KV_PAIR_SIZE, sizeof(int));
        memcpy(&val2, buffer2 + idx2 * KV_PAIR_SIZE + sizeof(int), sizeof(int));
        // Check if value is a tombstone at max level
        if (val2 != numeric_limits<int>::min() || levelnum != this->max_level) {
            // Append pair from sst2 to merged buffer
            memcpy(mergedBuffer + mergedIdx * KV_PAIR_SIZE, &key2, sizeof(int));
            memcpy(mergedBuffer + mergedIdx * KV_PAIR_SIZE + sizeof(int), &val2, sizeof(int));
            mergedIdx++;
        }
        // If tombstone at max level, discard it and update status
        total_idx2++;
        buffer2size -= KV_PAIR_SIZE;
        // Write/append the whole mergedBuffer to mergedFile
        if (mergedIdx != 0 && mergedIdx % (PAGE_SIZE / KV_PAIR_SIZE) == 0) {
            pwrite(merged_fd, mergedBuffer, PAGE_SIZE, merge_page * PAGE_SIZE);
            mergedIdx = 0;
            merge_page++;

            // update the file size of merged SST
            mergedSST->filesize += PAGE_SIZE;
            // TODO: KEY ARRAY
            // TODO: BLOOM FILTER
        }
    }
    // Write any remaining data in the mergedBuffer to mergedFile
    if (mergedIdx > 0) {
        pwrite(merged_fd, mergedBuffer, mergedIdx * KV_PAIR_SIZE, merge_page * PAGE_SIZE);

        // update the file size of merged SST
        mergedSST->filesize += mergedIdx * KV_PAIR_SIZE;
        // TODO: KEY ARRAY
        // TODO: BLOOM FILTER
    }
    // Postprocesses
    close(sst1_fd);
    close(sst2_fd);
    close(merged_fd);
    // Free memory
    delete[] buffer1;
    delete[] buffer2;
    delete[] mergedBuffer;
    // Return merged SST
    return mergedSST;
}

void SSTManager::buildAllHashFunctions() {
    // Seed values for the hash functions
    const size_t seeds[HASH_FUNCTION_NUM] = {
        0xEA529C2C, 0x9275AD99, 0xADFA52D9
    };
    // Populate the vector with hash functions
    for (int i = 0; i < HASH_FUNCTION_NUM; i++) {
        size_t seed = seeds[i];
        function<int(int)> hashFunction = [this, seed](int value) {
            return this->hashWithSeed(value, seed);
        };
        this->hashFunctions.push_back(hashFunction);
    }
}

size_t SSTManager::hashWithSeed(int key, size_t seed) {
    size_t hashValue = hash<int>{}(key);
    // Combine the hash code with the seed using XOR (^)
    return hashValue ^ seed;
} 