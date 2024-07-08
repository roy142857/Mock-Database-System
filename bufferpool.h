#ifndef BUFFERPOOL_H
#define BUFFERPOOL_H

#include <iostream>
#include <vector>
#include <unordered_map>
#include <bitset>
#include "SST.h"
#include "memtable.h"
#include "hashTable.h"

#define BUFFER_SIZE 1024

class BufferPool {
public:
    BufferPool();
    ~BufferPool();

    // Hash function
    int hashKey(int level, int pagenum);
    // Evict pages according to deleted SSTs
    void evictPages(SST *file, int pagenum);
    // FetchPage takes a SST file and page number as input, get the real page from file and store it in bufferpool
    vector<KV_Pair *> fetchPage(SST *file, int pagenum);
    void printBufferContents();
    // Some accessors are created for testing purpose
    HashTable getDictionary();
    bitset<BUFFER_SIZE> getReference();

private:
    std::array<KV_Pair *, BUFFER_SIZE> data;
    // std::unordered_map<int, int> dictionary; // map hashed key to index
    HashTable dictionary;
    std::array<pair<int, int>, BUFFER_SIZE> hashedKeysInBuffer;
    std::bitset<BUFFER_SIZE> referenced;    // bitmap to track referenced pages
    int hand;  // Clock hand position

    int findEmptySlot();
    int clockEvict();
};

#endif // BUFFERPOOL_H