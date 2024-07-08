#ifndef DATABASE_H
#define DATABASE_H

#include "memtable.h"
#include "bufferpool.h"
#include "SSTManager.h"
#include "hashTable.h"
#include <sys/stat.h>

class Database {
    public:
        size_t table_size;
        string name;

        // Constructor
        Database(string name, size_t table_size);

        // Database API
        Database *open(string name);
        void close();
        int get(int key);
        void put(int key, int val);
        vector<KV_Pair *> scan(int lowerbound, int upperbound);
        void delete_(int key);
        void update(int key, int value);

        // Other helper functions
        vector <string *> listSSTs();
        // Function creates directory
        bool createDirectory(const char *path);
        // Accessors for private data for testing purpose
        BufferPool* getBufferPool();
        // Accessors for private data for testing purpose
        SSTManager *getsstManager() {return sstManager;};

    private:
        // Keep track of current memtable
        Memtable *table;
        // SST path
        string SST_PATH;
        // Buffer pool
        BufferPool *bufferpool;
        // SST Manager that manages the metadata of all SSTs
        SSTManager *sstManager;
};

#endif