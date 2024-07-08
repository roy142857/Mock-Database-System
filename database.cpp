#include "database.h"


// Database Constructor
Database::Database(string name, size_t table_size) {
    this->name = name;
    this->table_size = table_size;
}


// Accessor functions for testing purpose
BufferPool* Database::getBufferPool() {
    return this->bufferpool;
}


// --- Helper functions ---

// Function to create a directory
bool Database::createDirectory(const char *path) {
    #ifdef _WIN32  // Windows
        if (CreateDirectory(path.c_str(), NULL) || GetLastError() == ERROR_ALREADY_EXISTS) {
            return true;
        } else {
            return false;
        }
    #else  // Unix-based systems
        if (mkdir(path, 0755) == 0) {
            return true;
        } else {
            return false;
        }
    #endif
}

// Combine two vector two make it sorted without duplicates
vector<KV_Pair *> combineVectors(vector<KV_Pair *> vec1, vector<KV_Pair *> vec2) {
    vector<KV_Pair *> results;
    map<int, KV_Pair *> combined_map;
    // Add kvpair in vec2 so later on vec1 will override
    for (KV_Pair *kv_pair : vec2) {
        combined_map[kv_pair->key] = kv_pair;
    }
    // Add kvpair in vec1, if there are duplicates, override
    for (KV_Pair *kv_pair : vec1) {
        combined_map[kv_pair->key] = kv_pair;
    }
    // Add values to results vector
    for (const auto kv_pair : combined_map) {
        results.push_back(kv_pair.second);
    }
    return results;
}

// Binary search on vector of key value pairs
int binarySearchKVPairs(vector<KV_Pair *> &pairs, int key) {
    int low = 0;
    int high = pairs.size() - 1;

    while (low <= high) {
        int mid = low + (high - low) / 2;
        KV_Pair *midPair = pairs[mid];

        if (midPair->key == key) {
            // Key found, return the value
            return midPair->val;
        } else if (midPair->key < key) {
            low = mid + 1;
        } else {
            high = mid - 1;
        }
    }
    // Key not found
    return -1;
}

// Filter all the keys with tombstone value
vector<KV_Pair *> filterTombstone(vector<KV_Pair *> pairs) {
    vector<KV_Pair *> result;
    for (const auto &pair : pairs) {
        if (pair->val != numeric_limits<int>::min()) {
            result.push_back(pair);
        }
    }
    return result;
}


// --- Database API ---
Database *Database::open(string name) {
    // Create Directory to store SSTs
    createDirectory(string("./SSTs/").c_str());
    // Initialize Memtable
    Memtable *table = new Memtable(NULL);
    this->table = table;
    // Set memtable size
    this->table->setSize(this->table_size);
    // Initialize buffer pool
    this->bufferpool = new BufferPool();
    // Create SST_PATH
    this->SST_PATH = "./SSTs/" + name + "/";
    createDirectory(SST_PATH.c_str());
    // Initialize SST Manager
    if (this->sstManager == NULL) {
        this->sstManager = new SSTManager();
    }
    return this;
}

void Database::close() {
    // If memtable is not empty, transform to SST
    if (this->table->root != NULL) {
        this->sstManager->createSST(this->table, this->SST_PATH, this->bufferpool);
    }
    // Deconstruct memtable and buffer pool
    delete this->table;
    this->bufferpool->~BufferPool();
}

int Database::get(int key) {
    // Search node in memtable
    Node * node = this->table->getNode(this->table->root, key);
    // If did not exist, search on all SSTs
    if (node == NULL) {
        // Traverse each level SST to search for the key
        for (int level = 1; level <= this->sstManager->max_level; level++) {
            SST* sst = this->sstManager->getSST(level);
            if (sst == NULL) { continue; };
            int potential_page = sst->getPotentialPageNumberOfASST(key, GET);
            if (potential_page != -1) {
                // Retrieve the page from buffer pool
                vector<KV_Pair *> pairs = this->bufferpool->fetchPage(sst, potential_page);
                int value = binarySearchKVPairs(pairs, key);
                // Return value, even it is a tombstone
                return value;
            }
        }
        // Key does not exist
        return numeric_limits<int>::min();
    }
    // Return value, even it is a tombstone
    return node->val;
}

void Database::put(int key, int val) {
    // Check for duplicate keys for memtable
    Node *node = this->table->getNode(this->table->root, key);
    if (node != NULL) {
        // Update value if key is in memtable
        node->val = val;
    } else {
        this->table->root = this->table->insertNode(this->table->root, key, val);
        this->table->increSize(KV_PAIR_SIZE);
        if (this->table->getCurrentSize() >= table_size) {
            // Move memtable to SST
            this->sstManager->createSST(table, this->SST_PATH, this->bufferpool);
            // Flush the memtable
            delete this->table;
            Memtable *new_table = new Memtable(NULL);
            this->table = new_table;
            this->table->setSize(this->table_size);
        }
    }
}

vector<KV_Pair *> Database::scan(int lowerbound, int upperbound) {
    // Search in memtable
    vector<KV_Pair *> result = this->table->scanMemtable(this->table->root, lowerbound, upperbound);
    // Return if all key from lowerbound to upperbound is already in the result
    if (int(result.size()) == (upperbound - lowerbound + 1)) {
        // Check if a tombstone value is in memtable
        return filterTombstone(result);
    }
    // Search in SSTs
    for (int level = 1; level <= this->sstManager->max_level; level++) {
        SST* sst = this->sstManager->getSST(level);
        if (sst == NULL) { continue; };
        // Determine potential pages for the scan range
        int lowerbound_pp = sst->getPotentialPageNumberOfASST(lowerbound, LOWER);
        int upperbound_pp = sst->getPotentialPageNumberOfASST(upperbound, UPPER);
        // If there are pages contains the range
        if (lowerbound_pp != -1) {
            vector<KV_Pair *> pageResults = {};
            for(int start = lowerbound_pp; start <= upperbound_pp; start++) {
                // Retrieve the page from the buffer pool
                vector<KV_Pair *> pairs = this->bufferpool->fetchPage(sst, start);
                // Iterate through the key-value pairs and add to result if within the range
                for (const auto &pair : pairs) {
                    // If page is in between scan range and not a tombstone, add to result
                    if (pair->val != numeric_limits<int>::min() && pair->key >= lowerbound && pair->key <= upperbound) {
                        pageResults.push_back(pair);
                    }
                }
            }
            result = combineVectors(result, pageResults);
            // Return if all key from lowerbound to upperbound is already in the result
            if (int(result.size()) == (upperbound - lowerbound + 1)) {
                return filterTombstone(result);
            }
        }
    }
    return filterTombstone(result);
}

void Database::delete_(int key) {
    // Put a tombstone value with key into the database
    this->put(key, numeric_limits<int>::min());
}

void Database::update(int key, int value) {
    // Since put handles duplicate keys, simply call put function
    this->put(key, value);
}
