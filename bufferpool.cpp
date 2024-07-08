#include "bufferpool.h"

BufferPool::BufferPool() {
    this->referenced.reset(); // Initialize bitmap to all zeros
    this->hand = 0;

    // Allocate memory for each item in the buffer
    for (int i = 0; i < BUFFER_SIZE; ++i) {
        this->data[i] = static_cast<KV_Pair *>(malloc(PAGE_SIZE));
    }

    for (int i = 0; i < BUFFER_SIZE; ++i) {
        this->hashedKeysInBuffer[i] = {};
    }
}

BufferPool::~BufferPool() {
    // Deallocate memory for each item in the buffer
    for (int i = 0; i < BUFFER_SIZE; ++i) {
        free(this->data[i]);
    }
}

int BufferPool::findEmptySlot() {
    for (int i = 0; i < BUFFER_SIZE; ++i) {
        if (!this->referenced[i]) {
            return i;
        }
    }
    return -1; // Buffer full
}

int BufferPool::clockEvict() {
    while (1) {
        if (!this->referenced[this->hand]) { // If referenced[hand] = 0, means evict this page
            // Evict this page
            int evictedIndex = this->hand;
            // Reset the evicted page
            pair<int, int> evictKeyPair = hashedKeysInBuffer[evictedIndex];
            this->dictionary.remove(evictKeyPair.first, evictKeyPair.second); // Remove it from dictionary
            this->hashedKeysInBuffer[evictedIndex] = {};

            // Move the hand to the next position
            this->hand = (this->hand + 1) % BUFFER_SIZE;
            return evictedIndex;
        } else {
            // Mark the page as unreferenced
            this->referenced[this->hand] = 0;
        }

        // Move the hand to the next position
        this->hand = (this->hand + 1) % BUFFER_SIZE;
    }
}

int BufferPool::hashKey(int level, int pagenum) {
    // Hash key according to it's level and page number
    int hash_key;
    if (level >= pagenum) {
        hash_key = level * (level + pagenum);
    } else {
        hash_key = level + pagenum * pagenum;
    } 
    return hash_key;
}

void BufferPool::evictPages(SST *file, int pagenum) {
    // Check if page is in buffer
    int pageIdx;
    if (this->dictionary.get(file->levelnum, pagenum, pageIdx)) {
        // Mark this page unreferenced
        this->referenced[pageIdx] = 0;
        // Remove hash key from dictionary
        this->dictionary.remove(file->levelnum, pagenum);
        // Reset the reference in hashedKeysInBuffer
        this->hashedKeysInBuffer[pageIdx] = {};
    }
}

vector<KV_Pair *> BufferPool::fetchPage(SST *file, int pagenum) {
    // Creating hash key
    vector<KV_Pair *> result = {};
    int pageIndex;
    if (this->dictionary.get(file->levelnum, pagenum, pageIndex)) {
        this->referenced[pageIndex] = 1; // Mark as referenced
    } else {
        // Page not in the buffer, fetch from disk
        pageIndex = findEmptySlot();
        if (pageIndex == -1) { // If buffer is full,
            // evict a page using clock algorithm
            pageIndex = clockEvict();
        }
        // Track buffer information
        // Update the buffer
        this->dictionary.insert(file->levelnum, pagenum, pageIndex);
        this->hashedKeysInBuffer[pageIndex] = make_pair(file->levelnum, pagenum);
        // pread the real data from disk
        int fd = open(file->filepath.c_str(), O_RDONLY);
        if (fd == -1) {
            cerr << "Error reading file" << endl;
            close(fd);
        }
        // Copy the page to buffer
        pread(fd, data[pageIndex], PAGE_SIZE, pagenum * PAGE_SIZE);
        this->referenced[pageIndex] = 1; // Mark as referenced
        close(fd);
    }
    // Get number of KV_Pair in the vector
    int num_pairs = PAGE_SIZE / KV_PAIR_SIZE;
    if (file->filesize % PAGE_SIZE != 0 && pagenum == file->filesize / PAGE_SIZE) {
        num_pairs = (file->filesize % PAGE_SIZE) / KV_PAIR_SIZE;
    }
    // Construct vector of kv pairs
    for (int i = 0; i < num_pairs; i++) {
        KV_Pair * pair = data[pageIndex] + i;
        result.push_back(pair);
    }
    // Return vector of kv pairs
    return result;
}

void BufferPool::printBufferContents() {
    for (int i = 0; i < BUFFER_SIZE; ++i) {
        if (this->referenced[i]) {
            std::cout << "Index: " << i << ", Page Offset: " << (i % PAGE_SIZE) << std::endl;
        }
    }
}

// Accessor functions for testing purporse
HashTable BufferPool::getDictionary() {
    return this->dictionary;
}

bitset<BUFFER_SIZE> BufferPool::getReference() {
    return this->referenced;
}