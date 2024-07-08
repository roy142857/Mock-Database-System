#include "SST.h"

// Constructor
SST::SST(int levelnum, string &prefix, bool istemp, vector<function<int(int)>> *hashFunctions) {
    // Set the levelnum and istemp attributes
    this->levelnum = levelnum;
    this->istemp = istemp;
    this->hashFunctions = hashFunctions;

    this->filepath = prefix + "L" + to_string(levelnum);
    if (istemp) {
        this->filepath += "Temp";
    }
    ofstream sstFile(this->filepath.c_str());
    sstFile.close();
}

// Destructor
SST::~SST() {
    // Attempt to remove the file
    if (std::remove(this->filepath.c_str()) != 0) {
        std::cerr << "Error deleting file." << std::endl;
    }
}

void SST::buildKeyArray() {
    // Open the file
    int fd = open(this->filepath.c_str(), O_RDONLY);
    if (fd == -1) {
        cerr << "Failed to open file: " << this->filepath << endl;
        return;
    }
    // Iterate through each page and read the first integer
    for (int i = 0; i < this->filesize; i += PAGE_SIZE) {
        // Read the first integer
        int firstInt;
        int intRead = pread(fd, &firstInt, sizeof(int), i);
        if (intRead == -1) {
            cerr << "Failed to read file by construct key array" << endl;
        }
        // Store the first integer in the array
        this->keyArray.push_back(firstInt);
    }
    // Close the file
    close(fd);
    return;
}

void SST::generateFileSize() {
    int fd = open(this->filepath.c_str(), O_RDONLY);
    this->filesize = lseek(fd, 0, SEEK_END);
    close(fd);
}

vector<int> SST::getKeyArray() {
    return this->keyArray;
}

void SST::buildBloomFilter() {
    // read the file and iterate all KV_pairs
    int fd = open(this->filepath.c_str(), O_RDONLY);
    KV_Pair* buffer = new KV_Pair[PAGE_SIZE / KV_PAIR_SIZE];

    // resize the bloomFIlterSize
    int bloomFilterSize = this->filesize / KV_PAIR_SIZE * BITS_PER_ENTRY;
    bloomFilter.resize(bloomFilterSize);

    // Calculate the number of pages
    int num_pages = this->filesize / PAGE_SIZE + (this->filesize % PAGE_SIZE != 0);
    // Iterate through each page
    for (int i = 0; i < num_pages; ++i) {        
        // read a whole page into buffer
        int errorcode = read(fd, buffer, PAGE_SIZE);
        if (errorcode == -1) {
            cerr << "Failed to read file by build bloom filter" << endl;
            return;
        }
        int numOfKVPairsInBuffer = PAGE_SIZE / sizeof(KV_Pair);
        if (i == num_pages - 1) { // this is the last page
            if (filesize % PAGE_SIZE != 0) { // this page is NOT FULL
                numOfKVPairsInBuffer = (filesize % PAGE_SIZE) / sizeof(KV_Pair);
            }
        }
        for (int j = 0; j < numOfKVPairsInBuffer; j++) {
            int key = buffer[j].key;
            // Iterate all hash functions
            for (const auto& hashfun : *hashFunctions) {
                int hashValue = hashfun(key);
                bloomFilter[abs(hashValue % bloomFilterSize)] = true;
            }
        }
    }
    
    // Postprocesses
    close(fd);
    delete[] buffer;
}

void SST::setBloomFilter(vector<bool>& bloomFilter) {
    this->bloomFilter = bloomFilter;
}

int SST::binarySearchPage(int key) {
    int left = 0;
    int right = this->keyArray.size() - 1;
    int result = -1;  // Default value if no such number is found

    while (left <= right) {
        int mid = left + (right - left) / 2;
        
        if (this->keyArray[mid] <= key) {
            result = mid;  // Update result and continue searching in the right half
            left = mid + 1;
        } else {
            right = mid - 1;  // Search in the left half
        }
    }

    return result;
}

int SST::getPotentialPageNumberOfASST(int key, int type) {   
    // If this is a GET and it doesn't pass bloom filter test, we return -1 directly
    if (type == GET && !bloomFilterCheck(key)) { 
        return -1;
    }

    // init check: If this array is empty or it's first key is larger than target key
    if (type == GET || type == UPPER) {
        // In GET operation, if the lowerest key in SST is greater than key,
        // Key should not exist in this SST.
        // In SCAN operation, if the lowerest key in SST is greater than upperbound,
        // all pairs in this SST should be outside of scan range.
        if (this->keyArray[0] > key) {
            return -1;
        }
    } else if (type == LOWER) {
        // If the minimum key in SST is greater than lowerbound, then we should start from first page
        if (this->keyArray[0] >= key) {
            return 0;
        }
    }
    // If searched key are potentially inside the SST, binary search for the potential page
    return this->binarySearchPage(key);
}

bool SST::bloomFilterCheck(int key) {
    int bloomFilterSize = this->filesize / KV_PAIR_SIZE * BITS_PER_ENTRY;
    for (const auto& hashfun : *hashFunctions) {
        int hashValue = hashfun(key);
        if (!bloomFilter[abs(hashValue % bloomFilterSize)]) {
            return false;
        }
    }
    return true;
}

// Functions for debug testing
void SST::printSST() {
    int fd = open(this->filepath.c_str(), O_RDONLY);
    int num_pairs = this->filesize / KV_PAIR_SIZE;
    for (int i = 0; i < num_pairs; i++) {
        int key, val;
        pread(fd, &key, sizeof(int), i * KV_PAIR_SIZE);
        pread(fd, &val, sizeof(int), i * KV_PAIR_SIZE  + sizeof(int));
        cout << "(" << key << "," << val << ") ";
    }
    cout << endl;
}

void SST::printKeyArray() {
    cout << "Key Array of " << this->filepath << ": ";
    for (auto num : this->keyArray) {
        cout << " " << num << " ";
    }
    cout << endl;
}

void SST::printBloomFilter() {
    cout << "Bloom Filter of " << this->filepath << ": ";
    for (auto bit : this->bloomFilter) {
        cout << bit;
    }
    cout << endl;
}

void SST::printBuffer(KV_Pair* buffer) {
    for (int i = 0; i < 512; i++) {
        cout << buffer[i].key << " ";
    }
}
