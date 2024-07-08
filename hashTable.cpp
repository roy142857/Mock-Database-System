#include "hashTable.h"

const double HashTable::loadFactorThreshold = 0.75;

HashTable::HashTable() : table(initialSize), numElements(0) {}

void HashTable::resizeTable() {
    int newSize = table.size() * 2;
    std::vector<std::list<HashElement>> newTable(newSize);

    for (const auto& bucket : table) {
        for (const auto& element : bucket) {
            int newIndex = hashFunction(element.key1, element.key2) % newSize;
            newTable[newIndex].push_back(element);
        }
    }

    table = std::move(newTable);
}

int HashTable::hashFunction(int key1, int key2) {
    // Hash key according to it's level and page number, where key1 is level and key2 is page number
    int hash_key;
    if (key1 >= key2) {
        hash_key = key1 * (key1 + key2);
    } else {
        hash_key = key1 + key2 * key2;
    } 
    return hash_key;
}

void HashTable::insert(int key1, int key2, int value) {
    int index = hashFunction(key1, key2) % table.size();

    // Check for duplicates and update value if keys already exist
    auto it = std::find_if(table[index].begin(), table[index].end(),
                           [key1, key2](const HashElement& elem) {
                               return elem.key1 == key1 && elem.key2 == key2;
                           });

    if (it != table[index].end()) {
        it->value = value;
    } else {
        table[index].emplace_back(key1, key2, value);
        ++numElements;

        // Check for load factor and resize if necessary
        if (static_cast<double>(numElements) / table.size() > loadFactorThreshold) {
            resizeTable();
        }
    }
}

bool HashTable::get(int key1, int key2, int& value) {
    int index = hashFunction(key1, key2) % table.size();
    for (const auto& element : table[index]) {
        if (element.key1 == key1 && element.key2 == key2) {
            value = element.value;
            return true;
        }
    }
    return false;
}

void HashTable::remove(int key1, int key2) {
    int index = hashFunction(key1, key2) % table.size();
    table[index].remove_if([key1, key2](const HashElement& elem) {
        return elem.key1 == key1 && elem.key2 == key2;
    });
    --numElements;
}



