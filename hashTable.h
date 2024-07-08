#ifndef HASHTABLE_H
#define HASHTABLE_H

#include <iostream>
#include <vector>
#include <list>
#include <algorithm>

class HashElement {
public:
    int key1;
    int key2;
    int value;

    HashElement(int k1, int k2, int v) : key1(k1), key2(k2), value(v) {}
};

class HashTable {
private:
    static const int initialSize = 100;
    static const double loadFactorThreshold;

    std::vector<std::list<HashElement>> table;
    int numElements;

    void resizeTable();

    int hashFunction(int key1, int key2);

public:
    HashTable();

    void insert(int key1, int key2, int value);

    bool get(int key1, int key2, int& value);

    void remove(int key1, int key2);
};

#endif // HASHTABLE_H
