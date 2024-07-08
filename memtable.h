#ifndef MEMTABLE_H
#define MEMTABLE_H

#include <iostream>
#include <vector>
#include <limits>
#include <queue>
#include <fstream>
#include <dirent.h>
#include <sys/types.h>
#include <unistd.h>
#include <fcntl.h>
#include <cstring>
#include <algorithm>
#include <map>

using namespace std;
using std::string;

class Node{
    public:
        int key;
        int val;
        int height;
        Node * left;
        Node * right;
        Node(int key, int value);
 };

class KV_Pair {
    public:
        int key;
        int val;
        KV_Pair();
        KV_Pair(int key, int val);
};

class Memtable{
    public:
        Node * root;

        // Tree methods
        Memtable(Node * root);
        int getNodeHeight(Node * node);
        int getNodeNum(Node *root);
        Node * rightRotate(Node * y);
        Node * leftRotate(Node * x);
        int getBalanceFactor(Node * N);
        Node * insertNode(Node *root, int key, int val);
        Node * getNode(Node* root, int key);

        // Other helper functions
        size_t getCurrentSize();
        void increSize(size_t size);
        void setSize(size_t size);
        // Write memtable data to a sst file
        void scanToFile(Node *cur, ofstream *MyFile);
        // Scan operation for memtable
        vector<KV_Pair *> scanMemtable(Node * cur, int lowerbound, int upperbound);
        void printTree(Node* root, int depth = 0, char prefix = 'R');

    private:
        size_t curr_size;
        size_t max_size;
};

#endif