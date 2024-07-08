#include "memtable.h"


// --- Tree methods ---
Node::Node(int key, int val) { // Node constructor
    this->key = key;
    this->val = val;
    this->left = NULL;
    this->right = NULL;
    this->height = 1;
}

Memtable::Memtable(Node* root){ // Memtable constructor
    this->root = root;
}


KV_Pair::KV_Pair() { // KV_Pair default constructor
}

KV_Pair::KV_Pair(int key, int val) { // KV_Pair constructor
    this->key = key;
    this->val = val;
}

int max(int a, int b) {
  return (a > b) ? a : b;
}

int Memtable::getNodeHeight(Node * node) {
    if (node == NULL)
        return 0;
    return node->height;
}

int Memtable::getNodeNum(Node *root){
    if (root == NULL)
        return 0;
    else
        return 1 + getNodeNum(root->left) + getNodeNum(root->right);
}

Node * Memtable::rightRotate(Node * y) {
    Node *x = y->left;
    Node *z = x->right;
    x->right = y;
    y->left = z;
    y->height = max(getNodeHeight(y->left), getNodeHeight(y->right)) +1;
    x->height = max(getNodeHeight(x->left), getNodeHeight(x->right)) +1;
    return x;
}

Node * Memtable::leftRotate(Node * x) {
    Node *y = x->right;
    Node *z = y->left;
    y->left = x;
    x->right = z;
    x->height = max(getNodeHeight(x->left), getNodeHeight(x->right)) + 1;
    y->height = max(getNodeHeight(y->left), getNodeHeight(y->right)) + 1;
    return y;
}

int Memtable::getBalanceFactor(Node * node) {
    if (node == NULL) 
        return 0;
    return getNodeHeight(node->left) - getNodeHeight(node->right);
}

Node * Memtable::insertNode(Node * root, int key, int val) {
    if (root == NULL)
        return new Node(key, val);

    // insert node
    if (key < root->key) {
        root->left = insertNode(root->left, key, val);
    }
    else if (key > root->key) {
        root->right = insertNode(root->right, key, val);
    }
    else { // Note: insert a key that is already in Memtable is handled here (which means Update?)
        root->val = val; // key == root->key, so we replace the old value with new value
        return root;
    }

    root->height = max(getNodeHeight(root->left), getNodeHeight(root->right)) + 1;

    // Update the balance factor of each node and balance the tree
    int balanceFactor = getBalanceFactor(root);
    if (balanceFactor > 1) {
        if (key < root->left->key) {
            return rightRotate(root);
        } else if (key > root->left->key) {
            root->left = leftRotate(root->left);
            return rightRotate(root);
        }
    }
    if (balanceFactor < -1) {
        if (key > root->right->key) {
            return leftRotate(root);
        } else if (key < root->right->key) {
            root->right = rightRotate(root->right);
            return leftRotate(root);
        }
    }

    return root;
}

Node * Memtable::getNode(Node* root, int key) {
    if (root == NULL)
        return NULL;
    if (root->key == key) {  // found
        return root;
    }
    else if (root->key < key) {
        return getNode(root->right, key);
    }
    else {
        return getNode(root->left, key);
    }
}

// helperful function for memtable
void Memtable::setSize(size_t size) {
    max_size = size;
}

// Get current size of memtable
size_t Memtable::getCurrentSize() {
    return this->curr_size;
}

// Increase memtable size
void Memtable::increSize(size_t size) {
    this->curr_size += size;
}

// Scan the memtable to SST
void Memtable::scanToFile(Node *cur, ofstream *MyFile) {
    // Base case:
    if (cur == NULL) {
        return;
    };
    // Recursive scan
    scanToFile(cur->left, MyFile);
    MyFile->write( reinterpret_cast<const char *>(&cur->key), sizeof(int));
    MyFile->write( reinterpret_cast<const char *>(&cur->val), sizeof(int));
    scanToFile(cur->right, MyFile);
}

// Helper function for binary search on the memtable
vector<KV_Pair *> Memtable::scanMemtable(Node * cur, int lowerbound, int upperbound) {
    vector<KV_Pair *> results;
    // If memtable is empty
    if (cur == NULL) {
        return results;
    }
    // Scan left node if key value is greater than lowerbound
    if (lowerbound < cur->key) {
        vector<KV_Pair *> leftResults = scanMemtable(cur->left, lowerbound, upperbound);
        results.insert(results.end(), leftResults.begin(), leftResults.end());
    }
    // Scan current node if it's in range
    if (lowerbound <= cur->key && cur->key <= upperbound) {
        results.push_back(new KV_Pair(cur->key, cur->val));
    }
    // Scan right node if key value is less than upperbound
    if (cur->key < upperbound) {
        vector<KV_Pair *> rightResults = scanMemtable(cur->right, lowerbound, upperbound);
        results.insert(results.end(), rightResults.begin(), rightResults.end());
    }
    return results;
}

void Memtable::printTree(Node* root, int depth, char prefix) {
    if (root == nullptr) {
        return;
    }

    // Adjust spacing based on depth for visual representation
    const int spacing = 4;
    for (int i = 0; i < depth * spacing; ++i) {
        cout << ' ';
    }

    // Print the current node
    cout << prefix << "--(" << root->key << ", " << root->val << ")" << endl;

    // Recursively print the right subtree
    printTree(root->right, depth + 1, 'R');

    // Print the left subtree
    printTree(root->left, depth + 1, 'L');
}











