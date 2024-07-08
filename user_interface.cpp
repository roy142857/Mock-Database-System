#include "user_interface.h"


const size_t MB = pow(2, 20);

void printUsage() {
    cout << "Usage:" << endl
              << "  put <key> <value>" << endl
              << "  get <key>" << endl
              << "  scan <lowerbound> <upperbound>" << endl
              << "  update <key> <new_value>" << endl
              << "  delete <key>" << endl
              << "  open <database name>" << endl
              << "  close" << endl
              << "  exit terminate the program" << endl;
}

int main() {
    // Initialize map to store database by name
    map<string, Database*> databases;
    // Keep track of current opened database
    Database *cur_db = NULL;
    // Executing the program
    while (true) {
        // Get command line input
        string command;
        cout << "> ";
        getline(cin, command);

        // Process the input
        istringstream iss(command);
        vector<std::string> args;

        // Extract space-separated arguments from the input line
        while (iss >> command) {
            args.push_back(command);
        }

        if (args.empty()) {
            continue;
        }

        // Make string all lowercase and check if it is a valid command
        string action = args[0];
        transform(action.begin(), action.end(), action.begin(), ::tolower);

        if (action == "exit") {
            break;
        } else if (action == "put") {
            // Check if there are enough parameters
            if (args.size() == 3) {
                // Check whether there is a database opened currently
                if (cur_db == NULL) {
                    cout << "No database are currently opened, please execute the open command." << endl;
                    cout << "Type 'help' for usage." << endl;
                    continue;
                }
                // Perform put operation in current opened database
                int key = stoi(args[1]);
                int value = stoi(args[2]);
                cur_db->put(key, value);
                cout << "Put KV-pair (" << key << ", " << value << ") into Database" << endl;
            } else {
                cout << "Invalid parameters for put. Type 'help' for usage." << endl;
            }
        } else if (action == "get") {
            // Check if there are enough parameters
            if (args.size() == 2) {
                // Check whether there is a database opened currently
                if (cur_db == NULL) {
                    cout << "No database are currently opened, please execute the open command." << endl;
                    cout << "Type 'help' for usage." << endl;
                    continue;
                }
                // Perform get operation in current opened database
                int key = stoi(args[1]);
                int value = cur_db->get(key);
                if (value != numeric_limits<int>::min()) {
                    cout << "Value = " << value << endl;
                } else {
                    cout << "Key does not exist" << endl;
                }
            } else {
                cout << "Invalid parameters for get. Type 'help' for usage." << endl;
            }
        } else if (action == "scan") {
            // Check if there are enough parameters
            if (args.size() == 3) {
                // Check whether there is a database opened currently
                if (cur_db == NULL) {
                    cout << "No database are currently opened, please execute the open command." << endl;
                    cout << "Type 'help' for usage." << endl;
                    continue;
                }
                // Perform scan operation in current opened database
                int lowerbound_key = stoi(args[1]);
                int upperbound_key = stoi(args[2]);
                vector<KV_Pair*> result = cur_db->scan(lowerbound_key, upperbound_key);
                cout << "Scan result = ";
                for (const auto &pair : result) {
                    cout << "(" << pair->key << ", " << pair->val << ") ";
                }
                cout << endl;
            } else {
                cout << "Invalid parameters for scan. Type 'help' for usage." << endl;
            }
        } else if (action == "update") {
            // Check if there are enough parameters
            if (args.size() == 3) {
                // Check whether there is a database opened currently
                if (cur_db == NULL) {
                    cout << "No database are currently opened, please execute the open command." << endl;
                    cout << "Type 'help' for usage." << endl;
                    continue;
                }
                // Perform update operation in current opened database
                int key = stoi(args[1]);
                int value = stoi(args[2]);
                cur_db->update(key, value);
                cout << "Updated key " << key << " with " << value << " in Database." << endl;
            } else {
                cout << "Invalid parameters for update. Type 'help' for usage." << endl;
            }
        } else if (action == "delete") {
            // Check if there are enough parameters
            if (args.size() == 2) {
                // Check whether there is a database opened currently
                if (cur_db == NULL) {
                    cout << "No database are currently opened, please execute the open command." << endl;
                    cout << "Type 'help' for usage." << endl;
                    continue;
                }
                // Perform delete operation in current opened database
                int key = stoi(args[1]);
                cur_db->delete_(key);
                cout << "Key " << key << " has been deleted in database." << endl;
            } else {
                cout << "Invalid parameters for delete. Type 'help' for usage." << endl;
            }
        } else if (action == "open") {
            // Check if there are enough parameters
            if (args.size() == 2) {
                string name = args[1];
                // We can open only 1 database at a time
                if (cur_db != NULL) {
                    cout << "Database " << cur_db->name << " is currently opened." << endl;
                    cout << "Please close " << cur_db->name << " before open a new database." << endl;
                    continue;
                }
                // Check whether this database already created
                cur_db = databases[name];
                if (cur_db == NULL) {
                    // Open an new database with associated name and Memtable size of 4MB
                    Database *database = new Database(name, 4 * MB);
                    // Set cur_db and add new database to databases map
                    cur_db = database;
                    databases[name] = database;
                    // Perform open operation
                    cur_db->open(name);
                    cout << "Opened Database: " << name << endl;
                } else {
                    cur_db->open(name);
                    cout << "Opened Database: " << name << endl;
                }
            } else {
                cout << "Invalid parameters for open. Type 'help' for usage." << endl;
            }
        } else if (action == "close") {
            // Check if there are enough parameters
            if (args.size() == 1) {
                // Check whether there is a database opened currently
                if (cur_db != NULL) {
                    // Close currently opened database
                    cur_db->close();
                    cout << "Database: " << cur_db->name << " closed." << endl;
                    // Set currently opened database to be NULL
                    cur_db = NULL;
                } else {
                    cout << "No database are currently opened, please execute the open command." << endl;
                    cout << "Type 'help' for usage." << endl;
                    continue;
                }
            } else {
                cout << "Invalid parameters for close. Type 'help' for usage." << endl;
            }
        } else if (action == "help" || action == "h") {
            printUsage();
        } else {
            cout << "Unknown command. Type 'help' for usage." << endl;
        }
    }

    return 0;
}