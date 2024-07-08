#include "experiments.h"

using namespace std;

const size_t KB = 1024;
const size_t MB = pow(2, 20);

// Generate a random number
int randomNumber(int lowerbound, int upperbound) {
    // Create a random number engine with a random seed
    random_device rd;
    mt19937 gen(rd());
    // Define a distribution
    uniform_int_distribution<int> distribution(lowerbound, upperbound);
    // Generate random numbers
    return distribution(gen);
}

// Experiment for put operation
void performExperiment(Database *database, vector<int> data, string size) {
    int put_counter = 0;
    double put_total_time = 0.0;
    // Perform experiment
    for (int volume : data) {
        // Perform put operation
        cout << "Performing put operations" << endl;
        // Record start time
        auto put_start_time = chrono::high_resolution_clock::now();
        while (put_counter < volume) {
            database->put(put_counter, put_counter * 10);
            put_counter++;
        }
        // Record time after completing operations for this volume
        auto put_end_time = chrono::high_resolution_clock::now();
        auto put_duration = chrono::duration_cast<std::chrono::milliseconds>(put_end_time - put_start_time).count();
        put_total_time += put_duration;
        // Calculate throughput in MB/sec
        double put_throughput = double((volume * KV_PAIR_SIZE / MB) / (put_total_time / 1000.0));
        // Keep track of experiment
        cout << volume << " Put Operations completed in " << put_duration << "ms" << endl;
        // Write the result for put to file
        ofstream put_outputFile("put_results_" + size + ".txt", ios::app);
        put_outputFile << (volume * KV_PAIR_SIZE / MB) << "," << put_throughput << endl;
        put_outputFile.close();

        // Perfor get operation when Input data size = volume
        cout << "Performing get operations" << endl;
        // Since get in step 1 is very costful, we only get 1000 times to estimates the throughput
        // Record start time
        auto get_start_time = chrono::high_resolution_clock::now();
        for (int i = 0; i < 1000; i++) {
            int key = randomNumber(0, volume);
            database->get(key);
        }
        // Record time after completing operations for this volume
        auto get_end_time = chrono::high_resolution_clock::now();
        auto get_duration = chrono::duration_cast<std::chrono::milliseconds>(get_end_time - get_start_time).count();
        // Calculate throughput in MB/sec
        double get_throughput = (1000.0 * KV_PAIR_SIZE / MB) / (get_duration / 1000.0);
        // Keep track of experiment
        cout << "1000 Get Operations completed in " << get_duration << "ms" << endl;
        // Write the result for get to file
        ofstream get_outputFile("get_results_" + size + ".txt", ios::app);
        get_outputFile << (volume * KV_PAIR_SIZE / MB) << "," << get_throughput << endl;
        get_outputFile.close();

        // Perfor scan operation when Input data size = volume
        cout << "Performing scan operations" << endl;
        // Since scan in step 1 is very costful, we only scan 1000 times to estimates the throughput
        // Record start time
        auto scan_start_time = chrono::high_resolution_clock::now();
        for (int j = 0; j < 1000; j++) {
            int key = randomNumber(0, volume);
            int range = randomNumber(0, 15);
            database->scan(key, key + range);
        }
        // Record time after completing operations for this volume
        auto scan_end_time = chrono::high_resolution_clock::now();
        auto scan_duration = chrono::duration_cast<std::chrono::milliseconds>(scan_end_time - scan_start_time).count();
        // Calculate throughput in MB/sec
        double scan_throughput = (1000.0 * KV_PAIR_SIZE / MB) / (scan_duration / 1000.0);
        // Keep track of experiment
        cout << "1000 Scan Operations completed in " << scan_duration << "ms" << endl;
        // Write the result for scan to file
        ofstream scan_outputFile("scan_results_" + size + ".txt", ios::app);
        scan_outputFile << (volume * KV_PAIR_SIZE / MB) << "," << scan_throughput << endl;
        scan_outputFile.close();
    }
}

// Clear SST data
void clearSST() {
    system("rm -f -r ./SSTs/database1MB/*");
    system("rm -f -r ./SSTs/database4MB/*");
}

int main(int argc, char* argv[]) {
    // By performing the experinment, we will open the database and operate
    // a series of API command. In this way we can prevent collisions
    if (argc != 2) {
        cerr << "Please execute ./experinment {memtable size}. E.g. ./test 1 for memtable size of 1MB" << endl;
        return 0;
    }

    clearSST();

    // Set data volumn for x-axis that increase exponantially
    // {1MB, 2MB, 4MB, 8MB, 16MB, 32MB, 64MB, 128MB, 256MB, 512MB, 1024MB}
    vector<int> data_volume = {};
    for (int i = 0; i <= 10; i++) {
        data_volume.push_back(pow(2, 20 + i) / KV_PAIR_SIZE);
    }

    string size = argv[1];

    if (size == "1") {
        // Initialize database and set memtable size to 1MB
        Database *database_1mb = new Database("database1MB", MB);
        database_1mb->open("database1MB");
        // Perform experiment with put, get and scan operation
        performExperiment(database_1mb, data_volume, "1MB");
        // Close the database
        database_1mb->close();
    } else if (size == "4") {
        // Initialize database amd set memtable size to 4MB
        Database *database_4mb = new Database("database4MB", 4 * MB);
        database_4mb->open("database4MB");
        // Perform experiment with put, get and scan operation
        performExperiment(database_4mb, data_volume, "4MB");
        // Close the database
        database_4mb->close();
    } else {
        cout << "please try size 1 or 4" << endl;
    }

    return 0;
}