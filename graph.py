import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
import numpy as np


# Define a custom formatter for the y-axis labels
def y_axis_formatter(value, _):
    return f"{value:.4f}"

# Read results from files and store them in lists
with open("put_results_1MB.txt") as put_file:
    put_data_volume_1mb, put_throughput_1mb = [], []
    for line in put_file:
        put_data_volume_1mb.append(line.strip("\n").split(",")[0])
        put_throughput_1mb.append(float(line.strip("\n").split(",")[1]))
put_file.close()
with open("put_results_4MB.txt") as put_file:
    put_data_volume_4mb, put_throughput_4mb = [], []
    for line in put_file:
        put_data_volume_4mb.append(line.strip("\n").split(",")[0])
        put_throughput_4mb.append(float(line.strip("\n").split(",")[1]))
put_file.close()

# Create a plot for put operation
plt.plot(put_data_volume_1mb, put_throughput_1mb, label="Memtable Size (1MB)")
plt.plot(put_data_volume_4mb, put_throughput_4mb, label="Memtable Size (4MB)")
plt.xticks(range(0, len(put_data_volume_1mb)), put_data_volume_1mb)
plt.xlabel("Input Data Size (MB)")
plt.ylabel("Throughput (MB/S)")
plt.title("Throughput vs. Input Data Size")
plt.legend()

# Save or display the plot
plt.savefig("put_throughput.png")
plt.show()
plt.close()

with open("get_results_1MB.txt") as get_file:
    get_data_volume_1mb, get_throughput_1mb = [], []
    for line in get_file:
        get_data_volume_1mb.append(line.strip("\n").split(",")[0])
        get_throughput_1mb.append(float(line.strip("\n").split(",")[1]))
get_file.close()
with open("get_results_4MB.txt") as get_file:
    get_data_volume_4mb, get_throughput_4mb = [], []
    for line in get_file:
        get_data_volume_4mb.append(line.strip("\n").split(",")[0])
        get_throughput_4mb.append(float(line.strip("\n").split(",")[1]))
get_file.close()

# Create a plot for get operation
plt.plot(get_data_volume_1mb, get_throughput_1mb, label="Memtable Size (1MB)")
plt.plot(get_data_volume_4mb, get_throughput_4mb, label="Memtable Size (4MB)")
plt.xticks(range(0, len(get_data_volume_1mb)), get_data_volume_1mb)
plt.xlabel("Input Data Size (MB)")
plt.ylabel("Throughput (MB/S)")
# plt.gca().yaxis.set_major_formatter(FuncFormatter(y_axis_formatter))
plt.title("Throughput vs. Input Data Size")
plt.legend()

# Save or display the plot
plt.savefig("get_throughput.png")
plt.show()
plt.close()

with open("scan_results_1MB.txt") as scan_file:
    scan_data_volume_1mb, scan_throughput_1mb = [], []
    for line in scan_file:
        scan_data_volume_1mb.append(line.strip("\n").split(",")[0])
        scan_throughput_1mb.append(float(line.strip("\n").split(",")[1]))
scan_file.close()
with open("scan_results_4MB.txt") as scan_file:
    scan_data_volume_4mb, scan_throughput_4mb = [], []
    for line in scan_file:
        scan_data_volume_4mb.append(line.strip("\n").split(",")[0])
        scan_throughput_4mb.append(float(line.strip("\n").split(",")[1]))
scan_file.close()

# Create a plot for scan operation
plt.plot(scan_data_volume_1mb, scan_throughput_1mb, label="Memtable Size (1MB)")
plt.plot(scan_data_volume_4mb, scan_throughput_4mb, label="Memtable Size (4MB)")
plt.xticks(range(0, len(scan_data_volume_1mb)), scan_data_volume_1mb)
plt.xlabel("Input Data Size (MB)")
plt.ylabel("Throughput (MB/S)")
# plt.gca().yaxis.set_major_formatter(FuncFormatter(y_axis_formatter))
plt.title("Throughput vs. Input Data Size")
plt.legend()

# Save or display the plot
plt.savefig("scan_throughput.png")
plt.show()
plt.close()