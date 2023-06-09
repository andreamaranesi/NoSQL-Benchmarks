import seaborn as sns
import pandas as pd
import matplotlib.pyplot as plt
import re
import os


# Function to parse the log file
def parse_log(filename):
    # Initialize dataframes for throughput and time series, and dictionary for global metrics
    throughput_df = pd.DataFrame(columns=['Time', 'Operations', 'Throughput'])
    time_series_df = pd.DataFrame(columns=['CRUD', 'Time', 'Latency'])
    global_metrics = {}

    # Open the log file
    with open(filename, 'r') as f:
        # Iterate over each line in the file
        for line in f:
            # If the line includes 'current ops/sec', it's related to throughput
            if 'current ops/sec' in line:
                # Extract time, operations, and throughput using regular expressions
                time = int(re.search(r'(\d+) sec', line).group(1))
                operations = int(re.search(r'(\d+) operations', line).group(1))
                throughput = float(re.search(r'(\d+\.?\d*) current ops/sec;', line).group(1))
                # Create a new row and append it to the dataframe
                new_row = pd.DataFrame({'Time': [time], 'Operations': [operations], 'Throughput': [throughput]})
                throughput_df = pd.concat([throughput_df, new_row])
            # If the line starts with '[OVERALL]', it's related to the overall runtime and throughput
            elif line.startswith('[OVERALL]'):
                metric, value = line.strip().split(',')[1:]
                global_metrics[metric.strip()] = float(value)
            # If the line starts with '[CRUD],' where CRUD can be 'READ', 'INSERT', 'UPDATE', or 'DELETE',
            # it's related to the time series or global metrics for CRUD operations
            elif any(line.startswith(f'[{crud_type}],') for crud_type in ['READ', 'INSERT', 'UPDATE', 'DELETE']):
                # If the line includes one of the global metrics, extract it and add to the dictionary
                if any(metric in line for metric in ['Operations', 'AverageLatency(us)', 'MinLatency(us)', 'MaxLatency(us)', 'Return=OK']):
                    crud_type, metric, value = line.strip().split(',')
                    global_metrics[f'{crud_type.strip("[]")}_{metric}'] = float(value)
                # Otherwise, it's related to the time series
                else:
                    crud_type, time, latency = line.strip().split(',')
                    # Create a new row and append it to the dataframe
                    new_row = pd.DataFrame({'CRUD': [crud_type.strip("[]")], 'Time': [int(time)], 'Latency': [float(latency)]})
                    time_series_df = pd.concat([time_series_df, new_row])

    return throughput_df, time_series_df, global_metrics

# Function to compare two benchmarks
def compare_benchmarks(filename1, filename2):
    # Get the labels for the plots from the filenames
    label1 = os.path.basename(filename1).split('.')[0]
    label2 = os.path.basename(filename2).split('.')[0]
    
    # Set seaborn style for better plots
    sns.set(style="darkgrid", palette="bright")

    # Parse the log files
    throughput_df1, time_series_df1, global_metrics1 = parse_log(filename1)
    throughput_df2, time_series_df2, global_metrics2 = parse_log(filename2)

    # Compare the global metrics
    with open('global_metrics.txt', 'w') as f:
        print("=== Global Metrics ===", file=f)
        for key in sorted(set(global_metrics1.keys()).union(global_metrics2.keys())):
            value1 = global_metrics1.get(key, 'N/A')
            value2 = global_metrics2.get(key, 'N/A')
            print(f'{key}: {value1} vs {value2}', file=f)
        print(file=f)

    # Compare the throughput time series
    plt.figure(figsize=(10, 6))
    sns.lineplot(x='Time', y='Throughput', data=throughput_df1, label=label1)
    sns.lineplot(x='Time', y='Throughput', data=throughput_df2, label=label2)
    plt.xlabel('Time (s)')
    plt.ylabel('Throughput (ops/sec)')
    plt.title('Throughput Time Series Comparison')
    plt.legend()
    plt.savefig('throughput_comparison.png')
    #plt.show()

    # Compare time series for CRUD operations
    crud_operations = set(time_series_df1['CRUD']).union(set(time_series_df2['CRUD']))
    crud_operations.discard("CLEANUP")
    
    for crud_type in crud_operations:
        # Compare the time series for each CRUD operation
        plt.figure(figsize=(10, 6))
        subset1 = time_series_df1[time_series_df1['CRUD'] == crud_type]
        subset2 = time_series_df2[time_series_df2['CRUD'] == crud_type]
        if not subset1.empty:
            sns.lineplot(x='Time', y='Latency', data=subset1, label=f'{crud_type} {label1}')
        if not subset2.empty:
            sns.lineplot(x='Time', y='Latency', data=subset2, label=f'{crud_type} {label2}')

        plt.xlabel('Time (ms)')
        plt.ylabel('Latency (us)')
        plt.title(f'Time Series Comparison for {crud_type} Operations')
        plt.legend()
        plt.savefig(f'{crud_type}_comparison.png')
       #plt.show()

# Test with the benchmark files
compare_benchmarks('test7_mongo_hbase/mongodb.dat', 'test7_mongo_hbase/hbase.dat')
