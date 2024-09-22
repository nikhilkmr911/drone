import pandas as pd

# Load the CSV file
input_csv = r'D:\normal logs\PTL\baro_log.csv'  # replace with your input CSV file path
output_csv = r'D:\normal logs\PTL\filtered_baro_log.csv'  # replace with your desired output CSV file path
error_csv = r'D:\normal logs\PTL\err_log.csv'  # replace with your error messages CSV file path

# Threshold value for altitude difference
threshold_altitude_difference = 1  # adjust as needed

# Read the CSV files into DataFrames
df = pd.read_csv(input_csv)
error_df = pd.read_csv(error_csv)

# Filter the DataFrame for rows where column 'I' is 0
filtered_df = df[df['I'] == 0]

# Initialize variables for tracking
current_max = None
current_max_timestamp = None
current_max_millisec = None
current_min = None
current_min_timestamp = None
current_min_millisec = None

# List to store filtered results
filtered_results = []

# Iterate through the DataFrame
for i in range(len(filtered_df)):
    row = filtered_df.iloc[i]
    timestamp = row['Timeslot']
    millisec = row['Milisec']
    altitude = row['Altitude']
    
    if current_max is None:
        # Initialize the first values
        current_max = altitude
        current_max_timestamp = timestamp
        current_max_millisec = millisec
        current_min = altitude
        current_min_timestamp = timestamp
        current_min_millisec = millisec
    else:
        if altitude > current_max:
            # If the current altitude is greater, reset current_max and current_min
            current_max = altitude
            current_max_timestamp = timestamp
            current_max_millisec = millisec
            current_min = altitude
            current_min_timestamp = timestamp
            current_min_millisec = millisec
        else:
            # Update current_min if a new minimum is found
            if altitude < current_min:
                current_min = altitude
                current_min_timestamp = timestamp
                current_min_millisec = millisec

            # Check if the altitude difference meets or exceeds the threshold and ensure a decline
            difference = current_max - current_min
            if difference >= threshold_altitude_difference:
                result = {
                    'Start Timeslot': current_max_timestamp,
                    'Start Millisec': current_max_millisec,
                    'Start Altitude': current_max,
                    'End Timeslot': current_min_timestamp,
                    'End Millisec': current_min_millisec,
                    'End Altitude': current_min,
                    'Altitude Difference': difference
                }
                
                filtered_results.append(result)
                
                # Print details of the decline
                print(f"Decline detected:")
                print(f"Start Timeslot: {result['Start Timeslot']}, Start Millisec: {result['Start Millisec']}, Start Altitude: {result['Start Altitude']}")
                print(f"End Timeslot: {result['End Timeslot']}, End Millisec: {result['End Millisec']}, End Altitude: {result['End Altitude']}")
                print(f"Altitude Difference: {result['Altitude Difference']}")
                
                # Check for corresponding error messages
                for index, error_row in error_df.iterrows():
                    error_time = error_row['Timeslot']
                    # Default to zero if 'Milisec' column is missing
                    error_millisec = error_row['Milisec'] if 'Milisec' in error_row else 0
                    if (result['Start Timeslot'] < error_time < result['End Timeslot']) or \
                       (result['Start Timeslot'] == error_time and result['Start Millisec'] <= error_millisec) or \
                       (result['End Timeslot'] == error_time and result['End Millisec'] >= error_millisec):
                        print(f"Error Message: {error_row['Error Message']}")
                
                print()
                
                # Reset current_max and current_min to the latest altitude for next potential decline
                current_max = altitude
                current_max_timestamp = timestamp
                current_max_millisec = millisec
                current_min = altitude
                current_min_timestamp = timestamp
                current_min_millisec = millisec

# Create a DataFrame from the filtered results
filtered_df = pd.DataFrame(filtered_results)

# Sort results by Start Timeslot and Start Millisec
filtered_df = filtered_df.sort_values(by=['Start Timeslot', 'Start Millisec'])

# Merge rows with the same End Millisec as the next row's Start Millisec
merged_results = []
i = 0
while i < len(filtered_df):
    current_row = filtered_df.iloc[i]
    start_timestamp = current_row['Start Timeslot']
    start_millisec = current_row['Start Millisec']
    
    # Initialize the merged result with current row's values
    merged_result = current_row.copy()
    merged_result['End Timeslot'] = current_row['End Timeslot']
    merged_result['End Millisec'] = current_row['End Millisec']
    
    # Accumulate altitude difference
    while i + 1 < len(filtered_df):
        next_row = filtered_df.iloc[i + 1]
        if next_row['Start Timeslot'] == current_row['End Timeslot'] and next_row['Start Millisec'] == current_row['End Millisec']:
            merged_result['End Timeslot'] = next_row['End Timeslot']
            merged_result['End Millisec'] = next_row['End Millisec']
            merged_result['Altitude Difference'] += next_row['Altitude Difference']
            i += 1
        else:
            break
    
    merged_results.append(merged_result)
    i += 1

# Create a DataFrame from the merged results
merged_df = pd.DataFrame(merged_results)

# Write the merged DataFrame to a new CSV file
merged_df.to_csv(output_csv, index=False)

print(f"Filtered and merged data has been written to {output_csv}")
