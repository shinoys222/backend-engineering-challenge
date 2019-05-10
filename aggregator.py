import json
from datetime import datetime
import time
from statistics import mean
import argparse
from pathlib import Path


def epoch_time(timestamp, date_format='%Y-%m-%d %H:%M:%S.%f'):
    """
    Blur the area in the provided bounding box from the image

    Parameters:
        timestamp (string): Timestamp string
        date_format (string): Dateformat to parse time from string

    Returns:
        epoch_time: returns the seconds passed since epoch 
    """
    date = datetime.strptime(timestamp, date_format)
    epoch_time = time.mktime(date.timetuple())
    return epoch_time


def main():

    parser = argparse.ArgumentParser(description='Unbabel Aggregator')

    parser.add_argument('-i', '--input_file',
                        help='Input Json File', required=True)
    parser.add_argument('-w', '--window_size', type=int,
                        help='Window Size',  required=True)
    parser.add_argument('-o', '--output_file',
                        help='Output Json File', required=False, default='output.json')

    args = parser.parse_args()

    input_file = args.input_file
    window_size = args.window_size
    output_file = args.output_file

    input_file_exists = False
    my_file = Path(input_file)
    if my_file.is_file():
        input_file_exists = True

    if not input_file_exists:
        print("{} not found ".format(input_file))
        return

    events = []
    windows = []

    # Reading each json line from file and adding to events list
    with open(input_file) as file:
        for line in file:
            event_dict = json.loads(line)
            events.append(event_dict)

    timestamps = [x['timestamp'] for x in events]

    # Finding the starting and ending timestamps from input data.
    initial_timestr = min(timestamps)
    final_timestr = max(timestamps)

    # Converting to epoch time(in seconds) and Disregarding seconds to take
    # minutes only for initial_timestamp
    initial_timestamp = int(int(epoch_time(initial_timestr)) / 60) * 60
    final_timestamp = epoch_time(final_timestr)

    # Number of windows to select based on initial and final timestamps.
    # An additional factor of 2 is used to consider seconds missed in time
    # difference and boundary conditions
    td_minutes = int(int(final_timestamp - initial_timestamp) / 60) + 2

    # The Window time is considered from 10 minutes before initial time of data.
    # Each iteration creates another sliding window 1 minute after the first
    # window.
    for i in range(0, td_minutes):
        window_end_time = initial_timestamp + i * 60
        window_start_time = window_end_time - window_size * 60

        avg = 0

        # Filtering out events that do not fall under current time window.
        window_event_durations = [x['duration']
                                  for x in events
                                  if window_start_time < epoch_time(x['timestamp']) < window_end_time
                                  ]

        # mean function fails for empty lists
        if(len(window_event_durations) > 0):
            avg = mean(window_event_durations)

        # Converting window_end_time back to normal timestamp string
        window_end_time_str = time.strftime(
            '%Y-%m-%d %H:%M:%S', time.localtime(window_end_time))

        window_dict = {'date': window_end_time_str,
                       'average_delivery_time': avg}
        windows.append(window_dict)

    # Writing json values to output file
    with open(output_file, 'w') as file:
        for window_dict in windows:
            json_str = json.dumps(window_dict)
            file.writelines(json_str + "\n")


if __name__ == "__main__":
    main()
