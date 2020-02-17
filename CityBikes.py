import argparse
import os
import pandas as pd
from collections import Counter
import time
import concurrent.futures
import glob


def first_task(data):
    trips_count = data.shape[0]

    longest_trip = data['tripduration'].max()

    unique_bikes = data['bikeid'].nunique()

    unknown_count = data.gender[data.gender == 0].count()
    male_count = data.gender[data.gender == 1].count()
    female_count = data.gender[data.gender == 2].count()

    all_genders_count = unknown_count + male_count + female_count

    male_cyclers = (male_count / all_genders_count) * 100
    female_cyclers = (female_count / all_genders_count) * 100

    nan_values = data.isna().sum().sum()

    general_stats = pd.DataFrame([
        'Trips count: ' + str(trips_count),
        'Longest trip: ' + str(longest_trip),
        'Unique bikes: ' + str(unique_bikes),
        'Male cyclers: ' + str(male_cyclers),
        'Female cyclers: ' + str(female_cyclers),
        'NaN values: ' + str(nan_values)
    ])

    print("Created General Stats\n")
    return general_stats


def second_task(data):
    month_list = [x[0] for x in data['stoptime'].str.rsplit('/', 0)]

    usage_stats = pd.DataFrame([Counter(month_list)])

    print("Created Usage Stats\n")
    return usage_stats


def third_task(data):
    bike_stats = data.groupby(['bikeid'])\
        .agg(Trips=('tripduration', 'count'), Duration=('tripduration', 'sum'))
    bike_stats = bike_stats.sort_values(by='Trips', ascending=False)

    print("Created Bike Stats\n")
    return bike_stats


def file_processing(filename):
    data = pd.read_csv(filename)
    results = [first_task(data), second_task(data), third_task(data)]
    return results


def command_line_input():
    start_time = time.time()

    parser = argparse.ArgumentParser(description='Reports management')
    parser.add_argument('folder_address', action='store')
    parser.add_argument('--skip-general-stats', action='store_true', dest="skip_general_stats")
    parser.add_argument('--skip-usage-stats', action='store_true', dest="skip_usage_stats")
    parser.add_argument('--skip-bike-stats', action='store_true', dest="skip_bike_stats")
    result = parser.parse_args()

    files = []
    first_task_results = []
    second_task_results = []
    third_task_results = []

    if not os.path.exists(result.folder_address):
        print("Incorrect Folder Address")
    else:
        for file in glob.glob(result.folder_address + "/*.csv"):
            files.append(file)
            print(file)

        with concurrent.futures.ProcessPoolExecutor(max_workers=os.cpu_count()) as executor:
            for i in executor.map(file_processing, files):
                first_task_results.append(i[0])
                second_task_results.append(i[1])
                third_task_results.append(i[2])

            pd.concat(first_task_results).to_csv('Data_Output/first.csv')
            pd.concat(second_task_results).to_csv('Data_Output/second.csv')
            pd.concat(third_task_results).to_csv('Data_Output/third.csv')

    print(time.time()-start_time)


if __name__ == "__main__":
    command_line_input()
