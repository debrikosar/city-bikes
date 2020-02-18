import argparse
import os
import pandas as pd
from collections import Counter
import time
import concurrent.futures
import glob
import datetime
from dateutil import parser


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

    general_stats = pd.DataFrame({
        'Trips count': [trips_count],
        'Longest trip': [longest_trip],
        'Unique bikes': [unique_bikes],
        'Male cyclers': [male_cyclers],
        'Female cyclers': [female_cyclers],
        'NaN values': [nan_values]
    })

    print("Created General Stats\n")
    return general_stats


def second_task(data):
    #month_list = [month for month in parser.parse(data['stoptime']).month]
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


def combine_first_task_pd(first_task_raw_results):
    datasets_number = len(first_task_raw_results)
    first_task_raw_results = pd.concat(first_task_raw_results)

    trips_count = first_task_raw_results['Trips count'].sum()
    longest_trip = first_task_raw_results['Longest trip'].max()
    unique_bikes = first_task_raw_results['Unique bikes'].sum()
    male_cyclers = first_task_raw_results['Male cyclers'].sum() / datasets_number
    female_cyclers = first_task_raw_results['Female cyclers'].sum() / datasets_number
    nan_values = first_task_raw_results['NaN values'].sum()

    first_task_results = pd.DataFrame({
        'Trips count': [trips_count],
        'Longest trip': [longest_trip],
        'Unique bikes': [unique_bikes],
        'Male cyclers': [male_cyclers],
        'Female cyclers': [female_cyclers],
        'NaN values': [nan_values]
    })

    return first_task_results


def combine_third_task_pd(third_task_raw_results):
    third_task_results = pd.concat(third_task_raw_results)

    third_task_results.groupby(['bikeid']).agg(Trips=('Trips', 'sum'), Duration=('Duration', 'sum'))
    third_task_results = third_task_results.sort_values(by='Trips', ascending=False)

    return third_task_results


def command_line_input():
    start_time = time.time()

    parser = argparse.ArgumentParser(description='Reports management')
    parser.add_argument('folder_address', action='store')
    result = parser.parse_args()

    files = []
    first_task_raw_results = []
    second_task_results = []
    third_task_raw_results = []

    if not os.path.exists(result.folder_address):
        print("Incorrect Folder Address")
    else:
        for file in glob.glob(result.folder_address + "/*.csv"):
            files.append(file)

        with concurrent.futures.ProcessPoolExecutor(max_workers=os.cpu_count()) as executor:
            for i in executor.map(file_processing, files):
                first_task_raw_results.append(i[0])
                second_task_results.append(i[1])
                third_task_raw_results.append(i[2])

            combine_first_task_pd(first_task_raw_results).to_csv('Data_Output/first.csv')
            pd.concat(second_task_results).to_csv('Data_Output/second.csv')
            combine_third_task_pd(third_task_raw_results).to_csv('Data_Output/third.csv')

    print(time.time()-start_time)


if __name__ == "__main__":
    command_line_input()
