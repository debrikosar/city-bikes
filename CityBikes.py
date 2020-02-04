import argparse
import os

import pandas as pd
from collections import Counter


def first_task(data, filename):
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

    general_stats.to_csv('Data_Output/'+filename+'-general-stats.csv')
    print("Created General Stats\n")


def second_task(data, filename):
    month_list = [x[0] for x in data['stoptime'].str.rsplit('/', 0)]

    usage_stats = pd.DataFrame([Counter(month_list)])

    usage_stats.to_csv('Data_Output/'+filename+'-usage-stats.csv')
    print("Created Usage Stats\n")


def third_task(data, filename):
    bike_stats = pd.DataFrame()

    unique_bikes_id = data['bikeid'].unique()

    for current_id in unique_bikes_id:
        bike_stats = bike_stats.append(pd.DataFrame(
            {'trips': data.bikeid[data.bikeid == current_id].count(),
             'usage time': data.tripduration[data.bikeid == current_id].sum()},
            index=[current_id]))

    bike_stats = bike_stats.sort_values(by='trips', ascending=False)

    bike_stats.to_csv('Data_Output/'+filename+'-bike-stats.csv')
    print("Created Bike Stats\n")


def command_line_input():
    parser = argparse.ArgumentParser(description='Reports management')
    parser.add_argument('data_address', action='store')
    parser.add_argument('--skip-general-stats', action='store_true', dest="skip_general_stats")
    parser.add_argument('--skip-usage-stats', action='store_true', dest="skip_usage_stats")
    parser.add_argument('--skip-bike-stats', action='store_true', dest="skip_bike_stats")
    result = parser.parse_args()

    if not os.path.isfile(result.data_address):
        print("Incorrect Data Adress")
    else:
        data = pd.read_csv(result.data_address)
        filename = os.path.splitext(os.path.basename(result.data_address))[0]
        if not result.skip_general_stats:
            first_task(data, filename)
        if not result.skip_usage_stats:
            second_task(data, filename)
        if not result.skip_bike_stats:
            third_task(data, filename)


if __name__ == "__main__":
    command_line_input()