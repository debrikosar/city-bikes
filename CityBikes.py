import pandas as pd
from collections import Counter

data = pd.read_csv('Data_Input/201608-citibike-tripdata.csv')


def first_task():
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

    general_stats.to_csv('Data_Output/general-stats.csv')


def second_task():
    temp = [x[0] for x in data['stoptime'].str.rsplit('/', 0)]

    usage_stats = pd.DataFrame([Counter(temp)])

    usage_stats.to_csv('Data_Output/usage-stats.csv')


def third_task():
    bike_stats = pd.DataFrame()

    unique_bikes_id = data['bikeid'].unique()

    for id in unique_bikes_id:
        bike_stats = bike_stats.append(pd.DataFrame(
            {'trips': data.bikeid[data.bikeid == id].count(), 'usage time': data.tripduration[data.bikeid == id].sum()},
            index=[id]))

    bike_stats = bike_stats.sort_values(by='trips', ascending=False)

    bike_stats.to_csv('Data_Output/bike-stats.csv')


first_task()
second_task()
third_task()
