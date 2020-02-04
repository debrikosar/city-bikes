# Description

City bikes project for Big Data courses

# Task definition

FirstTask()
Creating general-stats.csv file based on data from citibike-tripdata.csv which contains: 
1) Number of all trips
2) Longest trip duration
3) Unique bikes number
4) Percent of male and female cyclers
5) Number of NaN values

SecondTask()
Creating usage-stats.csv file which contains number of trips per month

ThirdTask()
Creating bike-stats.csv file which contains list of unique bikes id sorted by
 number of trips in descending order and bike's total trips duration

# Prerequisites

- Python v3.8
- pip 20.0.2
- pandas 1.0.0

# Usage

## Run

Place your data set into Data_Input folder
To execute file, install pandas by running `pip intall pandas`,
then run `CityBikes.py <yourfilename>`
To skip specific report generation, add flags `--skip-general-stats`,
`--skip-usage-stats`, `--skip-bike-stats`
