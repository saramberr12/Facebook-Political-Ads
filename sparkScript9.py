#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Mon Jul 29 17:57:54 2019

@author: sarah
"""

# Create findspark to get spark working
# import findspark
# findspark.init('/home/sarah/spark-2.4.3-bin-hadoop2.7')

# create entry points to spark
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

try:
    sc.stop()
except:
    pass
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
conf = SparkConf().setAppName("lecture15").setMaster("local[*]")
sc=SparkContext(conf = conf)
spark = SparkSession(sparkContext=sc)

# Create dataframe to read from csv file of all data collected from political campaigns
df = spark.read.csv('gs://big_data_political/fbpac-ads-en-US.csv', inferSchema=True, header=True)

# count the data rows, print the schema
df.count()
df.printSchema()


# Select Political and Non-Political ads from after November 2016
df.select(df['political'], df['title'], df['created_at'] <= '2016-11-08').show()
df.select(df['not_political'], df['title'], df['created_at'] <= '2016-11-08').show()

# Select the target data from dataframe
df.select(df['targetedness']).show(truncate = False)

# group by political ad names
df.groupBy(df['political'], df['title']).max().show(truncate = False)

# determine political probability 
probability = df.groupBy(df['political_probability'], df['title']).max()
probability = probability.show(400, truncate = False)

# select the entities 
df.select(df['entities'])

# all those who submitted their vote registration as Conservative
conservatives = df.filter("targetedness like '%""Conservative""%'").distinct()
print("Conservatives: ")
conservatives.count()

# All oters who submitted registration as Republican
republicans = df.filter("targetedness like '%""Republican Party""%'").distinct()
print("Republicans: ")
republicans.count()

# All voters who submitted their registration as Liberal
libs = df.filter("targetedness like '%""Liberal""%'").distinct()
print("Liberals: ")
libs.count()

# All voters who submitted their registration as Democrat
dems = df.filter("targetedness like '%""Democratic Party""%'").distinct()
print("Democrats: ")
dems.count()

# All voters who submitted their registration as Independent
ind = df.filter("targetedness like '%""Independent""%'").distinct()
print("Independent: ")
ind.count()

# Plot all registration status data in pie chart grid: 
# removed for munging selections to run on job

# Show the percentage of democrats targetedby ads
dems = df.filter("targetedness like '%""Democratic Party""%'").distinct()
df.select('advertiser').show(500, truncate = False)
df.select('targetedness').show(900, truncate = False)

### For the next 50 queries, each query is for each state
## of the United States.  Next comment is in 50 query blocks
### from now, please scroll down...

Alabama = df.filter("targetedness like '%""Alabama""%'")
print("Alabama: ")
Alabama.count()

Alaska = df.filter("targetedness like '%""Alaska""%'")
print("Alaska: ")
Alaska.count()

Arizona = df.filter("targetedness like '%""Arizona""%'")
print("Arizona: ")
Arizona.count()

Arkansas = df.filter("targetedness like '%""Arkansas""%'")
print("Arkansas: ")
Arkansas.count()

California = df.filter("targetedness like '%""California""%'")
print("California: ")
California.count()

Colorado = df.filter("targetedness like '%""Colorado""%'")
print("Colorado: ")
Colorado.count()

Connecticut = df.filter("targetedness like '%""Connecticut""%'")
print("Connecticut: ")
Connecticut.count()

Delaware = df.filter("targetedness like '%""Delaware""%'")
print("Delaware: ")
Delaware.count()

Florida = df.filter("targetedness like '%""Florida""%'")
print("Florida: ")
Florida.count()

Georgia = df.filter("targetedness like '%""Georgia""%'")
print("Georgia: ")
Georgia.count()

Hawaii = df.filter("targetedness like '%""Hawaii""%'")
print("Hawaii: ")
Hawaii.count()

Idaho = df.filter("targetedness like '%""CIdaho""%'")
print("Idaho: ")
Idaho.count()

Illinois = df.filter("targetedness like '%""Illinois""%'")
print("Illinois: ")
Illinois.count()

Indiana = df.filter("targetedness like '%""Indiana""%'")
print("Indiana: ")
Indiana.count()

Iowa = df.filter("targetedness like '%""Iowa""%'")
Iowa.count()

Kansas = df.filter("targetedness like '%""Kansas""%'")
print("Kansas: ")
Kansas.count()

Kentucky = df.filter("targetedness like '%""Kentucky""%'")
print("Kentucky: ")
Kentucky.count()

Louisiana = df.filter("targetedness like '%""Louisiana""%'")
print("Louisiana: ")
Louisiana.count()

Maine = df.filter("targetedness like '%""Maine""%'")
print("Maine: ")
Maine.count()

Maryland = df.filter("targetedness like '%""Maryland""%'")
print("Maryland: ")
Maryland.count()

Massachusetts = df.filter("targetedness like '%""Massachusetts""%'")
print("Massachusetts: ")
Massachusetts.count()

Michigan = df.filter("targetedness like '%""Michigan""%'")
print("Michigan: ")
Michigan.count()


Minnesota = df.filter("targetedness like '%""Minnesota""%'")
print("Minnesota: ")
Minnesota.count()

Mississippi = df.filter("targetedness like '%""Mississippi""%'")
print("Mississippi: ")
Mississippi.count()

Missouri = df.filter("targetedness like '%""Missouri""%'")
print("Missouri: ")
Missouri.count()

Montana = df.filter("targetedness like '%""Montana""%'")
print("Montana: ")
Montana.count()

Nebraska = df.filter("targetedness like '%""Nebraska""%'")
print("Nebraska: ")
Nebraska.count()

Nevada = df.filter("targetedness like '%""Nevada""%'")
print("Nevada: ")
Nevada.count()

NewHampshire = df.filter("targetedness like '%""New Hampshire""%'")
print("New Hampshire: ")
NewHampshire.count()

NewJersey = df.filter("targetedness like '%""New Jersey""%'")
print("New Jersey: ")
NewJersey.count()


NewMexico = df.filter("targetedness like '%""New Mexico""%'")
print("New Mexico: ")
NewMexico.count()

NewYork = df.filter("targetedness like '%""New York""%'")
print("New York: ")
NewYork.count()


NorthCarolina = df.filter("targetedness like '%""North Carolina""%'")
print("North Carolina: ")
NorthCarolina.count()

NorthDakota = df.filter("targetedness like '%""North Dakota""%'")
print("North Dakota: ")
NorthDakota.count()

Ohio = df.filter("targetedness like '%""Ohio""%'")
print("Ohio: ")
Ohio.count()

Oklahoma = df.filter("targetedness like '%""Oklahoma""%'")
print("Oklahoma: ")
Oklahoma.count()

Oregon = df.filter("targetedness like '%""Oregon""%'")
print("Oregon: ")
Oregon.count()

Pennsylvania = df.filter("targetedness like '%""Pennsylvania""%'")
print("Pennsylvania: ")
Pennsylvania.count()

RhodeIsland = df.filter("targetedness like '%""Rhode Island""%'")
print("Rhode Island: ")
RhodeIsland.count()

SouthCarolina = df.filter("targetedness like '%""South Carolina""%'")
print("South Carolina: ")
SouthCarolina.count()

SouthDakota = df.filter("targetedness like '%""South Dakota""%'")
print("South Dakota: ")
SouthDakota.count()

Tennessee = df.filter("targetedness like '%""Tennessee""%'")
print("Tennessee: ")
Tennessee.count()

Texas = df.filter("targetedness like '%""Texas""%'")
print("Texas: ")
Texas.count()

Utah = df.filter("targetedness like '%""Utah""%'")
print("Utah: ")
Utah.count()

Vermont = df.filter("targetedness like '%""Vermont""%'")
print("Vermont: ")
Vermont.count()

Virginia = df.filter("targetedness like '%""Virginia""%'")
print("Virginia: ")
Virginia.count()


Washington = df.filter("targetedness like '%""Washington""%'")
print("Washington: ")
Washington.count()

WestVirginia = df.filter("targetedness like '%""West Virginia""%'")
print("West Virginia: ")
WestVirginia.count()

Wisconsin = df.filter("targetedness like '%""Wisconsin""%'")
print("Wisconsin: ")
Wisconsin.count()

Wyoming = df.filter("targetedness like '%""Wyoming""%'")
print("Wyoming: ")
Wyoming.count()

### Gather all state totals, create dictionary of the data to be plotted
### removed for job to run with munging only

# show the number of not political ads
notPolitical = df.select(df['not_political']).distinct()
notPolitical.count()

#show the number of political ads 
political = df.select(df['political']).distinct()
political.count()

# create dictionary of data results
# removed for job to run with munging only 


# show the distinct messages from each ad
df.select(df['message']).distinct().count()

# Show the messages themselves
# messages = df.select(df['message']).distinct().show(500, truncate = False)
# messages.filter()




