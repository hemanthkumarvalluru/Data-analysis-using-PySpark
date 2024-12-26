# -*- coding: utf-8 -*-
"""The_PySpark_Data Analysis_cloudera.ipynb

# **Welcome to the Notebook**

### Let's mount the google drive
"""

from google.colab import drive
drive.mount('/content/drive')

"""# Task 1 :
Installing pyspark module
"""

!pip install pyspark

"""Importing the modules"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import count, desc , col, max, struct
import matplotlib.pyplot as plts

"""creating spark session"""

spark = SparkSession.builder.appName("SparkApp").getOrCreate()

"""# Task 2 :
importing the *Listenings.csv* file:
"""

listening_path = '/content/drive/MyDrive/Copy of dataset/listenings.csv'
listening_df = spark.read.format('csv').option('inferSchema', True).option('header', True).load(listening_path)

"""let's check the data:"""

listening_df.show()

"""let's delete useless columns:"""

listening_df = listening_df.drop('date')

"""drop the null rows:"""

listening_df = listening_df.na.drop()

"""let's check the dataset again:"""

listening_df.show()

"""let's see the schema:"""

listening_df.printSchema()

"""let's see the shape of our dataframe:"""

shape = (listening_df.count(), len(listening_df.columns))
print(shape)

"""# Task 3:

**Query #0:**
select two columns: track and artist
"""

q0 = listening_df.select('track', 'artist')
q0.show()

"""**Query #1**:

Let's find all of the records of those users who have listened to ***Rihanna***
"""

q1 = listening_df.select('*').filter(listening_df.artist == 'Rihanna')
q1.show()

"""**Query #2:**

Let's find top 10 users who are fan of ***Rihanna***
"""

q2 = listening_df.select('user_id').filter(listening_df.artist == 'Rihanna').groupby('user_id').agg(count('user_id').alias('count')).orderBy(desc('count'))
q2.show()

"""**Query #3:**

find top 10 famous tracks
"""

q3 = listening_df.select('track', 'artist').groupby('track', 'artist').agg(count('*').alias('count')).orderBy(desc('count'))
q3.show()

"""**Query #4:**

find top 10 famous tracks of ***Rihanna***
"""

q4 = listening_df.select('track', 'artist').filter(listening_df.artist == 'Rihanna').groupby('artist', 'track').agg(count('*').alias('count')).orderBy(desc('count')).limit(10)
q4.show()

"""**Query #5:**

find top 10 famous albums
"""

q5 = listening_df.select('artist', 'album').groupby('artist', 'album').agg(count('*').alias('count')).orderBy(desc('count')).limit(10)
q5.show()

"""# Task 4 :
importing the ***genre.csv*** file:
"""

genre_path = '/content/drive/MyDrive/Copy of dataset/genre.csv'
genre_df = spark.read.format('csv').option('inferSchema', True).option('header', True).load(genre_path)
genre_df.show()

"""let's check the data"""

data = listening_df.join(genre_df, how='inner', on=['artist'])
data.show()

"""Let's inner join these two data frames

**Query #6**

find top 10 users who are fan of ***pop*** music
"""

q6 = data.select('user_id').filter(data.genre == 'pop').groupby('user_id').agg(count('*').alias('count')).orderBy(desc('count')).limit(10)
q6.show()

"""**Query #7**

find top 10 famous genres
"""

q7 = data.select('genre').groupby('genre').agg(count('*').alias('count')).orderBy(desc('count')).limit(10)
q7.show()

"""# Task 5:
**Query #8**

find out each user favourite genre
"""

q8 = data.select('user_id', 'genre').groupby('user_id', 'genre').agg(count('*').alias('count')).orderBy('user_id')
q8.show()

q8_2 = q8.groupby('user_id').agg(max(struct(col('count'), col('genre'))).alias('max')).select(col('user_id'), col('max.genre'))
q8_2.show()

"""**Query #9**

find out how many pop,rock,metal and hip hop singers we have

and then visulize it using bar chart
"""

q9 = genre_df.select('genre').filter((col('genre') == 'pop') | (col('genre') == 'rock') | (col('genre') == 'metal') | (col('genre') == 'hip hop')).groupby('genre').agg(count('genre').alias('count'))
q9.show()

"""Now, let's visualize the results using ***matplotlib***"""

q9_list = q9.collect()

labels = [row['genre'] for row in q9_list]
counts = [row['count'] for row in q9_list]

print(labels)
print(counts)

"""now lets visualize these two lists using a bar chart"""

plts.bar(labels, counts)