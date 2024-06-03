# Databricks notebook source
# MAGIC %md
# MAGIC #Spotify - Gold Layer

# COMMAND ----------

# MAGIC %md
# MAGIC ##Read Data from Silver Layer

# COMMAND ----------

# from delta.tables import DeltaTable
# from pyspark.sql.functions import col
# from datetime import datetime

# spark.conf.set("spark.databricks.delta.formatCheck.enabled", "false")

# # Define the paths
# silver_table_path = "dbfs:/FileStore/The_Data_DJs/Auto_loader/silver_table"
# snapshot_base_path = "dbfs:/FileStore/The_Data_DJs/Auto_loader/silver_table/snapshots"

# # Get the current date and time as a string
# current_datetime_str = datetime.now().strftime("%Y-%m-%d")

# # Create the snapshot path
# snapshot_path = f"{snapshot_base_path}/{current_datetime_str}"

# # Load the Silver table
# silver_table = DeltaTable.forPath(spark, silver_table_path)

# # Create a snapshot
# silver_table.toDF().write.format("delta").mode("overwrite").save(snapshot_path)

# # Load and analyze the snapshot
# df = spark.read.format("delta").load(snapshot_path)

# display(df.selectExpr("count(*) as total_rows"))

# COMMAND ----------

df.display()

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def read_parquet(filepath: str) -> DataFrame:
    df = spark.read.parquet(filepath)
    return df

silver_dir = "dbfs:///FileStore/The_Data_DJs/Spotify/Silver/"    
df = read_parquet(silver_dir)


silver_table = DeltaTable.forPath(spark, silver_table_path)

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Analyze data
# MAGIC 1. Are there regional differences in music preferences?
# MAGIC 2. What factors contribute to a song's popularity?
# MAGIC 3. How has the distribution of songs from different release years in the Top 200 chart changed each year?
# MAGIC 4. Who is the most popular artist for each region and each year?
# MAGIC 5. What impact do specific artists or genres have on overall music trends?
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ###Are there regional differences in music preferences?
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import avg, min, max, round

def normalizeData(data_min: float, data_max: float, data_avg: float):
    return (data_avg - data_min) / (data_max - data_min)

regional_preferences = df.groupBy('region') \
    .agg(
        normalizeData(min('af_danceability'), max('af_danceability'), avg('af_danceability')).alias('danceability'),
        normalizeData(min('af_energy'), max('af_energy'), avg('af_energy')).alias('energy'),
        normalizeData(min('af_loudness'), max('af_loudness'), avg('af_loudness')).alias('loudness'),
        normalizeData(min('af_speechiness'), max('af_speechiness'), avg('af_speechiness')).alias('speechiness'),
        normalizeData(min('af_acousticness'), max('af_acousticness'), avg('af_acousticness')).alias('acousticness'),
        normalizeData(min('af_liveness'), max('af_liveness'), avg('af_liveness')).alias('liveness'),
        normalizeData(min('af_valence'), max('af_valence'), avg('af_valence')).alias('valence'),
        normalizeData(min('af_tempo'), max('af_tempo'), avg('af_tempo')).alias('tempo')
    )

display(regional_preferences)

# COMMAND ----------

# MAGIC %md
# MAGIC ### What factors contribute to a song's popularity?

# COMMAND ----------

# MAGIC %md
# MAGIC #### Different features ?

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Remove the null and zero value records and calculate the correlation

# COMMAND ----------

from pyspark.sql.functions import col

df_popularity = df

features = features = [
    "popularity", "af_danceability", "af_energy", "af_key", "af_loudness", 
    "af_mode", "af_speechiness", "af_acousticness", 
    "af_instrumentalness", "af_liveness", "af_valence", 
    "af_tempo", "af_time_signature"
]

df_popularity_features = df_popularity.select([col(feature) for feature in features]).dropna()

df_popularity_features = df_popularity_features.filter(col("popularity") != 0)

correlation_matrix = df_popularity_features.select(features).toPandas().corr()

correlation_matrix.display()


# COMMAND ----------

# MAGIC %md
# MAGIC 2. Visualize

# COMMAND ----------

import matplotlib.pyplot as plt
import seaborn as sns

plt.figure(figsize=(12, 10))
sns.heatmap(correlation_matrix, annot=True, cmap="coolwarm", vmin=-1, vmax=1)
plt.title("Feature Correlation with Popularity")
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Different artisit?

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Remove the null and zero value records and check the records

# COMMAND ----------

from pyspark.sql.functions import col
df_artist = df

df_artist = df_artist.filter(col("artist").isNotNull()).filter(col("popularity").isNotNull()).filter(col("popularity") != 0)

artist_count_df = df_artist.groupBy("artist").agg(count("id").alias("record_count"))

artist_count_sorted_df = artist_count_df.orderBy(col("record_count").desc())

artist_count_sorted_df.display()


# COMMAND ----------

# MAGIC %md
# MAGIC 2. Only culculate the artist with more than 1000 records

# COMMAND ----------

artist_count_filtered_df = artist_count_df.filter(col("record_count") > 1000)

artists_filtered = artist_count_filtered_df.select("artist").rdd.flatMap(lambda x: x).collect()

df_artist = df_artist.filter(col("artist").isin(artists_filtered))


# COMMAND ----------

# MAGIC %md
# MAGIC 3. Calculate the average popularity

# COMMAND ----------

from pyspark.sql.functions import avg

artist_popularity = df_artist.groupBy("artist").agg(avg("popularity").alias("average_popularity"))

artist_popularity_sorted = artist_popularity.orderBy(col("average_popularity").desc())

# COMMAND ----------

# MAGIC %md
# MAGIC 4. Visualize

# COMMAND ----------

import matplotlib.pyplot as plt

artist_popularity_pd = artist_popularity_sorted.toPandas()

top_n = 20
artist_popularity_top_n = artist_popularity_pd.head(top_n)

plt.figure(figsize=(14, 7))
plt.barh(artist_popularity_top_n['artist'], artist_popularity_top_n['average_popularity'], color='skyblue')
plt.xlabel('Average Popularity')
plt.ylabel('Artist')
plt.title('Top 20 Artists by Average Popularity (More than 1000 Records)')
plt.gca().invert_yaxis()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### How has the distribution of songs from different release years in the Top 200 chart changed each year?

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Select all the top200 records and remove the records with null release date

# COMMAND ----------

from pyspark.sql.functions import col, year, count, sum
import matplotlib.pyplot as plt


top_200_df = df.filter(col("chart") == "top200")

release_date_null_count = top_200_df.filter(col("release_date").isNull()).count()
release_date_not_null_count = top_200_df.filter(col("release_date").isNotNull()).count()

labels = ['Release Date Null', 'Release Date Not Null']
sizes = [release_date_null_count, release_date_not_null_count]
colors = ['#ff9999','#66b3ff']
explode = (0.1, 0)

plt.figure(figsize=(8, 6))
plt.pie(sizes, explode=explode, labels=labels, colors=colors, autopct='%1.1f%%',
        shadow=True, startangle=140)
plt.axis('equal') 
plt.title('Proportion of Null and Not Null Release Dates in Top 200 Chart')
plt.show()

top_200_df_filtered = top_200_df.filter(col("release_date").isNotNull())


# COMMAND ----------

# MAGIC %md
# MAGIC 2. Calculate the percentage

# COMMAND ----------

from pyspark.sql.functions import col, year, count, sum

top_200_df_filtered = top_200_df_filtered.withColumn("date_year", year(col("date")))
top_200_df_filtered = top_200_df_filtered.withColumn("release_year", year(col("release_date")))

songs_per_year = top_200_df_filtered.groupBy("date_year", "release_year").agg(count("id").alias("song_count"))

total_songs_per_date_year = songs_per_year.groupBy("date_year").agg(sum("song_count").alias("total_songs"))

songs_per_year = songs_per_year.join(total_songs_per_date_year, on="date_year")

songs_per_year = songs_per_year.withColumn("percentage", (col("song_count") / col("total_songs")) * 100)

songs_per_year.display()


# COMMAND ----------

# MAGIC %md
# MAGIC 3. Visualize

# COMMAND ----------

from matplotlib import colors as mcolors

songs_per_year_pd = songs_per_year.toPandas()

date_years = sorted(songs_per_year_pd['date_year'].unique())

color_palette = list(mcolors.TABLEAU_COLORS.values())

for date_year in date_years:
    data = songs_per_year_pd[songs_per_year_pd['date_year'] == date_year]
    
    data = data.sort_values(by='percentage', ascending=False)
    top_3 = data.head(3)
    others = data[3:]
    
    others_percentage = others['percentage'].sum()
    
    top_3 = top_3.append({'release_year': 'Others', 'percentage': others_percentage}, ignore_index=True)
    
    plt.figure(figsize=(8, 6))
    plt.pie(top_3['percentage'], labels=top_3['release_year'], autopct='%1.1f%%', startangle=140, colors=color_palette[:len(top_3)])
    plt.title(f'Proportion of Songs Released in Different Years for {date_year} in Top 200')
    plt.axis('equal')
    plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Who is the most popular artist for each region and each year?

# COMMAND ----------

# MAGIC %md 1. Group by region and year and find the most popular artists for each region each year

# COMMAND ----------

from pyspark.sql.functions import max, struct

df_with_year = df.withColumn('year', year(df['date']))
most_popular_artist_per_region_year = df_with_year.groupBy('region', 'year') \
    .agg(max(struct('popularity', 'artist')).alias('popularity_info')) \
    .select('region', 'year', 'popularity_info.artist', 'popularity_info.popularity')

display(most_popular_artist_per_region_year)

# COMMAND ----------

# MAGIC %md 2. data visualization
# MAGIC

# COMMAND ----------

import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd


most_popular_artist_pd = most_popular_artist_per_region_year.toPandas()

def preprocess_data(df):

    split_artists = df['artist'].str.split(', ', expand=True).stack().reset_index(level=1, drop=True)
    df = df.drop('artist', axis=1).join(split_artists.rename('artist'))
    df = df.groupby(['region', 'year', 'artist']).agg({'popularity': 'mean'}).reset_index()
    return df

processed_data = preprocess_data(most_popular_artist_pd)


regions = processed_data['region'].unique()
num_regions = len(regions)


palette = sns.color_palette("husl", n_colors=processed_data['artist'].nunique())
fig, axes = plt.subplots(nrows=(num_regions + 2) // 3, ncols=3, figsize=(18, 4 * ((num_regions + 2) // 3)))
axes = axes.flatten()


for ax, region in zip(axes, regions):
    region_data = processed_data[processed_data['region'] == region]
    sns.barplot(data=region_data, x='year', y='popularity', hue='artist', dodge=True, ax=ax, palette=palette)
    ax.set_title(f'{region} Region')
    ax.set_xlabel('Year')
    ax.set_ylabel('Popularity')
    ax.legend(title='Artist', bbox_to_anchor=(1.05, 1), loc='upper left')
    ax.grid(True)


for i in range(len(regions), len(axes)):
    fig.delaxes(axes[i])

plt.tight_layout()
plt.show()


# COMMAND ----------

# MAGIC %md The charts displays a series of bar charts, each representing the popularity of different artists across several regions from 2017 to 2021. The regions are Indonesia, Malaysia, Singapore, Thailand, and Vietnam. Each chart shows the popularity of specific artists over the years within that region. 

# COMMAND ----------

# MAGIC %md
# MAGIC ### What impact do specific artists or genres have on overall music trends?

# COMMAND ----------

# MAGIC %md 1. Group by year and calculate the mean of artists popularity

# COMMAND ----------

import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

most_popular_artist_pd = most_popular_artist_per_region_year.toPandas()


print(most_popular_artist_pd.head())

def preprocess_data(df):
    split_artists = df['artist'].str.split(', ', expand=True).stack().reset_index(level=1, drop=True)
    df = df.drop('artist', axis=1).join(split_artists.rename('artist'))

    df = df.groupby(['year', 'artist']).agg({'popularity': 'mean'}).reset_index()
    return df

processed_data = preprocess_data(most_popular_artist_pd)


overall_trends = processed_data.groupby('year').agg({'popularity': 'mean'}).reset_index()
overall_trends = overall_trends.rename(columns={'popularity': 'overall_popularity'})


# COMMAND ----------

# MAGIC %md 2.data visualization
# MAGIC

# COMMAND ----------

from matplotlib.ticker import FixedLocator
palette = sns.color_palette("husl", n_colors=processed_data['artist'].nunique())

plt.figure(figsize=(12, 8))
sns.lineplot(data=processed_data, x='year', y='popularity', hue='artist', palette=palette, marker='o')
plt.title('Popularity of Specific Artists Over Years')
plt.xlabel('Year')
plt.ylabel('Popularity')
plt.legend(title='Artist', bbox_to_anchor=(1.05, 1), loc='upper left')
plt.grid(True)
plt.gca().xaxis.set_major_locator(FixedLocator(combined_data['year'].unique()))
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC The line chart is "Popularity of Specific Artists Over Years." It visualizes the popularity of different artists from 2017 to 2021. It illustrates how the popularity of these artists has changed over the years, providing insights into trends.
