# from pyspark.sql import SparkSession
# from pyspark.sql.functions import *

# #create a new Spark Session
# spark = SparkSession \
#     .builder \
#     .appName('PySpark Project') \
#     .getOrCreate()

# #load a csv file as a Spark DataFrame
# path = '/home/administrator/Downloads/movielens/'
# movie = (spark.read
#       .format("csv")
#       .option('header', 'true') #means that the first line contains column names
#       .option("delimiter", ",") #set the delimiter to comma
#       .option("inferSchema", "true") #automatically try to infer the column data types
#       .load("/home/administrator/Downloads/movielens/movie.csv") #filename to read from
#      )
# tag = (spark.read
#       .format("csv")
#       .option('header', 'true') #means that the first line contains column names
#       .option("delimiter", ",") #set the delimiter to comma
#       .option("inferSchema", "true") #automatically try to infer the column data types
#       .load("/home/administrator/Downloads/movielens/tag.csv") #filename to read from
#      )
# rating = (spark.read
#       .format("csv")
#       .option('header', 'true') #means that the first line contains column names
#       .option("delimiter", ",") #set the delimiter to comma
#       .option("inferSchema", "true") #automatically try to infer the column data types
#       .load("/home/administrator/Downloads/movielens/rating.csv") #filename to read from
#      )

# # Register DataFrames as an SQL temporary view
# movie.createOrReplaceTempView("movie")
# rating.createOrReplaceTempView("rating")
# tag.createOrReplaceTempView("tag")


# 1st Query

# print(movie.join(rating, movie.movieId == rating.movieId, 'inner').filter(movie.title.contains('Jumanji')).count())

# 2nd Query

# movie.join(tag, tag.movieId == movie.movieId, 'inner').filter(lower(tag.tag).contains('boring')).drop_duplicates(subset=['movieId']).orderBy(movie.title).select(movie.title).show(5, truncate=60)


# 3rd Query

# spark.sql("SELECT DISTINCT rating.userId FROM rating JOIN tag ON tag.userId = rating.userId AND tag.movieId = rating.movieId WHERE rating > 3 AND LOWER(tag) LIKE '%bollywood%' AND LOWER(tag) NOT LIKE '%not bollywood%' ORDER BY rating.userId").show(5)


# 4th Query

# spark.sql("SELECT first(title) AS Title, AVG(rating) AS Average FROM movie INNER JOIN rating ON movie.movieId = rating.movieId WHERE YEAR(timestamp)=1995 GROUP BY rating.movieId ORDER BY Average DESC, Title ASC").show(10, truncate=60)

# 5th Query

# spark.sql("SELECT title, concat_ws(',', collect_list(tag)) Tags FROM tag INNER JOIN movie ON tag.movieId = movie.movieId WHERE YEAR(timestamp) = 2015 GROUP BY tag.movieId, title ORDER BY title").show(5, truncate=60)

# 6th Query

# spark.sql("SELECT first(title) Title, count(rating) Total_Ratings FROM rating JOIN movie ON movie.movieId = rating.movieId GROUP BY rating.movieId ORDER BY Total_Ratings DESC").show(5, truncate=60)

# 7th Query

# spark.sql("SELECT userId, count(*) Total_Ratings FROM rating WHERE YEAR(timestamp) = 1995 GROUP BY userId ORDER BY Total_Ratings DESC, userId ASC").show(10)

# 8th Query

# Find the genres
# genre = movie.select('title', 'movieId', split('genres', '[|]').alias('Genres')).select('title', 'movieId', explode('Genres')).where("col != '(no genres listed)'")

# # Find most popular movies
# popular = spark.sql("SELECT movieId, COUNT(movieId) Popularity FROM rating GROUP BY movieId ORDER BY Popularity DESC")

# popular.createOrReplaceTempView('popular')
# genre.createOrReplaceTempView('genre')

# # Group movies and order them by genre
# genre = spark.sql("SELECT title, movieId, first(col) Genre FROM genre GROUP BY title, movieId ORDER BY Genre")
# genre.createOrReplaceTempView('genre')

# # Join Genres and Popular movies together
# popular = spark.sql("SELECT Genre, title, Popularity FROM popular INNER JOIN genre ON popular.movieId = genre.movieId ORDER BY Popularity DESC")
# popular.createOrReplaceTempView('popular')

# # Group Genres and order by Genre ASC, Popularity DESC
# spark.sql("SELECT Genre, first(title) Title, first(Popularity) Popularity FROM popular GROUP BY Genre ORDER BY Genre, Popularity DESC").show(5)

# 9th Query

# rating.groupBy('movieId', 'timestamp').agg(countDistinct('userId').alias('Total')).filter('Total>1').select(sum('Total')).show()

# 10th Query

# Find the genres
genre = movie.select('title', 'movieId', split('genres', '[|]').alias('Genres')).select('title', 'movieId', explode('Genres')).where("col != '(no genres listed)'")

# Find movies with rating > 3.5 and tagged as funny
funny = spark.sql("SELECT movieId FROM tag WHERE LOWER(tag) LIKE '%funny%' AND LOWER(tag) NOT LIKE '%not funny%' GROUP BY movieId")

rated = spark.sql("SELECT movieId FROM rating WHERE rating > 3.5 GROUP BY movieId")

funny.createOrReplaceTempView('funny')
rated.createOrReplaceTempView('rated')

fr = funny.join(rated, funny.movieId == rated.movieId, 'inner').select(tag.movieId)

fr.join(genre, fr.movieId == genre.movieId, 'inner').groupby('col').count().sort('col').show(5)