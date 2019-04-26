# CMPE 256 - Large Scale Analytics Programming assignment-2
# Submitted by Chaitanya Krishna Kasaraneni (ID 013772642)

# Aim : To find the number of jpgs, gifs and total number of requests in a given access_log file using PySpark.

# Instructions:
# 	You need the following pre-installed in your system to work on this.
# - Java 8 (as the latest versions are not supported by Apache Spark)
# - Python
# - Scala
# - Spark

# My approach:
#  I used the regular expressions to find the number of '.jpg's, '.gifs' and the subtracted the count of '.jpg's, '.gifs' from the total number of requests.

# 	- For finding number of JPGs:
# 		Here I considered all the image file requests (jpg, jpeg and png) into JPG requests and counted the number.
# 		The regular expression '?i:jpg|jpeg|png' gives the count of total image requests. The 'i' in regular expression is for ignoring the cases i.e jpg and JPG are treated as single extension.
# 		There were some _jpg and 7jpg requests as well. This program considers those requests as well.

# 	- For finding number of GIFs:
# 		The regular expression '?i:gif' gives the count of total GIF requests. (gif and Gif included)

# 	- For finding the number of other requests I just counted the total number of requests and subtracted the number of JPG requests and GIF requests from total requests.

# Usage:
#	python(3) filetypecount.py <filename>

# References:
# 	https://medium.com/@GalarnykMichael/install-spark-on-ubuntu-pyspark-231c45677de0
# 	https://stackoverflow.com/questions/47554080/regular-expression-to-find-images-in-various-formats-tags
#	https://github.com/apache/spark/blob/master/examples/src/main/python/wordcount.py
#	https://nyu-cds.github.io/python-bigdata/02-mapreduce/
#	https://stackoverflow.com/questions/22350722/what-is-the-difference-between-map-and-flatmap-and-a-good-use-case-for-each
# 	https://data-flair.training/blogs/apache-spark-map-vs-flatmap/


#             SSSSSSSSSSSS          OOOOOOOOOO          UUUU        UUUU        RRRRRRRRRRRRRRRRRR              CCCCCCCCCCCCC       EEEEEEEEEEEEEEE
#             SSSSSSSSSSSS          OOOOOOOOOO          UUUU        UUUU        RRRRRRRRRRRRRRRRRRRR           CCCCCCCCCCCCC        EEEEEEEEEEEEEEE
#           SSS                     OO      OO          UUUU        UUUU        RRRR            RRRRR         CCCC                  EEEE
#           SSS                     OO      OO          UUUU        UUUU        RRRR            RRRRR        CCCC                   EEEE
#              SSSSSSSSSS           OO      OO          UUUU        UUUU        RRRR            RRRRR       CCCC                    EEEEEEEEEEEEEEE
#              SSSSSSSSSS           OO      OO          UUUU        UUUU        RRRRRRRRRRRRRRRRRRRR        CCCC                    EEEEEEEEEEEEEEE
#                       SSS         OO      OO          UUUU        UUUU        RRRRRRRRRRRRRRRRR            CCCC                   EEEE
#                       SSS         OO      OO          UUUU        UUUU        RRRR        RRRRRRR           CCCC                  EEEE
#             SSSSSSSSSSS           OOOOOOOOOO           UUUUUUUUUUUUUU         RRRR          RRRRRRR           CCCCCCCCCCCC        EEEEEEEEEEEEEEE
#             SSSSSSSSSSS           OOOOOOOOOO            UUUUUUUUUUUU          RRRR           RRRRRRR           CCCCCCCCCCCC       EEEEEEEEEEEEEEE

# Import the required libraries
import re
import sys
#import utils
from pyspark.sql import SparkSession
from pyspark.sql import Row
from operator import add
from pyspark.sql.functions import split, regexp_extract
from collections import Counter

# Main Function
if __name__ == "__main__":
    # To check whether access_log file is passed as an argument or not
    if len(sys.argv) < 2:
        print("Usage: filetypecount.py <filename>", filename=sys.stderr)
        exit(-1)

    # Building Spark Session
    spark = SparkSession.builder.master("local[*]").appName("Log Analyzer").getOrCreate()
    # conf = SparkConf()
    # sc = SparkContext(conf=conf)
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    # Loading the data from the file
    lines = sc.textFile(sys.argv[1])
    base_df = spark.read.text(sys.argv[1])
    out = []

    # Finding the number of all image files (including jpg,jpeg and png)
    # RDD for finding images.
    jpg = lines.flatMap(lambda l: re.findall(r'(?i:jpg|jpeg|png)',l))  # the 'i' in regular expression is for ignoring the cases i.e jpg and JPG are treated as one.
    pairs = jpg.map(lambda w: (len(w), 1))  # mapping the RDD of jpg to 1.
    counts = pairs.reduceByKey(
        lambda n1, n2: n1 + n2).sortByKey().values()  # reducing the mapped values and adding the total count.
    jpg_out = counts.collect()  # the output returns a list that contains the count of JPG+PNG at the first index and count of JPEG in the second index
    print("\nNumber of Images (all JPG, JPEG and PNG included): %d" % (jpg_out[0] + jpg_out[1]))  # Output the count of images
    i_str = str(jpg_out[0] + jpg_out[1])
    out.append("Number of Image requests (all JPG, JPEG and PNG included):" + i_str)

    # Finding the number of gif files.
    # RDD for finding gif.
    gif = lines.flatMap(lambda l: re.findall(r'(?i:gif)',l))  # the 'i' in regular expression is for ignoring the cases i.e gif and Gif are treated as one.
    pairs = gif.map(lambda w: (len(w), 1))  # mapping the RDD of jpg to 1.
    counts = pairs.reduceByKey(
        lambda n1, n2: n1 + n2).sortByKey().values()  # reducing the mapped values and adding the total count.
    gif_out = counts.collect()  # the output returns a list that contains the count of gif+Gif at the first index
    i_str = str(gif_out[0])
    print("\nNumber of GIF requests:%d" % (gif_out[0]))  # Output the count of images
    out.append("Number of GIFs:" + i_str)

    # Finding the number of remaining requests
    # RDD for finding thr number of total requests.
    rem_files = base_df.select(regexp_extract('value', r'^.*"\w+\s+([^\s]+)\s+HTTP.*', 1).alias('path'))
    print("\nNumber of other requests:%d" % (rem_files.count() - (jpg_out[0] + jpg_out[1]) - gif_out[0]))  # Subtracting the image requests and gif requests from the total requests
    i_str = str(rem_files.count() - (jpg_out[0] + jpg_out[1]) - gif_out[0])
    out.append("Number of other requests:" + i_str)

    with open("output.txt",'w') as f:
        for i in out:
            f.write("%s\n" %i)
    f.close()
    sc.stop()