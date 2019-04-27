## Experimenting with Apache Spark

### Objective
Creating a Spark application that will process a web server’s access log to count the number of times ‘.jpg’,’.gif’ and other resources are requested. Your program will report three numbers:
 - Number of ‘.gif’
 - Number of ‘.jpg’
 - Number of other requests like ‘.php’
 
 
### Instructions:
 You need the following pre-installed in your system to work on this.
 - Java 8 (as the latest versions are not supported by Apache Spark)
 - Python
 - Scala
 - Spark
 
### Installation
There are various ways out there which you can follow to install apache spark on your system. Some examples are:
- [Ubuntu](https://medium.com/@josemarcialportilla/installing-scala-and-spark-on-ubuntu-5665ee4b62b1)
- [Windows](https://medium.com/@dvainrub/how-to-install-apache-spark-2-x-in-your-pc-e2047246ffc3)
- You can install Spark on your machine or use the online platform such as [MatrixDS](https://matrixds.com/) to run your code so that Spark setup is minimal.

### My approach:
I used the regular expressions to find the number of '.jpg's, '.gifs' and the subtracted the count of '.jpg's, '.gifs' from the total number of requests.
 - For finding number of JPGs:
  		Here I considered all the image file requests (jpg, jpeg and png) into JPG requests and counted the number. 
    The regular expression '?i:jpg|jpeg|png' gives the count of total image requests. The 'i' in regular expression is for ignoring the cases i.e jpg and JPG are treated as single extension.here were some _jpg and 7jpg requests as well. This program considers those requests as well.
 - For finding number of GIFs:
    		- The regular expression '?i:gif' gives the count of total GIF requests. (gif and Gif included)
 - For finding the number of other requests I just counted the total number of requests and subtracted the number of JPG requests and GIF requests from total requests.

### Usage:
```
python(3) filetypecount.py <filename>
```
### References:
 - https://medium.com/@GalarnykMichael/install-spark-on-ubuntu-pyspark-231c45677de0
 - https://stackoverflow.com/questions/47554080/regular-expression-to-find-images-in-various-formats-tags
 -	https://github.com/apache/spark/blob/master/examples/src/main/python/wordcount.py
 -	https://nyu-cds.github.io/python-bigdata/02-mapreduce/
 -	https://stackoverflow.com/questions/22350722/what-is-the-difference-between-map-and-flatmap-and-a-good-use-case-for-each
 - 	https://data-flair.training/blogs/apache-spark-map-vs-flatmap/
