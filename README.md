# apache-spark-examples

Apache Spark Examples

## Overview

These examples were put together for some talks on Apache Spark by AgilData [http://www.agildata.com/]

[8/5/16] This repo has been updated to use Apache Spark 2.0. The original code for Apache Spark 1.6.x is 
available on this branch: https://github.com/AgilData/apache-spark-examples/tree/spark_1.6.x

Note that some of the Java examples do not currently work. This is to highlight some of the issues when using Java
with the DataFrame API and this is covered in our talk (we will update this README with a link to the slides soon).

## US Census 2010

There are various code samples in this repo in both Java and Scala for performing some trivial analytics on US census
data.

To download the full US census data for Colorado:

http://www2.census.gov/census_2010/04-Summary_File_1/Colorado/

Download the zip file and unzip into a `testdata` directory within this project.

usgeo2010.txt contains geographic information in fixed-width format. For the examples in this repo we are only
interested in the following fields:

```
s.substring(18,25),   // Logical Record No
s.substring(226,316), // Name
s.substring(8,11)     // Summary Level (050 is county)
```

For full documentation on the file formats, download http://www.census.gov/prod/cen2010/doc/sf1.pdf

## Word Count Examples

This repo also contains the classic word count examples, in Java and Scala, with some minor modifications. 

You can use any text file as an input and in our talk we used the complete works of Shakespeare in text format. The 
download is available here:
 
http://ocw.mit.edu/ans7870/6/6.006/s08/lecturenotes/files/t8.shakespeare.txt
 
 
