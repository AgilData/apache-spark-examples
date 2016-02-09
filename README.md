# apache-spark-examples

Apache Spark Examples

## Overview

These examples were put together for some talks on Apache Spark by AgilData [http://www.agildata.com/]

All required source data is in the demodata.tgz file. This file needs to be extracted into a `testdata` directory.

## US Census 2010

There are various code samples in this repo in both Java and Scala for performing some trivial analytics on US census
data.

To download the full US census data for Colorado:

http://www2.census.gov/census_2010/04-Summary_File_1/Colorado/

usgeo2010.txt contains geographic information in fixed-width format. For the examples in this repo we are only
interested in the following fields:

```
s.substring(18,25),   // Logical Record No
s.substring(226,316), // Name
s.substring(8,11)     // Summary Level (050 is county)
```

For full documentation on the file formats, download http://www.census.gov/prod/cen2010/doc/sf1.pdf

## Word Count Examples

This repo also contains the classic word count examples, in Java and Scala.
