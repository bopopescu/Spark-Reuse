Reuse intermediate data in Spark SQL based on physical plan match.

Make efforts to reuse data when operator doesn't completely match in three cases.

1. The partition number of exchange is different.

2. Different project operators, but have subsumption relationship. 

3. Same filter predicates but have different expressions, for example, a > 10 && c < b vs.  b < c && a > 10


Change the eager way of caching data to lazy way, that is, from rdd.saveAsFile to OFF_HEAP Global Storage

1. Cache. Implemented a global OFF_HEAP Storage on Spark, which stores rdd as a file on tachyon, and the path of the file is $Gloable_TACHYON_STORAGE/operatorId/operator_operatorId_splitIndex.

2. Read. Implemented a new rdd type called TachyonRDD, which reads data directly from tachyon file.
