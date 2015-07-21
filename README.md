Reuse intermediate data in Spark SQL based on physical plan match.

Make efforts to reuse data when operator doesn't completely match in three cases.

1. The partition number of exchange is different.

2. Different project operators, but have subsumption relationship. 

3. Same filter predicates but have different expressions, for example, a > 10 && c < b vs.  b < c && a > 10
