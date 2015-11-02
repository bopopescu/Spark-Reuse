cache=y
#query=$1
iter=1
for((i=$1;i<=$1;i++))
do
query=$i
bin/spark-submit --master local --class Main ~/spkSQL-benckmark-tpch/target/scala-2.10/spksql-benckmark-tpch_2.10-1.0.jar -type m $query $iter  -autocache $cache  -show y
#./compare.sh $i
done
#rm -rf AutoCache/*
