# run this script form Hadoop bin dir
# remove input file abc.csv from hadoop cluster
./hadoop dfs -rm -r inputtemp/abc.csv
# copy input file abc.csv from local system to hadoop cluster inputtemp folde
./hadoop dfs -copyFromLocal abc.csv inputtemp
# execute the jar
./hadoop jar /home/team15/workspace/HadoopEx/target/EquiJoin.jar inputtemp outtemp
# remove output folder from local system
rm -r outtemp
# copy output files from cluster to local system
./hadoop fs -copyToLocal outtemp/ .
