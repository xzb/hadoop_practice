# hadoop_practice
hadoop assignment of Big Data Management course of UTDallas.

Upload your .jar to a cluster of machines that support hadoop. (scp)

hadoop jar \<jar file> \<class name> \<input folder> \<output folder> \<parameter> <br>
hadoop jar hadoop.jar RecommendFriend /netid/input/hw1-Adj /netid/output 924,8941 <br>
hadoop jar hadoop.jar MutualFriend /netid/input/hw1-Adj /netid/output 29,30 <br>
hadoop jar hadoop.jar MutualFriendZip /netid/input/hw1-Adj /netid/input/hw1-userdata /netid/output 29,30 <br>
hadoop jar hadoop.jar TopAverageAge /netid/input/hw1-Adj /netid/input/hw1-userdata /netid/output <br>
