hdfs dfs -rm /user/ubuntu/RegressionOutput/*/*
hdfs dfs -rmdir /user/ubuntu/RegressionOutput/*
hadoop com.sun.tools.javac.Main programs/RegressionalComputation.java
cd programs/
jar cf RegressionalComputation.jar RegressionalComputation*.class
cd ..
hadoop jar programs/RegressionalComputation.jar RegressionalComputation RegressionInput RegressionOutput
sudo rm -R RegressionOutput/*
hdfs dfs -get /user/ubuntu/RegressionOutput/* RegressionOutput/
