i) Set the spark home based on installation on computer where the code is run :

System.setProperty("hadoop.home.dir", "C:/Users/rajab/Downloads/spark-2.0.0-bin-hadoop2.6/")

ii) spark.sql.warehouse.dir has to be modified based on installation on computer where the code is run : C:/Users/rajab/Downloads/spark-2.0.0-bin-hadoop2.6/bin/spark-warehouse

iii) import Project 

iv) Right click com.dataflair.spark->run configuration->select Project as DataflairProject

a)for Aadhar analysis, Specify Main class as 

com.dataflair.spark.aadhar_analysis

b) For Settopbx analsis
com.dataflair.spark.Set_top_box.scala