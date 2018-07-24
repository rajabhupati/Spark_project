package com.dataflair.spark

import scala.xml.XML
import java.text.SimpleDateFormat
import org.apache.spark.sql.SparkSession




object aadhar_analysis {
  def main(args: Array[String]) = {
			
		System.setProperty("hadoop.home.dir", "C:/Users/rajab/Downloads/spark-2.0.0-bin-hadoop2.6/")
		
		import org.apache.log4j.Logger
    import org.apache.log4j.Level

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

		
		val spark = SparkSession
				.builder
				.appName("Wordcount")
				.config("spark.master", "local")
				.config("spark.sql.warehouse.dir", "C:/Users/rajab/Downloads/spark-2.0.0-bin-hadoop2.6/bin/spark-warehouse") 
				.getOrCreate()
				
		import org.apache.spark.sql.types._
		
 
		
				 import org.apache.spark.sql.types._

val schema = new StructType()
  .add("Date",StringType,true)
  .add("Registrar",StringType,true)
  .add("Private_Agency",StringType,true)
  .add("State",StringType,true)
  .add("District",StringType,true)
  .add("Sub_District",StringType,true)
  .add("Pin_Code",StringType,true)
  .add("Gender",StringType,true)
  .add("Age",StringType,true)
  .add("Aadhar_Generated",StringType,true)
  .add("Rejected",StringType,true)
  .add("Mobile_no",StringType,true)
  .add("Email_id",StringType,true)
				
		 val aadhardata =spark.read.format("csv").
		                 format("com.databricks.spark.csv").          
		                 option("delimiter", ",").
		                 schema(schema).
                     load("C:/Users/rajab/Desktop/aadhaar_dataset.csv")
		 aadhardata.createTempView("aadhar_data_temp_table")
		 
		 //View or result of the top 25 rows from each individual store
		 spark.sql("""select Date,Registrar,Private_agency,State,District,Sub_District,Pin_Code,Gender,Age,Aadhar_Generated,Rejected,Mobile_no,Email_id from (select *,row_number() over(partition by Private_Agency order by Date desc) as rn from aadhar_data_temp_table)T where T.rn<=25""").rdd.foreach(println)
		
		 //1. Find the count and names of registrars in the table.
		 spark.sql("""select count(*),Registrar from aadhar_data_temp_table group by Registrar""").rdd.foreach(println)
    //2. Find the number of states, districts in each state and sub-districts in each district.
		 spark.sql("""select State,District,count(State),count(distinct District),count(distinct Sub_District) from aadhar_data_temp_table group by State,District""").rdd.foreach(println)
     //3. Find the number of males and females in each state from the table.
		 spark.sql("""select State,sum(case when Gender='M' then 1 else 0 end) as males_count,sum(case when Gender='F' then 1 else 0 end) as female_count from aadhar_data_temp_table group by State""").rdd.foreach(println)
     //4. Find out the names of private agencies for each state
		 spark.sql("""select collect_set(Private_agency),state from aadhar_data_temp_table group by state order by state""").rdd.foreach(println)
		 
		 //KPI-31. Find top 3 states generating most number of Aadhaar cards?
		 spark.sql("""select * from (select sum(Aadhar_Generated) sum,state from aadhar_data_temp_table group by state order by sum desc)T limit 3""").rdd.foreach(println)
		 //2. Find top 3 private agencies generating the most number of Aadhar cards?
		 spark.sql("""select * from (select sum(Aadhar_Generated) sum,Private_agency from aadhar_data_temp_table group by Private_agency order by sum desc)M limit 3""").rdd.foreach(println)
		 //Find the number of residents providing email, mobile number? (Hint: consider non-zero values.)
		 spark.sql("""select count(*) as tot_residents,sum(case when Mobile_no!=0 then 1 else 0 end) mobilenumber_provided_count,sum(case when Email_id!=0 then 1 else 0 end) Email_id_provided_count,sum(case when (Mobile_no!=0 and Email_id!=0) then 1 else 0 end) both_provided_count from aadhar_data_temp_table """).rdd.foreach(println)
		 //4. Find top 3 districts where enrolment numbers are maximum?
		 spark.sql("""select * from(select * from (select sum(Aadhar_Generated+Rejected) as sum,state from aadhar_data_temp_table group by state)T order by sum desc)M limit 3""").rdd.foreach(println)
      //5. Find the no. of Aadhaar cards generated in each state?
		 spark.sql("""select sum(Aadhar_Generated),state from aadhar_data_temp_table group by state""").rdd.foreach(println)
		 
		 //Write a command to see the correlation between “age” and “mobile_number”?
		 spark.sql("""select Age,((mobile_residents/count)*100) corr from (select Age,sum(Mobile_no) mobile_residents,count(*) count from aadhar_data_temp_table group by Age)T""").rdd.foreach(println)
		 
		 //Find the number of unique pincodes in the data?
		 spark.sql("""select count(distinct Pin_Code) from aadhar_data_temp_table""").rdd.foreach(println)
     //3. Find the number of Aadhaar registrations rejected in Uttar Pradesh and Maharashtra?
		spark.sql("""select sum(Rejected),state from aadhar_data_temp_table where state in ('Uttar Pradesh','Maharashtra') group by state """).rdd.foreach(println)
		
		
			//KPI-5 1. The top 3 states where the percentage of Aadhaar cards being generated for males is the highest.
		  val KPI5_DF=spark.sql("""select state,sum(Aadhar_Generated) total_generated from aadhar_data_temp_table group by state """)
		  KPI5_DF.createTempView("KPI5_DF")
		  KPI5_DF.rdd.take(2).foreach(println)
		  val KPI5=spark.sql("""select state,sum(case when Gender='M' then Aadhar_Generated else 0 end) male_count,sum(case when Gender='F' then Aadhar_Generated else 0 end) female_count from aadhar_data_temp_table group by state """)
		  KPI5.createTempView("KPI5")
		  KPI5.rdd.take(2).foreach(println)
		  
		   spark.sql("""select state,male_per from (select (male_count/total_generated)*100 as male_per,(female_count/total_generated)*100 as female_per,a.state from KPI5_DF a,KPI5 b where a.state=b.state group by a.state,male_count,female_count,total_generated)T where T.male_per>T.female_per order by male_per desc""").rdd.take(3).foreach(println)
		  
		 
     //2. In each of these 3 states, identify the top 3 districts where the percentage of Aadhaar cards being rejected for females is the highest.
		   val DF=spark.sql("""select District,sum(Rejected) as tot_rejected from aadhar_data_temp_table where state in ('Manipur','Arunachal Pradesh','Nagaland') group by District""")
		   DF.createTempView("DF")
		   val DF5=spark.sql("""select District,sum(case when Gender='M' then Rejected else 0 end) male_count,sum(case when Gender='F' then Rejected else 0 end) female_count from aadhar_data_temp_table where state in ('Manipur','Arunachal Pradesh','Nagaland') group by District """)
		   DF5.createTempView("DF5")
		   
		   spark.sql("""select District,female_per from (select (male_count/tot_rejected)*100 as male_per,(female_count/tot_rejected)*100 as female_per,a.District from DF a,DF5 b where a.District=b.District group by a.District,male_count,female_count,tot_rejected)T where T.male_per<T.female_per order by female_per desc""").rdd.take(3).foreach(println)
		  
      //3. The top 3 states where the percentage of Aadhaar cards being generated for females is the highest.
		   spark.sql("""select state,female_per from (select (male_count/total_generated)*100 as male_per,(female_count/total_generated)*100 as female_per,a.state from KPI5_DF a,KPI5 b where a.state=b.state group by a.state,male_count,female_count,total_generated)T where T.male_per<T.female_per order by female_per desc""").rdd.take(3).foreach(println)
		  
      //4. In each of these 3 states, identify the top 3 districts where the percentage of Aadhaar cards being rejected for males is the highest.
		   val DF1=spark.sql("""select District,sum(Rejected) as tot_rejected from aadhar_data_temp_table where state in ('Andaman and Nicobar Islands','Chhattisgarh','Goa') group by District""")
		   DF1.createTempView("DF1")
		   val DF15=spark.sql("""select District,sum(case when Gender='M' then Rejected else 0 end) male_count,sum(case when Gender='F' then Rejected else 0 end) female_count from aadhar_data_temp_table where state in ('Manipur','Arunachal Pradesh','Nagaland') group by District """)
		   DF15.createTempView("DF15")
		   spark.sql("""select District,male_per from (select (male_count/tot_rejected)*100 as male_per,(female_count/tot_rejected)*100 as female_per,a.District from DF a,DF5 b where a.District=b.District group by a.District,male_count,female_count,tot_rejected)T where T.male_per>T.female_per order by male_per desc""").rdd.take(3).foreach(println)
      //5. The summary of the acceptance percentage of all the Aadhaar cards applications by bucketing the age group into 10 buckets.
		   
		   val acp_sum=spark.sql("""select sum(Aadhar_Generated) as sum from aadhar_data_temp_table""")
		   val sum_aadhar=acp_sum.rdd.first().getDouble(0)
		   val acp_per_age=spark.sql("""select Age,sum(Aadhar_Generated) as per_age_sum from aadhar_data_temp_table group by Age""")
		   acp_per_age.createTempView("acp_per_age")
		   
		   spark.sql(s"select age,(per_age_sum/$sum_aadhar)* 100 acceptance_perentage from acp_per_age  group by age,per_age_sum order by per_age_sum desc").rdd.take(10).foreach(println)
}}