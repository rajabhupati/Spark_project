
package com.dataflair.spark

import scala.xml.XML
import java.text.SimpleDateFormat
import org.apache.spark.sql.SparkSession

object Set_top_box {
	def main(args: Array[String]) = {
			
		System.setProperty("hadoop.home.dir", "C:/Users/rajab/Downloads/spark-2.0.0-bin-hadoop2.6/")
		
		val spark = SparkSession
				.builder
				.appName("Wordcount")
				.config("spark.master", "local")
				.config("spark.sql.warehouse.dir", "C:/Users/rajab/Downloads/spark-2.0.0-bin-hadoop2.6/bin/spark-warehouse") 
				.getOrCreate()

				
				

    val data =spark.read.textFile("C:/Users/rajab/Desktop/Set_Top_Box_Data.txt")

			val format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
      val format2 = new SimpleDateFormat("yyyy-MM");
  
    case class stb (server_unique_id:Int,request_type:Int,event_id:Int,stb_timestamp:String,stb_xml:String,device_id:String,secondary_timestamp: String)  
      
    val results=data.rdd.map(p=>p.split("\\^")).filter(p=>(p(2).toInt==100))
    
 //Filter  all  the  record  with  event_id=100
    
var tup_Map = results.map{ line => stb (line(0).toInt,line(1).toInt,line(2).toInt,line(3),line(4),line(5),line(6))}
    .filter(line => (line.event_id == 100 && line.stb_xml.contains("Duration")));
    
    
//Get  the  top  five  devices  with  maximum  duration			
 var xml_Map = tup_Map.map{line =>
var xmld = XML.loadString(line.stb_xml);
var xmlnv = xmld \\ "nv";
var duration = 0;
for { i <- 0 to xmlnv.length-1 if xmlnv(i).attributes.toString().contains("Duration") } duration = (xmlnv(i) \\ "@v").text.toInt;
var channelNum = 0;
for { i <- 0 to xmlnv.length-1 if xmlnv(i).attributes.toString().contains("ChannelNumber") } channelNum = (xmlnv(i) \\ "@v").text.toInt;
var channelType = "";
for { i <- 0 to xmlnv.length-1 if xmlnv(i).attributes.toString().contains("ChannelType") } channelType = (xmlnv(i) \\ "@v").text;
(  channelNum, duration)
}.sortBy(_._2,false).take(5).foreach { println }			

//Get  the  top  five  Channels  with  maximum  duration
var xml_Map2 = tup_Map.map{line =>
var xmld = XML.loadString(line.stb_xml);
var xmlnv = xmld \\ "nv";
var duration = 0;
for { i <- 0 to xmlnv.length-1 if xmlnv(i).attributes.toString().contains("Duration") } duration = (xmlnv(i) \\ "@v").text.toInt;
var channelNum = 0;
for { i <- 0 to xmlnv.length-1 if xmlnv(i).attributes.toString().contains("ChannelNumber") } channelNum = (xmlnv(i) \\ "@v").text.toInt;
var channelType = "";
for { i <- 0 to xmlnv.length-1 if xmlnv(i).attributes.toString().contains("ChannelType") } channelType = (xmlnv(i) \\ "@v").text;
(  line.device_id, duration)
}.sortBy(_._2,false)
xml_Map2.take(5).foreach { println }			

//Total  number  of  devices  with  ChannelType="LiveTVMediaChannel"
var xml_Map3 = tup_Map.filter(p=>p.stb_xml.contains("LiveTVMediaChannel")).map{line =>
var xmld = XML.loadString(line.stb_xml);
var xmlnv = xmld \\ "nv";
var duration = 0;
for { i <- 0 to xmlnv.length-1 if xmlnv(i).attributes.toString().contains("Duration") } duration = (xmlnv(i) \\ "@v").text.toInt;
var channelNum = 0;
for { i <- 0 to xmlnv.length-1 if xmlnv(i).attributes.toString().contains("ChannelNumber") } channelNum = (xmlnv(i) \\ "@v").text.toInt;
var channelType = "";
for { i <- 0 to xmlnv.length-1 if xmlnv(i).attributes.toString().contains("ChannelType") } channelType = (xmlnv(i) \\ "@v").text;
(  line.device_id, duration)
}.sortBy(_._2,false)
xml_Map3.foreach { println }	

println("total count is " +xml_Map3.count())

//Filter  all  the  record  with  event_id=101 & Get  the  total  number  of  devices  with  PowerState="On/Off"
  
val results_on_off=data.rdd.map(p=>p.split("\\^")).filter(p=>(p(2).toInt==101))    


var tup_Map_on_off = results_on_off.map{ line => stb (line(0).toInt,line(1).toInt,line(2).toInt,line(3),line(4),line(5),line(6))}
    .filter(line => (line.event_id == 101 && line.stb_xml.contains("PowerState")));
    
var xml_Map4 = tup_Map_on_off.map{line =>
var xmld = XML.loadString(line.stb_xml);
var xmlnv = xmld \\ "nv";
var PowerState = "";
for { i <- 0 to xmlnv.length-1 if xmlnv(i).attributes.toString().contains("PowerState") } PowerState = (xmlnv(i) \\ "@v").text;
(  PowerState, 1)
}
xml_Map4.foreach(println)
    
   
println("total count is " +xml_Map4.count())		

//Filter  all  the  record  with  Event  102/113.Get  the  maximum  price  group  by  offer_id

val results_offerid=data.rdd.map(p=>p.split("\\^")).filter(p=>((p(2).toInt==(102)) ||(p(2).toInt==(113))))

var tup_Map_offerid = results_offerid.map{ line => stb (line(0).toInt,line(1).toInt,line(2).toInt,line(3),line(4),line(5),line(6))}
    .filter(line => ((line.event_id == 102 || line.event_id==113) && line.stb_xml.contains("Price") && !line.stb_xml.contains("n=\"Price\" v=\"\"")))
    
var xml_Map5 = tup_Map_offerid.map{line =>
var xmld = XML.loadString(line.stb_xml);
var xmlnv = xmld \\ "nv";
var Price =0.00;
for { i <- 0 to xmlnv.length-1 if xmlnv(i).attributes.toString().contains("Price")  } Price = (xmlnv(i) \\ "@v").text.toDouble;
var OfferId = ""
for { i <- 0 to xmlnv.length-1 if xmlnv(i).attributes.toString().contains("OfferId") } OfferId = (xmlnv(i) \\ "@v").text;
(  OfferId, Price)
}.sortBy(_._2,false)
xml_Map5.take(1).foreach(println)

//Filter  all  the  record  with  event_id=118.Get  the  min  and  maximum  duration

val results_min_max=data.rdd.map(p=>p.split("\\^")).filter(p=>(p(2).toInt==118))

var tup_Map_min_max = results_min_max.map{ line => stb (line(0).toInt,line(1).toInt,line(2).toInt,line(3),line(4),line(5),line(6))}
    .filter(line => (line.event_id == 118 && line.stb_xml.contains("Duration")));
    
    
//Get  the  top  five  devices  with  maximum  duration			
 var xml_Map_min_max = tup_Map_min_max.map{line =>
var xmld = XML.loadString(line.stb_xml);
var xmlnv = xmld \\ "nv";
var duration = 0;
for { i <- 0 to xmlnv.length-1 if xmlnv(i).attributes.toString().contains("Duration") } duration = (xmlnv(i) \\ "@v").text.toInt;
(duration)
}
 
println("maximum duration value is "+xml_Map_min_max.max())
println("minimum duration value is "+xml_Map_min_max.min())

//filter  all  the  record  with  Event  .Calculate  how  many  junk  records  are  thier  having  BadBlocks  in  xml  column

val results_badblocks=data.rdd.map(p=>p.split("\\^")).filter(p=>(p(2).toInt==0))
var tup_Map_badblocks = results_badblocks.map{ line => stb (line(0).toInt,line(1).toInt,line(2).toInt,line(3),line(4),line(5),line(6))}
    .filter(line => (line.event_id == 0 && line.stb_xml.contains("BadBlocks")));
println("badblock count is "+tup_Map_badblocks.count())    

//Filter  all  the  record  with  Event  107.group  all  the  ButtonName  with  thier  device_ids
val results_buttonname=data.rdd.map(p=>p.split("\\^")).filter(p=>(p(2).toInt==107))

var tup_Map_buttonname = results_buttonname.map{ line => stb (line(0).toInt,line(1).toInt,line(2).toInt,line(3),line(4),line(5),line(6))}
    .filter(line => (line.event_id == 107 && line.stb_xml.contains("ButtonName")));
var xml_Map_buttonname = tup_Map_buttonname.map{line =>
var xmld = XML.loadString(line.stb_xml);
var xmlnv = xmld \\ "nv";
var ButtonName = "";
for { i <- 0 to xmlnv.length-1 if xmlnv(i).attributes.toString().contains("ButtonName") } ButtonName = (xmlnv(i) \\ "@v").text;
(  line.device_id, ButtonName)
}

xml_Map_buttonname.groupByKey().mapValues(_.toList).foreach(println)

//.Filter  all  the  record  with  Event  115/118.Get  the    duration  group  by  program_idii.Total  number  of  devices  with  frequency="Once"
val results_programid=data.rdd.map(p=>p.split("\\^")).filter(p=>((p(2).toInt==(115)) ||(p(2).toInt==(118))))

var tup_Map_programid = results_programid.map{ line => stb (line(0).toInt,line(1).toInt,line(2).toInt,line(3),line(4),line(5),line(6))}
    .filter(line => ((line.event_id == 115 || line.event_id==118) && line.stb_xml.contains("Duration")));
var xml_Map_programid = tup_Map_programid.map{line =>
var xmld = XML.loadString(line.stb_xml);
var xmlnv = xmld \\ "nv";
var duration = 0;
for { i <- 0 to xmlnv.length-1 if xmlnv(i).attributes.toString().contains("Duration") } duration = (xmlnv(i) \\ "@v").text.toInt;
var ProgramId="";
for { i <- 0 to xmlnv.length-1 if xmlnv(i).attributes.toString().contains("ProgramId") } ProgramId = (xmlnv(i) \\ "@v").text;
(ProgramId,duration)
}.reduceByKey(_ + _)

xml_Map_programid.foreach(println)

val results_frequency=data.rdd.map(p=>p.split("\\^")).filter(p=>((p(2).toInt==(115)) ||(p(2).toInt==(118))))

var tup_Map_frequency = results_frequency.map{ line => stb (line(0).toInt,line(1).toInt,line(2).toInt,line(3),line(4),line(5),line(6))}
    .filter(line => ((line.event_id == 115 || line.event_id==118) && line.stb_xml.contains("\"Frequency\" v=\"Once\"")));

println("once_freq count is "+tup_Map_frequency.count())
spark.stop
	}
}