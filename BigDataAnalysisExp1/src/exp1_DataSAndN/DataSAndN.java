package exp1_DataSAndN;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import exp1_DataFilter.Count;
import exp1_DataFilter.DataFilter;

public class DataSAndN {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Job job = new Job();
		job.setJarByClass(DataFilter.class);
		job.setJobName("DataFilter");
		
//		Path inputPath = new Path("BDAnalysis/exp1/Output/D_Filtered/FinalFiltered/part-r-00000");
//		Path outputPath = new Path("BDAnalysis/exp1/Output/D_Filtered_SAndN");
		Path inputPath = new Path("BDAnalysis/exp1/OutputTest/D_Filtered/FinalFiltered/part-r-00000");
		Path outputPath = new Path("BDAnalysis/exp1/OutputTest/D_Filtered_SAndN");
		
		
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job,outputPath);
		
		//double max = new Count().count("hdfs://localhost:9000/user/hadoop/BDAnalysis/exp1/Output/D_Filtered/FinalFiltered/part-r-00000").get("max");
		//double min = new Count().count("hdfs://localhost:9000/user/hadoop/BDAnalysis/exp1/Output/D_Filtered/FinalFiltered/part-r-00000").get("min");
		double max = new Count().count("hdfs://localhost:9000/user/hadoop/BDAnalysis/exp1/OutputTest/D_Filtered/FinalFiltered/part-r-00000").get("max");
		double min = new Count().count("hdfs://localhost:9000/user/hadoop/BDAnalysis/exp1/OutputTest/D_Filtered/FinalFiltered/part-r-00000").get("min");
		
		Configuration conf = job.getConfiguration();
		conf.setDouble("max", max);
		conf.setDouble("min", min);
		
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(outputPath)) {
			fs.delete(outputPath,true);
		}
		
		job.setMapperClass(DSAndN_Mapper.class);
		//job.setReducerClass(DSAndN_Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		System.exit(job.waitForCompletion(true)?0:1);
		

	}
}
