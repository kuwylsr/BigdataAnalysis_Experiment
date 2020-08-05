package exp1_DataPreprocess;

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
import exp1_DataSAndN.DSAndN_Mapper;

public class DataPreprocess {

	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Job job = new Job();
		job.setJarByClass(DataFilter.class);
		job.setJobName("DataFilter");
		
//		Path inputPath = new Path("BDAnalysis/exp1/OutputTest/D_Filtered_SAndN/part-r-00000");
//		Path outputPath = new Path("BDAnalysis/exp1/OutputTest/D_Preprocessed");
		Path inputPath = new Path("BDAnalysis/exp1/Output/D_Filtered_SAndN/part-r-00000");
		Path outputPath = new Path("BDAnalysis/exp1/Output/D_Preprocessed");
		
		
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job,outputPath);

		Configuration conf = job.getConfiguration();
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(outputPath)) {
			fs.delete(outputPath,true);
		}
		
		job.setMapperClass(DPreprocess_Mapper2.class);
		job.setReducerClass(DPreprocess_Reducer2.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		System.exit(job.waitForCompletion(true)?0:1);

	}

}
