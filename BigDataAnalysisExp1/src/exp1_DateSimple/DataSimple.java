package exp1_DateSimple;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import exp1_DataFilter.Count;
import exp1_DataFilter.DataFilter;


public class DataSimple {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Job job = new Job();
		job.setJarByClass(DataSimple.class);
		job.setJobName("DataSimple");
		
		Path inputPath = new Path("BDAnalysis/exp1/Input/large_data.txt");
		Path outputPath = new Path("BDAnalysis/exp1/Output/D_Simple");
		
//		Path inputPath = new Path("BDAnalysis/exp1/Input/large_data.txt");
//		Path outputPath = new Path("BDAnalysis/exp1/OutputTest/D_Simple");
		
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job,outputPath);
		
		Configuration conf = job.getConfiguration();
		double lines = new Count().count("hdfs://localhost:9000/user/hadoop/BDAnalysis/exp1/Output/D_Simple/part-r-00000").get("numLine");
		//double lines = new Count().count("hdfs://localhost:9000/user/hadoop/BDAnalysis/exp1/OutputTest/D_Simple/part-r-00000").get("numLine");

		conf.setInt("FileLines", (int)lines);
		conf.setInt("test", 111);
		
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(outputPath)) {
			fs.delete(outputPath,true);
		}
		
		job.setMapperClass(DSimple_Mapper.class);
		job.setReducerClass(DSimple_Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.waitForCompletion(true);
		//System.out.println(job.getCounters().findCounter("test", "test").getValue());
		System.exit(job.waitForCompletion(true)?0:1);
		

	}

}