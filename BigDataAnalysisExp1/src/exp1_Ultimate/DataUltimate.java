package exp1_Ultimate;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import exp1_DataFilter.Count;


public class DataUltimate {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Job job = new Job();
		job.setJarByClass(DataUltimate.class);
		job.setJobName("DataUltimate");
		
		
//		Path inputPath = new Path("BDAnalysis/exp1/Input/large_data.txt");
//		Path outputPath = new Path("BDAnalysis/exp1/Output/D_Simple");
		
		Path inputPath = new Path("BDAnalysis/exp1/Input/large_data.txt");
		Path outputPath = new Path("BDAnalysis/exp1/OutputUiltTest/D_Ultimate");
		
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job,outputPath);
		
		Configuration conf = job.getConfiguration();
		
		double[] limits = new Count().ReadLimits("hdfs://localhost:9000/user/hadoop/BDAnalysis/exp1/Output/D_Filtered/FindLimit/part-r-00000");
		Map<String,Double> map = new Count().count("hdfs://localhost:9000/user/hadoop/BDAnalysis/exp1/Output/D_Filtered/FinalFiltered/part-r-00000");
		conf.setDouble("leftValue",limits[0]);
		conf.setDouble("rightValue",limits[1]);
		conf.setDouble("max", map.get("max"));
		conf.setDouble("min", map.get("min"));
		
//		double[] limits = new Count().ReadLimits("hdfs://localhost:9000/user/hadoop/BDAnalysis/exp1/OutputTest/D_Filtered/FindLimit/part-r-00000");
//		Map<String,Double> map = new Count().count("hdfs://localhost:9000/user/hadoop/BDAnalysis/exp1/OutputTest/D_Filtered/FinalFiltered/part-r-00000");
//		conf.setDouble("leftValue",limits[0]);
//		conf.setDouble("rightValue",limits[1]);
//		conf.setDouble("max", map.get("max"));
//		conf.setDouble("min", map.get("min"));
//		System.out.println(limits[0]);
//		System.out.println(limits[1]);
		
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(outputPath)) {
			fs.delete(outputPath,true);
		}
		
		job.setMapperClass(DUltimate_Mapper.class);
//		job.setReducerClass(DUltimate_Reducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		System.exit(job.waitForCompletion(true)?0:1);
		

	}

}