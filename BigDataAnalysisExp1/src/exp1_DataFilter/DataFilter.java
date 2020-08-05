package exp1_DataFilter;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class DataFilter {
	

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {	
		Configuration conf = new Configuration();
		// 该段代码是用来判断输出路径存不存在，若存在就删除。
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(new Path("BDAnalysis/exp1/Output/D_Filtered"))) {
			fs.delete(new Path("BDAnalysis/exp1/Output/D_Filtered"),true);
		}	
		
		Job findLimit = new Job();
		findLimit.setJarByClass(DataFilter.class);
		findLimit.setJobName("FindLimit");
		
		Path inputPath1 = new Path("BDAnalysis/exp1/Output/D_Simple/part-r-00000");
		Path outputPath1 = new Path("BDAnalysis/exp1/Output/D_Filtered/FindLimit");
		
//		Path inputPath1 = new Path("BDAnalysis/exp1/OutputTest/D_Simple/part-r-00000");
//		Path outputPath1 = new Path("BDAnalysis/exp1/OutputTest/D_Filtered/FindLimit");
		
		FileInputFormat.addInputPath(findLimit, inputPath1);
		FileOutputFormat.setOutputPath(findLimit, outputPath1);
		
		Configuration conf1 = findLimit.getConfiguration();
		double lines = new Count().count("hdfs://localhost:9000/user/hadoop/BDAnalysis/exp1/Output/D_Simple/part-r-00000").get("numLine");
		//double lines = new Count().count("hdfs://localhost:9000/user/hadoop/BDAnalysis/exp1/OutputTest/D_Simple/part-r-00000").get("numLine");

		conf1.setInt("FileLines", (int)lines);
		findLimit.setMapperClass(DFindLimit_Mapper.class);
		findLimit.setReducerClass(DFindLimit_Reducer.class);
		findLimit.setMapOutputKeyClass(DoubleWritable.class);
		findLimit.setMapOutputValueClass(Text.class);
		findLimit.setOutputKeyClass(Text.class);
		findLimit.setOutputValueClass(Text.class);		
		findLimit.waitForCompletion(true);
		
		
		Job filter = new Job();
		filter.setJarByClass(DataFilter.class);
		filter.setJobName("DataFilter");
		
		Path inputPath2 = new Path("BDAnalysis/exp1/Input/large_data.txt");
		Path outputPath2 = new Path("BDAnalysis/exp1/Output/D_Filtered/FinalFiltered");
//		Path inputPath2 = new Path("BDAnalysis/exp1/OutputTest/D_Simple/part-r-00000");
//		Path outputPath2 = new Path("BDAnalysis/exp1/OutputTest/D_Filtered/FinalFiltered");
		
		FileInputFormat.addInputPath(filter, inputPath2);
		FileOutputFormat.setOutputPath(filter, outputPath2);
		
		Configuration conf2 = filter.getConfiguration();
		double[] limits = new Count().ReadLimits("hdfs://localhost:9000/user/hadoop/BDAnalysis/exp1/Output/D_Filtered/FindLimit/part-r-00000");
		//double[] limits = new Count().ReadLimits("hdfs://localhost:9000/user/hadoop/BDAnalysis/exp1/OutputTest/D_Filtered/FindLimit/part-r-00000");

		conf2.setDouble("leftValue",limits[0]);
		conf2.setDouble("rightValue",limits[1]);
		
		filter.setMapperClass(DFilter_Mapper.class);
		filter.setMapOutputKeyClass(Text.class);
		filter.setMapOutputValueClass(NullWritable.class);
		
		System.exit(filter.waitForCompletion(true)?0:1);
		

	}

}
