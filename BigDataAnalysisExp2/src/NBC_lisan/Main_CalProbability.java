package NBC_lisan;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class Main_CalProbability {

	public static Map<String,Integer> ReadClassNums(String FilePath) throws IOException {
		Map<String,Integer> map = new HashMap<>();
		
		Configuration conf = new Configuration();
		Path srcPath = new Path(FilePath);	
		FileSystem fs = FileSystem.get(URI.create(FilePath), conf);// 通过URI来获取文件系统
		FSDataInputStream hdfsInStream = fs.open(srcPath);
		BufferedReader br = new BufferedReader(new InputStreamReader(hdfsInStream));
		
		String line = null;
		while(((line = br.readLine()) != null)) {
			String classification = line.split("\t")[0];
			int nums = Integer.parseInt(line.split("\t")[1]); 
			map.put(classification, nums);
		}
		br.close();
		hdfsInStream.close();
		return map;
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Job job = new Job();
		job.setJarByClass(Main_CalProbability.class);
		job.setJobName("CalProbability");
		
		Path inputPath  = new Path("BDAnalysis/exp2/NBC/Output/attriNumsByC/part-r-00000");
		Path outputPath = new Path("BDAnalysis/exp2/NBC/Output/probability");
		Configuration conf = job.getConfiguration();
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(outputPath)) {
			fs.delete(outputPath,true);
		}
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		Map<String,Integer> map = ReadClassNums("hdfs://localhost:9000/user/hadoop/BDAnalysis/exp2/NBC/Output/attriNumsByC/classificationsNum-r-00000");
		conf.setInt("T", map.get("T"));
		conf.setInt("F",map.get("F"));
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		job.setMapperClass(Mapper_calProbability.class);
		job.setReducerClass(Reducer_calProbability.class);

		System.exit(job.waitForCompletion(true)?0:1);
	}

}
