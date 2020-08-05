package NBC;

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


public class Main_CalSitaAndMu {

	public static Map<String,Double> ReadClassNums(String FilePath) throws IOException {
		Map<String,Double> map = new HashMap<>();
		
		Configuration conf = new Configuration();
		Path srcPath = new Path(FilePath);	
		FileSystem fs = FileSystem.get(URI.create(FilePath), conf);// 通过URI来获取文件系统
		FSDataInputStream hdfsInStream = fs.open(srcPath);
		BufferedReader br = new BufferedReader(new InputStreamReader(hdfsInStream));
		
		String line = null;
		while(((line = br.readLine()) != null)) {
			String classification = line.split("\t")[0];
			double nums = Double.parseDouble(line.split("\t")[1]); 
			map.put(classification, nums);
		}
		br.close();
		hdfsInStream.close();
		return map;
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Job job = new Job();
		job.setJarByClass(Main_CalSitaAndMu.class);
		job.setJobName("CalProbability");
		
		Path inputPath  = new Path("BDAnalysis/exp2/NBC/Input/SUSYTrain.txt");
		Path outputPath = new Path("BDAnalysis/exp2/NBC/Output/MuAndSita");
		Configuration conf = job.getConfiguration();
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(outputPath)) {
			fs.delete(outputPath,true);
		}
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		Map<String,Double> map = ReadClassNums("hdfs://localhost:9000/user/hadoop/BDAnalysis/exp2/NBC/Output/attriNumsByC/classificationsNum-r-00000");
		conf.setDouble("T", map.get("T"));
		conf.setDouble("F",map.get("F"));
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(Mapper_calSitaAndMu.class);
		job.setReducerClass(Reducer_calSitaAndMu.class);

		System.exit(job.waitForCompletion(true)?0:1);
	}

}
