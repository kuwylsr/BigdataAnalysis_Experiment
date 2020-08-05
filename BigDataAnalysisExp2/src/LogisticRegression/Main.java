package LogisticRegression;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Main {

	private static int iterationNum = 200;
	private static double alpha = 0.000001;
	private static double precision = 0.05;
	private static double lambda = Math.exp(-10);
	
	/**
	 * 从HDFS分布式文件系统中读取文件内容
	 * @param filePath HDFS中的目标文件路径
	 * @return 返回文件内容的字符串格式
	 * @throws IOException
	 * @throws URISyntaxException
	 */
	public static String readFileFromHDFS(String FilePath) throws IOException {
		Configuration conf = new Configuration();
		Path srcPath = new Path(FilePath);

		FileSystem fs = FileSystem.get(URI.create(FilePath), conf);// 通过URI来获取文件系统
		FSDataInputStream hdfsInStream = fs.open(srcPath);
		BufferedReader br = new BufferedReader(new InputStreamReader(hdfsInStream));

		StringBuilder sb = new StringBuilder();
		String line = null;
		while (((line = br.readLine()) != null)) {
			sb.append(line);
		}
		br.close();
		hdfsInStream.close();
		return sb.toString();
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.setDouble("alpha", alpha);
		conf.setDouble("lambda", lambda);
		Job job = new Job(conf,"LRInit");
		job.setJarByClass(Main.class);
		job.setJobName("LRInit");
		
		Path inputPath = new Path("BDAnalysis/exp2/NBC/Input/SUSYTrainSmall.txt");
		Path outputPath = new Path("BDAnalysis/exp2/LogisticR/Output/parameters0");
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(outputPath)) {
			fs.delete(outputPath,true);
		}
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		job.setMapperClass(Mapper_LogisticRInit.class);
		job.setReducerClass(Reducer_LogisticRInit.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.waitForCompletion(true);

		int index =-1;
		while(index < iterationNum) {
			index++;
			Configuration conf1 = new Configuration();
			conf1.setDouble("alpha", alpha);
			conf1.setDouble("precision", precision);
			conf1.setDouble("lambda", lambda);
			String parameters = readFileFromHDFS("hdfs://localhost:9000/user/hadoop/BDAnalysis/exp2/LogisticR/Output/parameters"+index+"/"+"part-r-00000");
			conf1.set("parameters", parameters);
			
			Job job1 = new Job(conf1,"LR"+index);
			job1.setJarByClass(Main.class);
			job1.setJobName("LR");
			
			Path inputPath1 = new Path("BDAnalysis/exp2/NBC/Input/SUSYTrainSmall.txt");
			Path outputPath1 = new Path("BDAnalysis/exp2/LogisticR/Output/parameters"+(index+1));
			FileSystem fs1 = FileSystem.get(conf1);
			if(fs1.exists(outputPath1)) {
				fs1.delete(outputPath1,true);
			}
			FileInputFormat.addInputPath(job1, inputPath1);
			FileOutputFormat.setOutputPath(job1, outputPath1);
			job1.setMapperClass(Mapper_LogisticR.class);
			job1.setReducerClass(Reducer_LogisticR.class);
			job1.setMapOutputKeyClass(Text.class);
			job1.setMapOutputValueClass(Text.class);
			job1.setOutputKeyClass(Text.class);
			job1.setOutputValueClass(Text.class);
			job1.waitForCompletion(true);
			
			if(job1.getCounters().findCounter("tag", "tag").getValue() == -1) {
				break;
			}
		}
		
		Configuration conf2 = new Configuration();
		String parameters = readFileFromHDFS("hdfs://localhost:9000/user/hadoop/BDAnalysis/exp2/LogisticR/Output/parameters"+(index+1)+"/"+"part-r-00000");
		conf2.set("parameters", parameters);
		
		Job job2 = new Job(conf2,"Predict");
		job2.setJarByClass(Main.class);
		job2.setJobName("Predict");
		
		Path inputPath2 = new Path("BDAnalysis/exp2/NBC/Input/SUSYTestSmall.txt");
		Path outputPath2 = new Path("BDAnalysis/exp2/LogisticR/Output/result/");
		FileSystem fs2 = FileSystem.get(conf2);
		if(fs2.exists(outputPath2)) {
			fs2.delete(outputPath2,true);
		}
		FileInputFormat.addInputPath(job2, inputPath2);
		FileOutputFormat.setOutputPath(job2, outputPath2);
		job2.setMapperClass(Mapper_Predict.class);
		job2.setReducerClass(Reducer_Predict.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(DoubleWritable.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		
		System.exit(job2.waitForCompletion(true)?0:1);	
	}
}
