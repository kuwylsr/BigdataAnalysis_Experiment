package CLARANS;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Main {
	private static int k = 18;
	private static int iterationNum = 30;
	
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
			sb.append("\n");
		}
		br.close();
		hdfsInStream.close();
		return sb.toString();
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		String path1 = "hdfs://localhost:9000/user/hadoop/BDAnalysis/exp2/KMeans/Input/USCpre10000.txt";
		String path2 = "hdfs://localhost:9000/user/hadoop/BDAnalysis/exp2/CLARANS/Output/InitCluster/Cluster";
		RandomCluster c = new RandomCluster(k, path1, path2);
		String clusterList = c.InitCluster();
		
		int index = 0;
		while(index < iterationNum) {
			index++;
			Configuration conf = new Configuration();
			conf.set("clusterList", clusterList);
			conf.setInt("flag", 0);
			
			Path inputFile = new Path(path1);
			Path refreshCluster = new Path("BDAnalysis/exp2/CLARANS/Output/Cluster"+index);
			FileSystem fs = FileSystem.get(conf);
			if(fs.exists(refreshCluster)) {
				fs.delete(refreshCluster,true);
			}
			
			Job job = new Job(conf,"CLARANS"+index);
			job.setJarByClass(Main.class);	
			FileInputFormat.addInputPath(job, inputFile);
			FileOutputFormat.setOutputPath(job,refreshCluster);
			job.setMapperClass(CLARANS_Mapper.class);
			job.setReducerClass(CLARANS_Reducer.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(Text.class);
			job.waitForCompletion(true);
			
			if(job.getCounters().findCounter("tag", "tag").getValue() == -1) {
				break;
			}
			clusterList = readFileFromHDFS("hdfs://localhost:9000/user/hadoop/"+refreshCluster.toString()+"/part-r-00000");
		}
	
		Path inputFile = new Path(path1);
		Path outputFile = new Path("BDAnalysis/exp2/CLARANS/Output/FinalResult");
		Path refreshCluster = new Path("BDAnalysis/exp2/CLARANS/Output/Cluster"+index);
		
		Configuration conf1 = new Configuration();
		clusterList = readFileFromHDFS("hdfs://localhost:9000/user/hadoop/"+refreshCluster.toString()+"/part-r-00000");
		conf1.set("clusterList", clusterList);
		conf1.setInt("flag", 1);
		FileSystem fs = FileSystem.get(conf1);
		if(fs.exists(outputFile)) {
			fs.delete(outputFile,true);
		}
		
		Job job1 = new Job(conf1,"KMeansFinal");
		job1.setJarByClass(Main.class);	
		FileInputFormat.addInputPath(job1, inputFile);
		FileOutputFormat.setOutputPath(job1,outputFile);
		job1.setMapperClass(CLARANS_Mapper.class);
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);
		System.exit(job1.waitForCompletion(true)?0:1);
	}
	
}
