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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper_bayesClassify extends Mapper<LongWritable, Text, Text, DoubleWritable> {

	private Map<String, String> map = new HashMap<>();

	@Override
	protected void setup(Mapper<LongWritable, Text, Text, DoubleWritable>.Context context)
			throws IOException, InterruptedException {
		map = ReadMuAndSita(
				"hdfs://localhost:9000/user/hadoop/BDAnalysis/exp2/NBC/Output/MuAndSita/part-r-00000");
	}

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, DoubleWritable>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] attributes = line.split("\t");
		String classification = attributes[0];
		if (Double.parseDouble(classification) == 1) {
			classification = "F";
		} else {
			classification = "T";
		}
		double trueProbability = Double.parseDouble(map.get("T"));
		double falseProbability = Double.parseDouble(map.get("F"));
		double trueMu = 0;
		double falseMu = 0;
		double trueSita = 0;
		double falseSita = 0;
		String temp1 = "";
		String temp2 = "";
		for (int i = 1; i < attributes.length; i++) {
			temp1 = map.get("attribute"+i+",T");
			temp2 = map.get("attribute"+i+",F");
			trueMu = Double.parseDouble(temp1.split(",")[0]);
			falseMu = Double.parseDouble(temp2.split(",")[0]);
			trueSita = Double.parseDouble(temp1.split(",")[1]);
			falseSita = Double.parseDouble(temp2.split(",")[1]);
			trueProbability *= calProbality(Double.parseDouble(attributes[i]), trueMu, trueSita);
			falseProbability *= calProbality(Double.parseDouble(attributes[i]), falseMu, falseSita);
		}
		if(trueProbability >= falseProbability) { //预测为阳
			if(classification.equals("T")) { 
				context.write(new Text("Tp"), new DoubleWritable(1));
			}else {
				context.write(new Text("Fp"), new DoubleWritable(1));
			}
		}else { //预测为阴
			if(classification.equals("T")) {
				context.write(new Text("Fn"), new DoubleWritable(1));
			}else {
				context.write(new Text("Tn"), new DoubleWritable(1));
			}
		}	
	}

	public static double calProbality(double attri , double mu , double sita) {
		double temp1 =(-(attri - mu)*(attri - mu))/(2*sita);
		double temp2 = Math.pow(2*Math.PI*sita, 0.5);
		return Math.exp(temp1)/temp2;
	}
	public static Map<String, String> ReadMuAndSita(String FilePath) throws IOException {
		Map<String, String> map = new HashMap<>();

		Configuration conf = new Configuration();
		Path srcPath = new Path(FilePath);
		FileSystem fs = FileSystem.get(URI.create(FilePath), conf);// 通过URI来获取文件系统
		FSDataInputStream hdfsInStream = fs.open(srcPath);
		BufferedReader br = new BufferedReader(new InputStreamReader(hdfsInStream));
		String line = null;
		while (((line = br.readLine()) != null)) {
			String[] lines = line.toString().split("\t");
			String attriAndClass = lines[0];
			String MuAndSita = lines[1];
			map.put(attriAndClass, MuAndSita);
		}
		br.close();
		hdfsInStream.close();
		return map;
	}
}
