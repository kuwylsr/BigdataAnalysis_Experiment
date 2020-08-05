package KMeans;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class KMeans_Mapper extends Mapper<LongWritable, Text, Text, Text>{
	
	ArrayList<String> clusterList = new ArrayList<>();
	int flag;
	@Override
	protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		String temp = context.getConfiguration().get("clusterList");
		for(String s : temp.split("\n")) {
			clusterList.add(s);
		}
		flag = context.getConfiguration().getInt("flag", 2);
	}
	

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String cluster = getCluster(clusterList,line); //找到与之最相近的质心
		if(flag == 0) {
			Text tCluster = new Text(cluster);
			context.write(tCluster, value);
		}else if(flag == 1) {
			Text tCluster = new Text(String.valueOf(cluster.hashCode()));
			context.write(value, tCluster);
		}else {
			System.exit(1);
		}
		
	}

	private String getCluster(ArrayList<String> clusterList, String line) {
		String[] temp1 = line.split(",");
		double[] attriTemp1 = new double[temp1.length-1];
		for(int i = 0 ;i<attriTemp1.length;i++) {
			attriTemp1[i] = Double.parseDouble(temp1[i+1]);
		}
		
		double min = Double.MAX_VALUE;
		String cluster = "";
		for(int i = 0;i<clusterList.size();i++) {
			String[] temp2 = clusterList.get(i).split(",");
			double[] attriTemp2 = new double[temp2.length];
			double sum = 0;
			for(int j = 0 ;j < attriTemp2.length;j++) {
				attriTemp2[j] = Double.parseDouble(temp2[j]);
				sum += ((attriTemp1[j] - attriTemp2[j]) * (attriTemp1[j] - attriTemp2[j]));
			}
			if(sum < min) {
				min = sum;
				cluster = clusterList.get(i);
			}
		}
		return cluster;
	}
}
