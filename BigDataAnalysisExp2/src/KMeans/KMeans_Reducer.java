package KMeans;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class KMeans_Reducer extends Reducer<Text, Text, Text, Text>{

	double varySum = 0;

	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {		
		String[] oldCluster = key.toString().split(",");// 旧的质心
		double[] newCluster = new double[oldCluster.length]; //新的质心
		
		Iterator<Text> iter = values.iterator();
		int count = 0;
		while(iter.hasNext()) {//计算新的质心
			count++;
			String node = iter.next().toString();
			String[] attriNode = node.split(",");
			for(int i = 0 ;i<newCluster.length;i++) {
				newCluster[i] += Double.parseDouble(attriNode[i+1]);
			}
		}
		double vary = 0;//质心的变化值(代价)
		StringBuilder sb = new StringBuilder();
		for(int i = 0 ; i< newCluster.length;i++) {
			double temp = newCluster[i]/count; //计算新的质心
			sb.append(temp).append(",");
			vary += ((temp - Double.parseDouble(oldCluster[i])) * (temp - Double.parseDouble(oldCluster[i])));
		}
		varySum += vary;
		Text tNewCluster = new Text(sb.toString());
		context.write(null, tNewCluster);
	}
	
	@Override
	protected void cleanup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
		if(varySum < context.getConfiguration().getDouble("varyLimit", 0.5)) {
			context.getCounter("tag","tag").setValue(-1);
		}else {
			context.getCounter("tag","tag").setValue(1);
		}
	}
}
