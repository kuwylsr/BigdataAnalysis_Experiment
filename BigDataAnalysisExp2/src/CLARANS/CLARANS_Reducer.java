package CLARANS;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CLARANS_Reducer extends Reducer<Text, Text, Text, Text>{

	int tag = 0;
	double varySum = 0;
	private int RandomExchangeNum = 50;
	
	public double calCost(String[] cluster,String[] node) {
		double cost = 0;
		for(int i = 0;i<cluster.length;i++) {
			cost += ((Double.parseDouble(node[i+1]) - Double.parseDouble(cluster[i])) * (Double.parseDouble(node[i+1]) - Double.parseDouble(cluster[i])));
		}
		return cost;
	}
	/**
	 * 随机指定范围内N个不重复的数 利用HashSet的特征，只能存放不同的值
	 * 
	 * @param min 指定范围最小值
	 * @param max 指定范围最大值
	 * @param n   随机数个数
	 * @param     HashSet<Integer> set 随机数结果集
	 */
	public static void randomSet(int min, int max, int n, HashSet<Integer> set) {
		if (n > (max - min + 1) || max < min) {
			return;
		}
		for (int i = 0; i < n; i++) {
			// 调用Math.random()方法
			int num = (int) (Math.random() * (max - min)) + min;
			set.add(num);// 将不同的数存入HashSet中
		}
		int setSize = set.size();
		// 如果存入的数小于指定生成的个数，则调用递归再生成剩余个数的随机数，如此循环，直到达到指定大小
		if (setSize < n) {
			randomSet(min, max, n - setSize, set);// 递归
		}
	}
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		String newCluster = "";
		String[] oldCluster = key.toString().split(",");
		ArrayList<String> valueList = new ArrayList<>();
		Iterator<Text> iter = values.iterator();
		double costTemp = 0;
		while(iter.hasNext()) { //将所有记录存入一个集合,便于随机抽取
			String node = iter.next().toString();
			String[] attriNode = node.split(",");
			costTemp += calCost(oldCluster, attriNode); //计算初始质心的代价costTemp
			valueList.add(node);
		}
		double cost = costTemp; //初始化代价
		newCluster = key.toString();
		
		HashSet<Integer> randomNum = new HashSet<>();
		randomSet(0, valueList.size()-1, RandomExchangeNum, randomNum);//生成RandomExchangeNum个随机数
		//System.out.println(randomNum);
		Iterator<Integer> randomIter = randomNum.iterator();
		while(randomIter.hasNext()) {
			costTemp = 0;
			String temp = valueList.get(randomIter.next());//按照生成的随机数进行抽取
			int length = temp.split(",")[0].length();
			String exchangeCluster = temp.substring(length+1, temp.length());
			String[] attriExchange = exchangeCluster.split(",");
			for(String node : valueList) { //计算新质心的代价costTemp
				String[] atttiNode = node.split(",");
				costTemp += calCost(attriExchange, atttiNode);
			}
			//System.out.println(costTemp + "||" + cost);
			if(costTemp < cost) {//更新最小代价和质心
				cost = costTemp;
				newCluster = exchangeCluster;
			}
		}
		if(!newCluster.equals(key.toString())) {
			tag = 1;//质心发生改变
		}
		Text tNewCluster = new Text(newCluster);
		context.write(null, tNewCluster);
	}
	
	@Override
	protected void cleanup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
		if(tag == 0) {
			context.getCounter("tag","tag").setValue(-1);
		}else {
			context.getCounter("tag","tag").setValue(1);
		}
	}
	
	
}
