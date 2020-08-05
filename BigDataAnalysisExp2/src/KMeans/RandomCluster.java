package KMeans;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class RandomCluster {

	public int k;
	public String rFilePath;
	public String wFilePath;

	public RandomCluster(int k, String path1, String path2) {
		this.k = k;
		this.rFilePath = path1;
		this.wFilePath = path2;
	}

	public String InitCluster() throws IOException {
		Configuration conf = new Configuration();
		Path srcPath = new Path(rFilePath);

		FileSystem fs = FileSystem.get(URI.create(rFilePath), conf);// 通过URI来获取文件系统
		FSDataInputStream hdfsInStream = fs.open(srcPath);
		BufferedReader br = new BufferedReader(new InputStreamReader(hdfsInStream));
		
		StringBuilder sb = new StringBuilder();
		HashSet<Integer> randomNum = new HashSet<>();
		randomSet(0, 10000, k, randomNum);
		String line = null;
		int lineNum = 0;
		while (((line = br.readLine()) != null)) {	
			lineNum++;
			if(randomNum.contains(lineNum)) {
				String temp = line.split(",")[0];
				int length = temp.length();
				String cluster = line.substring(length+1, line.length());
				sb.append(cluster);
				sb.append("\n");
			}
		}
		br.close();
		hdfsInStream.close();
		writeFileToHDFS(wFilePath, sb.toString());
		return sb.toString();
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
	
	/**
	 * 将数据写入HDFS当中
	 * 
	 * @param filePath 输出文件的路径
	 * @throws IOException
	 */
	public void writeFileToHDFS(String filePath, String content) throws IOException {
		Configuration conf = new Configuration();
		Path dstPath = new Path(filePath);
		FileSystem fs = dstPath.getFileSystem(conf);

		FSDataOutputStream outputStream = fs.create(dstPath);
		outputStream.write(content.getBytes());
		outputStream.close();
		fs.close();
	}
	
	public static void main(String[] args) throws IOException {
		String path1 = "hdfs://localhost:9000/user/hadoop/BDAnalysis/exp2/KMeansOutput/InitCluster";
		String path2 = "hdfs://localhost:9000/user/hadoop/BDAnalysis/exp2/Input/USCensus1990.data.txt";
		RandomCluster c = new RandomCluster(10, path2, path1);
		c.InitCluster();

	}
}
