package exp1_DataPreprocess;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import exp1_DataFilter.Count;

public class DataPreprocessed {

	public static int k = 1000;

	/**
	 * 从HDFS分布式文件系统中读取文件内容
	 * @param filePath HDFS中的目标文件路径
	 * @return 返回文件内容的字符串格式
	 * @throws IOException
	 * @throws URISyntaxException
	 */
	public String[] readFileFromHDFS(String FilePath) throws IOException {

		Configuration conf = new Configuration();
		Path srcPath = new Path(FilePath);

		FileSystem fs = FileSystem.get(URI.create(FilePath), conf);// 通过URI来获取文件系统
		FSDataInputStream hdfsInStream = fs.open(srcPath);
		BufferedReader br = new BufferedReader(new InputStreamReader(hdfsInStream));

//		Count c = new Count();
//		System.out.println(c.countLineNum(FilePath));
		String[] content = new String[4686303];
		//StringBuffer content = new StringBuffer();
		String line = null;
		int lineNum = 0;
		while (((line = br.readLine()) != null)) {
			//content.append(line).append("\n");
		
			content[lineNum] = line;
			lineNum++;
		}
		br.close();
		hdfsInStream.close();
		return content;
	}
	
	/**
	 * 将数据写入HDFS当中
	 * @param filePath 输出文件的路径
	 * @throws IOException 
	 */
	public static void writeFileToHDFS(String filePath,String content) throws IOException {
		Configuration conf = new Configuration();
		Path dstPath = new Path(filePath);
		FileSystem fs = dstPath.getFileSystem(conf);
		
		FSDataOutputStream outputStream = fs.create(dstPath);
		outputStream.write(content.getBytes());
		outputStream.close();
		fs.close();
	}

	public static Set<String> calDispersedD(String[] dataSet, String dataTarget) {
		Map<String, Double> map = new TreeMap<String, Double>();
		Set<String> s = new HashSet<>();

		String user_nationality1 = dataTarget.split("\\|")[9];
		String user_career1 = dataTarget.split("\\|")[10];

		for (String data : dataSet) {
			String user_income = data.split("\\|")[11];
			if (!user_income.equals("?")) {
				Double distance = 0.0;
				//String line = "0" + "|" + "0";
				//String review_id = data.split("\\|")[0];

				String user_nationality2 = data.split("\\|")[9];
				String user_career2 = data.split("\\|")[10];
				if (!user_nationality1.equals(user_nationality2)) {
					distance++;
					//line = "1" + "|" + "0";
				}
				if (!user_career1.equals(user_career2)) {
					distance++;
					//line = line.split("\\|")[0] + "|" + "1";
				}
				map.put(data, distance);
			}
		}
		// 这里将map.entrySet()转换成list
		List<Map.Entry<String, Double>> list = new ArrayList<Map.Entry<String, Double>>(map.entrySet());
		// 然后通过比较器来实现排序
		Collections.sort(list, new Comparator<Map.Entry<String, Double>>() {
			// 升序排序
			public int compare(Entry<String, Double> o1, Entry<String, Double> o2) {
				return o1.getValue().compareTo(o2.getValue());
			}
		});

		int num = 0;
		for (Map.Entry<String, Double> entry : list) {
			num++;
			s.add(entry.getKey());
			if (num == k)
				break;
		}
		return s;
	}

	public static Set<String> calConsecutiveED(String[] dataSet, String dataTarget) {
		Map<String, Double> map = new TreeMap<>();
		Set<String> s = new HashSet<>();

		Double user_income1 = Double.parseDouble(dataTarget.split("\\|")[11]);
		Double longtitude1 = Double.parseDouble(dataTarget.split("\\|")[1]);
		Double latitude1 = Double.parseDouble(dataTarget.split("\\|")[2]);
		Double altitude1 = Double.parseDouble(dataTarget.split("\\|")[3]);

		for (String data : dataSet) {
			String rating = data.split("\\|")[6];
			String user_incomeTemp = data.split("\\|")[11];
			if (!rating.equals("?")&&!user_incomeTemp.equals("?")) {
				//String review_id = data.split("\\|")[0];

				Double user_income2 = Double.parseDouble(user_incomeTemp);
				Double longtitude2 = Double.parseDouble(data.split("\\|")[1]);
				Double latitude2 = Double.parseDouble(data.split("\\|")[2]);
				Double altitude2 = Double.parseDouble(data.split("\\|")[3]);
				Double ED = Math.pow((Math.pow((user_income1 - user_income2), 2) + Math.pow((longtitude1 - longtitude2), 2)
						+ Math.pow((latitude1 - latitude2), 2) + Math.pow((altitude1 - altitude2), 2)),0.5);
				map.put(data, ED);
			}
		}
		// 这里将map.entrySet()转换成list
		List<Map.Entry<String, Double>> list = new ArrayList<Map.Entry<String, Double>>(map.entrySet());
		// 然后通过比较器来实现排序
		Collections.sort(list, new Comparator<Map.Entry<String, Double>>() {
			// 升序排序
			public int compare(Entry<String, Double> o1, Entry<String, Double> o2) {
				return o1.getValue().compareTo(o2.getValue());
			}
		});

		int num = 0;
		for (Map.Entry<String, Double> entry : list) {
			num++;
			//System.out.println(entry.getKey() + "===" + entry.getValue());
			s.add(entry.getKey());
			if (num == k)
				break;
		}
		return s;
	}
	
	public static double calAvg(Set<String> s,int index) {
		double sum = 0 ;
		for(String data : s) {
			double temp = Double.parseDouble(data.split("\\|")[index]);
			sum += temp;
		}
		return sum/s.size();
	}

//	public double calPCC(Set<String> dataSet , String dataTarget,int variableNum) {
//		double PCCSum = 0.0;
//		for(int i = 0;i<variableNum;i++) {
//			double avg_x = 0.0;
//			Iterator<String> it = dataSet.iterator();
//			while(it.hasNext()) {
//				String temp = it.next();
//				double 
//			}
//		}
//	}
	public static void main(String[] args) throws IOException {
		DataPreprocessed c = new DataPreprocessed();

//		String[] dataSet = c.readFileFromHDFS(
//				"hdfs://localhost:9000/user/hadoop/BDAnalysis/exp1/Input/large_data.txt");
		String[] dataSet = c.readFileFromHDFS(
				"hdfs://localhost:9000/user/hadoop/BDAnalysis/exp1/Output/D_Filtered_SAndN/part-r-00000");
//		String dataTemp = c.readFileFromHDFS(
//				"hdfs://localhost:9000/user/hadoop/BDAnalysis/exp1/Output/D_Filtered_SAndN/part-r-00000");
//		String[] dataSet = dataTemp.split("\n");
		
		StringBuilder content = new StringBuilder();
		int lineNum = 0;
		for (String data : dataSet) {
			lineNum ++;
			if(lineNum % 5000 == 0) System.out.println(lineNum);
			String user_income = data.split("\\|")[11];
			String rating = data.split("\\|")[6];
			if (user_income.equals("?")) {
				Set<String> s = calDispersedD(dataSet, data);
				double FillUser_income = calAvg(s, 11);
				String pattern = "\\?$";
				data = data.replaceAll(pattern,String.format("%.0f", FillUser_income));
			}
			if (rating.equals("?")) {
				Set<String> s = calConsecutiveED(dataSet, data);
				double FillRating = calAvg(s, 6);
				String pattern = "\\|\\?\\|";
				data = data.replaceAll(pattern, "|"+String.format("%.2f", FillRating)+"|");
			}
			content.append(data);
			content.append("\n");
		}
		
		writeFileToHDFS("hdfs://localhost:9000/user/hadoop/BDAnalysis/exp1/Output/D_Preprocessed/FinalData.txt",content.toString());
	}

}
