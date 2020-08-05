package exp1_DataPreprocess;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.Queue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DPreprocess_Reducer2 extends Reducer<Text, Text, Text, Text>{

	public static int k = 20;
	
	public void reduce(Text key, Iterable<Text> value,Context context) throws IOException, InterruptedException {
		Iterator<Text> item = value.iterator();
		String identification = key.toString();
		
		if(identification.equals("ui?")) {
			Set<String> uiSet = new HashSet<>();
			while(item.hasNext()) {
				Text tLine = item.next();
				uiSet.add(tLine.toString());
			}
			Map<String,Double> map = calDispersedD(uiSet);
			for(Map.Entry<String, Double> entry : map.entrySet()) {
				String dataT = entry.getKey();
				double FillUser_income = entry.getValue();
				String pattern = "\\?$";
				dataT = dataT.replaceAll(pattern,String.format("%.0f", FillUser_income));
				Text data = new Text();
				data.set(dataT);
				context.write(null, data);
			}
		}else if(identification.equals("r?")) {
			Set<String> rSet = new HashSet<>();
			while(item.hasNext()) {
				Text tLine = item.next();
				rSet.add(tLine.toString());
			}
			Map<String,Double> map = calConsecutiveED(rSet);
			for(Map.Entry<String, Double> entry : map.entrySet()) {
				String dataT = entry.getKey();
				double FillRating = entry.getValue();
				String pattern = "\\|\\?\\|";
				dataT = dataT.replaceAll(pattern,"|"+String.format("%.2f", FillRating)+"|");
				Text data = new Text();
				data.set(dataT);
				context.write(null, data);
			}
		}else if(identification.equals("normal")){
			while(item.hasNext()) {
				Text tLine = item.next();
				context.write(null, tLine);
			}
		}
		
	}
	
	public Map<String,Double> calDispersedD(Set<String> dataTargetSet) throws IOException {
		String FilePath = "hdfs://localhost:9000/user/hadoop/BDAnalysis/exp1/Output/D_Filtered_SAndN/part-r-00000";
		//String FilePath = "hdfs://localhost:9000/user/hadoop/BDAnalysis/exp1/OutputTest/D_Filtered_SAndN/part-r-00000";
		Configuration conf = new Configuration();
		Path srcPath = new Path(FilePath);
		FileSystem fs = FileSystem.get(URI.create(FilePath), conf);// 通过URI来获取文件系统
		FSDataInputStream hdfsInStream = fs.open(srcPath);
		BufferedReader br = new BufferedReader(new InputStreamReader(hdfsInStream));

		Map<String,Queue<String>> Mmap = new HashMap<>();
		for(String s : dataTargetSet) {
			Queue<String> Pqueue = new PriorityQueue<>(idComparator);
			Mmap.put(s, Pqueue);
		}
		String line = null;
		int lineNum = 0;
		System.out.println("进入文件系统1");
		long startTime = System.currentTimeMillis();//获取当前时间
		while (((line = br.readLine()) != null)) {
			lineNum++;
			if(lineNum % 1000 == 0) System.out.println(lineNum);
			if(lineNum % 10000 == 0 ) {
				long endTime = System.currentTimeMillis();//获取当前时间
				System.out.println("程序运行总时间:"+(double)(endTime-startTime)/60000 + "秒");
				startTime = endTime;
			}
			String[] lineAttributes = line.split("\\|");
			String user_income = lineAttributes[11];
			if(!user_income.equals("?")) {
				String user_nationality2 = lineAttributes[9];
				String user_career2 = lineAttributes[10];
				Iterator<String> item = dataTargetSet.iterator();
				while(item.hasNext()) {
					String dataTarget = item.next();
					String[] dataTargetAttributes = dataTarget.split("\\|");
					String user_nationality1 = dataTargetAttributes[9];
					String user_career1 = dataTargetAttributes[10];
					double distance = 0.0;
					
					if (!user_nationality1.equals(user_nationality2)) {
						distance++;
					}
					if (!user_career1.equals(user_career2)) {
						distance++;
					}
					
					Queue<String> Pqueue = Mmap.get(dataTarget);
					if(Pqueue.size() >= k) { //维持堆的大小不超过20个元素
						if(distance > Double.parseDouble(Pqueue.peek().split("\t")[1])) {
							Pqueue.poll();
							Pqueue.add(line + "\t" + String.valueOf(distance));
							Mmap.put(dataTarget, Pqueue);
						}
					}else {
						Pqueue.add(line + "\t" + String.valueOf(distance));
						Mmap.put(dataTarget, Pqueue);
					}
				}	
			}
		}
		br.close();
		hdfsInStream.close();
		
		Map<String,Double>  fillMap = new HashMap<>();
		StringBuilder content = new StringBuilder();
		for(Map.Entry<String, Queue<String>> entry : Mmap.entrySet()) {
			String dataT = entry.getKey();
			Queue<String> q = entry.getValue();
			double sum = 0;
			Iterator<String> item = q.iterator();
			while(item.hasNext()) {
				
				sum += Double.parseDouble(q.poll().split("\t")[0].split("\\|")[11]);
			}
			fillMap.put(dataT, sum/k);
		}
		return fillMap;
	}

	public static Map<String,Double> calConsecutiveED(Set<String> dataTargetSet) throws IOException {
		String FilePath = "hdfs://localhost:9000/user/hadoop/BDAnalysis/exp1/Output/D_Filtered_SAndN/part-r-00000";
		//String FilePath = "hdfs://localhost:9000/user/hadoop/BDAnalysis/exp1/OutputTest/D_Filtered_SAndN/part-r-00000";
		Configuration conf = new Configuration();
		Path srcPath = new Path(FilePath);
		FileSystem fs = FileSystem.get(URI.create(FilePath), conf);// 通过URI来获取文件系统
		FSDataInputStream hdfsInStream = fs.open(srcPath);
		BufferedReader br = new BufferedReader(new InputStreamReader(hdfsInStream));

		Map<String,Queue<String>> Mmap = new HashMap<>();
		for(String s : dataTargetSet) {
			Queue<String> Pqueue = new PriorityQueue<>(idComparator);
			Mmap.put(s, Pqueue);
		}
		String line = null;
		int lineNum = 0;
		System.out.println("进入文件系统2");
		
		double user_income2 = 0.0;
		double longtitude2 = 0.0;
		double latitude2 = 0.0;
		double altitude2 = 0.0;
		
		double user_income1 = 0.0;
		double longtitude1 = 0.0;
		double latitude1 = 0.0;
		double altitude1 = 0.0;
		double ED = 0.0;
		long startTime = System.currentTimeMillis();//获取当前时间
		while (((line = br.readLine()) != null)) {
			lineNum++;
			if(lineNum % 1000 == 0) System.out.println(lineNum);
			if(lineNum % 10000 == 0 ) {
				long endTime = System.currentTimeMillis();//获取当前时间
				System.out.println("程序运行总时间:"+(double)(endTime-startTime)/60000 + "秒");
				startTime = endTime;
			}
			String[] lineAttributes = line.split("\\|");
			String rating = lineAttributes[6];
			String user_incomeTemp = lineAttributes[11];
			
			if(!rating.equals("?")&&!user_incomeTemp.equals("?")) {
				user_income2 = Double.parseDouble(user_incomeTemp);
				longtitude2 = Double.parseDouble(lineAttributes[1]);
				latitude2 = Double.parseDouble(lineAttributes[2]);
				altitude2 = Double.parseDouble(lineAttributes[3]);
				Iterator<String> item = dataTargetSet.iterator();
				while(item.hasNext()) {
					String dataTarget = item.next();
					String[] dataTargetAttributes = dataTarget.split("\\|");
					user_income1 = Double.parseDouble(dataTargetAttributes[11]);
					longtitude1 = Double.parseDouble(dataTargetAttributes[1]);
					latitude1 = Double.parseDouble(dataTargetAttributes[2]);
					altitude1 = Double.parseDouble(dataTargetAttributes[3]);
					ED = Math.pow((Math.pow((user_income1 - user_income2), 2) + Math.pow((longtitude1 - longtitude2), 2)
							+ Math.pow((latitude1 - latitude2), 2) + Math.pow((altitude1 - altitude2), 2)),0.5);
					
					Queue<String> Pqueue = Mmap.get(dataTarget);
					if(Pqueue.size() >= k) { //维持堆的大小不超过20个元素
						if(ED < Double.parseDouble(Pqueue.peek().split("\t")[1])) {
							Pqueue.poll();
							Pqueue.add(line+ "\t" + String.valueOf(ED));
							Mmap.put(dataTarget, Pqueue);
						}
					}else {
						Pqueue.add(line+ "\t" + String.valueOf(ED));
						Mmap.put(dataTarget, Pqueue);
					}
				}	
			}
			
		}
		br.close();
		hdfsInStream.close();
		
		Map<String,Double>  fillMap = new HashMap<>();
		for(Map.Entry<String, Queue<String>> entry : Mmap.entrySet()) {
			String dataT = entry.getKey();
			Queue<String> q = entry.getValue();
			double sum = 0;
			Iterator<String> item = q.iterator();
			while(item.hasNext()) {
				
				sum += Double.parseDouble(q.poll().split("\t")[0].split("\\|")[6]);
			}
			fillMap.put(dataT, sum/k);
		}
		return fillMap;
	}

	//匿名Comparator实现
    public static Comparator<String> idComparator = new Comparator<String>(){
 
        @Override
        public int compare(String c1, String c2) {
            return (int) (Double.parseDouble(c2.split("\t")[1]) - Double.parseDouble(c1.split("\t")[1])); //最大堆
        }
    };
    
    /**
	 * 将数据写入HDFS当中
	 * @param filePath 输出文件的路径
	 * @throws IOException 
	 */
	public void writeFileToHDFS(String filePath,String content) throws IOException {
		Configuration conf = new Configuration();
		Path dstPath = new Path(filePath);
		FileSystem fs = dstPath.getFileSystem(conf);
		
		FSDataOutputStream outputStream = fs.create(dstPath);
		outputStream.write(content.getBytes());
		outputStream.close();
		fs.close();
	}
}

