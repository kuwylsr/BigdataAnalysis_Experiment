package exp1_DataFilter;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Count {

	public int countByByte(String FilePath) throws IOException {
		Configuration conf = new Configuration();
		Path srcPath = new Path(FilePath);
		
		FileSystem fs = FileSystem.get(URI.create(FilePath), conf);// 通过URI来获取文件系统
		FSDataInputStream hdfsInStream = fs.open(srcPath);
		
		int line = 0;
		String content = "";
		byte[] ioBuffer = new byte[1]; // 每次从文件系统中读取一个字节的数据
		int readLen = hdfsInStream.read(ioBuffer);
		while(readLen!=-1)
		{
			if(new String(ioBuffer, 0, readLen).indexOf('\n')!=-1) {
				line ++;
			}
			content = content + new String(ioBuffer, 0, readLen);
			readLen = hdfsInStream.read(ioBuffer);
		}
		hdfsInStream.close();
		fs.close();
		return line;
	}
	
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

		String[] content = new String[4686303];
		String line = null;
		int lineNum = 0;
		while (((line = br.readLine()) != null)) {	
			content[lineNum] = line;
			lineNum++;
		}
		br.close();
		hdfsInStream.close();
		return content;
	}
	
	public Map<String,Double> count(String FilePath) throws IOException {
		Map<String,Double> map = new HashMap<>();
		Double max = Double.MIN_VALUE;
		Double min = Double.MAX_VALUE;
		
		Configuration conf = new Configuration();
		Path srcPath = new Path(FilePath);
		
		FileSystem fs = FileSystem.get(URI.create(FilePath), conf);// 通过URI来获取文件系统
		FSDataInputStream hdfsInStream = fs.open(srcPath);
		BufferedReader br = new BufferedReader(new InputStreamReader(hdfsInStream));
		
		int numLine = 0;
		String line = null;
		while(((line = br.readLine()) != null)) {
			String Srating = line.split("\\|")[6];
			if(Srating.equals("?"));
			else {
				Double rating = Double.parseDouble(Srating);
				if(rating > max) max = rating;
				if (rating < min) min = rating;
			}
			numLine++;
		}
		br.close();
		hdfsInStream.close();
		map.put("numLine", (double)numLine);
		map.put("max", max);
		map.put("min", min);
		return map;
	}
	
	public double[] ReadLimits(String FilePath) throws IOException {	
		double[] limits = new double[2];
		Configuration conf = new Configuration();
		Path srcPath = new Path(FilePath);
		
		FileSystem fs = FileSystem.get(URI.create(FilePath), conf);// 通过URI来获取文件系统
		FSDataInputStream hdfsInStream = fs.open(srcPath);
		BufferedReader br = new BufferedReader(new InputStreamReader(hdfsInStream));
		
		String line = null;
		int i = 0;
		while(((line = br.readLine()) != null)) {
			limits[i] = Double.parseDouble(line);
			i++;
		}
		br.close();
		hdfsInStream.close();
		return limits;
	}
	public int countLineNum(String FilePath) throws IOException {	
		Configuration conf = new Configuration();
		Path srcPath = new Path(FilePath);
		
		FileSystem fs = FileSystem.get(URI.create(FilePath), conf);// 通过URI来获取文件系统
		FSDataInputStream hdfsInStream = fs.open(srcPath);
		BufferedReader br = new BufferedReader(new InputStreamReader(hdfsInStream));
		
		String line = null;
		int i = 0;
		while(((line = br.readLine()) != null)) {
			i++;
		}
		br.close();
		hdfsInStream.close();
		return i;
	}
	
	
	public static void main(String[] args) throws IOException {
		Count c = new Count();
		System.out.println(c.count("hdfs://localhost:9000/user/hadoop/BDAnalysis/exp1/Output/D_Filtered/part-r-00000"));
	}

}
