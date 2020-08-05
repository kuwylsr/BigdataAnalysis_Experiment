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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper_calSitaAndMu extends Mapper<LongWritable, Text, Text, DoubleWritable>{

	private Map<String,Double> MuMap = new HashMap<>();
	@Override
	protected void setup(Mapper<LongWritable, Text, Text, DoubleWritable>.Context context)
			throws IOException, InterruptedException {
		MuMap = ReadAttriSums(context, "hdfs://localhost:9000/user/hadoop/BDAnalysis/exp2/NBC/Output/attriNumsByC/part-r-00000");
		double TrueNums = context.getConfiguration().getDouble("T", 0);
		double FalseNums = context.getConfiguration().getDouble("F", 0);
		context.write(new Text("T"), new DoubleWritable((TrueNums/(TrueNums + FalseNums))));
		context.write(new Text("F"), new DoubleWritable((FalseNums/(TrueNums + FalseNums))));
		super.setup(context);
	}
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, DoubleWritable>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] attributions = line.split("\t");
		Text t = new Text();
		String classification = attributions[0];
		String temp = "";
		if(Double.parseDouble(classification) == 1) {
			classification = "F";
		}else {
			classification = "T";
		}
		for(int i = 1 ;i<attributions.length;i++) {
			temp = "attribute"+String.valueOf(i)+","+classification;
			double mu = MuMap.get(temp);
			double attri = Double.parseDouble(attributions[i]);
			double sitaTemp = Math.pow(attri - mu,2);
			t.set(temp+"_"+mu);
			context.write(t, new DoubleWritable(sitaTemp));
		}
	}
	
	public static Map<String,Double> ReadAttriSums(Context context,String FilePath) throws IOException {
		Map<String,Double> map = new HashMap<>();
		
		Configuration conf = new Configuration();
		Path srcPath = new Path(FilePath);	
		FileSystem fs = FileSystem.get(URI.create(FilePath), conf);// 通过URI来获取文件系统
		FSDataInputStream hdfsInStream = fs.open(srcPath);
		BufferedReader br = new BufferedReader(new InputStreamReader(hdfsInStream));
		String line = null;
		while(((line = br.readLine()) != null)) {
			String[] lines = line.toString().split("\t");
			String classification = lines[0].split(",")[1];
			double attriSums = Double.parseDouble(lines[1]);
			double classSums = 0;
			if(classification.equals("F")) {
				classSums = context.getConfiguration().getDouble("F", 0);
			}else {
				classSums = context.getConfiguration().getDouble("T",0);
			}
			map.put(lines[0], attriSums/classSums);
		}
		br.close();
		hdfsInStream.close();
		return map;
	}
}
