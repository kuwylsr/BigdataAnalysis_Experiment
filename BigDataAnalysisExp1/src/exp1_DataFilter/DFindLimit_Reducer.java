package exp1_DataFilter;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.google.protobuf.UnknownFieldSet.Field;

public class DFindLimit_Reducer extends Reducer<DoubleWritable, Text, Text, Text>{

	public static int Lines = 0;
	
	@Override
	/**
	 * reduce类
	 * 输入:经过shuffle的过程,输入的键值对为
	 *     key:属性值rating
	 *     value:该属性所以应的一条数据信息
	 * 输出:过滤掉rating取值排名1%之前和99%之后的数据
	 *     key:null
	 *     value:用户数据信息
	 * @throws InterruptedException 
	 * @throws IOException 
	 * 
	 */
	public void reduce(DoubleWritable key,Iterable<Text> values,Context context) throws IOException, InterruptedException {
		Iterator<Text> iter = values.iterator();
		int fileLines = context.getConfiguration().getInt("FileLines", 0);
		double rating = key.get();
		while(iter.hasNext()) {
			String temp = iter.next().toString();
			if(rating != -1) {
				Lines++;
			}
			
			if(Lines == (int)(fileLines*0.01)) {
				Text t1 = new Text();
				t1.set(String.valueOf(rating));
				context.write(null, t1);
			}else if(Lines == (int)(fileLines*0.99)) {
				Text t2 = new Text();
				t2.set(String.valueOf(rating));
				context.write(null, t2);
			}
		}
	}

}
