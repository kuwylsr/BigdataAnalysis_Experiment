package exp1_DateSimple;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DSimple_Reducer extends Reducer<Text,Text,Text,Text>{
	
	static Map<String,Double>  map = new HashMap<>();
	static int lines = 0;
	/**
	 * reduce类
	 * 输入:经过shuffle的过程,输入的键值对为
	 *     key:属性值user_career
	 *     value:该属性所以应的一条数据信息
	 * 输出:按1%进行分成抽样的数据信息
	 *     key:null
	 *     value:用户数据信息
	 * @throws InterruptedException 
	 * @throws IOException 
	 * 
	 */
	public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException {
		Iterator<Text> iter = values.iterator();
		while(iter.hasNext()) {	
			Text temp = iter.next();
			String line = temp.toString();
			double num = Math.random()*100;
			if(num <= 1) { //1%
				//context.write(null, temp);
				
				String rating = line.split("\\|")[6];
				if(!rating.equals("?")) {
					lines++;
					map.put(line,Double.parseDouble(rating));
				}
			}else {
				continue;
			}
		}
		
//		Iterator<Text> iter = values.iterator();
//		while(iter.hasNext()) {
//			Text temp = iter.next();
//			String line = temp.toString();
//			String rating = line.split("\\|")[6];
//			if(!rating.equals("?")) {
//				map.put(Double.parseDouble(rating), line);
//			}
//		}
	}
	
	protected void cleanup(Context context) throws IOException, InterruptedException {
		//context.getCounter("test","test").setValue(-1);
		int fileLines = context.getConfiguration().getInt("FileLines", 0);
		//这里将map.entrySet()转换成list
        List<Map.Entry<String,Double>> list = new ArrayList<Map.Entry<String,Double>>(map.entrySet());
        //然后通过比较器来实现排序
        Collections.sort(list,new Comparator<Map.Entry<String,Double>>() {
            //升序排序
            public int compare(Entry<String,Double> o1,
                    Entry<String,Double> o2) {
                return o1.getValue().compareTo(o2.getValue());
            }
            
        });
        
        String limit = "";
        int line = 0;
		lines = list.size();
		for (Map.Entry<String, Double> mapping : list) {
			if (line == (int) (lines * 0.01)) {
				limit = limit + mapping.getValue() + "\n";
			}
			if (line == (int) (lines * 0.99)) {
				limit = limit + mapping.getValue() + "\n";
			}
			line++;
		}
		Text t1 = new Text();
		t1.set(String.valueOf(limit));
		context.write(null, t1);

	}
}
