package exp1_Ultimate;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PCC_Reducer extends Reducer<Text, Text, Text, Text>{

	static double[] avgs = PCC_Mapper.list;
	static int lineNum = 4684466;
	static int line = 0;
	static double[] pcc_rating = new double[5];
	static double[] sum_rating1 = new double[5];
	static double sum_rating2 = 0.0; 
	static double[] sum_rating3 = new double[5];
	static double[] pcc_userincome = new double[5];
	static double[] sum_userincome1 = new double[5];
	static double sum_userincome2 = 0.0;
	static double[] sum_userincome3 = new double[5];

	
	public String toString(double[] temp) {
		String content = "";
		for(double d : temp) {
			content = content + String.valueOf(d) + "\t";
		}
		return content;
	}
	
	public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException {
		for(int i = 0 ;i< 6 ;i++) {
			System.out.println(avgs[i]/lineNum);
		}
		Iterator<Text> iter = values.iterator();
		while(iter.hasNext()) {
			line++;
			Text temp = iter.next();
			String line = temp.toString();
			String[] attribute = line.split("\\|");
			
 			double longitute = Double.parseDouble(attribute[1]);
			double latitude = Double.parseDouble(attribute[2]);
			double altitude = Double.parseDouble(attribute[3]);
			double temperature = Double.parseDouble(attribute[5].substring(0, attribute[5].length()-1));
			double rating = Double.parseDouble(attribute[6]);
			double userincome = Double.parseDouble(attribute[11]);
			double[] temp1 = new double[4];
			
			temp1[0] = longitute;
			temp1[1] = latitude;
			temp1[2] = altitude;
			temp1[3] = temperature;

			sum_rating2 += (rating - avgs[4]/lineNum)*(rating - avgs[4]/lineNum);
			sum_userincome2 += (userincome - avgs[5]/lineNum)*(userincome - avgs[5]/lineNum);
			for(int i =0 ;i<3;i++) {
				sum_rating1[i] += (rating - avgs[4]/lineNum)*(temp1[i] - avgs[i]/lineNum);
				sum_rating3[i] += (temp1[i] - avgs[i]/lineNum)*(temp1[i] - avgs[i]/lineNum);
				sum_userincome1[i] += (userincome - avgs[5]/lineNum)*(temp1[i] - avgs[i]/lineNum);
				sum_userincome3[i] += (temp1[i] - avgs[i]/lineNum)*(temp1[i] - avgs[i]/lineNum);
			}
			sum_rating1[4] += (rating - avgs[4]/lineNum)*(userincome - avgs[5]/lineNum);
			sum_rating3[4] = sum_userincome2;
			sum_userincome1[4] = sum_rating1[4];
			sum_userincome3[4] = sum_rating2;	
			
		}

		for(int i = 0 ;i<5;i++) {
			pcc_rating[i] = sum_rating1[i] / ((Math.pow(sum_rating2, 0.5)) * (Math.pow(sum_rating3[i], 0.5)));
			pcc_userincome[i] = sum_userincome1[i] / ((Math.pow(sum_userincome2, 0.5)) * (Math.pow(sum_userincome3[i], 0.5)));
		}
		
		String content = "";
		content = content + " " + "\t" + "longitute" + "\t" + "latitude" +"\t"+"altitude"+"\t"+"temperature" +"\t" + "==" ;
		content = content +"\n"+"rating:";
		content = content + toString(pcc_rating) + "\n" +"income:"+ toString(pcc_userincome);
		System.out.println(line);
		Text t = new Text();
		t.set(content);
		context.write(null,t);
		
	}
}
