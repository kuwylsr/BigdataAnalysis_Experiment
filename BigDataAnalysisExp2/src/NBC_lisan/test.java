package NBC_lisan;

import java.util.HashMap;
import java.util.Map;

public class test {

	public static void main(String[] args) {
		
		Map<String,Double> map = new HashMap<>();
		int count = 0;
		for(int i = 0 ;i<10000000 ; i++) {
			count++;
			if(count % 1000000 == 0) {
				System.out.println(count);
			}
			map.put("a"+i, (double) i);
		}
		System.out.println(map.get("a100323"));

	}

}
