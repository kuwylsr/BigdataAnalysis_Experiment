package BSP;

import java.util.ArrayList;
import java.util.List;

public abstract class Aggregator<V,E,I> {

	private ArrayList<Integer> content = new ArrayList<>();
	private int sum = 0;
	
	public ArrayList<Integer> getContent(){
		return this.content;
	}
	
	public int getSum() {
		return this.sum;
	}
	
	/**
	 * 指定每个顶点发送的内容
	 * @return
	 */
	public abstract Object report();
	
	/**
	 * 指定Aggregator的聚集方式
	 * @param arrayList
	 * @return
	 */
	public abstract Object aggregate(ArrayList<Integer> arrayList);
}
