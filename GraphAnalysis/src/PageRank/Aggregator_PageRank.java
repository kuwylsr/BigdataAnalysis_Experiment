package PageRank;

import java.util.ArrayList;
import java.util.List;

import BSP.Aggregator;

public class Aggregator_PageRank extends Aggregator<Double, Double, Double>{

	private Vertex_PageRank item;
	public Aggregator_PageRank(Vertex_PageRank item) {
		this.item = item;
	}
	@Override
	public Object report() {
		// TODO Auto-generated method stub
		return this.item.getVertexID();
	}
	
	@Override
	public Object aggregate(ArrayList<Integer> arrayList) {
		// TODO Auto-generated method stub
		return arrayList.size();
	}

}
