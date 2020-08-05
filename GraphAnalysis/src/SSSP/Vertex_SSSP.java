package SSSP;

import java.util.Map;
import java.util.Queue;

import BSP.Aggregator;
import BSP.Combiner;
import BSP.Master;
import BSP.MessageToW;
import BSP.Vertex;
import BSP.Worker;

public class Vertex_SSSP extends Vertex<Integer, Integer, Integer>{
	
	int sourceVertexID = 0;
	Master<Integer, Integer, Integer> master = this.getBelongWorker().getBelongMaster();
	
	public Vertex_SSSP(int id, int value, Worker<Integer,Integer,Integer> w) {
		super(id, value, w);
		w.addVertex(this);
		master.InstanceOfCombiner(new Combiner_SSSP());
	}
	
	@Override
	public void compute(Queue<Integer> messageQueueS) {
		int min_dist;//初始化各个顶点的值
		if(super.getVertexID() == sourceVertexID) {
			min_dist = 0;
		}else {
			min_dist = Integer.MAX_VALUE;
		}
		for(Integer m : messageQueueS) {
			min_dist = Math.min(m, min_dist);
		}
		if(min_dist < super.getVertexValue()) { //更新顶点的值，发送消息
			super.setVertexValue(min_dist);
			for(Map.Entry<Integer, Integer> entry : super.getEdge().entrySet()) {
				int dest_vertexID = entry.getKey();
				int edgeValue = entry.getValue();
				super.SendMessageTo(dest_vertexID, min_dist + edgeValue);
			}
		}
		super.VoteToHalt();
	}

	@Override
	public void compute(Queue<Integer> messageQueueS, Aggregator<Integer, Integer, Integer> aggregator) {
		// TODO Auto-generated method stub
		
	}
}
