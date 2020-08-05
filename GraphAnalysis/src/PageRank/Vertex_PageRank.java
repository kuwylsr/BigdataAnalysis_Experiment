package PageRank;

import java.util.Map;
import java.util.Queue;

import BSP.Aggregator;
import BSP.Combiner;
import BSP.Master;
import BSP.Vertex;
import BSP.Worker;
import BSP.Master.Statisticians;

public class Vertex_PageRank extends Vertex<Double, Double, Double>{

	Master<Double, Double, Double> master = this.getBelongWorker().getBelongMaster();
	Worker<Double,Double,Double> worker = this.getBelongWorker();
	
	public Vertex_PageRank(int id, Double value, Worker<Double, Double, Double> w) {
		super(id, value, w);
		w.addVertex(this);
		master.InstanceOfCombiner(new Combiner_PageRank());
		worker.InstanceOfAggregator(new Aggregator_PageRank(this));
	}

	@Override
	public void compute(Queue<Double> messageQueueS) {
		@SuppressWarnings("rawtypes")
		Statisticians s = master.getStatisticians();
		if(getSuperStep() >= 1) {
			double sum = 0;
			for(Double m : messageQueueS) {
				sum += m;
			}
			setVertexValue(0.15 / s.getNumsVertex() + 0.85 * sum);
		}
		
		if(getSuperStep() < 20) {
			int n = getEdge().size();
			for(Map.Entry<Integer,Double> entry : getEdge().entrySet()) {
				SendMessageTo(entry.getKey(), getVertexValue()/n);
			}
		}else {
			VoteToHalt();
		}
	}

	@Override
	public void compute(Queue<Double> messageQueueS, Aggregator<Double, Double, Double> aggregator) {
		if(getSuperStep() == 1) {
			aggregator.getContent().add((int)aggregator.report());
		}
		@SuppressWarnings("rawtypes")
		Statisticians s = master.getStatisticians();
		if(getSuperStep() >= 1) {
			double sum = 0;
			for(Double m : messageQueueS) {
				sum += m;
			}
			setVertexValue(0.15 / s.getNumsVertex() + 0.85 * sum);
		}
		
		if(getSuperStep() < 20) {
			int n = getEdge().size();
			for(Map.Entry<Integer,Double> entry : getEdge().entrySet()) {
				SendMessageTo(entry.getKey(), getVertexValue()/n);
			}
		}else {
			VoteToHalt();
		}
	}

}
