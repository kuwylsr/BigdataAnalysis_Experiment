package PageRank;

import java.util.Queue;

import BSP.Combiner;

public class Combiner_PageRank extends Combiner<Double, Double, Double>{

	@Override
	public Double combine(Queue<Double> messageQueueS) {
		double sum = 0.0;
		for(Double m : messageQueueS) {
			sum += m;
		}
		return sum;
	}
}
