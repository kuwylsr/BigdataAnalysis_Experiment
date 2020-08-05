package SSSP;

import java.util.Queue;

import BSP.Combiner;

public class Combiner_SSSP extends Combiner<Integer,Integer,Integer>{

	@Override
	public Integer combine(Queue<Integer> messageQueueS) {
		int min_dist = Integer.MAX_VALUE;
		for(Integer m : messageQueueS) {
			min_dist = Math.min(m, min_dist);
		}
		return min_dist;
	}
}
