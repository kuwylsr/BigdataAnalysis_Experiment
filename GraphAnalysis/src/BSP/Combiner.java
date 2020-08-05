package BSP;

import java.util.Queue;

public abstract class Combiner<V,E,I> {

	public abstract I combine(Queue<I> messageQueueS);
}
