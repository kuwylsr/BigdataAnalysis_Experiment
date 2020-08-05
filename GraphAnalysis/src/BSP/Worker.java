package BSP;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

public class Worker<V,E,I> {

	private Master<V,E,I> BelongMaster; //worker所属的master节点
	private int WorkerID;
	private Map<Integer, Vertex<V,E,I>> map = new HashMap<>(); //存储顶点ID到顶点内容的映射
	private Queue<MessageToW<I>> WmessageQueueS = new LinkedList<>(); //当前超级轮的消息队列
	private Queue<MessageToW<I>> WmessageQueueSplus = new LinkedList<>(); //下一超级轮的消息队列
	private Aggregator<V, E, I> aggregator; //每个worker中都有一个聚集器
	
	
	public Worker(Master<V,E,I> m, int id){
		this.BelongMaster = m;
		this.WorkerID = id;
		m.addWorker(this,id);
	}

	public void run() {
		for(Map.Entry<Integer, Vertex<V,E,I>> entry: this.map.entrySet()) {
			Vertex<V, E, I> v = entry.getValue();
			//System.out.println(Master.SuperStep + "-->"+ v.getVertexID()+"||" + v.getFlagS());
			if(v.getFlagS()) { //若为活跃的顶点
				v.compute(v.getVmessageQueueS());//顶点v用当前超级轮中的消息队列进行计算
				//v.compute(v.getVmessageQueueS(),aggregator);//顶点v用当前超级轮中的消息队列进行计算
			}
//			if(Master.SuperStep == 1) {
//				aggregator.aggregate(aggregator.getContent());
//			}
		}
	}
	
	/**
	 * 将worker中的消息分配到相应的顶点中去
	 * @param v
	 */
	public void addMessageToV(Vertex<V,E,I> v) {
		for(MessageToW<I> m : WmessageQueueS) {
			int dest_id = m.getDestID();
			if(v.getVertexID() == dest_id) {
				v.getVmessageQueueSplus().add(m.getMessage());
				v.setFlagSplus(true); //将相应的顶点的下一个超级轮的状态设置为活跃
			}
			
		}
	}
	
	/**
	 * 将worker中的消息分配到相应的顶点中去（使用combiner）
	 * @param v
	 */
	public void addMessageToV(Vertex<V,E,I> v, Combiner<V, E, I> combiner) {
		Queue<I> queueM = new LinkedList<>();
		for(MessageToW<I> m : WmessageQueueS) {
			int dest_id = m.getDestID();
			if(v.getVertexID() == dest_id) {
				queueM.add(m.getMessage());
				//v.getVmessageQueueSplus().add(m.getMessage());
				//v.setFlagSplus(true); //将相应的顶点的下一个超级轮的状态设置为活跃
			}
		}
		if(queueM.peek() != null) {
			v.getVmessageQueueSplus().add(combiner.combine(queueM)); //使用combiner
		}
		queueM = null; //释放对象
	}
	
	public void InstanceOfAggregator(Aggregator<V, E, I> aggregator){
		this.aggregator = aggregator;
	}
	
	public Map<Integer,Vertex<V,E,I>> getMap(){
		return this.map;
	}
	
	public void addVertex(Vertex<V,E,I> v) {
		this.map.put(v.getVertexID(), v);
	}
	
	public int getWorkerID() {
		return this.WorkerID;
	}
	
	public Master<V,E,I> getBelongMaster() {
		return this.BelongMaster;
	}
	
	public Queue<MessageToW<I>> getWmessageQueueS(){
		return this.WmessageQueueS;
	}
	
	public Queue<MessageToW<I>> getWmessageQueueSplus(){
		return this.WmessageQueueSplus;
	}
	
}
