package BSP;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

public abstract class Vertex<V,E,I> {
	
	private int VertexID; //节点id

	private V VertexValue; //节点值
	private Worker<V,E,I> BelongWorker; //节点所属的worker节点
	private boolean flagS; //在当前超级轮的活跃状态
	private boolean flagSplus; //在下一超级轮的活跃状态
	private Queue<I> VmessageQueueS = new LinkedList<>(); //当前超级轮的消息队列
	private Queue<I> VmessageQueueSplus = new LinkedList<>(); //下一超级轮的消息队列
	private Map<Integer,E> edge = new HashMap<>(); //节点的出边信息（出边节点id，出边值）
	
	public Vertex(int id, V value, Worker<V,E,I> w){
		this.VertexID = id;
		this.VertexValue = value;
		this.BelongWorker = w;
		this.flagSplus = true; //初始化顶点为活跃状态（下一轮开始为第一轮）
		//w.addVertex(this);
	}
	
	public abstract void compute(Queue<I> messageQueueS);
	
	public abstract void compute(Queue<I> messageQueueS,Aggregator<V, E, I> aggregator);
	
	
	/**
	 * 向目标节点发送消息
	 * @param dest_vertexID
	 * @param messageValue
	 */
	public void SendMessageTo(int dest_vertexID,I messageValue) {
		//Map<Integer,Vertex<V,E,I>> map = this.BelongWorker.getMap();
		Worker<V,E,I>[] workerList = this.BelongWorker.getBelongMaster().getWorkerList();
		for(Map.Entry<Integer, E> entry : edge.entrySet()) {
			if(entry.getKey() == dest_vertexID) { //如果目标点与该点形成出边
//				if(map.containsKey(dest_vertexID)) { //如果目标点与该点在一个worker上
//					map.get(dest_vertexID).getVmessageQueueSplus().add(messageValue); //将消息存放到目标节点下一轮的消息队列中
//				}else { //若不在同一个worker内
				for(Worker<V,E,I> w : workerList) { //遍历所有worker
					if(w.getMap().containsKey(dest_vertexID)) { //找到目标点所在worker
						MessageToW<I> m = new MessageToW<I>(dest_vertexID, messageValue);
						w.getWmessageQueueSplus().add(m); //将消息存放到目标节点所在的worker的消息队列中（需要表明目的节点id）
						m = null; //释放消息对象
					}
				}
//				}
			}
		}
	}
	
	/**
	 * 将节点设置为inactive状态
	 */
	public void VoteToHalt() {
		this.flagSplus = false;
	}
	
	public int getVertexID() {
		return this.VertexID;
	}
	
	public V getVertexValue() {
		return this.VertexValue;
	}
	
	public void setVertexValue(V value) {
		this.VertexValue = value;
	}
	
	public boolean getFlagS() {
		return this.flagS;
	}
	
	public void setFlagS(boolean b) {
		this.flagS = b;
	}
	
	public boolean getFlagSplus() {
		return this.flagSplus;
	}
	
	public void setFlagSplus(boolean b) {
		this.flagSplus = b;
	}
	
	public Queue<I> getVmessageQueueS(){
		return this.VmessageQueueS;
	}
	
	public Queue<I> getVmessageQueueSplus(){
		return this.VmessageQueueSplus;
	}
	
	public Map<Integer,E> getEdge(){
		return this.edge;
	}
	
	public void addEdge(int destID, E edgeValue) {
		this.edge.put(destID, edgeValue);
	}
	
	public Worker<V,E,I> getBelongWorker(){
		return this.BelongWorker;
	}
	
	public int getSuperStep() {
		return Master.SuperStep;
	}
	
}
