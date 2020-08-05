package BSP;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;

public class Master<V, E, I> {

	public static int SuperStep = 0; // 当前的超级轮数
	private Worker<V, E, I>[] workerList;
	private Combiner<V, E, I> combiner; //
	private Statisticians statistics; // 统计器
	
	
	@SuppressWarnings("unchecked")
	public Master(int n) {
		workerList = new Worker[n];
		statistics = new Statisticians(n);
	}

	/**
	 * Edge-Cut的Random划分算法，划分图
	 * 
	 * @param n worker的个数
	 * @throws IOException
	 */
	public void partition(int n, String graphInPath, String graphOutPath) throws IOException {
		System.out.print("大图划分中......");
		Map<String, StringBuilder> contentMap = new HashMap<>();// 倒排索引，key为入顶点id，value为所属worker以及出顶点id
		File file_in = new File(graphInPath);
		FileInputStream fis = new FileInputStream(file_in);
		BufferedReader br = new BufferedReader(new InputStreamReader(fis));
		String line = null;
		int count = 0;
		int flag;
		int randomNum;
		while ((line = br.readLine()) != null) {
			// 过滤掉前4行的注释信息
			count++;
			if (count <= 4)
				continue;
			
			String[] vertexs = line.split("\t");
			String vertexID_In = vertexs[0];
			String vertexID_Out = vertexs[1];
			flag = 0;
			if (!contentMap.containsKey(vertexID_In)) { //入点不存在
				randomNum = (int) (Math.random() * n);
				flag = 1;
				contentMap.put(vertexID_In,
						new StringBuilder(String.valueOf(randomNum)).append("\t").append(vertexID_Out).append(","));
				//更新统计器
				statistics.NumsVertex++;
				statistics.NumsVertexOfWorker[randomNum] += 1; 
				statistics.NumsEdgeOfWorker[randomNum] += 1;
			} 
			if(!contentMap.containsKey(vertexID_Out)) { //出点不存在
				randomNum = (int) (Math.random() * n);
				contentMap.put(vertexID_Out,
						new StringBuilder(String.valueOf(randomNum)).append("\t"));
				//更新统计器
				statistics.NumsVertex++;
				statistics.NumsVertexOfWorker[randomNum] += 1;
			}
			if(flag == 0) { //入点和出点都存在
				contentMap.get(vertexID_In).append(vertexID_Out).append(",");
				randomNum = Integer.parseInt(contentMap.get(vertexID_In).toString().split("\t")[0]);
				statistics.NumsEdgeOfWorker[randomNum] += 1;//更新统计器
			}
			statistics.NumsEdge++;
		}
		File file = new File(graphOutPath);
		FileWriter fw = new FileWriter(file.getAbsoluteFile());
		BufferedWriter bw = new BufferedWriter(fw);
		bw.write(mapToString(contentMap));
		bw.close();
		br.close();
		System.out.println("划分完成！");
	}

	public String mapToString(Map<String, StringBuilder> contentMap) {
		StringBuilder content = new StringBuilder();
		content.append("#图划分的结果：").append("\n").append("#第一列为顶点ID").append("\n").append("#第二列为顶点所属Worker的ID").append("\n")
				.append("#第三列为顶点的出边顶点ID").append("\n");
		for (Map.Entry<String, StringBuilder> entry : contentMap.entrySet()) {
			content.append(entry.getKey()).append("\t").append(entry.getValue().toString()).append("\n");
		}
		return content.toString();
	}

	/**
	 * 计算引擎
	 */
	public void run() {
		int flag = 0;
		long startTime;
		long endTime;
		refreshState();// 更新worker以及worker中的顶点（消息队列和状态）
		System.out.println("正在执行超级轮：");
		while (flag == 0) {
			System.out.print("\t"+"S"+SuperStep+".....");
			for (Worker<V, E, I> w : workerList) {
				startTime = System.currentTimeMillis();    //获取开始时间
				w.run();
				endTime = System.currentTimeMillis();    //获取结束时间
				statistics.getTimesOfWorker()[w.getWorkerID()].add(endTime - startTime); //更新统计器
			}
			refreshState();// 更新worker以及worker中的顶点（消息队列和状态）
			// 判断所有worker中是否还存在active的节点
			if (judge())
				flag = 0;
			else
				flag = 1;
			SuperStep++; //更新超级轮数
			//if(SuperStep % 10 > 0) 
			System.out.println("执行完成！");
		}
		System.out.println();
	}

	/**
	 * 更新所有状态，包括：worker的消息队列；顶点的活跃状态和消息队列
	 */
	public void refreshState() {
		for (Worker<V, E, I> w : this.workerList) {
			refreshWorker(w);
			for (Map.Entry<Integer, Vertex<V, E, I>> entry : w.getMap().entrySet()) {
				Vertex<V, E, I> v = entry.getValue();
			//	w.addMessageToV(v); //不使用combiner
				w.addMessageToV(v,combiner); //使用combiner
				refreshVertex(v);
			}
		}
	}

	/**
	 * 更新worker的消息队列
	 * 
	 * @param w
	 */
	public void refreshWorker(Worker<V, E, I> w) {
		Queue<MessageToW<I>> q = w.getWmessageQueueS();
		q.clear();
		Queue<MessageToW<I>> qPlus = w.getWmessageQueueSplus();
		while (qPlus.peek() != null) { // 将下一超级轮的消息队列传递给当前超级轮
			q.add(qPlus.poll());
		}
		qPlus.clear();
		
	}

	/**
	 * 更新顶点的活跃状态以及消息队列
	 * 
	 * @param v
	 */
	public void refreshVertex(Vertex<V, E, I> v) {
		Queue<I> q = v.getVmessageQueueS();
		q.clear();
		Queue<I> qPlus = v.getVmessageQueueSplus();	
		//更新顶点状态
		if (qPlus.peek() != null)
			v.setFlagSplus(true); // 若顶点下一轮的消息队列不为空，则将顶点的此轮状态设置为active
		v.setFlagS(v.getFlagSplus()); // 更新顶点的状态
		v.setFlagSplus(false);
		//更新顶点的消息队列
		while (qPlus.peek() != null) { // 将下一超级轮的消息队列传递给当前超级轮
			q.add(qPlus.poll());
		}
		qPlus.clear();
		statistics.getTrafficOfWorker()[v.getBelongWorker().getWorkerID()] += q.size();//更新统计器
	}

	public boolean judge() {
		for (Worker<V, E, I> w : this.workerList) {
			// 判断该worker的所有顶点在下一个超级轮中的状态是否存在活跃节点
			for (Map.Entry<Integer, Vertex<V, E, I>> entry : w.getMap().entrySet()) {
				Vertex<V, E, I> v = entry.getValue();
				if (v.getFlagS()) {
					return true;
				}
			}
		}
		return false;
	}

	public Object getSumValue(int index, List<Object> content) {
		int len = content.size();
		if(2 * index + 2 < len - 1) {
			return (int)getSumValue(2 * index + 1, content) + (int)getSumValue(2 * index + 2, content);
		}else {
			return (int)content.get(index);
		}
	}
	
	public Statisticians getStatisticians() {
		return this.statistics;
	}

	public void addWorker(Worker<V, E, I> w, int id) {
		workerList[id] = w;
	}

	public Worker<V, E, I>[] getWorkerList() {
		return this.workerList;
	}
	
	public void InstanceOfCombiner(Combiner<V, E, I> combiner){
		this.combiner = combiner;
	}

	// 统计器
	public class Statisticians {
		private int NumsVertex; //图的所有顶点数目
		private int NumsEdge; //图的所有边的数目
		private int[] NumsVertexOfWorker; // 每个Worker存储顶点的数量
		private int[] NumsEdgeOfWorker; // 每个Worker存储边的数量
		private ArrayList<Long>[] TimesOfWorker; // 每个Worker在各个超级轮中的计算时间
		private int[] TrafficOfWorker; // 每个Worker的通信量

		@SuppressWarnings("unchecked")
		public Statisticians(int n) {
			NumsVertex = 0;
			NumsEdge = 0;
			NumsVertexOfWorker = new int[n];
			NumsEdgeOfWorker = new int[n];
			TimesOfWorker = new ArrayList[n];
			TrafficOfWorker = new int[n];
			for(int i = 0 ; i< n ;i++) {
				TimesOfWorker[i] = new ArrayList<>();
			}
		}

		public int[] getNumsVertexOfWorker() {
			return this.NumsVertexOfWorker;
		}

		public int[] getNumsEdgeOfWorker() {
			return this.NumsEdgeOfWorker;
		}

		public ArrayList<Long>[] getTimesOfWorker() {
			return this.TimesOfWorker;
		}

		public int[] getTrafficOfWorker() {
			return this.TrafficOfWorker;
		}
		
		public int getNumsVertex() {
			return this.NumsVertex;
		}
		
		public int getNumsEdge() {
			return this.NumsEdge;
		}
		
		@Override
		public String toString() {
			String showContent = "\n======统计器的统计信息======\n";
			for(int i = 0 ; i<NumsEdgeOfWorker.length;i++) {
				showContent = showContent + "Worker id: " + i + "\n";
				showContent = showContent + "存储顶点的数量：" + NumsVertexOfWorker[i]+ "\n";
				showContent = showContent + "存储边的数量：" + NumsEdgeOfWorker[i]+ "\n";
				showContent = showContent + "通讯量：" + NumsEdgeOfWorker[i]+ "\n";
				showContent = showContent + "各个超级轮中的计算时间：" +"(超级轮数："+SuperStep+")" + "\n";
				for(int j = 0 ;j < TimesOfWorker[i].size() ;j++) {
					showContent = showContent + "\t" +"超级轮S"+j+"：" +TimesOfWorker[i].get(j)+"毫秒"+ "\n";
				}
				showContent = showContent + "\n";
			}
			showContent = showContent + "==============================";
			return showContent;
		}
	}
}
