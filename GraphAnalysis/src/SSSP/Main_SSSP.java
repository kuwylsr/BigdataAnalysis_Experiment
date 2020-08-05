package SSSP;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;

import BSP.Master;
import BSP.Vertex;
import BSP.Worker;

public class Main_SSSP {

	static int n = 3;// 大图划分的个数0

	public static void loadGraph(Master<Integer, Integer, Integer> master, String graphPath) throws IOException {
		System.out.print("大图加载中......");
		Worker<Integer, Integer, Integer>[] workerList = master.getWorkerList();
		for (int i = 0; i < n; i++) {
			workerList[i] = new Worker<>(master, i);
		}
		
		File file_in = new File(graphPath);
		FileInputStream fis = new FileInputStream(file_in);
		BufferedReader br = new BufferedReader(new InputStreamReader(fis));
		String line = null;
		int count = 0;
		while ((line = br.readLine()) != null) {
			// 过滤掉前4行的注释信息
			count++;
			if (count <= 4)
				continue;
			String[] content = line.split("\t");
			//顶点的值初始化为int最大值
			Vertex<Integer, Integer, Integer> v = new Vertex_SSSP(Integer.parseInt(content[0]), Integer.MAX_VALUE,
					workerList[Integer.parseInt(content[1])]);
			if(content.length>2) {
				String[] destIDs = content[2].split(",");
				for(String destID : destIDs) {
					v.addEdge(Integer.parseInt(destID), 1); //边的权值默认为1
				}
			}
		}
		br.close();
		System.out.println("加载完成！");
	}

	public static void main(String[] args) throws IOException {
//		String inputGraphPath = "src/GraphFile/web-Google.txt";
//		String outputGraphPath = "src/GraphFile/web-GoogleCut.txt";
		String inputGraphPath = "src/GraphFile/testGraph.txt";
		String outputGraphPath = "src/GraphFile/testGraphCut.txt";
		Master<Integer, Integer, Integer> master = new Master<>(n);
		master.partition(n, inputGraphPath, outputGraphPath);
		loadGraph(master, outputGraphPath);
		
		master.run();
		System.out.println("最短路径求解结果："+"（源点为顶点0）");
		for(Worker<Integer, Integer, Integer> w :master.getWorkerList()) {
			for(Map.Entry<Integer, Vertex<Integer, Integer, Integer>> entry : w.getMap().entrySet()) {
				Vertex<Integer, Integer, Integer> v = entry.getValue();
				System.out.println("到达顶点"+v.getVertexID() + "的最短距离为： " + v.getVertexValue());
			}
		}
		System.out.println(master.getStatisticians().toString());
	}

}
