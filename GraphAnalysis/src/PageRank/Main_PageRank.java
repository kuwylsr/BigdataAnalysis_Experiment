package PageRank;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;

import BSP.Master;
import BSP.Vertex;
import BSP.Worker;

public class Main_PageRank {

	static int n = 3;// 大图划分的个数

	public static void loadGraph(Master<Double, Double, Double> master, String graphPath) throws IOException {
		System.out.print("大图加载中......");
		Worker<Double, Double, Double>[] workerList = master.getWorkerList();
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
			//顶点的权值初始化为 (1/顶点总数)
			Vertex<Double, Double, Double> v = new Vertex_PageRank(Integer.parseInt(content[0]), 1.0/master.getStatisticians().getNumsVertex(),
					workerList[Integer.parseInt(content[1])]);
			if(content.length>2) {
				String[] destIDs = content[2].split(",");
				for(String destID : destIDs) {
					v.addEdge(Integer.parseInt(destID), 1.0); //边的权值默认为1
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
		Master<Double, Double, Double> master = new Master<>(n);
		master.partition(n, inputGraphPath, outputGraphPath);
		loadGraph(master, outputGraphPath);
		
		master.run();
		System.out.println("PageRank的结果：");
		for(Worker<Double, Double, Double> w :master.getWorkerList()) {
			for(Map.Entry<Integer, Vertex<Double, Double, Double>> entry : w.getMap().entrySet()) {
				Vertex<Double, Double, Double> v = entry.getValue();
				System.out.println("顶点"+v.getVertexID() + "的PageRank值为： " + v.getVertexValue());
			}
		}
		System.out.println(master.getStatisticians().toString());
	}
}
