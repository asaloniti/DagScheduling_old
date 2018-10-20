package utils;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.jgrapht.graph.DefaultWeightedEdge;
import org.jgrapht.graph.SimpleDirectedWeightedGraph;
import model.Problem;
import model.Task;

public class DAGImporter {

	private String filename;
	private Problem aProblem;

	public DAGImporter(String fn) {
		this.filename = fn;
		aProblem = new Problem();
	}

	public void parseFile() {
		try {
			BufferedReader in = new BufferedReader(new FileReader(filename));
			String str;
			String status = "";
			while ((str = in.readLine()) != null) {
				if (str.length()==0)
					continue;
				if (str.charAt(0) == '#') {
					continue;
				}
				if (str.indexOf("Processors") != -1) {
					String[] s = str.split(":");
					aProblem.setNumberOfProcessors(Integer.parseInt(s[1].trim()));
				} else if (str.indexOf("Tasks") != -1) {
					status = "Tasks";
				} else if (str.indexOf("Dependencies") != -1) {
					status = "Dependencies";
				} else {
					if (status.equalsIgnoreCase("Tasks")) {
						processTask(str);
					} else if (status.equalsIgnoreCase("Dependencies")) {
						processDepencency(str);
					} else {
						throw new IllegalStateException("wrong input");
					}
				}
			}
			in.close();

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		ensureSingleSourceNode();
		ensureSingleSinkNode();
		SimpleDirectedWeightedGraph<String, DefaultWeightedEdge> aGraph = constructGraph();
	}

	// add a virtual sink node in case multiple sink nodes exist
	private void ensureSingleSinkNode() {
		List<String> sinkNodes = new ArrayList<String>();
		for (Task aTask : aProblem.getTasks()) {
			if (aTask.getDependentTasks().isEmpty())
				sinkNodes.add(aTask.getId());
		}

		if (sinkNodes.size() > 1) {
			System.out.printf("Multiple sink nodes (%s) exist. A virtual node will be added!\n", sinkNodes);
			int[] demand_virtual_sink_node = new int[aProblem.getNumberOfProcessors()];
			for (int i = 0; i < demand_virtual_sink_node.length; i++) {
				demand_virtual_sink_node[i] = 10;
			}
			Task virtualTask = new Task("vt", demand_virtual_sink_node);
			aProblem.addTask(virtualTask);
			for (String t_id1 : sinkNodes) {
				Task task1 = aProblem.getTask(t_id1);
				task1.addDependentTask(aProblem, virtualTask.getId(), 1);
			}
		} else {
			System.out.println("Single sink node");
		}
	}

	// add a source node in case multiple source nodes exist
	private void ensureSingleSourceNode() {
		List<String> sourceNodes = new ArrayList<String>();
		for (Task aTask : aProblem.getTasks()) {
			if (aTask.getDependedOnTasks().isEmpty()) {
				sourceNodes.add(aTask.getId());
			}
		}
		if (sourceNodes.size() > 1) {
			System.out.printf("Multiple source nodes (%s) exist. A virtual node will be added!\n", sourceNodes);
			int[] demand_virtual_source_node = new int[aProblem.getNumberOfProcessors()];
			for (int i = 0; i < demand_virtual_source_node.length; i++) {
				demand_virtual_source_node[i] = 20;
			}
			Task virtualTask = new Task("vt_so", demand_virtual_source_node);
			aProblem.addTask(virtualTask);
			for (String t_id1 : sourceNodes) {
				virtualTask.addDependentTask(aProblem, t_id1, 1);
			}
		} else {
			System.out.printf("Single source node.\n");
		}
	}

	private void processDepencency(String str) {
		String[] s = str.split("\\s+");
		// s[0] = Integer.toString(Integer.parseInt(s[0]) - 1);
		// s[1] = Integer.toString(Integer.parseInt(s[1]) - 1);
		String t_id1 = s[0];
		String t_id2 = s[1];
		int weight = Integer.parseInt(s[2]);
		Task task1 = aProblem.getTask(t_id1);
		task1.addDependentTask(aProblem, t_id2, weight);
	}

	private void processTask(String str) {
		String s[] = str.split("\\s+");
		int[] re = new int[s.length - 1];
		for (int i = 0; i < s.length - 1; i++) {
			re[i] = Integer.parseInt(s[i + 1]);
		}
		// s[0] = Integer.toString(Integer.parseInt(s[0]) - 1);
		Task aTask = new Task(s[0], re);
		aProblem.addTask(aTask);
	}

	public String getFilename() {
		return filename;
	}

	public void setFilename(String filename) {
		this.filename = filename;
	}

	public Problem getProblem() {
		return aProblem;
	}

	public void setProblem(Problem problem) {
		this.aProblem = problem;
	}

	public SimpleDirectedWeightedGraph<String, DefaultWeightedEdge> constructGraph() {
		SimpleDirectedWeightedGraph<String, DefaultWeightedEdge> aGraph = new SimpleDirectedWeightedGraph<String, DefaultWeightedEdge>(
				DefaultWeightedEdge.class);
		for (Task aTask : aProblem.getTasks()) {
			aGraph.addVertex(String.valueOf(aTask.getId()));
		}
		for (Task aTask : aProblem.getTasks()) {
			for (String b_task_id : aTask.getDependentTasks()) {
				DefaultWeightedEdge e1 = aGraph.addEdge(String.valueOf(aTask.getId()), String.valueOf(b_task_id));
				int index = aTask.getDependentTasks().indexOf(b_task_id);
				aGraph.setEdgeWeight(e1, aTask.getCommunicationData().get(index));
			}
		}
		return aGraph;
	}
}
