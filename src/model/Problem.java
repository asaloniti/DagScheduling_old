package model;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jgrapht.alg.DijkstraShortestPath;
import org.jgrapht.ext.DOTExporter;
import org.jgrapht.ext.GmlExporter;
import org.jgrapht.graph.DefaultWeightedEdge;
import org.jgrapht.graph.SimpleDirectedWeightedGraph;
import org.jgrapht.traverse.BreadthFirstIterator;
import org.jgrapht.traverse.TopologicalOrderIterator;

public class Problem {


	private int numberOfProcessors;
	private List<Task> tasksList; // seems to be redundant
	private Map<String, Task> tasksMap;
	private SimpleDirectedWeightedGraph<String, DefaultWeightedEdge> fullGraph;
	private Map<String, List<String>> notConnectedAllTasks = new HashMap<String, List<String>>();
	FullLegalityChecker lc;
	private List<String> topologicallyOrderedList;

	private int virtualSourceNodeExecCost = 20;
	private int virtualSourceNodeComCost = 1;
	private int virtualSinkNodeExecCost = 20;
	private int virtualSinkNodeComCost = 1;

	// --------------
	public int getVirtualSourceNodeExecCost() {
		return virtualSourceNodeExecCost;
	}

	public int getVirtualSourceNodeComCost() {
		return virtualSourceNodeComCost;
	}

	public int getVirtualSinkNodeExecCost() {
		return virtualSinkNodeExecCost;
	}

	public int getVirtualSinkNodeComCost() {
		return virtualSinkNodeComCost;
	}

	// --------------------


	public Problem() {
		tasksList = new ArrayList<Task>();
		tasksMap = new HashMap<String, Task>();
		lc = new FullLegalityChecker(this);
	}

	public void addTask(Task aTask) {
		tasksMap.put(aTask.getId(), aTask);
		tasksList.add(aTask);
	}

	public int getNumberOfProcessors() {
		return numberOfProcessors;
	}

	public void setNumberOfProcessors(int numberOfProcessors) {
		this.numberOfProcessors = numberOfProcessors;
	}

	public List<Task> getTasks() {
		return tasksList;
	}

	public List<Task> getTasksOfLevel(int k) {
		List<Task> taList = new ArrayList<Task>();
		for (Task ta : tasksList) {
			if (ta.getLevel() == k) {
				taList.add(ta);
			}
		}
		return taList;
	}


	public void printProblemDetails() {
		System.out.printf("Processors = %d\n", numberOfProcessors);
		for (Task aTask : tasksList) {
			System.out.println(aTask.toString());
		}
	}

	public Task getTask(String t_id1) {
		Task aTask = tasksMap.get(t_id1);
		if (aTask == null) 
			throw new IllegalStateException("Task id does not exist " + t_id1);
		 else 
			return aTask;
	}

	public Task getTask(int t_ind) {
		if (t_ind < tasksList.size()) 
			return tasksList.get(t_ind);
		throw new IllegalStateException("Task index does not exist " + t_ind);
	}

	public SimpleDirectedWeightedGraph<String, DefaultWeightedEdge> getFullGraph() {
		return fullGraph;
	}

	public boolean isFeasible(DAGSolution aSolution) {
		return lc.isFeasible(aSolution);
	}

	public int getTaskIndex(String task_id) {
		for (int i = 0; i < tasksList.size(); i++) {
			if (tasksList.get(i).getId().equalsIgnoreCase(task_id)) {
				return i;
			}
		}
		return -1;
	}

	public void exportFullGraphToGml(String fn) {
		exportGraphToGml(this.getFullGraph(), fn);
	}

	public void exportGraphToGml(SimpleDirectedWeightedGraph<String, DefaultWeightedEdge> graph, String fn) {
		java.io.Writer writer;
		GmlExporter<String, DefaultWeightedEdge> ge = new GmlExporter<String, DefaultWeightedEdge>();
		ge.setPrintLabels(GmlExporter.PRINT_EDGE_VERTEX_LABELS);
		try {
			writer = new FileWriter(fn);
			ge.export(writer, graph);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void exportFullGraphToDot(String fn) {
		exportGraphToDot(this.getFullGraph(), fn);
	}

	public void exportGraphToDot(SimpleDirectedWeightedGraph<String, DefaultWeightedEdge> graph, String fn) {
		java.io.Writer writer;
		DOTExporter<String, DefaultWeightedEdge> de = new DOTExporter<String, DefaultWeightedEdge>();
		try {
			writer = new FileWriter(fn);
			de.export(writer, graph);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void buildFullGraph() {
		fullGraph = new SimpleDirectedWeightedGraph<String, DefaultWeightedEdge>(DefaultWeightedEdge.class);
		// vertex
		for (Task aTask : getTasks()) {
			fullGraph.addVertex(String.valueOf(aTask.getId()));
		}
		// edge
		for (Task aTask : getTasks()) {
			for (String b_task_id : aTask.getDependentTasks()) {
				DefaultWeightedEdge e1 = fullGraph.addEdge(String.valueOf(aTask.getId()), String.valueOf(b_task_id));
				int index = aTask.getDependentTasks().indexOf(b_task_id);
				fullGraph.setEdgeWeight(e1, aTask.getCommunicationData().get(index));
			}
		}
		setMapOfNotConnectedTasks();
	}

	// built a graph from level1 to level2
	public SimpleDirectedWeightedGraph<String, DefaultWeightedEdge> builtGraphBetweenLevels(int level1, int level2) {
		SimpleDirectedWeightedGraph<String, DefaultWeightedEdge> aGraph = new SimpleDirectedWeightedGraph<String, DefaultWeightedEdge>(
				DefaultWeightedEdge.class);
		// vertex
		for (int i = level1; i <= level2; i++) {
			for (Task ta : getTasksOfLevel(i)) {
				aGraph.addVertex(String.valueOf(ta.getId()));
			}
		}
		// virtual sink node
		String vt = "vt_" + level1 + "_" + level2;
		aGraph.addVertex(vt);

		// edge
		for (int i = level1; i < level2; i++) {
			for (Task ta : getTasksOfLevel(i)) {
				for (String b_task_id : ta.getDependentTasks()) {
					Task b_ta = getTask(b_task_id);

					String b_vertex;
					int w;
					if (b_ta.getLevel() <= level2) {
						b_vertex = b_task_id;
						int index = ta.getDependentTasks().indexOf(b_task_id);
						w = ta.getCommunicationData().get(index);
					} else {
						b_vertex = vt;
						w = virtualSinkNodeComCost;
					}

					DefaultWeightedEdge e1 = aGraph.addEdge(String.valueOf(ta.getId()), b_vertex);

					aGraph.setEdgeWeight(e1, w);
				}
			}
		}
		// last level to virtual sink node
		for (Task ta : getTasksOfLevel(level2)) {
			int w = virtualSinkNodeComCost;
			DefaultWeightedEdge e1 = aGraph.addEdge(String.valueOf(ta.getId()), vt);
			aGraph.setEdgeWeight(e1, w);
		}

		return aGraph;
	}

	// built a graph from level
	public List<SimpleDirectedWeightedGraph<String, DefaultWeightedEdge>> builtGraphsFromLevel(int level,
			int nodesPerProc) {

		List<SimpleDirectedWeightedGraph<String, DefaultWeightedEdge>> glist = new ArrayList<SimpleDirectedWeightedGraph<String, DefaultWeightedEdge>>();

		int num = nodesPerProc * numberOfProcessors;
		int num_all = getTasksOfLevel(level).size();
		int gnum;
		if (num_all <= num) {
			gnum = 1;
		} else if (num_all % num <= num / 2) {
			gnum = num_all / num;
		} else {
			gnum = num_all / num + 1;
		}
		List<Task> tlist = getTasksOfLevel(level);

		int n = 0;
		for (int k = 0; k < gnum; k++) {

			SimpleDirectedWeightedGraph<String, DefaultWeightedEdge> aGraph = new SimpleDirectedWeightedGraph<String, DefaultWeightedEdge>(
					DefaultWeightedEdge.class);

			// nodes to graph
			int k1 = k * num;
			int k2 = (k + 1) * num;
			if (k == gnum - 1) {
				k2 = tlist.size();
			}
			List<Task> cur_list = new ArrayList<Task>();
			for (int i = k1; i < k2; i++) {
				Task ta = tlist.get(i);
				cur_list.add(ta);
				aGraph.addVertex(String.valueOf(ta.getId()));
			}
			n += cur_list.size();
			// virtual sink node at the end
			String vt = "vt_" + level;
			aGraph.addVertex(vt);

			// edges to virtual sink node
			for (Task ta : cur_list) {
				int w = virtualSinkNodeComCost;
				DefaultWeightedEdge e1 = aGraph.addEdge(String.valueOf(ta.getId()), vt);
				aGraph.setEdgeWeight(e1, w);
			}
			glist.add(aGraph);
		}
		if (num_all != n) {
			System.out.println("nodes missing..");
			System.exit(-1);
		}
		return glist;
	}

	private void setMapOfNotConnectedTasks() {
		TopologicalOrderIterator<String, DefaultWeightedEdge> toi = new TopologicalOrderIterator<String, DefaultWeightedEdge>(
				fullGraph);

		List<String> nodeList = new ArrayList<String>();
		List<List<String>> notConnectedAll = new ArrayList<List<String>>();
		while (toi.hasNext()) {
			String node = (String) toi.next();
			nodeList.add(node);
			notConnectedAll.add(new ArrayList<String>());
		}
		for (int n1 = 0; n1 < nodeList.size(); n1++) {
			String node1 = nodeList.get(n1);
			notConnectedAll.get(n1).addAll(getAllConnectedNodesFor(node1));
			notConnectedAllTasks.put(node1, notConnectedAll.get(n1));
			// System.out.printf("Node %s (all) ==> %s\n", node1,
		}
	}

	private List<String> getAllConnectedNodesFor(String node1) {
		List<String> connectedNodesList = new ArrayList<String>();
		List<String> tempList1 = new ArrayList<String>();
		for (DefaultWeightedEdge dwe : fullGraph.outgoingEdgesOf(node1)) {
			tempList1.add(fullGraph.getEdgeTarget(dwe));
		}
		while (!tempList1.isEmpty()) {
			String node2 = tempList1.remove(0);
			for (DefaultWeightedEdge dwe : fullGraph.outgoingEdgesOf(node2)) {
				String node3 = fullGraph.getEdgeTarget(dwe);
				if (!tempList1.contains(node3))
					tempList1.add(node3);
			}
			connectedNodesList.add(node2);
		}
		List<String> tempList2 = new ArrayList<String>();
		for (DefaultWeightedEdge dwe : fullGraph.incomingEdgesOf(node1)) {
			tempList2.add(fullGraph.getEdgeSource(dwe));
		}
		while (!tempList2.isEmpty()) {
			String node2 = tempList2.remove(0);
			for (DefaultWeightedEdge dwe : fullGraph.incomingEdgesOf(node2)) {
				String node3 = fullGraph.getEdgeSource(dwe);
				if (!tempList2.contains(node3))
					tempList2.add(node3);
			}
			connectedNodesList.add(node2);
		}
		List<String> notConnectedNodesList = new ArrayList<String>();
		for (Task aTask : tasksList) {
			if (!connectedNodesList.contains(aTask.getId()) && !aTask.getId().equalsIgnoreCase(node1)) {
				notConnectedNodesList.add(aTask.getId());
			}
		}

		return notConnectedNodesList;
	}

	// Uses pairwise Dijkstra Shortest Path
	private void setMapOfNotConnectedTasks2() {
		TopologicalOrderIterator<String, DefaultWeightedEdge> toi = new TopologicalOrderIterator<String, DefaultWeightedEdge>(
				fullGraph);

		List<String> nodeList = new ArrayList<String>();
		List<List<String>> notConnectedAll = new ArrayList<List<String>>();
		while (toi.hasNext()) {
			String node = (String) toi.next();
			nodeList.add(node);
			notConnectedAll.add(new ArrayList<String>());
		}

		for (int n1 = 0; n1 < nodeList.size(); n1++) {
			String node1 = nodeList.get(n1);

			for (int n2 = n1 + 1; n2 < nodeList.size(); n2++) {
				String node2 = nodeList.get(n2);
				List<DefaultWeightedEdge> res = DijkstraShortestPath.findPathBetween(fullGraph, node1, node2);
				if (res == null) {
					notConnectedAll.get(n1).add(node2);
					notConnectedAll.get(n2).add(node1);
				}
			}
		}

		for (int n1 = 0; n1 < nodeList.size(); n1++) {
			String node1 = nodeList.get(n1);
			notConnectedAllTasks.put(node1, notConnectedAll.get(n1));
			// System.out.printf("Node %s ==> %s\n", node1,
			System.out.printf("Node %s (all) ==> %s\n", node1, notConnectedAll.get(n1));
		}
	}

	public List<String> getNotConnectedAllTasks(String node) {
		return notConnectedAllTasks.get(node);
	}

	/**
	 * 10/02/2013 - CG
	 * 
	 * compute upward ranks compute static upward ranks compute downward ranks
	 */
	public void computeRanks() {
		topologicallyOrderedList = getTopologicalOrderList();
		// compute upward rank
		Collections.reverse(topologicallyOrderedList);
		for (String sourceNode : topologicallyOrderedList) {
			Task sourceTask = getTask(sourceNode);
			double max = Double.MIN_VALUE;
			boolean isSinkNode = true;
			for (DefaultWeightedEdge dwe : fullGraph.outgoingEdgesOf(sourceNode)) {
				isSinkNode = false;
				String targetNode = fullGraph.getEdgeTarget(dwe);
				Task targetTask = getTask(targetNode);
				double v = fullGraph.getEdgeWeight(dwe) + targetTask.ranku;
				if (v > max) {
					max = v;
				}
			}
			if (isSinkNode) {
				sourceTask.ranku = sourceTask.getAverageDemandPerResource();
			} else {
				sourceTask.ranku = sourceTask.getAverageDemandPerResource() + max;
			}
		}

		// compute static upward rank
		for (String sourceNode : topologicallyOrderedList) {
			Task sourceTask = getTask(sourceNode);
			double max = Double.MIN_VALUE;
			boolean isSinkNode = true;
			for (DefaultWeightedEdge dwe : fullGraph.outgoingEdgesOf(sourceNode)) {
				isSinkNode = false;
				String targetNode = fullGraph.getEdgeTarget(dwe);
				Task targetTask = getTask(targetNode);
				double v = targetTask.ranksu;
				if (v > max) {
					max = v;
				}
			}
			if (isSinkNode) {
				sourceTask.ranksu = sourceTask.getAverageDemandPerResource();
			} else {
				sourceTask.ranksu = sourceTask.getAverageDemandPerResource() + max;
			}
		}

		// compute downward rank
		Collections.reverse(topologicallyOrderedList);
		for (String targetNode : topologicallyOrderedList) {
			Task targetTask = getTask(targetNode);
			double max = Double.MIN_VALUE;
			boolean isEntryNode = true;
			for (DefaultWeightedEdge dwe : fullGraph.incomingEdgesOf(targetNode)) {
				isEntryNode = false;
				String sourceNode = fullGraph.getEdgeSource(dwe);
				Task sourceTask = getTask(sourceNode);
				double v = sourceTask.rankd + sourceTask.getAverageDemandPerResource() + fullGraph.getEdgeWeight(dwe);
				if (v > max) {
					max = v;
				}
			}
			if (isEntryNode) {
				targetTask.rankd = 0.0;
			} else {
				targetTask.rankd = max;
			}
		}
	}

	public void computeLevels() {
		for (String sourceNode : topologicallyOrderedList) {
			Task sourceTask = getTask(sourceNode);
			if (fullGraph.incomingEdgesOf(sourceNode).isEmpty())
				sourceTask.setLevel(0);
			for (DefaultWeightedEdge dwe : fullGraph.outgoingEdgesOf(sourceNode)) {
				String targetNode = fullGraph.getEdgeTarget(dwe);
				Task targetTask = getTask(targetNode);
				if (targetTask.getLevel() < sourceTask.getLevel() + 1) {
					targetTask.setLevel(sourceTask.getLevel() + 1);
				}
			}

		}
	}

	/*
	 * Topological order: A topological order of a DAG G=(P,E) is a linear
	 * ordering of all its vertices such that if E contains an edge e(uv) then u
	 * appears before v in the orderings.
	 */
	public List<String> getTopologicalOrderList() {
		List<String> topoList = new ArrayList<String>();
		TopologicalOrderIterator<String, DefaultWeightedEdge> toi = new TopologicalOrderIterator<String, DefaultWeightedEdge>(
				getFullGraph());
		while (toi.hasNext()) {
			String node = (String) toi.next();
			topoList.add(node);
		}
		return topoList;
	}

	public int getNumOfLevels() {
		int max = 0;
		for (Task aTask : tasksList) {
			if (aTask.getLevel() > max)
				max = aTask.getLevel();
		}
		return max + 1;
	}

	public String getSingleSourceNode() {
		List<String> aList = new ArrayList<String>();
		for (Task aTask : tasksList) {
			if (aTask.dependedOnTasks.isEmpty())
				aList.add(aTask.getId());
		}
		if (aList.size() != 1) {
			throw new IllegalStateException("No single source node");
		} else {
			return aList.get(0);
		}
	}

}
