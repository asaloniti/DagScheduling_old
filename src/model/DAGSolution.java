package model;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import utils.Pair;

public class DAGSolution {

	final Logger logger = LoggerFactory.getLogger(DAGSolution.class);
	private Problem aProblem;
	private HashMap<String, Integer> taskMapping;
	private HashMap<String, Integer> taskToFinishTime;
	private int[] processorReadyTime;

	public static DAGSolution loadSolution(Problem aProblem, String fn){
		DAGSolution sol = new DAGSolution(aProblem);
		BufferedReader in;
		try {
			in = new BufferedReader(new FileReader(fn));
			String str;
			while ((str = in.readLine()) != null) {
				if (str.length()==0)
					continue;
				String[] s = str.split(",");
				sol.scheduleStartTime(s[0], Integer.parseInt(s[1]), Integer.parseInt(s[2]));
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return sol;
		
	}
	
	public DAGSolution(Problem aProblem) {
		this.aProblem = aProblem;
		taskMapping = new HashMap<String, Integer>();
		taskToFinishTime = new HashMap<String, Integer>();
		for (Task aTask : aProblem.getTasks()) {
			taskMapping.put(aTask.getId(), -1);
			taskToFinishTime.put(aTask.getId(), -1);
		}
		processorReadyTime = new int[aProblem.getNumberOfProcessors()];
		for (int i = 0; i < processorReadyTime.length; i++) {
			processorReadyTime[i] = 0;
		}
	}

	public int totalTasksScheduled() {
		int n = 0;
		Iterator it = taskMapping.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry pairs = (Map.Entry) it.next();
			int proc = (Integer) pairs.getValue();
			if (proc > -1) {
				n++;
			}
		}
		return n;
	}

	public void display() {
		System.out.println("##################FINAL SCHEDULE##################");
		for (Task aTask : aProblem.getTasks()) {
			int resource_index = taskMapping.get(aTask.getId());
			int c = aTask.getDemandIn(resource_index);
			System.out.printf(
					"Task=%s Resource=%d Start Time=%d Finish Time=%d\n",
					aTask.getId(), taskMapping.get(aTask.getId()),
					taskToFinishTime.get(aTask.getId()) - c,
					taskToFinishTime.get(aTask.getId()));
		}
		System.out.println("makespan = "+ computemakespan());
		System.out.println("speedup = "+ computespeedup());
		System.out.println("efficiency = "+ computeefficiency());
		for (int p = 0; p < aProblem.getNumberOfProcessors(); p++) {
			System.out.println("utilization[" + p + "] = "+ computeputilization()[p]);	
		}
	}
	
	public int computemakespan() {
		int makespan=0;
		for (int p = 0; p < processorReadyTime.length; p++) {
			if (processorReadyTime[p]>makespan) {
				makespan = processorReadyTime[p];
			}
		}
		//System.out.println("makespan = "+ makespan);
		return makespan;
	}
	
	public double computespeedup() {
		int min = Integer.MAX_VALUE;
		for (int p = 0; p < aProblem.getNumberOfProcessors(); p++) {
			int s = 0;
			for (Task aTask : aProblem.getTasks()) {
				s = s + aTask.getDemandPerResource()[p];
			}
			if (s < min) {
				min = s;
			}
		}
		double speedup = (double) min/computemakespan();
		return speedup;
	}
	
	public double computeefficiency() {
		double efficiency = computespeedup()/aProblem.getNumberOfProcessors();
		return efficiency;
	}
	
	public double[] computeputilization () {
		double[] utilization  = new double[aProblem.getNumberOfProcessors()];
		for (int p = 0; p < aProblem.getNumberOfProcessors(); p++) {
			int s = 0;
			for (Task aTask : aProblem.getTasks()) {
				if (taskMapping.get(aTask.getId()) == p) {
					s = s + aTask.getDemandIn(p);
				}
				utilization[p] = 100* (double) s/computemakespan();
			}
		}
		return utilization;
	}
	
	public void exportMetrics(String name) {
		StringBuilder sb = new StringBuilder();
	//sb.append(String.format("makespan,speedup,efficiency,processor utilization\n"));
		sb.append(String.format("\n%d\t%f\t%f\t",computemakespan(), computespeedup(),computeefficiency()));
		for (int p = 0; p < aProblem.getNumberOfProcessors(); p++) {
			sb.append(String.format("\t%f",computeputilization()[p]));	
		}
		FileWriter out; 
		BufferedWriter bw;
		try {
			out = new FileWriter("solfiles\\" + name + ".txt", true);
			bw = new BufferedWriter(out);
			bw.write(sb.toString());
			bw.close();
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void exportToSOL(String name) {
		StringBuilder sb = new StringBuilder();
		for (Task aTask : aProblem.getTasks()) {
			//int resource_index = taskMapping.get(aTask.getId());
			//int c = aTask.getDemandIn(resource_index);
			sb.append(String.format("%s,%d,%d\n", aTask.getId(), taskMapping.get(aTask.getId()),taskToFinishTime.get(aTask.getId())));
		}
		PrintWriter out;
		try {
			out = new PrintWriter("solfiles\\" + name + ".sol");
			out.println(sb.toString());
			out.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}
	

	public int getAllTasksEarliestTimeInResource(int resource_id) {
		int max = 0;
		for (Task aTask : aProblem.getTasks()) {
			if (taskMapping.get(aTask.getId()).intValue() == resource_id) {
				if (taskToFinishTime.get(aTask.getId()) > max) {
					max = taskToFinishTime.get(aTask.getId());
				}
			}
		}
		return max;
	}

	public int getFinishTime(String task_id) {
		return taskToFinishTime.get(task_id);
	}

	public int getProcessor(String task_id) {
		return taskMapping.get(task_id);
	}

	public int getProcessorReadyTime(int processor) {
		return processorReadyTime[processor];
	}

	public int minProcessorReadyTime() {
		int min = processorReadyTime[0];
		for (int i = 1; i < processorReadyTime.length; i++) {
			if (processorReadyTime[i] < min) {
				min = processorReadyTime[i];
			}
		}
		return min;
	}

	public int getFinishTime() {
		int max = 0;
		for (int i = 0; i < processorReadyTime.length; i++) {
			if (processorReadyTime[i] > max) {
				max = processorReadyTime[i];
			}
		}
		return max;
	}

	public int getStartTime(String task_id) {
		int ft = getFinishTime(task_id);
		if (ft == -1) {
			return -1;
		}
		return ft
				- aProblem.getTask(task_id).getDemandIn(getProcessor(task_id));
	}

	public void pushTasksUp() {

		// listOfTasks per processor
		List<List<String>> listOfTasks = new ArrayList<List<String>>();
		for (int i = 0; i < processorReadyTime.length; i++) {
			listOfTasks.add(new ArrayList<String>());
		}

		// fill listOfTasks for sorting
		Iterator it = taskMapping.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry pairs = (Map.Entry) it.next();
			String node = (String) pairs.getKey();
			int proc = (Integer) pairs.getValue();
			if (proc != -1) {
				listOfTasks.get(proc).add(node);
			}
		}
		boolean isChanged = true;
		while (isChanged) {
			isChanged = false;
			for (int i = 0; i < processorReadyTime.length; i++) {

				List<KeyValuePair> taskList = new ArrayList<KeyValuePair>();
				for (String node : listOfTasks.get(i)) {
					taskList.add(new KeyValuePair(node, getStartTime(node)));
				}
				Collections.sort(taskList);

				for (int k = 0; k < taskList.size(); k++) {
					KeyValuePair pair = taskList.get(k);
					int pre_time = 0;
					if (k > 0) {
						KeyValuePair pre_pair = taskList.get(k - 1);
						pre_time = taskToFinishTime.get(pre_pair.key); // pre_pair.value
																		// +
																		// aProblem.getTask(pre_pair.key).getDemandIn(i);
					}

					String node = pair.key;
					int st = pair.value;

					if (st > pre_time) {
						int min_st = pre_time;// Integer.MAX_VALUE;
						for (String node_pre : aProblem.getTask(node).dependedOnTasks) {
							int proc = taskMapping.get(node_pre);
							if (proc == i) {
								int node_pre_st = taskToFinishTime
										.get(node_pre);
								if (node_pre_st >= pre_time) {
									if (min_st < node_pre_st) {
										min_st = node_pre_st;
									}
								}
							} else {
								int ind = aProblem.getTask(node_pre).successorTasks
										.indexOf(node);
								int node_pre_st = taskToFinishTime
										.get(node_pre)
										+ aProblem.getTask(node_pre).communicationData
												.get(ind);
								if (node_pre_st >= pre_time) {
									if (min_st < node_pre_st) {
										min_st = node_pre_st;
									}
								}
							}
						}
						if (st > min_st) {// && min_st >= pre_time) {
							// System.out.println(node + " moved " + st +
							// " --> " + min_st);
							taskToFinishTime.put(node, min_st
									+ aProblem.getTask(node).getDemandIn(i));
							isChanged = true;
						}
					}
				}

				// update processorReadyTime
				if (taskList.size() > 0) {
					String lastTask = taskList.get(taskList.size() - 1).key;
					processorReadyTime[i] = taskToFinishTime.get(lastTask);
				}
			}
		}
	}

	public void removeTasks(List<String> tlist) {

		// remove tlist from taskMapping, taskToFinishTime
		for (String node : tlist) {
			taskMapping.put(node, -1);
			taskToFinishTime.put(node, -1);
		}

		// update processorReadyTime
		// listOfTasks per processor
		List<List<String>> listOfTasks = new ArrayList<List<String>>();
		for (int i = 0; i < processorReadyTime.length; i++) {
			listOfTasks.add(new ArrayList<String>());
		}

		// fill listOfTasks for sorting
		Iterator it = taskMapping.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry pairs = (Map.Entry) it.next();
			String node = (String) pairs.getKey();
			int proc = (Integer) pairs.getValue();
			if (proc != -1) {
				listOfTasks.get(proc).add(node);
			}
		}
		for (int i = 0; i < processorReadyTime.length; i++) {
			List<KeyValuePair> taskList = new ArrayList<KeyValuePair>();
			for (String node : listOfTasks.get(i)) {
				taskList.add(new KeyValuePair(node, getStartTime(node)));
			}
			Collections.sort(taskList);
			if (taskList.size() > 0) {
				String lastTask = taskList.get(taskList.size() - 1).key;
				processorReadyTime[i] = taskToFinishTime.get(lastTask);
			} else {
				processorReadyTime[i] = 0;
			}
		}
	}

	public void removeTasksAtLevel(int k) {

		// find tasks in solution with level = k
		List<String> tlist = new ArrayList<String>();
		Iterator it = taskMapping.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry pairs = (Map.Entry) it.next();
			String node = (String) pairs.getKey();
			if (aProblem.getTask(node).level == k) {
				tlist.add(node);
			}
		}

		removeTasks(tlist);
	}

	public void removeTask(String t_id) {
		List<String> tlist = new ArrayList<String>();
		tlist.add(t_id);

		removeTasks(tlist);
	}

	public int findStartTimeOfTaskAtProcessor(String t_id, int p) {
		int n = getProcessorReadyTime(p);
		for (String s : aProblem.getTask(t_id).dependedOnTasks) {
			int proc = taskMapping.get(s);
			if (proc != -1 && proc != p) {
				int n1 = taskToFinishTime.get(s);
				int ind = aProblem.getTask(s).successorTasks.indexOf(t_id);
				n1 += aProblem.getTask(s).communicationData.get(ind);

				if (n < n1) {
					n = n1;
				}
			}
		}
		return n;
	}

	public int findMinStartTimeOfTask(String t_id) {
		int n = Integer.MAX_VALUE;
		for (int p = 0; p < aProblem.getNumberOfProcessors(); p++) {
			int st = findStartTimeOfTaskAtProcessor(t_id, p);
			if (n > st) {
				n = st;
			}
		}
		return n;
	}

	public int findFinishTimeOfTaskAtProcessor(String t_id, int p) {
		int n = findStartTimeOfTaskAtProcessor(t_id, p);
		n += aProblem.getTask(t_id).getDemandIn(p);
		return n;
	}


	// refactoring 11/02/2013
	public void scheduleStartTime(String taskId, int processor, int startTime) {
		int finishTime = aProblem.getTask(taskId).getDemandIn(processor)
				+ startTime;
		scheduleFinishTime(taskId, processor, finishTime);
	}

	public void scheduleFinishTime(String taskId, int processor, int finishTime) {
		// logger.info(String
		// .format("Task %s scheduled in processor %d starts at %d and finishes at %d",
		// taskId,
		// processor,
		// finishTime
		// - aProblem.getTask(taskId).getDemandIn(
		// processor), finishTime));
		taskMapping.put(taskId, processor);
		taskToFinishTime.put(taskId, finishTime);
		if (processorReadyTime[processor] < finishTime) {
			processorReadyTime[processor] = finishTime;
		}
	}

	public void uschedule(String task_id) {
		int formerProcessor = taskMapping.get(task_id);
		taskMapping.put(task_id, -1);
		taskToFinishTime.put(task_id, -1);
		// start {speed problem}
		int max = 0;
		for (String t_id : taskMapping.keySet()) {
			if (taskMapping.get(t_id) == formerProcessor) {
				if (taskToFinishTime.get(t_id) > max)
					max = taskToFinishTime.get(t_id);
			}
		}
		processorReadyTime[formerProcessor] = max;
		// end
	}
}
