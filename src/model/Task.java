package model;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Task implements Cloneable {

	String id;
	private int[] demandPerResource;
	double averageDemandPerResource;
	List<String> successorTasks; // tasks that depend on this task
	List<Integer> communicationData;// amount of data communication between two
									// dependent tasks
	List<String> dependedOnTasks; // tasks that the current task depends on

	// upward rank
	double ranku = Double.MIN_VALUE;
	// static upward rank
	double ranksu = Double.MIN_VALUE;
	// downward rank
	double rankd = Double.MIN_VALUE;

	int level = -1;
	int rank = -1;

	public Task(String id, int[] demandPerResource) {
		this.id = id;
		setDemandPerResource(demandPerResource);
		successorTasks = new ArrayList<String>();
		communicationData = new ArrayList<Integer>();
		dependedOnTasks = new ArrayList<String>();
	}

	public int[] getDemandPerResource() {
		return demandPerResource;
	}

	public double getAverageDemandPerResource() {
		return averageDemandPerResource;
	}

	public void setDemandPerResource(int[] demandPerResource) {
		this.demandPerResource = demandPerResource;
		averageDemandPerResource = 0.0;
		for (int i = 0; i < demandPerResource.length; i++) {
			averageDemandPerResource += demandPerResource[i];
		}
		averageDemandPerResource = averageDemandPerResource
				/ demandPerResource.length;
	}

	public void addDependentTask(Problem aProblem, String taskid, int weight) {
		successorTasks.add(taskid);
		communicationData.add(weight);
		aProblem.getTask(taskid).addDependsOnTask(this.id);
	}

	private void addDependsOnTask(String taskid) {
		dependedOnTasks.add(taskid);
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public int getLevel() {
		return level;
	}

	public void setLevel(int level) {
		this.level = level;
	}

	public int getRank() {
		return rank;
	}

	public void setRank(int rank) {
		this.rank = rank;
	}

	public List<String> getDependentTasks() {
		return successorTasks;
	}

	public void setDependentTasks(List<String> dependentTasks) {
		this.successorTasks = dependentTasks;
	}

	public List<Integer> getCommunicationData() {
		return communicationData;
	}

	public void setCommunicationData(List<Integer> communicationData) {
		this.communicationData = communicationData;
	}

	// public double getNodeWeight() {
	// double sum = 0.0;
	// for (int i = 0; i < demandPerResource.length; i++) {
	// sum += demandPerResource[i];
	// }
	// return sum / demandPerResource.length;
	// }

	public int getDemandIn(int resource_id) {
		return demandPerResource[resource_id];
	}

	public List<String> getDependedOnTasks() {
		return dependedOnTasks;
	}

	public void setIsDependedOnTasks(List<String> dependedOnTasks) {
		this.dependedOnTasks = dependedOnTasks;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < successorTasks.size(); i++) {
			sb.append(String.format("%s-%d", successorTasks.get(i),
					communicationData.get(i)));
			sb.append(" ");
		}
		return String
				.format("Task Id=%s Node cost=%.2f Level=%d RANK[u|su|d]=[%.2f|%.2f|%.2f] Demand=%s Tasks that depends on it=%s Tasks that it depends on=%s",
						id, averageDemandPerResource, level, ranku, ranksu,
						rankd, Arrays.toString(demandPerResource),
						sb.toString(), dependedOnTasks);
	}

	@Override
	public Object clone() {
		try {
			return super.clone();
		} catch (CloneNotSupportedException e) {
			return null;
		}
	}

	public double getRankUpward() {
		return ranku;
	}

}
