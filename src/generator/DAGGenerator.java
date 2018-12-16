// Generator
package generator;

import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import utils.Pair;

public class DAGGenerator {

	private double length;
	private int processors;
	private int tasks;
	private int fanout;
	private int maxdist;

	private List<List<Double>> timelines;
	private List<GTask> allTasks;
	private Set<Pair> dependencies;
	private long seed = 1234567890L;
	private Random randomGenerator;

	private List<List<Integer>> computation_costs;
	private List<Integer> communication_costs;

	public DAGGenerator(double l, int p, int t, int f, int m) {
		length = l;
		processors = p;
		tasks = t;
		fanout = f;
		maxdist = m;
		timelines = new ArrayList<>();
		for (int i = 0; i < p; i++) {
			List<Double> aTimeline = new ArrayList<Double>();
			aTimeline.add(0.0);
			aTimeline.add(length);
			timelines.add(aTimeline);
		}
		allTasks = new ArrayList<>();
		dependencies = new HashSet<>();
		randomGenerator = new Random(seed);
	}

	public void setSeed(long seed) {
		this.seed = seed;
		randomGenerator = new Random(seed);
	}

	public void generate() {
		int t_id = 1;
		for (int p = 0; p < processors; p++) {
			int number_of_tasks = randomGenerator.nextInt(tasks / processors) + 1;
			for (int j = 0; j < number_of_tasks; j++) {
				double time = randomGenerator.nextDouble() * length;
				timelines.get(p).add(time);
			}
			Collections.sort(timelines.get(p));
			for (int i = 0; i < timelines.get(p).size() - 1; i++) {
				GTask aTask = new GTask();
				aTask.id = t_id;
				t_id++;
				aTask.cpu = p;
				aTask.start = timelines.get(p).get(i);
				aTask.end = timelines.get(p).get(i + 1);
				allTasks.add(aTask);
			}
		}

		for (GTask task1 : allTasks) {
			int task1_i = allTasks.indexOf(task1);
			int fo = randomGenerator.nextInt(fanout) + 1;
			for (int i = 0; i < fo; i++) {
				boolean retry = true;
				int task2_i = -1;
				GTask task2 = null;
				while (retry) {
					task2_i = randomGenerator.nextInt(allTasks.size());
					task2 = allTasks.get(task2_i);
					if (task2.start >= task1.end || task1.start >= task2.end) {
						double d1 = Math.abs(task2.start - task1.end);
						double d2 = Math.abs(task1.start - task2.end);
						double min = d1;
						if (d2 < min)
							min = d2;
						if (min > length / maxdist) {
							continue;
						}
						retry = false;
					}
				}
				if (task1_i < task2_i) {
					Pair pair = new Pair(task1_i + 1, task2_i + 1);
					dependencies.add(pair);
					task1.addSuccessor(task2);
					task2.addPredecessor(task1);
				} else {
					Pair pair = new Pair(task2_i + 1, task1_i + 1);
					dependencies.add(pair);
					task2.addSuccessor(task1);
					task1.addPredecessor(task2);
				}
			}
		}

		// add start and end tasks;
		GTask start = new GTask();
		start.id = 0;
		start.cpu = 0;
		start.start = 0.0;
		start.end = 0.0;
		GTask end = new GTask();
		end.id = allTasks.size() + 1;
		end.cpu = 0;
		end.start = length;
		end.end = length;
		for (GTask task1 : allTasks) {
			if (task1.successors.isEmpty()) {
				task1.addSuccessor(end);
				dependencies.add(new Pair(task1.id, end.id));
			}
			if (task1.predecessors.isEmpty()) {
				task1.addPredecessor(start);
				dependencies.add(new Pair(start.id, task1.id));
			}
		}
		allTasks.add(end);
		allTasks.add(0, start);

		// task computation costs on each processor
		int total_computation_cost = 0;
		computation_costs = new ArrayList<>();
		for (GTask t : allTasks) {
			List<Integer> computation_cost_of_task = new ArrayList<Integer>();
			double computation_cost = t.end - t.start;
			for (int p = 0; p < processors; p++) {
				int temp;
				if (t.cpu == p) {
					temp = (int) computation_cost;
					computation_cost_of_task.add(temp);
				} else {
					temp = (int) (randomGenerator.nextDouble() * computation_cost + 0.25 * computation_cost);
					computation_cost_of_task.add(temp);
				}
				total_computation_cost += temp;
			}
			System.out.print(t + " ");
			for (Integer c : computation_cost_of_task)
				System.out.printf("%d ", c);
			System.out.println();
			computation_costs.add(computation_cost_of_task);
		}

		double CCR = 0.5; 
		double total_communication_cost = total_computation_cost / CCR;
		int actual_total_communication_cost = 0;
		int number_of_dependencies = dependencies.size();
		double average_communication_cost = total_communication_cost / number_of_dependencies;
		List<Pair> sorted_dependencies = new ArrayList<>(dependencies);
		Collections.sort(sorted_dependencies);
		communication_costs = new ArrayList<Integer>();
		for (Pair pair : sorted_dependencies) {
			int temp = (int) (average_communication_cost / 2.0
					+ randomGenerator.nextDouble() * average_communication_cost);
			communication_costs.add(temp);
			actual_total_communication_cost += temp;
		}

		double actualCCR = (double) total_computation_cost / (double) actual_total_communication_cost;
		System.out.println("Actual CCR " + actualCCR);

	}

	public void printDetails() {
		StringBuffer sb = new StringBuffer();
		sb.append("Tasks(" + allTasks.size() + "):\n");
		for (GTask t : allTasks) {
			sb.append(t.toString() + " ");
		}
		sb.append("\nDependencies(" + dependencies.size() + "):\n");
		List<Pair> deps = new ArrayList<>(dependencies);
		Collections.sort(deps);
		for (Pair pair : deps) {
			sb.append(String.format("%d->%d ", pair.x, pair.y));
		}
		System.out.println(sb.toString());
	}

	public void exportToTXT(String name) {
		StringBuffer sb = new StringBuffer();
		sb.append("#\n");
		sb.append("Processors:" + processors + "\n");
		sb.append("Tasks:" + allTasks.size() + "\n");
		sb.append("# task computation cost on each processor");
		for (GTask t : allTasks) {
			sb.append(String.format("\n%d ", t.id));
			for (int p = 0; p < processors; p++) {
				sb.append(String.format("%d ", computation_costs.get(t.id).get(p)));
			}
		}
		sb.append("\nDependencies:" + dependencies.size() + "\n");
		sb.append("# from_task_id to_task_id weight");
		List<Pair> deps = new ArrayList<>(dependencies);
		Collections.sort(deps);
		int i = 0;
		for (Pair pair : deps) {
			i++;
			sb.append(String.format("\n%d %d %d", pair.x, pair.y, communication_costs.get(i - 1)));
		}
		// System.out.println(sb.toString());
		PrintWriter out;
		try {
			out = new PrintWriter("txtfiles\\" + name + ".txt");
			out.println(sb.toString());
			out.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	public void exportToDOT(String name) {
		StringBuilder sb = new StringBuilder();
		sb.append("digraph ");
		sb.append(name);
		sb.append(" {\n");
		List<Pair> deps = new ArrayList<>(dependencies);
		Collections.sort(deps);
		int i = 0;
		for (Pair p : deps) {
			i++;
			sb.append(String.format("%d -> %d [ label = %d ];\n", p.x, p.y, communication_costs.get(i - 1)));
		}
		sb.append(" }\n");
		// System.out.println(sb.toString());
		PrintWriter out;
		try {
			out = new PrintWriter("dotfiles\\" + name + ".dot");
			out.println(sb.toString());
			out.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	private static void small_instance(String name) {
		double length = 100.0;
		int processors = 3;
		int tasks = 10;
		int fanout = 3;
		int maxDistanceFactor = 10;
		DAGGenerator generator = new DAGGenerator(length, processors, tasks, fanout, maxDistanceFactor);
		generator.setSeed(new Random().nextLong());
		generator.generate();
		generator.printDetails();
		generator.exportToDOT(name);
		generator.exportToTXT(name);
	}

	private static void big_instance() {
		double length = 1000000.0;
		int processors = 4;
		int tasks = 100;
		int fanout = 6;
		int maxDistanceFactor = 7;
		DAGGenerator generator = new DAGGenerator(length, processors, tasks, fanout, maxDistanceFactor);
		generator.generate();
		generator.printDetails();
		generator.exportToDOT("big");
		generator.exportToTXT("datasets\\data_b");
	}

	public static boolean log = false;

	private static void writeFile(String contents, String FileName) {
		try {
			OutputStream stream = new FileOutputStream(FileName);
			BufferedOutputStream output = new BufferedOutputStream(stream, 4096);
			for (int i = 0; i < contents.length(); i += 2048) {
				if (i + 2048 < contents.length())
					output.write(contents.substring(i, i + 2048).getBytes());
				else
					output.write(contents.substring(i).getBytes());
			}
			if (log)
				System.out.println("Write Finished, Closing Stream");
			output.close();
			if (log)
				System.out.println("Stream closed");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
//		for(int i = 0; i < 10; i++) {
//			small_instance("data"+i);
//		}
		small_instance("data");
		// big_instance();
	}
}
