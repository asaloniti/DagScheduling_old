package solver;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.jgrapht.graph.DefaultWeightedEdge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import model.Problem;
import model.Task;

public class Heft extends BaseSolver {
	final Logger logger = LoggerFactory.getLogger(Heft.class);
	// rank is a Map having key the task_id and value the rank
	protected Map<String, Integer> rank;

	public Heft(Problem aProblem) {
		super(aProblem);
		rank = new HashMap<String, Integer>();
		for (Task aTask : aProblem.getTasks()) {
			rank.put(aTask.getId(), -1);
		}
	}

	// HEFT original
	public void solve() {
		sortByRankUpwardValuesDesc();
		int T = aProblem.getTasks().size();
		for (int i = 0; i < T; i++) {
			String current_task = findTaskHavingPriority(i);
			int min_eft = Integer.MAX_VALUE;
			int r_min_eft = -1;
			for (int r = 0; r < aProblem.getNumberOfProcessors(); r++) {
				int earliest_finish_time_in_r = getEarliestFinishTime(
						current_task, r);
				// logger.info(String.format(
				// "Earliest finish time of task %s in resource %d is %d",
				// current_task, r, earliest_finish_time_in_r));
				if (earliest_finish_time_in_r < min_eft) {
					min_eft = earliest_finish_time_in_r;
					r_min_eft = r;
				}
			}
			solution.scheduleFinishTime(current_task, r_min_eft, min_eft);
		}
		solution.exportToSOL("heftsolfile");
		solution.exportMetrics("heftmetrics");
		solution.display();
	}

	protected void sortByRankUpwardValuesDesc() {
		int T = aProblem.getTasks().size();
		String[] taskIds = new String[T];
		double[] c_ranku = new double[T];
		int k = 0;
		for (Task aTask : aProblem.getTasks()) {
			taskIds[k] = aTask.getId();
			c_ranku[k] = aTask.getRankUpward();
			k++;
		}

		for (int i = 1; i < T; i++) {
			for (int j = T - 1; j >= i; j--) {
				if (c_ranku[j - 1] < c_ranku[j]) {
					String temp = taskIds[j];
					taskIds[j] = taskIds[j - 1];
					taskIds[j - 1] = temp;
					double temp2 = c_ranku[j];
					c_ranku[j] = c_ranku[j - 1];
					c_ranku[j - 1] = temp2;
				}
			}
		}
		for (int i = 0; i < T; i++) {
			rank.put(taskIds[i], i);
			aProblem.getTask(taskIds[i]).setRank(i);
		}
		
		
//		 System.out.println("\nRANKING OF TASKS BASED ON HEFT");
//		 for (int i = 0; i < T; i++) {
//			 logger.info(String.format("Rank=%d value=%.2f task=%s", i, c_ranku[i], taskIds[i]));
//		 }
//		 System.out.println("\n--------------------");
//		 System.out.println("rank keySet : " + rank.keySet().toString());
//		 System.out.println("rank values : " + rank.values());
	}

	protected String findTaskHavingPriority(int priority) {
		for (String task_id : rank.keySet()) {
			if (rank.get(task_id) == priority)
				return task_id;
		}
		throw new IllegalStateException();
	}

	protected int getEarliestFinishTime(String task_id, int resource_id) {
		Set<DefaultWeightedEdge> edges = aProblem.getFullGraph().incomingEdgesOf(task_id);
		int max = solution.getAllTasksEarliestTimeInResource(resource_id);
		for (DefaultWeightedEdge dwe : edges) {
			String source_task_id = aProblem.getFullGraph().getEdgeSource(dwe);
			int sft = solution.getFinishTime(source_task_id);
			int rs = solution.getProcessor(source_task_id);
			if (resource_id != rs)
				sft = sft + (int) aProblem.getFullGraph().getEdgeWeight(dwe);
			if (sft > max)
				max = sft;

		}
		max = max + aProblem.getTask(task_id).getDemandIn(resource_id);
		return max;
	}
}
