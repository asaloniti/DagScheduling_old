package solver;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jgrapht.graph.DefaultWeightedEdge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import model.Problem;
import model.Task;

public class CPHeft extends BaseSolver {
	final Logger logger = LoggerFactory.getLogger(CPHeft.class);
	// rank is a Map having key the task_id and value the rank
	protected Map<String, Integer> rank, cprank;

	public CPHeft(Problem aProblem) {
		super(aProblem);
		rank = new HashMap<String, Integer>();
		for (Task aTask : aProblem.getTasks()) {
			rank.put(aTask.getId(), -1);
		}
		cprank = new HashMap<String, Integer>();		
	}

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
		solution.exportToSOL("cpheftsolfile");
		solution.exportMetrics("cpheftmetrics");
		solution.display();
		//System.out.println( solution.computemakespan());
	}
	
	protected void sortByRankUpwardValuesDesc() {
		int T = aProblem.getTasks().size();
		String[] taskIds = new String[T];
		double[] c_ranku = new double[T];
		double[] c_rankd = new double[T];
		double[] c_p = new double[T];
		int k = 0;
		for (Task aTask : aProblem.getTasks()) {
			taskIds[k] = aTask.getId();
			c_ranku[k] = aTask.getRankUpward();
			c_rankd[k] = aTask.getRankDownward();
			c_p[k] = c_ranku[k] + c_rankd[k];
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
					double temp3 = c_p[j];
					c_p[j] = c_p[j - 1];
					c_p[j - 1] = temp3;
				}
			}
		}
		
		List<String> cplink=new ArrayList<String>();
		for (int i = 0; i < T; i++) {
			rank.put(taskIds[i], i);
			aProblem.getTask(taskIds[i]).setRank(i);
			if (Math.abs(c_p[i] - c_p[0])<0.001) {
				cplink.add(taskIds[i]);
			}
		}
	

		for (String task_id : cplink) {
			update_cprank(task_id);
		}		
	}
	
	//Sort the tasks in scheduling queue by critical task, decreasing order of rank value and increasing order of predecessors.
	protected void update_cprank(String t_id) {
		int length = aProblem.getTask(t_id).getDependedOnTasks().size();
		int[] ptur = new int[length];  //the order of rank upward values
		String[] ptids = new String[length]; 
		int j=0;
		for (String parentTask_id : aProblem.getTask(t_id).getDependedOnTasks()) {
			if (cprank.containsKey(parentTask_id)){
				length--;
			}
			else {
				ptur[j] = aProblem.getTask(parentTask_id).getRank();
				ptids[j] = parentTask_id;
				j++;
			}
		}
			
		for (int p = 1; p < length; p++) {
			for (int q = length - 1; q >= p; q--) {
				if ((ptur[q - 1] > ptur[q]) || ((ptur[q - 1] == ptur[q])  && (aProblem.getTask(ptids[q - 1]).getDependedOnTasks().size() > aProblem.getTask(ptids[q]).getDependedOnTasks().size()))){
					String temp = ptids[q];
					ptids[q] = ptids[q - 1];
					ptids[q - 1] = temp;
					int temp2 = ptur[q];
					ptur[q] = ptur[q - 1];
					ptur[q - 1] = temp2;
				}
			}
		}
	
		for (int p = 0; p < length; p++) {
			update_cprank(ptids[p]);
		}
		
		int t=0;
		for(Integer x: cprank.values()) {
			if (x>=t)
				t=x+1;
		}
		cprank.put(t_id, t);
	}
	
	

	protected String findTaskHavingPriority(int priority) {
		for (String task_id : cprank.keySet()) {
			if (cprank.get(task_id) == priority)
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
