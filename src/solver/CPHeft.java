package solver;

import java.util.HashMap;
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
	protected Map<String, Integer> rank, cprank, myrank;

	public CPHeft(Problem aProblem) {
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
		// solution.display();
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
		
		//logger.info(String.format("t1=%f t10=%f",  c_p[0], c_p[9]));
		
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
		
				
		for (int i = 0; i < T; i++) {
			logger.info(String.format("i=%d taskId=%s c_ranku=%f c_p=%f",i, taskIds[i] ,c_ranku[i], c_p[i]));
		}
		
		System.out.println("...................................");
		
		cprank = new HashMap<String, Integer>();
		for (int i = 0; i < T; i++) {
			rank.put(taskIds[i], i);
			aProblem.getTask(taskIds[i]).setRank(i);
			//logger.info(String.format("----> task=%s", taskIds[i]));
			if (c_p[i] == c_p[0]) {
				cprank.put(taskIds[i], i);
				logger.info(String.format("i=%d --> task=%s c_p=%f", i, taskIds[i], c_p[i]));
			}
		}
		cprank.put(taskIds[9], 9);
		logger.info(String.format("c_p[9]=%f --> task=%s c_p[0]=%f", c_p[9], taskIds[9], c_p[0]));
	
		
		System.out.println("._____________________________..");
		
		for (String task_id : rank.keySet()) {
			logger.info(String.format("task=%s", task_id));
			for (String parentTask_id : aProblem.getTask(task_id).getDependedOnTasks()) {
				logger.info(String.format("parenttask=%s", parentTask_id));
			}
		}
		
		System.out.println(".+++++++++++++++++++++++++++++..");
		
		myrank = new HashMap<String, Integer>();
		int i=0;
		//myrank.put(taskIds[0], i);
		//int value0 = myrank.get(taskIds[0]);
		//logger.info(String.format("myrank=%d task=%s", i, taskIds[0]));
	//	cprank.remove(taskIds[0]);
		for (String task_id : cprank.keySet()) {
			logger.info(String.format("  critical_task : %s", task_id));
			int length = aProblem.getTask(task_id).getDependedOnTasks().size();
			if (length == 1) {
				String parentTask_id = aProblem.getTask(task_id).getDependedOnTasks().get(0);
				if (myrank.containsKey(parentTask_id)==false){
					myrank.put(parentTask_id, i++);
					logger.info(String.format("myrank=%d parenttask_id=%s", myrank.get(parentTask_id), parentTask_id));
				}
			}
			else {
				int[] ptur = new int[length];
				String[] ptids = new String[length]; 
				int j=0;
				for (String parentTask_id : aProblem.getTask(task_id).getDependedOnTasks()) {
					if (myrank.containsKey(parentTask_id)){
						length--;
					}
					else {
						ptur[j] = aProblem.getTask(parentTask_id).getRank();
						ptids[j] = parentTask_id;
						j++;
					}
				}
			
				for (int p = 1; p < length; p++) {
					for (int q = length - 1; q >= p; q--) 
						if ((ptur[q - 1] > ptur[q]) || ((ptur[q - 1] == ptur[q])  && (aProblem.getTask(ptids[q - 1]).getDependedOnTasks().size() > aProblem.getTask(ptids[q]).getDependedOnTasks().size()))){
							String temp = ptids[q];
							ptids[q] = ptids[q - 1];
							ptids[q - 1] = temp;
							int temp2 = ptur[q];
							ptur[q] = ptur[q - 1];
							ptur[q - 1] = temp2;
						}
				}
				for (int p = 0; p < length; p++) {
					myrank.put(ptids[p], i++);
					logger.info(String.format("myrank=%d parenttask_id=%s rank=%d ", myrank.get(ptids[p]), ptids[p], ptur[p]));
				}
			}
			myrank.put(task_id, i++);
			logger.info(String.format("myrank=%d critical_task_id=%s", myrank.get(task_id), task_id));
		
		}
		System.out.println("........................................");
		
//		 System.out.println("\nRANKING OF TASKS BASED ON CPHEFT");
//		 for (int j = 0; j < T; j++) {
//			 logger.info(String.format("Rank=%d value=%.2f task=%s", j, c_ranku[j], taskIds[j]));
//		 }
		
	}
		
	
//					
//				for (int p = 1; p < parents; p++) {
//					for (int t = parents - 1; t >= p; t--) {
//						if ((c_ranku[t - 1] < c_ranku[t]) || ((c_ranku[t - 1] == c_ranku[t]) && (aProblem.getTask(taskIds[t - 1]).getDependedOnTasks().size() > aProblem.getTask(taskIds[t]).getDependedOnTasks().size()))){
//							String temp = taskIds[t];
//							taskIds[t] = taskIds[t - 1];
//							taskIds[t - 1] = temp;
//							double temp2 = c_ranku[t];
//							c_ranku[t] = c_ranku[t - 1];
//							c_ranku[t - 1] = temp2;
//						}
//					}
//				}
//				
//				
//				
//				for (int p = 0; p < parents; p++) {
//					rank.put(taskIds[p], p);
//					aProblem.getTask(taskIds[p]).setRank(p);
//					 logger.info(String.format("Rank=%d value=%.2f task=%s", i, c_ranku[i], taskIds[i]));
//				}
//				
//			}
//		}
//		System.out.println("\nSORTING TASKS BASED ON CPHEFT");
//		for (int i = 0; i < T; i++) {
//			 logger.info(String.format("Rank=%d value=%.2f task=%s", i, c_ranku[i], taskIds[i]));
//		 }
		


	protected String findTaskHavingPriority(int priority) {
		for (String task_id : myrank.keySet()) {
			if (myrank.get(task_id) == priority)
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
