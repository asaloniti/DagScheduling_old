package solver;

import java.util.ArrayList;
import java.util.List;

import org.jgrapht.graph.DefaultWeightedEdge;
import org.jgrapht.traverse.TopologicalOrderIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import model.Problem;

public class SimpleListScheduling extends BaseSolver {
	final Logger logger = LoggerFactory.getLogger(SimpleListScheduling.class);

	public SimpleListScheduling(Problem aProblem) {
		super(aProblem);
	}

	// start time minimization
	public void solve() {
		int processors_size = aProblem.getNumberOfProcessors();
		List<String> L = sortNodesAccordingToPrioritySchemeAndPrecedenceConstraints();
		for (String task_id : L) {
			int tmin = Integer.MAX_VALUE;
			int pmin = -1;
			for (int p = 0; p < processors_size; p++) {
				int tdr = getDataReadyTime(task_id, p);
				int tf = solution.getProcessorReadyTime(p);
				int max = tdr > tf ? tdr : tf;
				if (max < tmin) {
					tmin = max;
					pmin = p;
				}
			}
			solution.scheduleStartTime(task_id, pmin, tmin);
		}
	}

	private List<String> sortNodesAccordingToPrioritySchemeAndPrecedenceConstraints() {
		TopologicalOrderIterator<String, DefaultWeightedEdge> toi = new TopologicalOrderIterator<String, DefaultWeightedEdge>(
				aProblem.getFullGraph());
		List<String> L = new ArrayList<String>();
		while (toi.hasNext()) {
			String node = (String) toi.next();
			L.add(node);
		}
		return L;
	}

	private int getDataReadyTime(String task_id, int processor_id) {
		int max = Integer.MIN_VALUE;
		for (DefaultWeightedEdge dwe : aProblem.getFullGraph().incomingEdgesOf(task_id)) {
			int x;
			String sourceVertex = aProblem.getFullGraph().getEdgeSource(dwe);
			int tfsv = solution.getFinishTime(sourceVertex);
			if (processor_id == solution.getProcessor(sourceVertex))
				x = tfsv;
			else {
				int communication_cost = (int) aProblem.getFullGraph().getEdgeWeight(dwe);
				x = tfsv + communication_cost;
			}
			if (x > max)
				max = x;
		}
		return max;
	}
}
