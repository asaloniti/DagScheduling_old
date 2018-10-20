package model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.jgrapht.graph.DefaultWeightedEdge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FullLegalityChecker implements LegalityCheckerInterface {

    final Logger logger = LoggerFactory.getLogger(FullLegalityChecker.class);
    Problem problem;

    public FullLegalityChecker(Problem p) {
        problem = p;
    }

    @Override
    public boolean isFeasible(DAGSolution aSolution) {
        if (verifyExclusiveProcessorAllocation(aSolution) == false) {
            return false;
        }
        if (verifyPrecendenceConstraint(aSolution) == false) {
            return false;
        }
        return true;
    }

    @Override
    public boolean isComplete(DAGSolution aSolution) {
        boolean success = true;
        for (Task aTask : problem.getTasks()) {
            String taskj = aTask.getId();
            int p_taskj = aSolution.getProcessor(taskj);
            if (p_taskj == -1) {
                success = false;
                break;
            }
        }
        return success;

    }

    private boolean verifyExclusiveProcessorAllocation(DAGSolution aSolution) {
        // System.out.println("#### EXCLUSIVE PROCESSOR ALLOCATION ####");
        for (int p = 0; p < problem.getNumberOfProcessors(); p++) {
            System.out.printf("Processor %d\n", p);
            List<KeyValuePair> taskList = new ArrayList<KeyValuePair>();
            for (Task aTask : problem.getTasks()) {
                if (aSolution.getProcessor(aTask.getId()) == p) {
                    taskList.add(new KeyValuePair(aTask.getId(), aSolution
                            .getStartTime(aTask.getId())));
                }
            }
            Collections.sort(taskList);
            for (int i = 1; i < taskList.size(); i++) {
                KeyValuePair sip0 = taskList.get(i - 1);
                KeyValuePair sip1 = taskList.get(i);
                String t_id0 = sip0.key;
                int ts0 = sip0.value;
                int tf0 = aSolution.getFinishTime(t_id0);
                String t_id1 = sip1.key;
                int ts1 = sip1.value;
                int tf1 = aSolution.getFinishTime(t_id1);
                if (!((ts0 < tf0) && (tf0 <= ts1) && (ts1 < tf1))) {
                    System.out
                            .println("#### Violation of exclusive processor allocation ####");
                    logger.error(String
                            .format("ts(%s,%d)=%d < tf(%s,%d)=%d <= ts(%s,%d)=%d < tf(%s,%d)=%d\n",
                            t_id0, p, ts0, t_id0, p, tf0, t_id1, p,
                            ts1, t_id1, p, tf1));
                    return false;
                } else {
                    // System.out
                    // .printf("ts(%s,%d)=%d < tf(%s,%d)=%d <= ts(%s,%d)=%d < tf(%s,%d)=%d\n",
                    // t_id0, p, ts0, t_id0, p, tf0, t_id1, p,
                    // ts1, t_id1, p, tf1);
                }
            }
        }
        return true;
    }

    private boolean verifyPrecendenceConstraint(DAGSolution aSolution) {
        // System.out.println("#### PRECEDENCE CONSTRAINTS ####");
        for (Task aTask : problem.getTasks()) {
            String taskj = aTask.getId();
            int p_taskj = aSolution.getProcessor(taskj);
            if (p_taskj == -1) {
                continue;
            }
            int ts_taskj = aSolution.getStartTime(taskj);
            for (DefaultWeightedEdge dwe : problem.getFullGraph().incomingEdgesOf(
                    taskj)) {
                String taski = problem.getFullGraph().getEdgeSource(dwe);
                int tf_taski = aSolution.getFinishTime(taski);
                int p_taski = aSolution.getProcessor(taski);
                if (p_taski == -1) {
                    continue;
                }
                if (p_taskj == p_taski) {
                    if (!(ts_taskj >= tf_taski)) {
                        System.out
                                .println("#### Violation of precedence constraints ####");
                        return false;
                    }
                } else {
                    if (!(ts_taskj >= tf_taski
                            + problem.getFullGraph().getEdgeWeight(dwe))) {
                        System.out
                                .println("#### Violation of precedence constraints ####");
                        return false;
                    }
                }
            }
        }
        return true;
    }
    // @Override
    // public boolean isFeasible(DAGSolution aSolution, AbstractMove m) {
    // m.executeMove();
    // return isFeasible(aSolution);
    // }
}
