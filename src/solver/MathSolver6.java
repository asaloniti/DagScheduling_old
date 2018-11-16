package gr.teimes.alma.coarsegrain.mpm.solver;

import gr.teimes.alma.coarsegrain.mpm.model.Problem;
import gr.teimes.alma.coarsegrain.mpm.model.Task;
import gr.teimes.alma.coarsegrain.mpm.utils.MPLinExpr;
import gr.teimes.alma.coarsegrain.mpm.utils.ORT;
import gurobi.GRB;
import gurobi.GRBEnv;
import gurobi.GRBException;
import gurobi.GRBLinExpr;
import gurobi.GRBModel;
import gurobi.GRBVar;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jgrapht.graph.DefaultWeightedEdge;
import org.jgrapht.traverse.TopologicalOrderIterator;

import java.util.Collections;
import java.util.Random;
import org.jgrapht.graph.SimpleDirectedWeightedGraph;

public class MathSolver6 extends BaseSolver {

    private int T, P, M = 10000;
    //
    SimpleDirectedWeightedGraph<String, DefaultWeightedEdge> graph;
    List<String> nodeList = new ArrayList<String>();
    //
    private int xvarUB;
    //
    Random my_random = new Random(777);
    //
    double msolverTime = 0;
    //
    List<List<String>> tasksPerProc = new ArrayList<List<String>>();
    
    public MathSolver6(Problem problem) throws GRBException {
        super(problem);
        this.aProblem = problem;
        P = problem.getNumberOfProcessors();
        
        for (int p = 0; p < P; p++) {
            tasksPerProc.add(new ArrayList<String>());
        }
    }

    public double getMSolverTime() {
        return msolverTime;
    }

    public double solve() throws GRBException {

        findXvarUB();

        applyHeuristicM();

        return 0;
    }

    // calc upper bound of xvars for nodeList
    // consider possible start time of first node in nodeList
    private void findXvarUB() {
        int ub = Integer.MAX_VALUE;
        int ub_max = 0;

        for (int p = 0; p < P; p++) {
            String first_id = nodeList.get(0);
            int ubound = getSolution().findStartTimeOfTaskAtProcessor(first_id, p);
            // 
            for (int t = 0; t < nodeList.size() - 1; t++) {
                String t_id = nodeList.get(t);
                ubound += aProblem.getTask(t_id).getDemandIn(p);
            }
            if (ubound < ub) {
                ub = ubound;
            }
            if (ubound > ub_max) {
                ub_max = ubound;
            }
        }
        xvarUB = ub_max;
        M = (P * ub_max / 100) * 100;
    }

    private void applyHeuristicM() throws GRBException {

        GRBEnv menv = new GRBEnv(null); // "mip1.log");
        menv.set(GRB.IntParam.OutputFlag, 0);
        GRBModel gmsolver = new GRBModel(menv);

        int T = nodeList.size() - 1;

        GRBVar[] gmx = new GRBVar[P];
        GRBVar[] gmk = new GRBVar[P - 1];
        GRBVar[] gmm = new GRBVar[P - 1];
        GRBVar[][] gmy = new GRBVar[T][P];

        // x vars
        for (int p = 0; p < P; p++) {
            String xvar_name = String.format("x%d", p);
            int ready_time = getSolution().getProcessorReadyTime(p);
            gmx[p] = gmsolver.addVar(ready_time, 2 * xvarUB, 0, GRB.INTEGER, xvar_name);
        }

        // k, m vars
        for (int p = 0; p < P - 1; p++) {
            String kvar_name = String.format("k%d", p);
            gmk[p] = gmsolver.addVar(0, 2 * xvarUB, 0, GRB.INTEGER, kvar_name);

            String mvar_name = String.format("m%d", p);
            gmm[p] = gmsolver.addVar(0, 2 * xvarUB, 0, GRB.INTEGER, mvar_name);
        }

        // y vars
        for (int t = 0; t < nodeList.size() - 1; t++) {
            String task_id = nodeList.get(t);
            for (int p = 0; p < P; p++) {
                String yvar_name = String.format("y%s_%d", task_id, p);
                int c = 0;
//                if (nodeList.indexOf(task_id) < nodeList.size() - 1) {
//                    c = aProblem.getTask(task_id).getDemandIn(p) / 2;
//                }
                gmy[t][p] = gmsolver.addVar(0, 1, c, GRB.BINARY, yvar_name);
            }
        }
        gmsolver.update();

        GRBLinExpr gexpr = null;
        String c_s;

        // ObjectiveFunction
        // max(x1, x2, ...) = max(max(x1, x2), x3)...
        gmm[P - 2].set(GRB.DoubleAttr.Obj, 1);

        // x1 = d1 * y1 + d2 * y2 + .... + getSolution().getProcessorReadyTime(p);
        for (int p = 0; p < P; p++) {
            c_s = String.format("ct_m1_%d", p);
            gexpr = new GRBLinExpr();
            gexpr.addTerm(1, gmx[p]);

            for (int t = 0; t < nodeList.size() - 1; t++) {
                String t_id = nodeList.get(t);
                int d = aProblem.getTask(t_id).getDemandIn(p);
                gexpr.addTerm(-d, gmy[t][p]);
            }
            int st_min = getSolution().getProcessorReadyTime(p);
            gmsolver.addConstr(gexpr, GRB.EQUAL, st_min, c_s);
        }

        // Sy = 1 
        for (int t = 0; t < nodeList.size() - 1; t++) {
            String t_id = nodeList.get(t);
            c_s = String.format("ct_m2_%s", t_id);
            gexpr = new GRBLinExpr();
            for (int p = 0; p < P; p++) {
                gexpr.addTerm(1, gmy[t][p]);
            }
            gmsolver.addConstr(gexpr, GRB.EQUAL, 1, c_s);
        }

        // m1 = max(x1, x2) => m1 = 0.5 x1 + 0.5 x2 + 0.5 |x1 - x2|
        // m1 = 0.5 x1 + 0.5 x2 + 0.5 k1
        // k1 >= x1 - x2
        // k1 >= -x1 + x2
        // m2 = max(m1, x3) ...
        for (int p = 0; p < P - 1; p++) {
            c_s = String.format("ct_m3_%d", p);
            gexpr = new GRBLinExpr();
            if (p == 0) {
                gexpr.addTerm(2, gmm[p]);
                gexpr.addTerm(-1, gmx[p]);
                gexpr.addTerm(-1, gmx[p + 1]);
                gexpr.addTerm(-1, gmk[p]);
                gmsolver.addConstr(gexpr, GRB.EQUAL, 0, c_s);
                //
                c_s = String.format("ct_m3_1_%d_%d", p, p + 1);
                GRBLinExpr gexpr1 = new GRBLinExpr();
                gexpr1.addTerm(1, gmk[p]);
                gexpr1.addTerm(-1, gmx[p]);
                gexpr1.addTerm(1, gmx[p + 1]);
                gmsolver.addConstr(gexpr1, GRB.GREATER_EQUAL, 0, c_s);
                //
                c_s = String.format("ct_m3_2_%d_%d", p, p + 1);
                GRBLinExpr gexpr2 = new GRBLinExpr();
                gexpr2.addTerm(1, gmk[p]);
                gexpr2.addTerm(1, gmx[p]);
                gexpr2.addTerm(-1, gmx[p + 1]);
                gmsolver.addConstr(gexpr2, GRB.GREATER_EQUAL, 0, c_s);
            } else {
                gexpr.addTerm(2, gmm[p]);
                gexpr.addTerm(-1, gmm[p - 1]);
                gexpr.addTerm(-1, gmx[p + 1]);
                gexpr.addTerm(-1, gmk[p]);
                gmsolver.addConstr(gexpr, GRB.EQUAL, 0, c_s);
                //
                c_s = String.format("ct_m3_1_%d_%d", p, p + 1);
                GRBLinExpr gexpr1 = new GRBLinExpr();
                gexpr1.addTerm(1, gmk[p]);
                gexpr1.addTerm(-1, gmm[p]);
                gexpr1.addTerm(1, gmx[p + 1]);
                gmsolver.addConstr(gexpr1, GRB.GREATER_EQUAL, 0, c_s);
                //
                c_s = String.format("ct_m3_2_%d_%d", p, p + 1);
                GRBLinExpr gexpr2 = new GRBLinExpr();
                gexpr2.addTerm(1, gmk[p]);
                gexpr2.addTerm(1, gmm[p]);
                gexpr2.addTerm(-1, gmx[p + 1]);
                gmsolver.addConstr(gexpr2, GRB.GREATER_EQUAL, 0, c_s);
            }
        }

        gmsolver.update();
        //gmsolver.write("alma_grb_m.lp");

        gmsolver.optimize();
        int solveStatus = gmsolver.get(GRB.IntAttr.Status);
        //System.out.println("GRB state= " + getSolverStatus(solveStatus));

        double objValue = -1;
        if (solveStatus == GRB.INFEASIBLE) {
            System.out.println("m problem is infeasible");
            return;
        }
        objValue = gmsolver.get(GRB.DoubleAttr.ObjVal);
        if (solveStatus == GRB.TIME_LIMIT && objValue >= Integer.MAX_VALUE) {
            System.out.println("m Problem not solved by Gurobi, time limit");
            return;
        }
        for (int p = 0; p < P; p++) {
            tasksPerProc.get(p).clear();
        }
        if (solveStatus == GRB.OPTIMAL || solveStatus == GRB.SUBOPTIMAL || solveStatus == GRB.TIME_LIMIT) {
            //if (solveStatus == GRB.OPTIMAL) {
            //    System.out.println("Problem was solved to optimality. Cost=" + objValue);
            //}
            for (int t = 0; t < T; t++) {
                String task_id = nodeList.get(t);
                for (int p = 0; p < P; p++) {
                    int ytp = (int) gmy[t][p].get(GRB.DoubleAttr.X);
                    if (ytp > 0.999) {
                        tasksPerProc.get(p).add(task_id);
                    }
                }
            }
        } else {
            System.out.println("m Problem not solved by Gurobi");
        }
        double st = gmsolver.get(GRB.DoubleAttr.Runtime);
        msolverTime += st;
    }

    private String getSolverStatus(int optimstatus) {
        if (optimstatus == GRB.OPTIMAL) {
            return "OPTIMAL";
        }
        if (optimstatus == GRB.SUBOPTIMAL) {
            return "FEASIBLE";
        } else if (optimstatus == GRB.INFEASIBLE) {
            return "INFEASIBLE";
        } else if (optimstatus == GRB.UNBOUNDED) {
            return "UNBOUNDED";
        } else if (optimstatus == GRB.TIME_LIMIT) {
            return "TIME_LIMIT";
        } else if (optimstatus == GRB.SOLUTION_LIMIT) {
            return "SOLUTION_LIMIT";
        } else {
            return "UNKNOWN";
        }
    }

    // solve all levels
    public double solve_initial_1(int levelNum) throws GRBException {
        return solve_initial_1(levelNum, levelNum, 0);
    }

    // solve step levels a time 
    public double solve_initial_1(int levelNum, int step) throws GRBException {
        return solve_initial_1(levelNum, step, 0);
    }

    // solve stepFor levels and go back stepBack
    public double solve_initial_1(int levelNum, int stepFor, int stepBack) throws GRBException {
        double rv = -1;
        int i = 0;

        while (true) {
            // 1
            int k1 = i;
            int k2 = i + stepFor - 1;
            if (k2 >= levelNum) {
                k2 = levelNum - 1;
            }

            graph = aProblem.builtGraphBetweenLevels(k1, k2);
            //aProblem.exportGraphToGml(graph, k1 + "_" + k2 + ".gml");

            T = graph.vertexSet().size();

            nodeList.clear();
            TopologicalOrderIterator<String, DefaultWeightedEdge> toi1 =
                    new TopologicalOrderIterator<String, DefaultWeightedEdge>(graph);
            while (toi1.hasNext()) {
                nodeList.add((String) toi1.next());
            }

            do {
                rv = solve();
            } while (rv < 0);

            if (k2 == levelNum - 1) {
                break;
            }

            for (int m = 0; m < stepBack; m++) {
                solution.removeTasksAtLevel(k2 - m);
            }

            i -= stepBack;
            i += stepFor;
        }

        return rv;
    }

    public double solve_initial_2(int levelNum, int nodesPerProc) throws GRBException {
        double rv = -1;

        for (int k1 = 0; k1 < levelNum; k1++) {

            List<SimpleDirectedWeightedGraph<String, DefaultWeightedEdge>> glist = aProblem.builtGraphsFromLevel(k1, nodesPerProc);

            for (int k2 = 0; k2 < glist.size(); k2++) {

                graph = glist.get(k2);
                //aProblem.exportGraphToGml(graph, k1 + "_" + k2 + ".gml");

                T = graph.vertexSet().size();

                nodeList.clear();
                TopologicalOrderIterator<String, DefaultWeightedEdge> toi1 =
                        new TopologicalOrderIterator<String, DefaultWeightedEdge>(graph);
                while (toi1.hasNext()) {
                    nodeList.add((String) toi1.next());
                }

                do {
                    rv = solve();
                } while (rv < 0);

                solution.pushTasksUp();
            }
        }
        return rv;
    }
}
