package solver;

import model.DAGSolution;
import model.Problem;

public abstract class BaseSolver {
	protected Problem aProblem;
	protected DAGSolution solution;

	public BaseSolver(Problem aProblem) {
		this.aProblem = aProblem;
		this.solution = new DAGSolution(aProblem);
	}

	public Problem getaProblem() {
		return aProblem;
	}

	public DAGSolution getSolution() {
		return solution;
	}

	// public abstract double solve();
}
