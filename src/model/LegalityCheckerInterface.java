package model;

//import gr.teimes.alma.spmf.neighborhood.AbstractMove;

public interface LegalityCheckerInterface {
	public boolean isFeasible(DAGSolution aSolution);
	public boolean isComplete(DAGSolution aSolution);
//	public boolean isFeasible(DAGSolution aSolution, AbstractMove m);
}
