package generator;

import java.util.HashSet;
import java.util.Set;

public class GTask {
	int id;
	double start;
	double end;
	int cpu;
	Set<GTask> successors = new HashSet<>();
	Set<GTask> predecessors = new HashSet<>();

	void addSuccessor(GTask atask) {
		successors.add(atask);
	}

	void addPredecessor(GTask atask) {
		predecessors.add(atask);
	}
	
	@Override
	public String toString() {
		return String.format("id=%d", id);
	}
}
