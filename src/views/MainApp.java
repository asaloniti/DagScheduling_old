package views;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Scanner;

import model.DAGSolution;
import model.FullLegalityChecker;
import model.Problem;
import solver.SimpleListScheduling;
import solver.CPHeft;
import solver.Heft;
import utils.DAGImporter;

public class MainApp {

	public static void main(String[] args) {
		MainApp app = new MainApp();
		app.menu();
	}

	private String filename;
	private DAGImporter importer;
	private Problem aProblem;
	private DAGSolution sol;

	private void menu() {
		Scanner in = new Scanner(System.in);
		boolean flag = true;
		while (flag) {
			System.out.println("1. DAG info");
			System.out.println("2. Topological Sort of DAG Nodes");
			System.out.println("3. Start Time Minimization + visualize schedule");
			System.out.println("4. Heft + visualize schedule");
			System.out.println("5. CPHeft + visualize schedule");
			System.out.println("6. Load solution");
			System.out.println("0. Exit");
			int c = in.nextInt();
			if (c == 1) {
				method1();
			} else if (c == 2) {
				method2();
			} else if (c == 3) {
				method3();
			} else if (c == 4) {
				method4();
			} else if (c == 5) {
				method5();
			} else if (c == 6) {
				method6();	
			}else if (c == 0) {
				flag = false;	
				in.close();
			}	
		}	
	}

	void method1() {
		loadDataset(loadFile(new File("txtfiles"), "txt"));
		aProblem.printProblemDetails();
	}
	
	void method2() {
		loadDataset(loadFile(new File("txtfiles"), "txt"));
		for (String node : aProblem.getTopologicalOrderList()) {
			System.out.print(node + " ");
		}
		System.out.println();
	}
	
	void method3() {
		loadDataset(loadFile(new File("txtfiles"), "txt"));
		SimpleListScheduling sls = new SimpleListScheduling(aProblem);
		sls.solve();
		VisualizeJFrame app = new VisualizeJFrame(aProblem, sls.getSolution(),
				480, 640, filename);
		app.createAndShowGUI();

		boolean feasible = aProblem.isFeasible(sls.getSolution());
		if (feasible)
			System.out
					.println("Solution is feasible according to EXCLUSIVE PROCESSOR ALLOCATION and PRECEDENCE CONSTRAINTS");
		else
			System.out.println("Solution is NOT feasible");
	}
	
	void method4() {
		loadDataset(loadFile(new File("txtfiles"), "txt"));
		Heft sls = new Heft(aProblem);
		sls.solve();
		VisualizeJFrame app = new VisualizeJFrame(aProblem, sls.getSolution(),
				480, 640, filename);
		app.createAndShowGUI();

		boolean feasible = aProblem.isFeasible(sls.getSolution());
		if (feasible)
			System.out.println("Solution is feasible according to EXCLUSIVE PROCESSOR ALLOCATION and PRECEDENCE CONSTRAINTS");
		else
			System.out.println("Solution is NOT feasible");
	}
	
	void method5() {
		loadDataset(loadFile(new File("txtfiles"), "txt"));
		CPHeft sls = new CPHeft(aProblem);
		sls.solve();
		VisualizeJFrame app = new VisualizeJFrame(aProblem, sls.getSolution(),
				480, 640, filename);
		app.createAndShowGUI();

		boolean feasible = aProblem.isFeasible(sls.getSolution());
		if (feasible)
			System.out
					.println("Solution is feasible according to EXCLUSIVE PROCESSOR ALLOCATION and PRECEDENCE CONSTRAINTS");
		else
			System.out.println("Solution is NOT feasible");
	}

	void method6() {
		loadDataset(loadFile(new File("txtfiles"),"txt"));
		loadSolution(loadFile(new File("solfiles"), "sol"));
		FullLegalityChecker flc = new FullLegalityChecker(aProblem);
		if (flc.isFeasible(sol) && flc.isComplete(sol))
			System.out.println("Feasible solution");
		else
			System.out.println("Infeasible solution");
		sol.display();
		
	}

	
	private void loadSolution(String fn) {
		sol = DAGSolution.loadSolution(aProblem, fn);
	}

	private void loadDataset(String fn) {
		this.filename = fn;
		importer = new DAGImporter(filename);
		importer.parseFile();
		aProblem = importer.getProblem();
		aProblem.buildFullGraph();
		aProblem.computeRanks();
		aProblem.computeLevels();
	}

	
	private String loadFile(File folder, String extension) {
		String file_name = null;
		//File folder = new File("txtfiles");
		File[] listOfFiles = folder.listFiles();
		List<File> matchingFiles = new ArrayList<File>();
		for (File aFile : listOfFiles) {
			if (aFile.getName().endsWith(String.format(extension))) {
				matchingFiles.add(aFile);
			}
		}
		if (matchingFiles.isEmpty()) {
			System.out.printf("No files found");
			System.exit(-1);
			return "DNF";
		} else {
			System.out.println("Matching files");
			int i = 1;
			Collections.sort(matchingFiles);
			for (File aFile : matchingFiles) {
				System.out.printf("%02d. %s\n", i, aFile.getName());
				i++;
			}
			System.out.printf("Select file (1-%d): ", i - 1);
			Scanner input = new Scanner(System.in);
			file_name = matchingFiles.get(input.nextInt() - 1).getAbsolutePath();
			System.out.printf("Loading %s\n", file_name);
			return file_name;
		}
	}

	
}