package views;

import java.awt.BorderLayout;
import java.awt.Container;

import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;

import model.DAGSolution;
import model.Problem;
import solver.SimpleListScheduling;
import utils.DAGImporter;


public class VisualizeJFrame {
	JPanel processingPanel;
	VisualizeApplet sketch;
	int width, height;
	String title;
	JFrame frame;
	JLabel titleLabel;

	public VisualizeJFrame(Problem aProblem, DAGSolution aSolution, int width,
			int height, String title) {
		sketch = new VisualizeApplet(aProblem, aSolution, width, height);
		this.width = width;
		this.height = height;
		this.title = title;
	}
	public VisualizeJFrame(Problem aProblem, DAGSolution aSolution, int width,
			int height, String title, int makespan) {
		sketch = new VisualizeApplet(aProblem, aSolution, width, height, makespan);
		this.width = width;
		this.height = height;
		this.title = title;
	}

	private void addComponentsToPane(Container pane) {
		BorderLayout layout = new BorderLayout();
		pane.setLayout(layout);
		processingPanel = new JPanel();
		processingPanel.add(sketch);
		pane.add(processingPanel, BorderLayout.CENTER);
		sketch.init();
		titleLabel = new JLabel(title);
		pane.add(titleLabel, BorderLayout.NORTH);
	}

	public void createAndShowGUI() {
		// Create and set up the window.
		frame = new JFrame("Schedule Visualization");
		frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

		// Set up the content pane.
		addComponentsToPane(frame.getContentPane());

		// Display the window.
		frame.pack();
		// frame.setSize(width, height + titleLabel.getHeight());
		frame.setSize(width, height + 60);
		frame.setLocationRelativeTo(null);
		frame.setVisible(true);
		frame.toFront();
	}

	public static void main(String[] args) {
//		String fn = "datasets//Sinnen_page81.txt";
		 String fn = "datasets//BittencourtRizosMadeira.txt";
		DAGImporter importer = new DAGImporter(fn);
		importer.parseFile();
		Problem aProblem = importer.getProblem();
		SimpleListScheduling sls = new SimpleListScheduling(aProblem);
		sls.solve();
		DAGSolution aSolution = sls.getSolution();
		VisualizeJFrame app = new VisualizeJFrame(aProblem, aSolution, 600,
				640, fn);
		app.createAndShowGUI();
	}
}
