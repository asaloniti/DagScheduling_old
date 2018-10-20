package views;

import java.util.HashMap;
import java.util.Map;

import org.jgrapht.graph.DefaultWeightedEdge;

import model.DAGSolution;
import model.Problem;
import model.Task;
import processing.core.PApplet;

public class VisualizeApplet extends PApplet {

	Problem aProblem;
	DAGSolution aSolution;
	int width, height;
	int numberOfProcessors;
	int lane_width;
	int lane_height;
	int processor_ribbon_height;
	int scale_width;
	double task_width_perc = 0.7;
	Map<String, TaskBox> taskBoxes;

	public VisualizeApplet(Problem aProblem, DAGSolution aSolution, int width,
			int height, int makespan_limit) {
		this.aProblem = aProblem;
		this.aSolution = aSolution;
		this.processor_ribbon_height = 30;
		this.scale_width = 40;
		this.width = width;
		this.height = height;
		this.lane_width = (width - scale_width)
				/ aProblem.getNumberOfProcessors();
		this.lane_height = makespan_limit;
		taskBoxes = new HashMap<String, TaskBox>();
	}

	public VisualizeApplet(Problem aProblem, DAGSolution aSolution, int width,
			int height) {
		this(aProblem, aSolution, width, height, aSolution.getFinishTime());
	}

	public void setup() {
		size(width, height);
		background(255);
		textFont(createFont("Times New Roman", 12));
		// textFont(createFont("Georgia", 12));
		// textFont(createFont("Arial", 12));

	}

	public void draw() {
		strokeWeight(1);
		drawProcessorsRibbon();
		drawScale();
		// drawCommunications();
		drawTasks();
		drawArrows();

	}

	private void drawArrows() {
		for (Task aTask : aProblem.getTasks()) {
			for (DefaultWeightedEdge dwe : aProblem.getFullGraph()
					.incomingEdgesOf(aTask.getId())) {
				String source_task_id = aProblem.getFullGraph().getEdgeSource(
						dwe);
				int source_task_p = aSolution.getProcessor(source_task_id);
				int task_p = aSolution.getProcessor(aTask.getId());
				if (source_task_p == -1 || task_p == -1) {
					continue;
				}
				TaskBox source = taskBoxes.get(source_task_id);
				TaskBox destination = taskBoxes.get(aTask.getId());
				if (source_task_p < task_p) {
					arrowLine(source.getXRightDownCorner(),
							source.getYRightDownCorner(), destination.getX(),
							destination.getY(), 0, radians(30), true);
				} else if (source_task_p > task_p) {
					arrowLine(source.getX(),
							source.getY() + source.getHeight(),
							destination.getX() + destination.getWidth(),
							destination.getY(), 0, radians(30), true);
				}
			}
		}

	}

	private void drawProcessorsRibbon() {
		textAlign(CENTER);
		for (int processor = 0; processor < aProblem.getNumberOfProcessors(); processor++) {
			String text = String.format("PU %d", processor);
//			fill(0, 255, 0);
			fill(255, 255, 255);
			rect(processor * lane_width + scale_width, 0, lane_width,
					processor_ribbon_height);
			fill(0);
			text(text, processor * lane_width + scale_width + lane_width / 2,
					20);
			fill(255, 255, 255);
			line((processor + 1) * lane_width + scale_width,
					processor_ribbon_height, (processor + 1) * lane_width
							+ scale_width, height);
		}
		textAlign(LEFT);
	}

	private void drawScale() {
		fill(0);
		line(scale_width, 0, scale_width, height);
		for (int y = 0; y < height - processor_ribbon_height; y++) {
			if (y % 50 == 0) {
				line(0, y + processor_ribbon_height, 15, y
						+ processor_ribbon_height);
				int value = (int) ((double) y
						/ (double) (height - processor_ribbon_height) * lane_height);
				text(String.format("%d", value), 15 + 2, y
						+ processor_ribbon_height);
			}
		}

	}

	private void drawTasks() {
		for (Task aTask : aProblem.getTasks()) {
			int processor = aSolution.getProcessor(aTask.getId());
			if (processor == -1) {
				continue;
			}
			int finish_time = aSolution.getFinishTime(aTask.getId());
			int start_time = finish_time - aTask.getDemandIn(processor);
//			fill(255, 255, 0);
			fill(255, 255, 255);
			int x = processor * lane_width
					+ (int) (lane_width * (1.0 - task_width_perc) / 2.0)
					+ scale_width;
			int y = start_time * (height - processor_ribbon_height)
					/ lane_height + processor_ribbon_height;
			int task_width = (int) (lane_width * task_width_perc);
			int task_height = aTask.getDemandIn(processor)
					* (height - processor_ribbon_height) / lane_height;
			TaskBox bt = new TaskBox(x, y, task_width, task_height);
			taskBoxes.put(aTask.getId(), bt);
			rect(x, y, task_width, task_height, 18, 18, 18, 18);
			fill(0);
			String text = String.format("(%s) s=%d f=%d", aTask.getId(),
					start_time, finish_time);
			text(text, x + 5, y + 20);

		}
	}

	private void drawCommunications() {
		int[] incoming = new int[aProblem.getTasks().size()];
		int[] usedSlots = new int[aProblem.getTasks().size()];
		int taskWidth = (int) (lane_width * task_width_perc);
		for (int t = 0; t < aProblem.getTasks().size(); t++) {
			Task aTask = aProblem.getTasks().get(t);
			int processor = aSolution.getProcessor(aTask.getId());
			if (processor == -1) {
				continue;
			}
			int finish_time = aSolution.getFinishTime(aTask.getId());
			int start_time = finish_time - aTask.getDemandIn(processor);

			int ndt = aTask.getDependentTasks().size();
			for (int i = 0; i < ndt; i++) {
				String dt = aTask.getDependentTasks().get(i);
				if (aSolution.getProcessor(dt) != processor) {
					incoming[aProblem.getTaskIndex(dt)]++;
				}
			}
		}

		for (Task aTask : aProblem.getTasks()) {

			int processor = aSolution.getProcessor(aTask.getId());
			if (processor == -1) {
				continue;
			}
			int finish_time = aSolution.getFinishTime(aTask.getId());
			int start_time = finish_time - aTask.getDemandIn(processor);

			int ndt = aTask.getDependentTasks().size();
			for (int i = 0; i < ndt; i++) {
				String dt = aTask.getDependentTasks().get(i);
				Task t1 = aProblem.getTask(dt);
				int tI = aProblem.getTaskIndex(dt);
				if (aSolution.getProcessor(dt) != processor) {
					pushMatrix();
					fill(255, 0, 255);
					int x = aSolution.getProcessor(dt) * lane_width
							+ scale_width + usedSlots[tI]
							* (int) (taskWidth / (double) incoming[tI]);
					usedSlots[tI]++;
					int y = finish_time * (height - processor_ribbon_height)
							/ lane_height + processor_ribbon_height;
					translate(x, y);
					int task_width = (int) (taskWidth / (double) incoming[tI]);
					int task_height = aTask.getDemandIn(processor)
							* (height - processor_ribbon_height) / lane_height;
					int task_height2 = Math.min(aTask.getCommunicationData()
							.get(i), t1.getDemandIn(processor))
							* (height - processor_ribbon_height) / lane_height;

					rect(0, 0, task_width, task_height2);
					fill(0);
					// String commText = String.format("(%s->%s) s=%d f=%d",
					// aTask
					// .getId(), t1.getId(), finish_time, finish_time + aTask
					// .getCommunicationData().get(i));
					String commText = String.format("(%s->%s)", aTask.getId(),
							t1.getId());
					this.cursor(0);
					// rotate(PI/2);
					text(commText, 5, 12);
					// rotate(-PI/2);
					popMatrix();
				}
			}
		}
	}

	/*
	 * Draws a lines with arrows of the given angles at the ends. x0 - starting
	 * x-coordinate of line y0 - starting y-coordinate of line x1 - ending
	 * x-coordinate of line y1 - ending y-coordinate of line startAngle - angle
	 * of arrow at start of line (in radians) endAngle - angle of arrow at end
	 * of line (in radians) solid - true for a solid arrow; false for an "open"
	 * arrow
	 */
	void arrowLine(float x0, float y0, float x1, float y1, float startAngle,
			float endAngle, boolean solid) {
		line(x0, y0, x1, y1);
		if (startAngle != 0) {
			arrowhead(x0, y0, atan2(y1 - y0, x1 - x0), startAngle, solid);
		}
		if (endAngle != 0) {
			arrowhead(x1, y1, atan2(y0 - y1, x0 - x1), endAngle, solid);
		}
	}

	/*
	 * Draws an arrow head at given location x0 - arrow vertex x-coordinate y0 -
	 * arrow vertex y-coordinate lineAngle - angle of line leading to vertex
	 * (radians) arrowAngle - angle between arrow and line (radians) solid -
	 * true for a solid arrow, false for an "open" arrow
	 */
	void arrowhead(float x0, float y0, float lineAngle, float arrowAngle,
			boolean solid) {
		float phi;
		float x2;
		float y2;
		float x3;
		float y3;
		final float SIZE = 8;

		x2 = x0 + SIZE * cos(lineAngle + arrowAngle);
		y2 = y0 + SIZE * sin(lineAngle + arrowAngle);
		x3 = x0 + SIZE * cos(lineAngle - arrowAngle);
		y3 = y0 + SIZE * sin(lineAngle - arrowAngle);
		if (solid) {
			triangle(x0, y0, x2, y2, x3, y3);
		} else {
			line(x0, y0, x2, y2);
			line(x0, y0, x3, y3);
		}

	}
}
