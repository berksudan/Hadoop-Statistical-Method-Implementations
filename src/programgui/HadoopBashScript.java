package program_GUI;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class HadoopBashScript {
	private static final String hdfs_input_path = "/inputs";
	private static final String[] statisticalOperations = { "Sum_Operation", "Mean_Operation", "StdDev_Operation",
			"Range_Operation", "Median_Operation" };

	private String inputDataset, inputDatasetName, outputPath, chosenOperationName, hadoopPath,
			statisticalOperationsJar; // Input Parameters
	private String bashScript;

	public HadoopBashScript(String hadoopPath, String input_dataset, String output_path, int chosen_operation_idx,
			String statisticalOperationsJar) {
		if (chosen_operation_idx < 0 || chosen_operation_idx >= statisticalOperations.length) {
			System.out.println("Chosen operation number is not valid!\nExiting..");
			System.exit(-1);
		}

		this.hadoopPath = hadoopPath;
		this.inputDataset = input_dataset;
		this.inputDatasetName = inputDataset.substring(inputDataset.lastIndexOf("/") + 1);
		this.outputPath = output_path;
		this.chosenOperationName = statisticalOperations[chosen_operation_idx];
		this.statisticalOperationsJar = statisticalOperationsJar;

		constructBashScript();
	}

	private void constructBashScript() {
		StringBuilder sb = new StringBuilder();
		sb.append("echo \"Bash Commands are starting.\"" + "\n");
		sb.append(hadoopPath + "sbin/start-dfs.sh" + "\n");
		sb.append(hadoopPath + "sbin/start-yarn.sh" + "\n");
		sb.append("jps" + "\n");
		sb.append(hadoopPath + "bin/hdfs dfs -mkdir -p " + hdfs_input_path + "\n");
		sb.append(String.format("%sbin/hdfs dfs -put %s %s\n", hadoopPath, inputDataset, hdfs_input_path));
		sb.append("hadoop fs -rmr " + outputPath + "\n");
		sb.append(String.format("%sbin/hadoop jar %s bigdataproject.%s %s/%s %s\n", hadoopPath,
				statisticalOperationsJar, chosenOperationName, hdfs_input_path, inputDatasetName, outputPath));
		sb.append(String.format("%sbin/hadoop fs -cat %s/part-r-00000\n", hadoopPath, outputPath));
		sb.append("echo \"Bash Commands are finished.\"" + "\n");

		bashScript = sb.toString();
	}

	public String runBashCommand(String command) {
		Process process;
		BufferedReader reader;
		StringBuilder output = new StringBuilder();
		ProcessBuilder processBuilder = new ProcessBuilder();

		processBuilder.command("bash", "-c", command);
		try {
			process = processBuilder.start();
			reader = new BufferedReader(new InputStreamReader(process.getInputStream()));

			String line;
			while ((line = reader.readLine()) != null)
				output.append(line + "\n");

			process.waitFor();

		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return output.toString();
	}

	public String getBashScript() {
		return bashScript;
	}
}
