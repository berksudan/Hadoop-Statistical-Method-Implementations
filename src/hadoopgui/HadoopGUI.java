package hadoopgui;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.Font;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.File;
import java.util.Random;

import javax.swing.ImageIcon;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.SwingUtilities;
import javax.swing.UIManager;
import javax.swing.filechooser.FileNameExtensionFilter;

public class HadoopGUI extends JFrame {

	private JPanel hadoopContentPanel;

	private static final long serialVersionUID = 1L;
	private static final String statisticalFunctions[] = { "SUM", "MEAN", "STDDEV", "RANGE", "MEDIAN" };
	private JButton buttonBrowseDataset, buttonBrowseJar, buttonCalculate;
	private JComboBox<String> functionPicker;
	private JTextField selectedInputDataset, selectedJarFile;
	private JTextArea outputTextArea;
	private HadoopMenuBar menubar;
	JFrame outputFrame;

	private void adjustFrameSize() {
		Dimension constantDimension = new Dimension(500, 350);
		setSize(constantDimension);
		setResizable(false);
	}

	private void configureComponents() {
		buttonBrowseDataset.setAlignmentX(CENTER_ALIGNMENT);
		buttonBrowseDataset.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent arg0) {
				selectedInputDataset.setText(openFileInBrowser());
			}
		});
		buttonBrowseJar.setAlignmentX(CENTER_ALIGNMENT);
		buttonBrowseJar.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent arg0) {
				selectedJarFile.setText(openFileInBrowser());
			}
		});
		buttonCalculate.setAlignmentX(CENTER_ALIGNMENT);
		buttonCalculate.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				showOutputScreen();
			}
		});

		selectedInputDataset.setEditable(false);
		selectedInputDataset.setFont(new Font("Arial", Font.BOLD, 11));

		selectedJarFile.setEditable(false);
		selectedJarFile.setFont(new Font("Arial", Font.BOLD, 11));

	}

	private void adjustMenuBar() {
		menubar.addMenu("Info", new String[] { "This Program is implemented\n by Berk Sudan and Simay Sanlı." });
		menubar.addMenu("Version", new String[] { "v1.0" });
		this.setJMenuBar(menubar);

	}

	private void initAttributes() {
		outputFrame = new JFrame("Program Output");
		outputTextArea = new JTextArea();
		menubar = new HadoopMenuBar();
		buttonBrowseDataset = new JButton("Browse Dataset...");
		buttonBrowseJar = new JButton("Browse Jar File...");
		buttonCalculate = new JButton("Calculate");
		selectedInputDataset = new JTextField("Chosen_Input_Dataset");
		selectedJarFile = new JTextField("Chosen_Hadoop_Controller_Jar");
		functionPicker = new JComboBox<>(statisticalFunctions);
		hadoopContentPanel = new HadoopPanel(new JComponent[] { selectedInputDataset, buttonBrowseDataset,
				selectedJarFile, buttonBrowseJar, functionPicker, buttonCalculate });

	}

	public HadoopGUI() {
		super("Statistical Function Calculator via Hadoop 3.0");
		initAttributes();
		setIconImage(new ImageIcon("./logo/logo.png").getImage());

		adjustFrameSize();
		adjustMenuBar();

		configureComponents();
		setLocationRelativeTo(null);
		setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		this.setContentPane(hadoopContentPanel);
		setVisible(true);

	}

	private void executeBashScript(HadoopBashScript hbs) {
		int commandCounter = 0;
		String bashScriptText = hbs.getBashScript();
		String[] bashScriptCommands = bashScriptText.split("\\n");
		String commandOutput = new String("");

		for (String command : bashScriptCommands) {
			outputTextArea.append(String.format("[%d] COMMAND IS EXECUTED: <<%s>>\n", commandCounter, command));
			outputTextArea.update(outputTextArea.getGraphics());
			commandOutput = hbs.runBashCommand(command);
			if (!commandOutput.isEmpty()) {
				outputTextArea.append("OUTPUT:\n" + commandOutput);
				outputTextArea.update(outputTextArea.getGraphics());
			}
			outputTextArea.append("------------------------------------------------\n");
			commandCounter++;
		}
	}

	private HadoopBashScript obtainBashScript() {
		String hadoopPath = "/usr/local/hadoop-3.0.0/";
		String inputDataset = selectedInputDataset.getText().toString();
		String jarPath = selectedJarFile.getText().toString();
		String randSeq = String.format("%03d", new Random().nextInt(1000));// 000 ~ 999 random number
		String outputPath = "mrdir-" + randSeq; // E.g. mrdir-344
		int chosenOperationIdx = functionPicker.getSelectedIndex();

		return new HadoopBashScript(hadoopPath, inputDataset, outputPath, chosenOperationIdx, jarPath);
	}

	private void showOutputScreen() {

		outputTextArea.setText("Waiting for the result...\n");

		JScrollPane scrollPane = new JScrollPane(outputTextArea);
		JFrame outputFrame = new JFrame("Program Output");
		outputFrame.getContentPane().add(scrollPane, BorderLayout.CENTER);
		outputFrame.setSize(new Dimension(900, 700));
		outputFrame.setLocationRelativeTo(null);// center the frame
		outputFrame.setVisible(true);// make it visible to the user

		HadoopBashScript hbs = obtainBashScript();
		executeBashScript(hbs);
	}

	private String openFileInBrowser() {
		JFileChooser fileChooser = new JFileChooser();
		fileChooser.setCurrentDirectory(new File(System.getProperty("user.dir")));
		fileChooser.setFileSelectionMode(JFileChooser.FILES_ONLY);
		fileChooser.setAcceptAllFileFilterUsed(true);
		fileChooser.addChoosableFileFilter(new FileNameExtensionFilter("CSV Documents", "csv"));
		fileChooser.addChoosableFileFilter(new FileNameExtensionFilter("Jar Files", "jar"));

		int result = fileChooser.showOpenDialog(this);
		if (result == JFileChooser.APPROVE_OPTION) {
			File selectedFile = fileChooser.getSelectedFile();
			return selectedFile.getAbsolutePath();
		} else {
			return "File is not recognized";
		}
	}

	public static void main(String[] args) {
		try {
			UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());
		} catch (Exception e) {
			e.printStackTrace();
		}
		SwingUtilities.invokeLater(new Runnable() {
			public void run() {
				new HadoopGUI();
			}
		});
	}

}
