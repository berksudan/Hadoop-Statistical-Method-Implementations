package programgui;

import java.awt.Color;

import javax.swing.BoxLayout;
import javax.swing.JComponent;
import javax.swing.JPanel;

public class HadoopPanel extends JPanel {
	private static final long serialVersionUID = 1L;

	public HadoopPanel(JComponent[] componentList) {
		super();

		setLayout(null);
		setBackground(Color.ORANGE);// Default Background Color
		setBounds(55, 55, 400, 150); // Default Bound values
		setLayout(new BoxLayout(this, BoxLayout.Y_AXIS));


		for (JComponent component : componentList)
			addPanel(component);

	}

	public void addPanel(JComponent aJComponent) {
		add(aJComponent);
	}

}
