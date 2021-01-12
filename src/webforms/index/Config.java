package webforms.index;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * @author abujarour
 * @author so
 * 
 */
public class Config {
	private static final long serialVersionUID = -2303752380162272495L;
	private static String propsPath = "config.properties";
	
	private static Properties props;


	public static String getProperty(String prop) {
		// Singleton implementation
		// We don't want to open a new File each time a property is requested
		if (props == null) {
			// read and initialize the properties for the first time
			props = new Properties();
			try {
				FileInputStream propsStream = new FileInputStream(propsPath);
				props.load(propsStream);
				
				// close the input stream
				if (propsStream != null) {
					propsStream.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		// if we couldn't read the requested property, we return an empty String
		String value = "";
		value = props.getProperty(prop);
		
		return value;
	}
}
