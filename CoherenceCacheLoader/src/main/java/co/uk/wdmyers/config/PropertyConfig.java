package co.uk.wdmyers.config;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map.Entry;
import java.util.Properties;
import org.joda.time.LocalDate;

public class PropertyConfig {
	private static Properties prop = null;

	private static void ensurePropertiesLoaded() {
		if (prop == null) {
			loadProperties();
		}
	}

	public static void setCoherenceProperties() {
		ensurePropertiesLoaded();
	}

	public static String getDbUrl() {
		ensurePropertiesLoaded();
		return prop.getProperty("jdbc.url");
	}

	public static String getdbUser() {
		ensurePropertiesLoaded();
		return prop.getProperty("jdbc.user");
	}

	public static String getdbPassword() {
		ensurePropertiesLoaded();
		return prop.getProperty("jdbc.password");
	}

	public static String getEndDate() {
		ensurePropertiesLoaded();
		if (prop.getProperty("load.end.date") == null || prop.getProperty("load.end.date").isEmpty()) {
			LocalDate endDate = (new LocalDate()).plusYears(1);
			System.out.println("No End Date specified so using default of 1 year....endDate set to " + endDate);
			return endDate.toString("MM/dd/yy");
		} else {
			return prop.getProperty("load.end.date");
		}

	}

	public static String getStartDate() {
		ensurePropertiesLoaded();
		if (prop.getProperty("load.start.date") == null || prop.getProperty("load.start.date").isEmpty()) {
			LocalDate startDate = new LocalDate();
			String offsetString = prop.getProperty("tangosol.coherence.eviction.offset");
			if (offsetString != null && !offsetString.isEmpty()) {
				startDate = (new LocalDate()).minusDays(new Integer(offsetString));
				System.out.println("No Start Date specified so using offset(" + offsetString
						+ " days)...startDate set to " + startDate);
			} else {
				startDate = (new LocalDate()).minusDays(10);
				System.out.println(
						"No Start Date or offset specified so using default offset of 10 days...startDate set to "
								+ startDate);
			}
			return startDate.toString("MM/dd/yy");
		} else {
			return prop.getProperty("load.start.date");
		}

	}

	private static void loadProperties() {
		System.out.println("Loading properties file...");
		prop = new Properties();
		InputStream input = null;
		try {
			input = new FileInputStream("loader.properties");
			// load a properties file
			prop.load(input);
			for (Entry<Object, Object> entry : prop.entrySet()) {
				String key = entry.getKey().toString();
				String value = entry.getValue().toString();
				System.setProperty(key, value);
				if ("jdbc.password".equals(key)) {
					value = "************";
				}
				System.out.println(key + "=" + value);
			}
		} catch (IOException ex) {
			ex.printStackTrace();
		} finally {
			if (input != null) {
				try {
					input.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
}