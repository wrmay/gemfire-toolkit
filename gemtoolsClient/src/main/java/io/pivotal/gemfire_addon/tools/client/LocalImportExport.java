package io.pivotal.gemfire_addon.tools.client;

import com.gemstone.gemfire.cache.client.ClientCache;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.logging.log4j.Logger;

public abstract class LocalImportExport {
	protected static final byte         EOL = System.lineSeparator().getBytes()[0];
	protected static final long 		globalStartTime = System.currentTimeMillis();
	protected static Logger 			LOGGER = null;
	protected static ClientCache 		clientCache = null;
	protected static int 				errorCount=0;
	
	// Size limit for collection handling for getAll()/putAll()
	protected static int 				BLOCK_SIZE=-1;
	protected static final int 			DEFAULT_BLOCK_SIZE=1000;

	/* Expecting exactly two locators, formatted as "host:port,host:port" or
	 * as "host[port],host[port]".
	 * Parse these and set as system properties for parameterized cache.xml file.
	 */
	protected void parseLocators(final String arg) throws Exception {
		Pattern patternSquareBracketStyle = Pattern.compile("(.*)\\[(.*)\\]$");
		Pattern patternCommaStyle = Pattern.compile("(.*):(.*)$");
		String[] locators = arg.split(",");
		
		if(locators.length!=2) {
			errorCount++;
			throw new Exception("'" + arg + "' should list two locators separated by a comma");
		}
		
		for(int i=0 ; i< locators.length ; i++) {
			// "host:port" or "host[port]" ??
			Matcher matcher = patternSquareBracketStyle.matcher(locators[i]);
			
			if(matcher.matches()) {
				// "host[port]" style
				System.setProperty("LOCATOR_" + (i+1) + "_HOST", matcher.group(1));
				System.setProperty("LOCATOR_" + (i+1) + "_PORT", Integer.parseInt(matcher.group(2)) + "");
			} else {
				// "host:port" style ?
				matcher = patternCommaStyle.matcher(locators[i]);
				if(matcher.matches()) {
					System.setProperty("LOCATOR_" + (i+1) + "_HOST", matcher.group(1));
					System.setProperty("LOCATOR_" + (i+1) + "_PORT", Integer.parseInt(matcher.group(2)) + "");
				} else {
					errorCount++;
					throw new Exception("Could not parse '" + locators[i] + "' as \"host[port]\"");
				}
			}
		
		}
	}

}
