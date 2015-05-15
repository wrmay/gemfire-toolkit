package io.pivotal.gemfire_addon.tools;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.Logger;

/**
 * <P>
 * Methods common to clientside or serverside export and import.
 * </P>
 *
 */
public abstract class CommonExportImport {
	protected static final String     	FILE_SEPARATOR = System.getProperty("file.separator");
	
	// Size limit for collection handling for getAll()/putAll()
	private   static int 				BLOCK_SIZE=-1;
	private	  static final int 			DEFAULT_BLOCK_SIZE=1000;
	
	protected Logger 					logger;
	protected Logger getLogger() {
		return logger;
	}
	protected void setLogger(Logger logger) {
		this.logger = logger;
	}
	
	/* Expecting exactly two locators, formatted as "host:port,host:port" or
	 * as "host[port],host[port]".
	 * Parse these and set as system properties for parameterized cache.xml file.
	 * 
	 * This method is unlikely to be of use to the serverside, but goes here
	 * to simplify the abstract/extends hierarchy with single inheritance.
	 */
	protected void parseLocators(final String arg) throws Exception {
		Pattern patternSquareBracketStyle = Pattern.compile("(.*)\\[(.*)\\]$");
		Pattern patternCommaStyle = Pattern.compile("(.*):(.*)$");
		String[] locators = arg.split(",");
		
		if(locators.length!=2) {
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
					throw new Exception("Could not parse '" + locators[i] + "' as \"host[port]\"");
				}
			}
		
		}
	}

	
	/*  Allow a system property to tune the number of keys for a getAll()/putAll()
	 * 
	 *TODO: What would be a sensible upper limit ?
	 */
	protected int getBlockSize() {
		if(BLOCK_SIZE>0) {
			return BLOCK_SIZE;
		}

		// If specified and valid, use it
		String tmpStr = System.getProperty("BLOCK_SIZE");
		if(tmpStr!=null) {
			try {
				int tmpValue = Integer.parseInt(tmpStr);
				if(tmpValue<1) {
					throw new Exception("BLOCK_SIZE must be positive");
				}
				BLOCK_SIZE = tmpValue;
			} catch (Exception e) {
				this.logger.error("Can't use '" + tmpStr + "' for BLOCK_SIZE", e);
			}
		}
		
		// If unset, use default
		if(BLOCK_SIZE<=0) {
			BLOCK_SIZE = DEFAULT_BLOCK_SIZE;
		}
		
		this.logger.debug("Block size={} being used for getAll()", BLOCK_SIZE);
		return BLOCK_SIZE;
	}



}
