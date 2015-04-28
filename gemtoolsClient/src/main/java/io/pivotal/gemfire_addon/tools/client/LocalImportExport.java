package io.pivotal.gemfire_addon.tools.client;

import io.pivotal.gemfire_addon.tools.ImportExport;
import io.pivotal.gemfire_addon.types.ExportFileType;

import java.util.regex.Matcher;
import java.util.regex.Pattern;


/*  Export/import utils for client side export.
 */
public abstract class LocalImportExport extends ImportExport {
	// File suffix indicates internal format
	private static ExportFileType	FILE_CONTENT_TYPE = null;
	
	/* Expecting exactly two locators, formatted as "host:port,host:port" or
	 * as "host[port],host[port]".
	 * Parse these and set as system properties for parameterized cache.xml file.
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
	
	/*  For now, preset the output file format. Allow for future to specify type
	 * as enum choices.
	 */
	protected ExportFileType getFileContentType() {
		if(FILE_CONTENT_TYPE!=null) {
			return FILE_CONTENT_TYPE;
		}
		
		// If unset, use default
		if(FILE_CONTENT_TYPE==null) {
			FILE_CONTENT_TYPE = ExportFileType.ADP_DEFAULT_FORMAT;
		}
		
		return FILE_CONTENT_TYPE;
	}

}
