package io.pivotal.gemfire_addon.tools.client;

import io.pivotal.gemfire_addon.tools.CommonExport;
import io.pivotal.gemfire_addon.tools.client.utils.Bootstrap;
import io.pivotal.gemfire_addon.types.ExportFileType;

import org.apache.logging.log4j.LogManager;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.client.ClientCache;

/**
 * <P>
 * Common processing for client invoked data extract. The class that extends
 * this need only implement the export region method to do the export on
 * the client or remotely.
 * </P>
 */
public abstract class DataExport extends CommonExport {
	protected 	static final String     TMP_DIR = System.getProperty("java.io.tmpdir");
	// File suffix indicates internal format
	private 	static ExportFileType	FILE_CONTENT_TYPE = null;
	protected 	static boolean			error = false;
	protected 	static final long 		globalStartTime = System.currentTimeMillis();
	private 	static ClientCache 		clientCache = null;
	
	protected void usage() {
		System.err.println(this.getClass().getSimpleName() + ": usage: " + this.getClass().getSimpleName()
				+ " <locators> region [region] [region]...");
	}
	
	protected void process(String[] args) throws Exception {
		int regionCount=0;
		
		if(args==null || args.length<2) {
			this.usage();
			error=true;
			return;
		}
		
		parseLocators(args[0]);

		clientCache = Bootstrap.createDynamicCache();
		LOGGER = LogManager.getLogger(this.getClass());

		LOGGER.info("Export begins:");

		for(int i=1; i<args.length;i++) {
			try {
				if(args[i]!=null&&args[i].length()>0) {
					regionCount += exportRegions(args[i]);
				}
			} catch (Exception e) {
				LOGGER.error("Region '" + args[i] + "'", e);
				error=true;
			}
		}
		
		long globalEndTime = System.currentTimeMillis();
		LOGGER.info("Export ends: {} regions exported in {}ms", regionCount, (globalEndTime - globalStartTime));
	}

	
	/* Find regions with the given naming pattern and export each
	 */
	private int exportRegions(final String arg) throws Exception {
		int matches=0;
		
		ExportFileType exportFileType = getFileContentType();
		LOGGER.debug("Export file type '.{}' selected", exportFileType);
		
		String regionPattern = produceRegionPattern(arg);
		
		for(Region<?,?> region : clientCache.rootRegions()) {
			String regionName = region.getName();
			if(regionName.startsWith("__")) {
				/* Don't throw an exception if wildcard accidentally matches against hidden regions. 
				 * Eg. "*" will match "__regionAttributesMetadata"
				 */
				LOGGER.trace("Ignore matching '{}' region against pattern '{}' -> regex '{}'", regionName, arg, regionPattern);
			} else {
				if(regionName.matches(regionPattern)) {
					LOGGER.trace("Match of '{}' region against pattern '{}' -> regex '{}'", regionName, arg, regionPattern);
					exportRegion(region, null, null, globalStartTime, TMP_DIR, exportFileType);
					matches++;
				} else {
					LOGGER.trace("No match of '{}' region against pattern '{}' -> regex '{}'", regionName, arg, regionPattern);
				}
			}
		}
		
		return matches;
	}

	protected abstract void exportRegion(Region<?, ?> region, String member, String host, long globalstarttime2, String tmpDir, ExportFileType exportFileType);

	/*  Validate user input and turn it into a regex.
	 */
	private String produceRegionPattern(String arg) throws Exception {
		String regionNameWithPossibleWildcard = arg;
		if(arg.charAt(0)==Region.SEPARATOR_CHAR) {
			regionNameWithPossibleWildcard = regionNameWithPossibleWildcard.substring(1);
		}
		
		if(regionNameWithPossibleWildcard.indexOf(Region.SEPARATOR_CHAR)>=0) {
			error=true;
			throw new Exception("Region name '" + arg + "' not valid, subregions are not yet supported");
		}
		
		// Disallow deliberate attempts to get at system data, such as system users.
		if(regionNameWithPossibleWildcard.startsWith("__")) {
			error=true;
			throw new Exception("Region name '" + arg + "' not valid, system regions beginning '__' may not be exported");
		}

		return regionNameWithPossibleWildcard.replace("*", ".*?");
	}
	
	/*  For now, preset the output file format. Allow for future to specify type
	 * as enum choices.
	 */
	private ExportFileType getFileContentType() {
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
