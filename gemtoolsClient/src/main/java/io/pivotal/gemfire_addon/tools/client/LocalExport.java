package io.pivotal.gemfire_addon.tools.client;

import io.pivotal.gemfire_addon.tools.CommonExport;
import io.pivotal.gemfire_addon.tools.client.utils.Bootstrap;
import io.pivotal.gemfire_addon.types.ExportFileType;

import org.apache.logging.log4j.LogManager;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.client.ClientCache;

/**
 * <P>A simple version of export functionality, complementary to GFSH's "{@code export data}".
 * </P>
 * <P>The difference is that this version of the export pulls all the data across the
 * network to the memory of this process, and writes to the local filesystem. This may
 * be more useful as the file can go in a predicatble place.
 * </P>
 * <P>This is also a better way should bespoke code be needed to encrypt/decript
 * or augment/deplete the data.
 * </P>
 * <P>Like GFSH, any specialized domain classes must be on the classpath
 * </P>
 * <P>Drawbacks
 * <OL>
 * <LI>The code uses {@link com.gemstone.gemfire.cache.Region.keySetOnServer} to
 * retrieve back to the clients the entries that are to be exported. If this list
 * is massive the client may run out of memory. Alternatively, the client may time
 * out while waiting on this list to be assembled and returned.
 * </LI>
 * <LI>Similar to GFSH, the cluster is not paused while the extract runs as this
 * would impact on other processes. Updates which are happening at the exact same
 * time may be being committed while this runs and not included.
 * </LI>
 * </OL>
 * </P>
 * <HR/>
 * <P>Usage:
 * </P>
 * <P>{@code java LocalExport} <I>host[port],host[port] region1 region2 regi*</I>
 * <P>Eg:
 * </P>
 * <P>{@code java LocalExport 51.19.239.100[10355],51.19.239.87[10355] objects}
 * </P>
 * <P>The first argument is a pair of locators, in the same format as used in
 * a Gemfire properties file on the server side.
 * </P>
 * <P>The second and any subsequent arguments list the regions to be extracted.
 * These can be named completely (eg. "users"), partially (eg. "user*") or
 * all regions can be selected (ie. "*").
 * </P>
 */
public class LocalExport extends CommonExport {
	private static final String     TMP_DIR = System.getProperty("java.io.tmpdir");
	// File suffix indicates internal format
	private static ExportFileType	FILE_CONTENT_TYPE = null;
	private static boolean			error = false;
	private static final long 		globalStartTime = System.currentTimeMillis();
	private static ClientCache 		clientCache = null;
	
	public static void main(String[] args) throws Exception {
		new LocalExport().process(args);
		System.exit(error?1:0);
	}

	private void usage() {
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
					exportRegion(region, null, globalStartTime, TMP_DIR, exportFileType);
					matches++;
				} else {
					LOGGER.trace("No match of '{}' region against pattern '{}' -> regex '{}'", regionName, arg, regionPattern);
				}
			}
		}
		
		return matches;
	}
	
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
