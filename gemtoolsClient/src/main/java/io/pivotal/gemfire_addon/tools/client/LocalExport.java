package io.pivotal.gemfire_addon.tools.client;

import io.pivotal.gemfire_addon.tools.client.utils.Bootstrap;
import io.pivotal.gemfire_addon.types.AdpExportRecordType;
import io.pivotal.gemfire_addon.types.ExportFileType;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.File;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.pdx.JSONFormatter;
import com.gemstone.gemfire.pdx.PdxInstance;

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
 */
public class LocalExport {
	private static final String     FILE_SEPARATOR = System.getProperty("file.separator");
	private static final String     TMP_DIR = System.getProperty("java.io.tmpdir");
	private static final long 		globalStartTime = System.currentTimeMillis();
	private static Logger 			LOGGER = null;
	private static ClientCache 		clientCache = null;
	private static int 				errorCount=0;
	
	// For retrieving multiple rows
	private static int 				BLOCK_SIZE=-1;
	private static final int 		DEFAULT_BLOCK_SIZE=1000;
	
	// File suffix indicates internal format
	private static ExportFileType	FILE_CONTENT_TYPE = null;
	
	
	public static void main(String[] args) throws Exception {
		new LocalExport().process(args);
		System.exit(errorCount);
	}

	private void usage() {
		System.err.println(this.getClass().getSimpleName() + ": usage: " + this.getClass().getSimpleName()
				+ " <locators> region [region] [region]...");
	}
	
	private void process(final String[] args) throws Exception {
		int regionCount=0;
		
		if(args==null || args.length<2) {
			this.usage();
			errorCount++;
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
				errorCount++;
			}
		}
		
		long globalEndTime = System.currentTimeMillis();
		LOGGER.info("Export ends: {} regions exported in {}ms", regionCount, (globalEndTime - globalStartTime));
	}

	
	/* Expecting exactly two locators, formatted as "host:port,host:port" or
	 * as "host[port],host[port]".
	 * Parse these and set as system properties for parameterized cache.xml file.
	 */
	private void parseLocators(final String arg) throws Exception {
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

	
	/* Find regions with the given naming pattern and export each
	 */
	private int exportRegions(final String arg) throws Exception {
		int matches=0;
		
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
					exportRegion(region);
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
			errorCount++;
			throw new Exception("Region name '" + arg + "' not valid, subregions are not yet supported");
		}
		
		// Disallow deliberate attempts to get at system data, such as system users.
		if(regionNameWithPossibleWildcard.startsWith("__")) {
			errorCount++;
			throw new Exception("Region name '" + arg + "' not valid, system regions beginning '__' may not be exported");
		}

		return regionNameWithPossibleWildcard.replace("*", ".*?");
	}

	
	/* Get all keys in one go, retrieve the corresponding values in groups of a manageable
	 * size and write to a file. Produce the file even if empty.
	 */
	private void exportRegion(Region<?, ?> region) {
		try {
			LOGGER.info("Export begins: Region {}", region.getFullPath());
			
			String filename = region.getName() + "." + globalStartTime + "." + getFileContentType().toString().toLowerCase();
			File file = new File(TMP_DIR + FILE_SEPARATOR + filename);
			LOGGER.trace("Output file {}", file.getPath());
			
			long localStartTime = System.currentTimeMillis();
			
			int recordCount=0;
			try (	FileOutputStream fileOutputStream = new FileOutputStream(file);
					DataOutputStream dataOutputStream = new DataOutputStream(fileOutputStream);
					) {
				recordCount = this.writeRecords(dataOutputStream,region, recordCount);
			}			
						
			long localEndTime = System.currentTimeMillis();
			LOGGER.info("Export ends: Region {}: {} records exported in {}ms to file '{}'", 
					region.getFullPath(), recordCount, (localEndTime - localStartTime), filename);

		} catch (Exception e) {
			errorCount++;
			LOGGER.error("Fail for " + region.getFullPath(), e);
		}
	}

	
	private int writeRecords(DataOutputStream dataOutputStream, Region<?, ?> region, int writeCount) throws Exception {
		Set<?> keySet = region.keySetOnServer();
		
		this.startFile(dataOutputStream, keySet, region.getFullPath());

		int blockSize = getBlockSize();
		int readCount=0;
		Set<Object> keySubSet = new HashSet<>(blockSize);
		for(Object key : keySet) {
			if(readCount%blockSize==0) {
				if(readCount>0) {
					writeCount=writeRecordBlock(dataOutputStream, region, keySubSet, writeCount);
				}
				keySubSet.clear();
			}
			readCount++;
			keySubSet.add(key);
		}
		if(keySubSet.size()!=0) {
			writeCount=writeRecordBlock(dataOutputStream, region, keySubSet, writeCount);
		}
		
		this.endFile(dataOutputStream, writeCount);
		return writeCount;
	}

	private int writeRecordBlock(DataOutputStream dataOutputStream,
			Region<?, ?> region, Set<?> keySubSet, int writeCount) throws Exception {

		Map<?,?> map = region.getAll(keySubSet);
		for(Map.Entry<?, ?> entry: map.entrySet()) {
			writeCount=writeRecord(dataOutputStream,entry,writeCount);
		}
		
		return writeCount;
	}

	/* Write one key/value pair and increment the running total.
	 * At the moment, there is no filtering.
	 */
	private int writeRecord(DataOutputStream dataOutputStream,
			Entry<?, ?> entry, int writeCount) throws Exception{

		if(writeCount==0) {
			firstRecordHint(dataOutputStream,entry);
		}
		
		writeCount++;
		if(FILE_CONTENT_TYPE==ExportFileType.ADP_DEFAULT_FORMAT) {
			dataOutputStream.write(AdpExportRecordType.DATA.getB());
			for(Object o : new Object[] { entry.getKey(), entry.getValue() }) {
				
				if(o==null) {
					throw new Exception("Cannot export null");
				} else {
					if(o instanceof PdxInstance) {
						PdxInstance pdxInstance = (PdxInstance) o;
						DataSerializer.writeObject(JSONFormatter.toJSON(pdxInstance),dataOutputStream);
					} else {
						DataSerializer.writeObject(o,dataOutputStream);
					}
				}
				
			}
		} else {
			throw new Exception("Export type not supported yet: " + FILE_CONTENT_TYPE);
		}
		
		return writeCount;
	}

	private void startFile(DataOutputStream dataOutputStream, Set<?> keySet, String regionPath) throws Exception {
		if(FILE_CONTENT_TYPE==ExportFileType.ADP_DEFAULT_FORMAT) {
			dataOutputStream.write(AdpExportRecordType.HEADER.getB());
			String header = String.format("#SOF,%d,%d,%s%s%s", 
					globalStartTime, keySet.size(), regionPath, System.lineSeparator(),System.lineSeparator());
			dataOutputStream.write(header.getBytes());
		}
	}

	/* Writing the key/value type prior to the data can allow the import to be optimized
	 */
	private void firstRecordHint(DataOutputStream dataOutputStream, Entry<?, ?> entry) throws Exception {
		if(FILE_CONTENT_TYPE==ExportFileType.ADP_DEFAULT_FORMAT) {
			Object key = entry.getKey();
			Object value = entry.getValue();
			
			dataOutputStream.write(AdpExportRecordType.HINT.getB());
			String hint = String.format("#HINT,%s,%s%s%s", 
					key.getClass().getCanonicalName(),
					(value==null?"":value.getClass().getCanonicalName()),
					System.lineSeparator(),System.lineSeparator());
			dataOutputStream.write(hint.getBytes());
		}
	}

	private void endFile(DataOutputStream dataOutputStream, int writeCount) throws Exception {
		if(FILE_CONTENT_TYPE==ExportFileType.ADP_DEFAULT_FORMAT) {
			dataOutputStream.write(AdpExportRecordType.FOOTER.getB());
			String footer = String.format("%s#EOF,%d%s", 
					System.lineSeparator(), writeCount, System.lineSeparator());
			dataOutputStream.write(footer.getBytes());
		}
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
		
		LOGGER.debug("Export file type '.{}' selected", FILE_CONTENT_TYPE);
		return FILE_CONTENT_TYPE;
	}

	/*  Allow a system property to tune the number of keys for a getAll()
	 * 
	 *TODO: What would be a sensible upper limit ?
	 */
	private int getBlockSize() {
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
				errorCount++;
				LOGGER.error("Can't use '" + tmpStr + "' for BLOCK_SIZE", e);
			}
		}
		
		// If unset, use default
		if(BLOCK_SIZE<=0) {
			BLOCK_SIZE = DEFAULT_BLOCK_SIZE;
		}
		
		LOGGER.debug("Block size={} being used for getAll()", BLOCK_SIZE);
		return BLOCK_SIZE;
	}
	
}
