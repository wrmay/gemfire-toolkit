package io.pivotal.gemfire_addon.tools.client;

import io.pivotal.gemfire_addon.tools.CommonImport;
import io.pivotal.gemfire_addon.tools.client.utils.Bootstrap;
import io.pivotal.gemfire_addon.types.AdpExportRecordType;
import io.pivotal.gemfire_addon.types.ExportFileType;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.pdx.JSONFormatter;
import com.gemstone.gemfire.pdx.PdxInstance;

/**
 * <P>A simple version of import functionality, complementary to GFSH's "{@code import data}".
 * This works with files produced by {@link io.pivotal.gemfire_addon.tools.client.LocalExport}
 * </P>
 * <P>The mechanism is to read data from the provided extract files and insert these into
 * Gemfire. The file is read by this process, and the data sent across the network to the
 * cluster as Gemfire records.
 * </P>
 * <P>The write uses Gemfire's {@link com.gemstone.gemfire.cache.Region#put} method in
 * the simple form, so will replace any value already there with the same key. The target region
 * does not need to be empty, so this process can be used to merge data into a populated cluster.
 * </P>
 * <P>
 * Similarly, if the import is abandoned partway through for any reason, it can be rerun from
 * the beginning. Any values already inserted will be inserted a second time, so the net effect
 * is idempotence.
 * </P>
 * <P><B>NOTE</B></P>
 * <P>
 * The export utility names the file with the source region, the timestamp, and the format.
 * For example, {@code objects.1429873958506.adp}. This import utility uses the file name as the 
 * basis to determine the target region to insert into. If you rename the file, you can use the export
 * from one region to import into a different region.
 * </P>
 * <HR/>
 * <P>Usage:
 * </P>
 * <P>{@code java LocalImport} <I>host[port],host[port] file1 file2 ....</I>
 * <P>Eg:
 * </P>
 * <P>{@code java LocalImport 51.19.239.100[10355],51.19.239.87[10355] /tmp/objects.1429873958506.adp}
 * </P>
 * <P>The first argument is a pair of locators, in the same format as used in
 * a Gemfire properties file on the server side.
 * </P>
 * <P>The second and any subsequent arguments list the files to be imported.
 * </P>
 */
public class LocalImport extends CommonImport {
	private static boolean			error = false;
	private static ClientCache 		clientCache = null;
	private static final long 		globalStartTime = System.currentTimeMillis();


	public static void main(String[] args) throws Exception {
		new LocalImport().process(args);
		System.exit(error?1:0);
	}

	private void usage() {
		System.err.println(this.getClass().getSimpleName() + ": usage: " + this.getClass().getSimpleName()
				+ " <locators> file [file] [file]...");
	}
	
	protected void process(final String[] args) throws Exception {
		int fileCount=0;
		
		if(args==null || args.length<2) {
			this.usage();
			error=true;
			return;
		}
		
		parseLocators(args[0]);

		clientCache = Bootstrap.createDynamicCache();
		LOGGER = LogManager.getLogger(this.getClass());
		LOGGER.info("Import begins:");

		for(int i=1; i<args.length;i++) {
			try {
				if(args[i]!=null&&args[i].length()>0) {
					importFile(args[i]);
					fileCount += 1;
				}
			} catch (Exception e) {
				LOGGER.error("File '" + args[i] + "'", e);
				error=true;
			}
		}
		
		long globalEndTime = System.currentTimeMillis();
		LOGGER.info("Import ends: {} files imported in {}ms", fileCount, (globalEndTime - globalStartTime));
	}

	
	/* Import a file to a region with the same name.
	 */
	private void importFile(final String arg) throws Exception {
		File file = new File(arg);
		
		if(!file.exists() || !file.canRead()) {
			throw new Exception("File '" + arg + "' cannot be read");
		}
		
		String filename = file.getName();

		// Parse filename back into region name
		String[] tokens = filename.split("\\.");
		if(tokens.length<3) {
			error=true;
			throw new Exception("File name '" + arg + "' not valid, needs region name, timestamp and format");
		}
		
		String suffix = tokens[tokens.length - 1];
		@SuppressWarnings("unused")
		String timestamp = tokens[tokens.length - 2];
		// The region name is the prefix, but can contain the dot character itself
		StringBuffer sb = new StringBuffer("");
		for(int i=0 ; i < (tokens.length-2) ; i++) {
			if(i>0) {
				sb.append(".");
			}
			sb.append(tokens[i]);
		}
		String regionName = sb.toString();
		

		if(regionName.charAt(0)==Region.SEPARATOR_CHAR) {
			regionName = regionName.substring(1);
		}
		
		if(regionName.indexOf(Region.SEPARATOR_CHAR)>=0) {
			error=true;
			throw new Exception("Region name '" + arg + "' not valid, subregions are not yet supported");
		}
		
		// Disallow deliberate attempts to overwrite system data, such as system users.
		if(regionName.startsWith("__")) {
			error=true;
			throw new Exception("Region name '" + arg + "' not valid, system regions beginning '__' may not be imported");
		}
		
		importRegion(file, regionName, suffix);
	}
	

	private void importRegion(File file, String regionName, String suffix) throws Exception {

		Region<?,?> region = clientCache.getRegion(regionName);
		if(region==null) {
			error=true;
			throw new Exception("Region name '" + regionName + "' not found");
		}

		if(suffix.equalsIgnoreCase(ExportFileType.ADP_DEFAULT_FORMAT.toString())) {
			importRegionFromAdpFormatFile(file,region);
		} else {
			throw new Exception("Export type not supported yet: '" + suffix + "'");
		}
		
	}

	
}
