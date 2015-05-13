package io.pivotal.gemfire_addon.tools.client;

import io.pivotal.gemfire_addon.types.ExportFileType;
import io.pivotal.gemfire_addon.types.ImportRequest;

import java.io.File;
import java.util.List;

import com.gemstone.gemfire.cache.Region;

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
public class LocalImport extends DataImport {

	public static void main(final String[] args) throws Exception {
		new LocalImport().process(args);
		System.exit(error?1:0);
	}

	protected void usage() {
		System.err.println(this.getClass().getSimpleName() + ": usage: " + this.getClass().getSimpleName()
				+ " <locators> file [file] [file]...");
	}
	
	protected ImportRequest importRequest(final String arg) throws Exception {
		File file = new File(arg);
		
		if(!file.exists() || !file.canRead()) {
			throw new Exception("File '" + arg + "' cannot be read");
		}
		
		ImportRequest importRequest = new ImportRequest();
		
		importRequest.setFile(file);
		importRequest.setFileDir(file.getPath());
		importRequest.setFileName(file.getName());
		
		this.validateFileName(importRequest.getFileName());
		
		return importRequest;
	}

	/* Local import, so iterate through the list
	 */
	protected void processImportRequestList(final List<ImportRequest> importRequestList) throws Exception {
		for(ImportRequest importRequest : importRequestList) {
			LOGGER.debug("Start import of '{}'", importRequest.getFile().getAbsoluteFile());
			this.processImportRequest(importRequest);
			LOGGER.debug("End import of '{}'", importRequest.getFile().getAbsoluteFile());
		}
	}

		
	/* Import a file to a region with the same name.
	 */
	private void processImportRequest(final ImportRequest importRequest) throws Exception {
		
		// Parse filename back into region name
		String[] tokens = importRequest.getFileName().split("\\.");
		if(tokens.length<3) {
			error=true;
			throw new Exception("File name '" + importRequest.getFile().getAbsoluteFile() + "' not valid, needs region name, timestamp and format");
		}
		
		String suffix = tokens[tokens.length - 1];
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
			throw new Exception("Region name '" + importRequest.getFile().getAbsoluteFile() + "' not valid, subregions are not yet supported");
		}
		
		// Disallow deliberate attempts to overwrite system data, such as system users.
		if(regionName.startsWith("__")) {
			error=true;
			throw new Exception("Region name '" + importRequest.getFile().getAbsoluteFile() + "' not valid, system regions beginning '__' may not be imported");
		}
		
		importRegion(importRequest.getFile(), regionName, suffix);
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