package io.pivotal.gemfire_addon.tools.client;

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
public class LocalImport extends LocalImportExport {
	private Class<?> 	keyClass = null;
	private Class<?>	valueClass = null;
			
	public static void main(String[] args) throws Exception {
		new LocalImport().process(args);
		System.exit(errorCount);
	}

	private void usage() {
		System.err.println(this.getClass().getSimpleName() + ": usage: " + this.getClass().getSimpleName()
				+ " <locators> file [file] [file]...");
	}
	
	private void process(final String[] args) throws Exception {
		int fileCount=0;
		
		if(args==null || args.length<2) {
			this.usage();
			errorCount++;
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
				errorCount++;
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
			errorCount++;
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
			errorCount++;
			throw new Exception("Region name '" + arg + "' not valid, subregions are not yet supported");
		}
		
		// Disallow deliberate attempts to overwrite system data, such as system users.
		if(regionName.startsWith("__")) {
			errorCount++;
			throw new Exception("Region name '" + arg + "' not valid, system regions beginning '__' may not be imported");
		}
		
		importRegion(file, regionName, suffix);
	}
	

	private void importRegion(File file, String regionName, String suffix) throws Exception {

		Region<?,?> region = clientCache.getRegion(regionName);
		if(region==null) {
			errorCount++;
			throw new Exception("Region name '" + regionName + "' not found");
		}

		if(suffix.equalsIgnoreCase(ExportFileType.ADP_DEFAULT_FORMAT.toString())) {
			importRegionFromAdpFormatFile(file,region);
		} else {
			throw new Exception("Export type not supported yet: '" + suffix + "'");
		}
		
	}

	private void importRegionFromAdpFormatFile(File file, Region<?, ?> region) throws Exception {
		try {
			LOGGER.info("Import begins: Region {}", region.getFullPath());
			LOGGER.trace("Input file {}", file.getPath());
			
			long localStartTime = System.currentTimeMillis();
			
			int recordCount=0;
			try (	FileInputStream fileInputStream = new FileInputStream(file);
					DataInputStream dataInputStream = new DataInputStream(fileInputStream);
					) {
				keyClass=null;
				valueClass=null;
				
				byte nextByte = this.startFileAdpFormat(dataInputStream, file.getName());

				nextByte = this.firstRecordHintAdpFormatKey(dataInputStream, file.getName(), nextByte);
				nextByte = this.firstRecordHintAdpFormatValue(dataInputStream, file.getName(), nextByte);
				
				recordCount = this.readRecordsAdpFormat(dataInputStream, keyClass, valueClass, region, recordCount, file.getName(), nextByte);
			}			
						
			long localEndTime = System.currentTimeMillis();
			LOGGER.info("Import ends: Region {}: {} records imported in {}ms from file '{}'", 
					region.getFullPath(), recordCount, (localEndTime - localStartTime), file.getName());

		} catch (Exception e) {
			errorCount++;
			LOGGER.error("Fail for " + region.getFullPath(), e);
		}
	}

	private int readRecordsAdpFormat(DataInputStream dataInputStream, 
			Class<?> keyClass, Class<?> valueClass,
			Region<?, ?> region, int writeCount, String filename, byte nextByte) throws Exception {
		int blockSize = getBlockSize();
		int readCount=0;
		
		Map<?,?> entries = new HashMap<>(blockSize);
		while(nextByte==AdpExportRecordType.DATA.getB()) {
			if(readCount%blockSize==0) {
				if(readCount>0) {
					writeCount+=writeRecordBlock(entries, region);
				}
				entries.clear();
			}
			readRecordAdpFormat(dataInputStream, entries, keyClass, valueClass);
			readCount++;
			nextByte=dataInputStream.readByte();
		}
		if(entries.size()!=0) {
			writeCount+=writeRecordBlock(entries, region);
		}
				
		this.endFileAdpFormat(dataInputStream,filename, nextByte);
		return writeCount;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private int writeRecordBlock(Map entries, Region region) throws Exception {
		if(entries.size()!=0) {
			region.putAll(entries);
		}
		return entries.size();
	}

	/* Read one key/value pair from the file
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private void readRecordAdpFormat(DataInputStream dataInputStream, Map entries,
			Class<?> keyClass, Class<?> valueClass) throws Exception{

		Object key = readKeyOrValueAdpFormat(dataInputStream, keyClass);
		Object value = readKeyOrValueAdpFormat(dataInputStream, valueClass);
		
		entries.put(key, value);
	}
		
	private Object readKeyOrValueAdpFormat(DataInputStream dataInputStream, Class<?> klass) throws Exception {
		if(klass==null) {
			// If no preceeding hint to object class, guess
			return DataSerializer.readObject(dataInputStream);
		} else {
			if (PdxInstance.class.isAssignableFrom(klass)) {
				String json = DataSerializer.readObject(dataInputStream).toString();
				return JSONFormatter.fromJSON(json);
			} else {
				return DataSerializer.readObject(dataInputStream);
			}
		}
	}
	
	/* Only check for presence of header, there is no useful content
	 */
	private byte startFileAdpFormat(DataInputStream dataInputStream, String filename) throws Exception {
		
		if(dataInputStream.readByte()!=AdpExportRecordType.HEADER.getB()) {
			throw new Exception("Missing header in " + filename);
		}
		
		int i=0;
		while(dataInputStream.readByte()!=EOL) {
			i++;
			if(i>1000) {
				throw new Exception("Incomplete header in " + filename);
			}
		}
		
		return dataInputStream.readByte();
	}

	private byte firstRecordHintAdpFormatKey(DataInputStream dataInputStream, String filename, byte nextByte) throws Exception {
		return this.firstRecordHintAdpFormat(dataInputStream, filename, AdpExportRecordType.HINT_KEY, "KEY", nextByte);
	}
	private byte firstRecordHintAdpFormatValue(DataInputStream dataInputStream, String filename, byte nextByte) throws Exception {
		return this.firstRecordHintAdpFormat(dataInputStream, filename, AdpExportRecordType.HINT_VALUE, "VALUE", nextByte);
	}
	private byte firstRecordHintAdpFormat(DataInputStream dataInputStream,
			String filename, AdpExportRecordType hintType, String hintText, byte nextByte) throws Exception{
		
		// Class hints are optional, not present in empty files
		if(nextByte!=hintType.getB()) {
			LOGGER.debug("Missing {} class in {}", hintText, filename);
			return nextByte;
		}

		String textToSkip="#HINT," + hintText + ",";
		for(int i=0 ; i< textToSkip.length(); i++) {
			dataInputStream.readByte();
		}
		
		StringBuffer sb = new StringBuffer("");
		byte b;
		int i=0;
		while((b=dataInputStream.readByte())!=EOL) {
			char c = (char) (b & 0xFF);
			sb.append(c);
			i++;
			if(i>1000) {
				throw new Exception("Incomplete hint " + hintText + " in " + filename);
			}
		}
		
		try {
			if(hintType==AdpExportRecordType.HINT_KEY) {
				this.keyClass = Class.forName(sb.toString());
			} else {
				this.valueClass = Class.forName(sb.toString());
			}
			
			return dataInputStream.readByte();
		} catch (ClassNotFoundException cnfe) {
			throw new Exception("Header " + hintText + " requires class '" + sb + "' not found on classpath");
		}
	}

	/* Only check for presence of footer, there is no useful content
	 */
	private void endFileAdpFormat(DataInputStream dataInputStream, String filename, byte previousByte) throws Exception {
		
		if(previousByte!=AdpExportRecordType.FOOTER.getB()) {
			throw new Exception("Missing footer in " + filename);
		}
		
		int i=0;
		while(dataInputStream.readByte()!=EOL) {
			i++;
			if(i>1000) {
				throw new Exception("Incomplete footer in " + filename);
			}
		}
	}

	/*  Allow a system property to tune the number of keys for a putAll()
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
		
		LOGGER.debug("Block size={} being used for putAll()", BLOCK_SIZE);
		return BLOCK_SIZE;
	}
	
}
