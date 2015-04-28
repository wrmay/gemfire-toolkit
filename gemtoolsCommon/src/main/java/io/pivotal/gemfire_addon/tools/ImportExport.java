package io.pivotal.gemfire_addon.tools;

import io.pivotal.gemfire_addon.types.AdpExportRecordType;
import io.pivotal.gemfire_addon.types.ExportFileType;
import io.pivotal.gemfire_addon.types.ExportResult;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.gemstone.gemfire.pdx.JSONFormatter;
import com.gemstone.gemfire.pdx.PdxInstance;

public abstract class ImportExport {
	private static final String     	FILE_SEPARATOR = System.getProperty("file.separator");
	
	// Size limit for collection handling for getAll()/putAll()
	protected static int 				BLOCK_SIZE=-1;
	protected static final int 			DEFAULT_BLOCK_SIZE=1000;
	protected static Logger 			LOGGER = null;
	
	/** <P>Get all keys in one go, retrieve the corresponding values in groups of a manageable
	 * size and write to a file. Produce the file even if empty.
	 * </P>
	 * @param region	 		The region to export
	 * @param member     		[Optional] the name of the process writing the export, for the export filename
	 * @param timestamp  		The filestamp for when the export started, for the export filename
	 * @param directory			The directory to write to
	 * @param exportFileType    The format to use
	 * @return                  A results object, indicating export file details, size, name, etc
	 */
	public ExportResult exportRegion(final Region<?, ?> region, final String member, 
			final long timestamp, final String directory, final ExportFileType exportFileType) {
		ExportResult exportResult = new ExportResult();

		boolean isClient=true;
		try {
			ClientCacheFactory.getAnyInstance();
		} catch (Exception exception) {
			isClient=false;
		}

		try {
			LOGGER.info("Export begins: Region {}", region.getFullPath());
			
			this.deriveFile(exportResult, region, member, timestamp, directory, exportFileType, isClient);
			LOGGER.trace("Output file {}", exportResult.getFileName());
			
			long localStartTime = System.currentTimeMillis();
			
			try (	FileOutputStream fileOutputStream = new FileOutputStream(exportResult.getFile());
					DataOutputStream dataOutputStream = new DataOutputStream(fileOutputStream);
					) {
				this.writeRecords(dataOutputStream,region,exportResult,localStartTime,exportFileType,isClient);
			}			
						
			long localEndTime = System.currentTimeMillis();
			LOGGER.info("Export ends: Region {}: {} records exported in {}ms to file '{}'", 
					region.getFullPath(), exportResult.getRecordsWritten(), (localEndTime - localStartTime), exportResult.getFileName());

		} catch (Exception e) {
			LOGGER.error("Fail for " + region.getFullPath(), e);
		}
		
		return exportResult;
	}

	private void deriveFile(ExportResult exportResult, final Region<?, ?> region,
			String member, long timestamp, String directory, final ExportFileType exportFileType, boolean isClient) {
		StringBuffer sb = new StringBuffer();
		
		sb.append(region.getName());
		
		// Needed if running on multiple JVMs, not needed for local export from client
		if(member!=null && member.length()>0 && (!isClient)) {
			sb.append("." + member);
		}
		
		sb.append("." + timestamp);
		sb.append("." + exportFileType.toString().toLowerCase());
		
		exportResult.setFileName(sb.toString());
		if(directory!=null && directory.length()>0) {
			exportResult.setFileDir(directory);
		} else {
			exportResult.setFileDir(".");
		}

		exportResult.setFile(new File(exportResult.getFileDir() + FILE_SEPARATOR + exportResult.getFileName()));
	}

	private void writeRecords(DataOutputStream dataOutputStream, Region<?, ?> region, 
			ExportResult exportResult, long startTime, ExportFileType exportFileType, boolean isClient) throws Exception {
		Set<?> keySet = null;
		if(isClient) {
			keySet = region.keySetOnServer();
		} else {
			keySet = region.keySet();
		}
		
		this.startFile(dataOutputStream, region.getFullPath(), startTime, exportFileType);

		int blockSize = getBlockSize();
		int readCount=0;
		int writeCount=0;
		Set<Object> keySubSet = new HashSet<>(blockSize);
		for(Object key : keySet) {
			if(readCount%blockSize==0) {
				if(readCount>0) {
					writeCount=writeRecordBlock(dataOutputStream, region, keySubSet, writeCount, exportFileType);
				}
				keySubSet.clear();
			}
			readCount++;
			keySubSet.add(key);
		}
		if(keySubSet.size()!=0) {
			writeCount=writeRecordBlock(dataOutputStream, region, keySubSet, writeCount, exportFileType);
		}
		
		this.endFile(dataOutputStream,exportFileType);
		exportResult.setRecordsRead(readCount);
		exportResult.setRecordsWritten(writeCount);
	}

	private int writeRecordBlock(DataOutputStream dataOutputStream,
			Region<?, ?> region, Set<?> keySubSet, int writeCount, ExportFileType exportFileType) throws Exception {

		Map<?,?> map = region.getAll(keySubSet);
		for(Map.Entry<?, ?> entry: map.entrySet()) {
			writeCount=writeRecord(dataOutputStream,entry,writeCount,exportFileType);
		}
		
		return writeCount;
	}

	/* Write one key/value pair and increment the running total.
	 * At the moment, there is no filtering.
	 */
	private int writeRecord(DataOutputStream dataOutputStream,
			Entry<?, ?> entry, int writeCount, ExportFileType exportFileType) throws Exception{

		if(writeCount==0) {
			firstRecordHint(dataOutputStream,entry,exportFileType);
		}
		
		writeCount++;
		if(exportFileType==ExportFileType.ADP_DEFAULT_FORMAT) {
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
			throw new Exception("Export type not supported yet: " + exportFileType);
		}
		
		return writeCount;
	}

	private void startFile(DataOutputStream dataOutputStream, String regionPath, long startTime, ExportFileType exportFileType) throws Exception {
		if(exportFileType==ExportFileType.ADP_DEFAULT_FORMAT) {
			dataOutputStream.write(AdpExportRecordType.HEADER.getB());
			String header = String.format("#SOF,%d,%s%s", 
					startTime, regionPath, System.lineSeparator());
			dataOutputStream.write(header.getBytes());
		}
	}

	/* Writing the key/value type prior to the data can allow the import to be optimized
	 */
	private void firstRecordHint(DataOutputStream dataOutputStream, Entry<?, ?> entry, ExportFileType exportFileType) throws Exception {
		if(exportFileType==ExportFileType.ADP_DEFAULT_FORMAT) {
			Object key = entry.getKey();
			Object value = entry.getValue();
			
			dataOutputStream.write(AdpExportRecordType.HINT_KEY.getB());
			String hintKey = String.format("#HINT,KEY,%s%s", 
					key.getClass().getCanonicalName(),
					System.lineSeparator());
			dataOutputStream.write(hintKey.getBytes());

			dataOutputStream.write(AdpExportRecordType.HINT_VALUE.getB());
			String hintValue = String.format("#HINT,VALUE,%s%s", 
					(value==null?"":value.getClass().getCanonicalName()),
					System.lineSeparator());
			dataOutputStream.write(hintValue.getBytes());
		}
	}

	private void endFile(DataOutputStream dataOutputStream, ExportFileType exportFileType) throws Exception {
		if(exportFileType==ExportFileType.ADP_DEFAULT_FORMAT) {
			dataOutputStream.write(AdpExportRecordType.FOOTER.getB());
			String footer = String.format("#EOF%s", System.lineSeparator());
			dataOutputStream.write(footer.getBytes());
		}
	}


	/*  Allow a system property to tune the number of keys for a getAll()/putAll()
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
