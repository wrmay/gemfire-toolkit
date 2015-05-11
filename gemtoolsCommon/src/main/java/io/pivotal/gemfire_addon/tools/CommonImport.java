package io.pivotal.gemfire_addon.tools;

import io.pivotal.gemfire_addon.types.AdpExportRecordType;
import io.pivotal.gemfire_addon.types.ImportResponse;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.pdx.JSONFormatter;
import com.gemstone.gemfire.pdx.PdxInstance;

/**
 * <P>
 * Methods common to clientside or serverside import
 * </P>
 *
 */
public abstract class CommonImport extends CommonExportImport {
	private static final byte       EOL = System.lineSeparator().getBytes(StandardCharsets.UTF_8)[0];
	private Class<?> 				keyClass = null;
	private Class<?>				valueClass = null;

	protected ImportResponse importRegion(Region<?, ?> region, String fileDir,
			String fileName) {
		// TODO Auto-generated method stub
		return null;
	}

	protected void importRegionFromAdpFormatFile(File file, Region<?, ?> region) throws Exception {
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

}
