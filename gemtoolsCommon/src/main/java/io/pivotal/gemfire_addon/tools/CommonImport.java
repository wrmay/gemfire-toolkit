package io.pivotal.gemfire_addon.tools;

import io.pivotal.gemfire_addon.functions.FunctionCatalog;
import io.pivotal.gemfire_addon.types.AdpExportRecordType;
import io.pivotal.gemfire_addon.types.ImportRequest;
import io.pivotal.gemfire_addon.types.ImportResponse;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.execute.Execution;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.cache.execute.ResultCollector;
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

	@SuppressWarnings("unchecked")
	protected List<ImportResponse> importRegionFunction(Region<?, ?> region, List<ImportRequest> args) {
		
		Execution execution = FunctionService.onRegion(region).withArgs(args);
		ResultCollector<?, ?> resultsCollector = execution.execute(FunctionCatalog.PARALLEL_IMPORT_FN.toString());
		List<ImportResponse> results = new ArrayList<>();

		try {
			super.getLogger().info("Import begins: Region {}", region.getFullPath());
			
			long localStartTime = System.currentTimeMillis();

			List<List<ImportResponse>> outerList = (List<List<ImportResponse>>) resultsCollector.getResult();
			for(List<ImportResponse> innerList : outerList) {
				for(ImportResponse item : innerList) {
					results.add(item);
				}
			}
			
			Collections.sort(results);
			
			long localEndTime = System.currentTimeMillis();

			int recordsWritten=0;
			for(ImportResponse result : results) {
				/* Log the details of which files are where in CSV format,
				 * making it easier for any script to parse, to then
				 * copy the files to a different location
				 */
				super.getLogger().info("host,{},server,{},directory,{},file,{}", 
						result.getHostName(),result.getMemberName(),result.getFileDir(),result.getFileName());
				recordsWritten += result.getRecordsWritten();
			}
			
			super.getLogger().info("Import ends: Region {}: {} records imported in {}ms", 
					region.getFullPath(), recordsWritten, (localEndTime - localStartTime));
		
		} catch (Exception e) {
			super.getLogger().error("Fail for " + region.getFullPath(), e);

		}

		return results;
	}
	
	
	protected ImportResponse importRegion(final Region<?, ?> region, final String fileDir,
			final String fileName, final String member, final String host) throws Exception {

		this.validateFileName(fileName);
		
		String target;
		if(fileDir==null || fileDir.length()==0) {
			target = fileName;
		} else {
			target = fileDir + System.getProperty("file.separator","/") + fileName;
		}
		
		File file = new File(target);
				
		return this.importRegionFromAdpFormatFile(file,region, member, host);
	}

	private void validateFileName(String fileName) throws Exception {
		if(fileName==null) {
			throw new RuntimeException("filename cannot be null");
		}
		
		if(fileName.length()==0) {
			throw new RuntimeException("filename cannot be empty");
		}
	}

	protected ImportResponse importRegionFromAdpFormatFile(File file, Region<?, ?> region, final String member, final String host) throws Exception {
		ImportResponse importResponse = new ImportResponse();
		
		importResponse.setFile(file);
		importResponse.setFileDir(file.getParent()==null?"":file.getParent());
		importResponse.setFileName(file.getName());
		importResponse.setMemberName(member==null?"":member);
		importResponse.setHostName(host==null?"":host);
		
		try {
			super.getLogger().info("Import begins: Region {}", region.getFullPath());
			super.getLogger().trace("Input file {}", file.getPath());
			
			long localStartTime = System.currentTimeMillis();
			
			try (	FileInputStream fileInputStream = new FileInputStream(file);
					DataInputStream dataInputStream = new DataInputStream(fileInputStream);
					) {
				keyClass=null;
				valueClass=null;
				
				byte nextByte = this.startFileAdpFormat(dataInputStream, file.getName());

				nextByte = this.firstRecordHintAdpFormatKey(dataInputStream, file.getName(), nextByte);
				nextByte = this.firstRecordHintAdpFormatValue(dataInputStream, file.getName(), nextByte);
				
				this.readRecordsAdpFormat(dataInputStream, keyClass, valueClass, region, importResponse, file.getName(), nextByte);
			}			
						
			long localEndTime = System.currentTimeMillis();
			super.getLogger().info("Import ends: Region {}: {} records imported in {}ms from file '{}'", 
					region.getFullPath(), importResponse.getRecordsWritten(), (localEndTime - localStartTime), file.getName());

		} catch (Exception e) {
			super.getLogger().error("Fail for " + region.getFullPath(), e);
		}
		
		return importResponse;
	}

	private void readRecordsAdpFormat(DataInputStream dataInputStream, 
			Class<?> keyClass, Class<?> valueClass,
			Region<?, ?> region, ImportResponse importResponse, String filename, byte nextByte) throws Exception {
		int blockSize = getBlockSize();
		int readCount=0;
		int writeCount=0;
		
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
		importResponse.setRecordsRead(readCount);
		importResponse.setRecordsWritten(writeCount);
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
			super.getLogger().debug("Missing {} class in {}", hintText, filename);
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
