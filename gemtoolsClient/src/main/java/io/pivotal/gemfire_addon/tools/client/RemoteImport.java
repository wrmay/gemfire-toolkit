package io.pivotal.gemfire_addon.tools.client;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import com.gemstone.gemfire.cache.Region;

import io.pivotal.gemfire_addon.types.ImportRequest;
import io.pivotal.gemfire_addon.types.ImportResponse;

/**
 *TODO
 */
public class RemoteImport extends DataImport {
	private static final String fileSeparator = System.getProperty("file.separator");
	
	public static void main(final String[] args) throws Exception {
		new RemoteImport().process(args);
		System.exit(error?1:0);
	}

	protected void usage() {
		System.err.println(this.getClass().getSimpleName() + ": usage: " + this.getClass().getSimpleName()
				+ " <locators> server,file [server,file] [server,file]...");
	}

	protected ImportRequest importRequest(final String arg) throws Exception {
		ImportRequest importRequest = new ImportRequest();
		
		String[] tokens = arg.split(",");
		if(tokens.length<2 || tokens[0].length()==0) {
			error=true;
			throw new Exception("Argument '" + arg + "' not valid, needs server name, comma then file");
		}
		
		importRequest.setMember(tokens[0]);
		
		int lastDir = tokens[1].lastIndexOf(fileSeparator);
		
		if(lastDir==-1) {
			importRequest.setFileDir("");
			importRequest.setFileName(tokens[1]);
		} else {
			importRequest.setFileDir(tokens[1].substring(0, lastDir));
			importRequest.setFileName(tokens[1].substring(lastDir + 1));
		}

		importRequest.setRegionName(this.extractRegionName(importRequest.getFileName()));

		return importRequest;
	}

	/* Remote import, so send the list to the servers
	 */
	protected void processImportRequestList(final List<ImportRequest> importRequestList) throws Exception {

		// Do each region in the request list once
		Set<String> regionNames = new TreeSet<>();
		for(ImportRequest nextImportRequest : importRequestList) {
			
			String regionName = nextImportRequest.getRegionName();
					
			if(!regionNames.contains(regionName)) {
				regionNames.add(regionName);
				
				List<ImportRequest> importRequestSubset = new ArrayList<>();
				for(ImportRequest possibleImportRequest : importRequestList) {
					if(regionName.equals(possibleImportRequest.getRegionName())) {
						importRequestSubset.add(possibleImportRequest);
					}
				}
				
				Region<?,?> region = clientCache.getRegion(regionName);
				if(region!=null) {
					this.importRegionFunction(region, importRequestSubset);
				} else {
					throw new Exception("Region '" + regionName + "' not found in client cache");
				}
			}
			
		}
		
	}

}
