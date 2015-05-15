package io.pivotal.gemfire_addon.tools.client;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import com.gemstone.gemfire.cache.Region;

import io.pivotal.gemfire_addon.types.ImportRequest;

/**
 * <P>The paralle version of import functionality, complementary to GFSH's "{@code import data}".
 * This works with files produced by {@link io.pivotal.gemfire_addon.tools.client.LocalExport}
 * or {@link io.pivotal.gemfire_addon.tools.client.RemoteExport}
 * </P>
 * <P>Of the four combinations of import &amp; export, local &amp; remote, the use cases for
 * remote import are more specialized.
 * </P>
 * <P>
 * This is due to the difficulties in predicting the routing. Remote export is fast by operating
 * in parallel on all servers simulatenously. Each server writes out the keys that each holds,
 * there is no routing or network transfer, the operation happens locally. Remote import can
 * be equally fast, if network transfer can be avoided. For this to occur, the import file
 * needs to be local to the server that needs it.
 * </P>
 * <P>
 * Recall that with Gemfire, the key routing is abstracted. Any server can be used to update
 * a key, and it is "hidden" whether the server has the key and updates it directly, or 
 * whether the server acts as a proxy and merely passes the update request to the correct
 * server.
 * </P>
 * <P>
 * The challenge for remote import then is to allocate the data files to the servers
 * correctly. Import will work whether or not this allocation is correct, but import will
 * be fastest if the allocation is correct.
 * </P>
 * <P>
 * Circumstances in which it is easy to get the allocation correct would be when the
 * import uses an export from the same cluster (ie. backup and restore) and there has
 * been no rebalance or similar activity moving the keys around in the interim.
 * </P>
 * <HR/>
 * <P>Usage:
 * </P>
 * <P>{@code java RemoteImport} <I>host[port],host[port] server,file1 [server,file2] ....</I>
 * <P>Eg:
 * </P>
 * <P>{@code java LocalImport 51.19.239.100[10355],51.19.239.87[10355] titanium-dit1-cdldfgemf01s101-server2,/tmp/objects.1429873958506.adp titanium-dit1-cdldfgemf01s101-server2,/tmp/prices.1429873958506.adp titanium-dit1-cdldfgemf01s101-server4,/tmp/objects.1429873958506.adp}
 * </P>
 * <P>The first argument is a pair of locators, in the same format as used in
 * a Gemfire properties file on the server side.
 * </P>
 * <P>The second and any subsequent arguments list the files to be imported. The format
 * for each is to list the server to import the file and the file itself.
 * </P>
 */
public class RemoteImport extends DataImport {
	private static final String fileSeparator = System.getProperty("file.separator");
	
	public static void main(final String[] args) throws Exception {
		RemoteImport remoteImport = new RemoteImport();
		remoteImport.process(args);
		System.exit(remoteImport.isError()?1:0);
	}

	protected void usage() {
		System.err.println(this.getClass().getSimpleName() + ": usage: " + this.getClass().getSimpleName()
				+ " <locators> server,file [server,file] [server,file]...");
	}

	protected ImportRequest importRequest(final String arg) throws Exception {
		ImportRequest importRequest = new ImportRequest();
		
		String[] tokens = arg.split(",");
		if(tokens.length<2 || tokens[0].length()==0) {
			super.setError(true);
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
				
				Region<?,?> region = super.getClientCache().getRegion(regionName);
				if(region!=null) {
					this.importRegionFunction(region, importRequestSubset);
				} else {
					throw new Exception("Region '" + regionName + "' not found in client cache");
				}
			}
			
		}
		
	}

}
