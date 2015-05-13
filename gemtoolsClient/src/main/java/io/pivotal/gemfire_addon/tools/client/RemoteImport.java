package io.pivotal.gemfire_addon.tools.client;

import java.util.List;

import io.pivotal.gemfire_addon.types.ImportRequest;

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
		this.validateFileName(tokens[1]);
		
		int lastDir = tokens[1].lastIndexOf(fileSeparator);
		
		if(lastDir==-1) {
			importRequest.setFileDir("");
			importRequest.setFileName(tokens[1]);
		} else {
			importRequest.setFileDir(tokens[1].substring(0, lastDir));
			importRequest.setFileName(tokens[1].substring(lastDir + 1));
		}
		
		return importRequest;
	}

	/* Remote import, so send the list to the servers
	 */
	protected void processImportRequestList(final List<ImportRequest> importRequest) throws Exception {
		
		//FIXME Add function call
		
	}

}
