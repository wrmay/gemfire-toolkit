package io.pivotal.gemfire_addon.tools.client;

import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;

import com.gemstone.gemfire.cache.client.ClientCache;

import io.pivotal.gemfire_addon.tools.CommonImport;
import io.pivotal.gemfire_addon.tools.client.utils.Bootstrap;
import io.pivotal.gemfire_addon.types.ImportRequest;

/**
 * <P>
 * Common processing for client invoked data import. This varies slightly
 * depending if the import is local (from the client) or invoked from
 * the client on remote servers -- so these functions are abstract and
 * have to be provided with an appropriate implementation.
 * </P>
 */
public abstract class DataImport extends CommonImport {
	protected 	static boolean			error = false;
	protected 	static ClientCache 			clientCache = null;
	private 	static final long 			globalStartTime = System.currentTimeMillis();

	protected abstract void usage();
	
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

		List<ImportRequest> request = new ArrayList<>();
		for(int i=1; i<args.length;i++) {
			try {
				if(args[i]!=null&&args[i].length()>0) {
					request.add(this.importRequest(args[i]));
					fileCount += 1;
				}
			} catch (Exception e) {
				LOGGER.error("File '" + args[i] + "'", e);
				error=true;
			}
		}
		
		this.processImportRequestList(request);
		
		long globalEndTime = System.currentTimeMillis();
		LOGGER.info("Import ends: {} files imported in {}ms", fileCount, (globalEndTime - globalStartTime));
	}

	protected abstract ImportRequest importRequest(final String arg) throws Exception;
	
	protected void validateFileName(final String filename) throws Exception {
		// Parse filename back into region name
		String[] tokens = filename.split("\\.");
		if(tokens.length<3) {
			error=true;
			throw new Exception("File name '" + filename + "' not valid, needs region name, timestamp and format");
		}
	}
	
	protected abstract void processImportRequestList(final List<ImportRequest> importRequest) throws Exception;

}
