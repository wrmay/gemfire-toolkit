package io.pivotal.gemfire_addon.functions;

import io.pivotal.gemfire_addon.functions.FunctionCatalog;
import io.pivotal.gemfire_addon.tools.CommonExport;
import io.pivotal.gemfire_addon.tools.CommonImport;
import io.pivotal.gemfire_addon.types.ExportFileType;
import io.pivotal.gemfire_addon.types.ExportResponse;
import io.pivotal.gemfire_addon.types.ImportRequest;
import io.pivotal.gemfire_addon.types.ImportResponse;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.RegionFunctionContext;
import com.gemstone.gemfire.cache.partition.PartitionRegionHelper;

/**
 * <P>
 * Parallel import is the counterpart to parallel export, and taken together provide
 * an efficient parallelized and customizable alternative to GFSH's {@data import data}
 * command.
 * </P>
 * <P>
 * The export function {@link io.pivotal.gemfire_addon.functions.ADPParallelExport}
 * produces one export file per server for a region. Typically it will be called
 * on a partitioned region and this will produce as many files as there are servers
 * hosting the region.
 * </P>
 * <P>
 * The target cluster for the import may have a different number of servers from
 * the source cluster of the export. It may be a different cluster, or the same
 * cluster with servers added or removed.
 * </P>
 * <P>
 * This means that the import is constrained in efficiency gains from parallelization.
 * </P>
 * <P>
 * Superficially, it might not be possible to give each server the same amount of
 * data to import. If the source cluster has 10 servers then that is 10 export files.
 * If the target cluster has 7 servers, then some must be given 2 import files to
 * upload and some 1 import file to upload.
 * </P>
 * <P>
 * In more depth, which keys are stored on which server is a relevant factor. An
 * import file given to server 1 might contain entries which need to be hosted
 * by server 2. In this situation, server 1 would upload the file and stream
 * the entries to server 2. So network load may go up as the import runs. 
 * </P>
 * <P>
 * In practice then, what this means is the import process needs to make a best
 * guess for allocating export files to the servers that will import them. This
 * function delegates this responsibility to the caller, which provides a list
 * of tuples indicator which member should handle each file.
 * </P>
 * <I>Usage:</I>
 * <UL>
 * <LI>Standard function execution of "{@code ADPParallelImport}" invoked from a client.
 * Function should be invoked {@code onRegion(Region)} and will run on all
 * servers hosting the region.
 * </LI>
 * <LI>1st argument, a {@link java.util.List}&lt;{@link io.pivotal.gemfire_addon.types.ImportRequest}&gt;
 * object. The server processes ones that it matches on the list and ignores the others
 * </LI>
 * </UL>
 * <B>NOTES:</B>
 * <OL>
 * <LI>
 * The target region is not emptied by this process. The contents after the import completes
 * could include any data already present.
 * </LI>
 * <LI>
 * Entries in the export file are written to the region overwriting any values already present
 * with the given key.
 * </LI>
 * <LI>
 * The two previous points allow the import to be idempotent, if no other updates are occuring
 * from other sources.
 * </LI>
 */
public class ADPParallelImport extends CommonImport implements Declarable, Function {
	private static final long serialVersionUID = 1L;
	private Cache cache = null;

	@SuppressWarnings("unchecked")
	public void execute(final FunctionContext functionContext) {
		String myName = cache.getName();

		try {

			if(!(functionContext instanceof RegionFunctionContext)) {
				throw new Exception("'" + this.getId() + "' must be run on a region.");
			}

			RegionFunctionContext regionFunctionContext = (RegionFunctionContext) functionContext;
			Region<?,?> region = regionFunctionContext.getDataSet();
			String regionName = region.getFullPath();

			LOGGER.debug("Import to {} begins", regionName);

			Object args = functionContext.getArguments();
			
			List<ImportResponse> results = new ArrayList<>();
			for(ImportRequest importRequest : (List<ImportRequest>)args) {
				if(myName.equalsIgnoreCase(importRequest.getMember())) {
					LOGGER.trace("Starting import of {}", importRequest.getFileName());
					LOGGER.error("XXXStarting import of {}", importRequest.getFileName());//XXX
					ImportResponse importResponse = this.importRegion(region, importRequest.getFileDir(), importRequest.getFileName());
					LOGGER.trace("Completed import of {} as {}", importRequest.getFileName(), importResponse);
					LOGGER.error("XXXCompleted import of {} as {}", importRequest.getFileName(), importResponse);//XXX
					results.add(importResponse);
				} else {
					LOGGER.trace("Ignoring import of {} for {}", importRequest.getFileName(), importRequest.getMember());
					LOGGER.error("XXXIgnoring import of {} for {}", importRequest.getFileName(), importRequest.getMember());//XXX
				}
				
			}
			LOGGER.debug("Import to {} ends, result {}", regionName, results);
			
			functionContext.getResultSender().lastResult(results);
		} catch (Exception exception) {
			LOGGER.debug("Export failed:", exception.getMessage());
			RuntimeException serializableException = new RuntimeException(exception.getMessage());
	        serializableException.setStackTrace(exception.getStackTrace());
	        functionContext.getResultSender().sendException(serializableException);
		}

	}

	public String getId() {
		return FunctionCatalog.PARALLEL_IMPORT_FN;
	}

	public boolean hasResult() {
		return true;
	}

	/* Although it would be safe to automatically re-execute on
	 * a different node in failure, don't handle the error so
	 * that the client is notified.
	 */
	public boolean isHA() {
		return false;
	}

	public boolean optimizeForWrite() {
		return true;
	}

	public void init(final Properties arg0) {
		cache = CacheFactory.getAnyInstance();
		LOGGER = LogManager.getLogger(this.getClass());
	}

}
