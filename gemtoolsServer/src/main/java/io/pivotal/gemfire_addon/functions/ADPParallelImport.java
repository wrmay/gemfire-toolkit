package io.pivotal.gemfire_addon.functions;

import io.pivotal.gemfire_addon.functions.FunctionCatalog;
import io.pivotal.gemfire_addon.tools.ImportExport;

import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.RegionFunctionContext;

/**
 * TODO Explain could be slow, plus augment
 */
public class ADPParallelImport extends ImportExport implements Declarable, Function {
	private static final long serialVersionUID = 1L;
	private Cache cache = null;

	public void execute(final FunctionContext functionContext) {
		String myName = cache.getName();
		String startTime = System.currentTimeMillis() + "";

		try {
			if(!(functionContext instanceof RegionFunctionContext)) {
				throw new Exception("'" + this.getId() + "' must be run on a region.");
			}
			
			RegionFunctionContext regionFunctionContext = (RegionFunctionContext) functionContext;
			
			// TODO Auto-generated method stub
			
		} catch (Exception exception) {
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
	 * a different not in failure, don't handle the error so
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
