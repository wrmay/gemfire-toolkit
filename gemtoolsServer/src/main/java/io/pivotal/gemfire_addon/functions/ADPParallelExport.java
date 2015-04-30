package io.pivotal.gemfire_addon.functions;

import io.pivotal.gemfire_addon.functions.FunctionCatalog;
import io.pivotal.gemfire_addon.tools.CommonExport;
import io.pivotal.gemfire_addon.types.ExportFileType;
import io.pivotal.gemfire_addon.types.ExportResponse;

import java.util.Properties;

import org.apache.logging.log4j.LogManager;

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
 * Provide a more efficient and customizable method for data export from a region than GFSH.
 * </P>
 * <P>
 * Efficiency is achieved by running the export in parallel. This function runs on all
 * server nodes in the cluster, and each of these exports only the data that it keeps
 * rather than across the cluster as a whole.
 * </P>
 * <P>That is, ten server nodes produce ten export files of a tenth of the data each,
 * in a tenth of the time.
 * </P>
 * <P>
 * Future version can be customized with bespoke processing of the data as it is
 * written to the export. For example, SSNs and other personally identifiable data
 * may need to be masked.
 * </P>
 * <P>
 * <I>Usage:</I>
 * <UL>
 * <LI>Standard function execution of "{@code ADPParallelExport}" invoked from a client
 * or GFSH. Function should be invoked {@code onRegion(Region)} and will run on all
 * servers hosting the region.
 * </LI>
 * <LI>1st argument, {@link java.lang.Long}, optional but recommended timestamp to use when
 * naming the produced files.
 * </LI>
 * </UL>
 * <B>NOTES:</B>
 * <OL>
 * <LI>
 * The function runs on several servers. If a timestamp argument is given, they all receive
 * this so all produced files embed the same timestamp and are easily collated. If omitted
 * each server will use its current time and this may vary slightly from process to process.
 * </LI>
 * <LI>
 * To avoid impacting on other processes, the cluster is not frozen while the export
 * is running. This means the export may miss updates from other processes that are
 * being committed at the same instant. To minimise this, it is better to run any
 * export when the system is as quiet as can be reasonably achieved.
 * </LI>
 * <LI>
 * Each server produces a file, on that server's host. So there are multiple files
 * produced on the serverside hosts, that will need transported to and collated on
 * a more appropriate host for storage.
 * </LI>
 * <LI>
 * If run on a replicated region, only one file is produced as every server has the
 * same content.
 * </LI>
 * <LI>
 * Export efficiency is not a guarantee of import efficiency. See {@link
 * io.pivotal.gemfire_addon.functions.ADPParallelImport}
 * </OL>
 */
public class ADPParallelExport extends CommonExport implements Declarable, Function {
	private static final long serialVersionUID = 1L;
	private Cache cache = null;

	/*  As this function will run on multiple members in parallel for a partitioned region,
	 * expect a single java.lang.Long as an argument to use as a co-ordinated timestamp
	 * across all the members.
	 */
	public void execute(final FunctionContext functionContext) {
		String myName = cache.getName();
		long currentTime = System.currentTimeMillis();

		try {

			if(!(functionContext instanceof RegionFunctionContext)) {
				throw new Exception("'" + this.getId() + "' must be run on a region.");
			}

			RegionFunctionContext regionFunctionContext = (RegionFunctionContext) functionContext;
			Region<?,?> region = regionFunctionContext.getDataSet();
			String regionName = region.getFullPath();

			LOGGER.debug("Export of {} begins", regionName);

			Object args = functionContext.getArguments();
			long startTime;
			if(args!=null) {
				startTime = Long.parseLong(args.toString());
				long diff = currentTime - startTime;
				if(Math.abs(diff) > 1000L) {
					LOGGER.warn("Export of {}, provided timestamp {} different from reality by {}ms", regionName, startTime, diff);
				}
			} else {
				startTime = currentTime;
			}

			if(PartitionRegionHelper.isPartitionedRegion(region)) {
				region = PartitionRegionHelper.getLocalPrimaryData(region);
			}
			
			ExportResponse exportResponse = this.exportRegion(region, myName, startTime, "", ExportFileType.ADP_DEFAULT_FORMAT);
			
			LOGGER.debug("Export of {} ends, result {}", regionName, exportResponse);
			
			functionContext.getResultSender().lastResult(exportResponse);
		} catch (Exception exception) {
			LOGGER.debug("Export failed:", exception.getMessage());
			RuntimeException serializableException = new RuntimeException(exception.getMessage());
	        serializableException.setStackTrace(exception.getStackTrace());
	        functionContext.getResultSender().sendException(serializableException);
		}
	}

	public String getId() {
		return FunctionCatalog.PARALLEL_EXPORT_FN;
	}

	public boolean hasResult() {
		return true;
	}

	/* Do not divert export processing to other nodes (which will have already
	 * run export). Make the client aware and the whole snapshot can be repeated.
	 */
	public boolean isHA() {
		return false;
	}

	public boolean optimizeForWrite() {
		return false;
	}

	public void init(final Properties arg0) {
		cache = CacheFactory.getAnyInstance();
		LOGGER = LogManager.getLogger(this.getClass());
	}

}
