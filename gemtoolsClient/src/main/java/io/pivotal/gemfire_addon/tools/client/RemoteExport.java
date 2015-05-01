package io.pivotal.gemfire_addon.tools.client;

import io.pivotal.gemfire_addon.types.ExportFileType;
import com.gemstone.gemfire.cache.Region;

/**
 * <P>An enhanced version of export functionality, complementary to GFSH's "{@code export data}"
 * but parallelized so faster. See also {@link io.pivotal.gemfire_addon.tools.client.ImportExport} which
 * is a more simple serial equivalent.
 * </P>
 * <P>
 * How this version works is to execute a Gemfire function <I>onRegion()</I>. What this means is
 * the function code {@link io.pivotal.gemfire_addon.functions.ADPParallelExport} is executed on
 * every server that hosts data for this region. <B>Each server exports the data that only it
 * has a copy of.</B>
 * </P>
 * <P>
 * On a replicated region, each server has a complete copy of the data. The function runs on
 * one server. The amount of work done is essentially the same as for a normal export.
 * </P>
 * <P>
 * On a partitioned region, each server has part of the data. If there were 10 servers, than
 * might mean each has a unique 1/10th. The function runs in parallel on all servers at once,
 * each exports its 1/10th, and the run time is about 1/10th of a serial export. This is
 * a substantial time saving.
 * </P>
 * <P>Drawbacks
 * <OL>
 * <LI>
 * <P>
 * Each server writes its export file to the local filesystem. This means multiple files
 * are produced as there are multiple servers, and these could be on multiple hosts for a
 * cluster. These files will need collated to serve as a complete backup, although you
 * can use them individually.
 * </P>
 * <P>Collating the files, and transporting to an appropriate location via {@code sFtp} or
 * similar should be a fairly trivial scripting task.
 * </P>
 * </LI>
 * <LI><P>Similar to GFSH and {@link io.pivotal.gemfire_addon.tools.client.ImportExport} the
 * cluster is not paused while this extract runs, and so it may or may not include updates
 * happening concurrently.</P>
 * <P>It is best if the cluster is quiet if it can't be idle while the export runs as this
 * reduces the doubt on which in-flight updates are included. Running any maintenance activity
 * is best while the cluster is quiet anyway.
 * </P>
 * </LI>
 * </OL>
 * </P>
 * <HR/>
 * <P>Usage:
 * </P>
 * <P>{@code java RemoteExport} <I>host[port],host[port] region1 region2 regi*</I>
 * <P>Eg:
 * </P>
 * <P>{@code java RemoteExport 51.19.239.100[10355],51.19.239.87[10355] objects}
 * </P>
 * <P>The first argument is a pair of locators, in the same format as used in
 * a Gemfire properties file on the server side.
 * </P>
 * <P>The second and any subsequent arguments list the regions to be extracted.
 * These can be named completely (eg. "users"), partially (eg. "user*") or
 * all regions can be selected (ie. "*").
 * </P>
 */
public class RemoteExport extends DataExport {
	
	public static void main(String[] args) throws Exception {
		new RemoteExport().process(args);
		System.exit(error?1:0);
	}
	
	/* Use the function mechanism for export. This runs on the servers in
	 * parallel, with each server exporting its share of the data
	 */
	protected void exportRegion(Region<?, ?> region, String member_not_used, String host_not_used,
			long globalstarttime2, String tmpDir, ExportFileType exportFileType) {
		this.exportRegionFunction(region, globalStartTime, exportFileType);
	}

}
