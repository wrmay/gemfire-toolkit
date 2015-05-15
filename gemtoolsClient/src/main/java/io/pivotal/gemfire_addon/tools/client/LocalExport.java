package io.pivotal.gemfire_addon.tools.client;

import io.pivotal.gemfire_addon.types.ExportFileType;

import com.gemstone.gemfire.cache.Region;

/**
 * <P>A simple version of export functionality, complementary to GFSH's "{@code export data}".
 * See also {@link io.pivotal.gemfire_addon.tools.client.RemoteExport} which is a more
 * complex counterpart.
 * </P>
 * <P>The difference is that this version of the export pulls all the data across the
 * network to the memory of this process, and writes to the local filesystem. This may
 * be more useful as the file can go in a predicatble place.
 * </P>
 * <P>This is also a better way should bespoke code be needed to encrypt/decrypt
 * or augment/deplete the data.
 * </P>
 * <P>Like GFSH, any specialized domain classes must be on the classpath
 * </P>
 * <P>Drawbacks
 * <OL>
 * <LI>The code uses {@link com.gemstone.gemfire.cache.Region.keySetOnServer} to
 * retrieve back to the clients the entries that are to be exported. If this list
 * is massive the client may run out of memory. Alternatively, the client may time
 * out while waiting on this list to be assembled and returned.
 * </LI>
 * <LI>Similar to GFSH, the cluster is not paused while the extract runs as this
 * would impact on other processes. Updates which are happening at the exact same
 * time may be being committed while this runs and not included.
 * </LI>
 * </OL>
 * </P>
 * <HR/>
 * <P>Usage:
 * </P>
 * <P>{@code java LocalExport} <I>host[port],host[port] region1 region2 regi*</I>
 * <P>Eg:
 * </P>
 * <P>{@code java LocalExport 51.19.239.100[10355],51.19.239.87[10355] objects}
 * </P>
 * <P>The first argument is a pair of locators, in the same format as used in
 * a Gemfire properties file on the server side.
 * </P>
 * <P>The second and any subsequent arguments list the regions to be extracted.
 * These can be named completely (eg. "users"), partially (eg. "user*") or
 * all regions can be selected (ie. "*").
 * </P>
 */
public class LocalExport extends DataExport {
	
	public static void main(String[] args) throws Exception {
		LocalExport localExport = new LocalExport();
		localExport.process(args);
		System.exit(localExport.isError()?1:0);
	}
	
	/* Use the keySet() mechanism for local export.
	 * The client gets all the keys from the server, then retrieves these in a
	 * bloack.
	 */
	protected void exportRegion(Region<?, ?> region, final String member, final String host,
			long globalstarttime2, String tmpDir, ExportFileType exportFileType) {
		exportRegionKeySet(region, member, host, globalStartTime, TMP_DIR, exportFileType);
	}

}
