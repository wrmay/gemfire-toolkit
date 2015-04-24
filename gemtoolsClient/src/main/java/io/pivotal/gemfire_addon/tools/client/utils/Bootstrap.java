package io.pivotal.gemfire_addon.tools.client.utils;

import com.gemstone.gemfire.cache.CacheListener;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.gemstone.gemfire.cache.client.ClientRegionFactory;
import com.gemstone.gemfire.cache.client.ClientRegionShortcut;
import com.gemstone.gemfire.cache.client.Pool;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.pdx.PdxInstance;
import io.pivotal.adp_dynamic_region_management.MetadataRegionCacheListener;
import java.util.Set;

/**
 * <P>A helper class to create a {@link com.gemstone.gemfire.cache.client.ClientCache}
 * instance initialized for Dynamic Region Management.
 * </P>
 * <P>The approach taken here is to assume that a {@code cache.xml} file has been defined,
 * and should be augmented with the necessary elements. The alternative is to build the
 * cache via the API, but this then needs a code release to add features unrelated to
 * Dynamic Region Management, such as other connection pools.
 * </P>
 */
public class Bootstrap {
	private static final String 				DYNAMIC_REGION_MANAGEMENT_NAME = "__regionAttributesMetadata";
	private static final String 				DYNAMIC_REGION_MANAGEMENT_POOL = "myPool";
	private static final ClientRegionShortcut 	DYNAMIC_REGION_MANAGEMENT_TYPE = ClientRegionShortcut.CACHING_PROXY;
	private static ClientCache	 				clientCache;
	
	/*  Create a client cache suitable for use with dynamic region management.
	 *  Largely this consists of checking the metadata region has been set-up and
	 *  will be populated by the event mechanism.
	 */
	public static synchronized ClientCache createDynamicCache() throws Exception {
		
		ClientCacheFactory clientCacheFactory = new ClientCacheFactory();
		clientCacheFactory.set("name", System.getProperty("gemfire.name", getClientName()));
		
		clientCache = clientCacheFactory.create();
		
		createRegionAttributesMetadata();
		validateRegionAttributesMetadata();
		activateRegionAttributesMetadata();
			
		return clientCache;
	}
	

	/*  The metadata region is required for dynamic region management, so build it if it not specified
	 *  already in the cache.xml.
	 */
	private static void createRegionAttributesMetadata() {

		Region<String,PdxInstance> __regionAttributesMetadata = clientCache.getRegion(DYNAMIC_REGION_MANAGEMENT_NAME);
		
		if(__regionAttributesMetadata==null) {
			ClientRegionFactory<String,PdxInstance> clientRegionFactory = clientCache.createClientRegionFactory(DYNAMIC_REGION_MANAGEMENT_TYPE);
			clientRegionFactory.setPoolName(DYNAMIC_REGION_MANAGEMENT_POOL);
			
			__regionAttributesMetadata = (Region<String, PdxInstance>) clientRegionFactory.create(DYNAMIC_REGION_MANAGEMENT_NAME);
			
			__regionAttributesMetadata.getAttributesMutator().addCacheListener(new MetadataRegionCacheListener());
		}
		
	}
	

	/*  Check the metadata region has been defined with the correct attributes.
	 */
	private static void validateRegionAttributesMetadata() throws Exception {
		Region<String,PdxInstance> __regionAttributesMetadata = clientCache.getRegion(DYNAMIC_REGION_MANAGEMENT_NAME);
		
		if(__regionAttributesMetadata==null) {
			throw new Exception("Region '" + DYNAMIC_REGION_MANAGEMENT_NAME + "' missing");
		}
		
		if(!__regionAttributesMetadata.getAttributes().getPoolName().equals(DYNAMIC_REGION_MANAGEMENT_POOL)) {
			throw new Exception("Region '" + DYNAMIC_REGION_MANAGEMENT_NAME + "' not using pool '" + DYNAMIC_REGION_MANAGEMENT_POOL + "'");
		}

		boolean dynamicRegionManagementEnabled = false;
		for(CacheListener<String, PdxInstance> cacheListener : __regionAttributesMetadata.getAttributes().getCacheListeners()) {
			if(cacheListener instanceof MetadataRegionCacheListener) {
				dynamicRegionManagementEnabled = true;
			}
		}
		if(!dynamicRegionManagementEnabled) {
			throw new Exception("Region '" + DYNAMIC_REGION_MANAGEMENT_NAME + "' does not have a listener of type '" + MetadataRegionCacheListener.class.getCanonicalName() + "'");
		}
		
		if(!clientCache.getPdxReadSerialized()) {
			//FIXME : Validate if this is needed for client side
			throw new Exception("Cache PDX option should be set for read-serialized");
		}

		Pool myPool = PoolManager.find(DYNAMIC_REGION_MANAGEMENT_POOL);
		if(myPool==null || !myPool.getSubscriptionEnabled()) {
			throw new Exception("Pool '" + DYNAMIC_REGION_MANAGEMENT_POOL + "' must be defined 'subscription-enabled=true'");
		}
		
	}

	
	/*  Get all metadata region entries into the client's cache, now and in the future.
	 *  This will trigger the listener to create the specified regions in the client cache.
	 */
	private static void activateRegionAttributesMetadata() throws Exception {
		Region<String,PdxInstance> __regionAttributesMetadata = clientCache.getRegion(DYNAMIC_REGION_MANAGEMENT_NAME);
		
		Set<String> keys = __regionAttributesMetadata.keySetOnServer();
		for(String key : keys) {
			__regionAttributesMetadata.get(key);
		}

		__regionAttributesMetadata.registerInterest("ALL_KEYS");
	}


	/*  Provide a client cache name, to augment the system generate client cache id, for diagnostics
	 */
	private static String getClientName() {
		return System.getProperty("gemfire.security-username","");
	}
	
}
