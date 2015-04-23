package io.pivotal.gemfire_addon.tools.client.utils;

import static org.junit.Assert.*;
import io.pivotal.adp_dynamic_region_management.MetadataRegionCacheListener;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.gemstone.gemfire.cache.CacheListener;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.client.ClientCache;

public class BootstrapTest {
	
	@Mock
	private static ClientCache clientCache;
	
	private static Method activateRegionAttributesMetadata = null;
	private static Method createRegionAttributesMetadata = null;
	private static Method getClientName = null;
	private static Method validateRegionAttributesMetadata = null;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		activateRegionAttributesMetadata = Bootstrap.class.getDeclaredMethod("activateRegionAttributesMetadata");
		activateRegionAttributesMetadata.setAccessible(true);
		createRegionAttributesMetadata = Bootstrap.class.getDeclaredMethod("createRegionAttributesMetadata");
		createRegionAttributesMetadata.setAccessible(true);
		getClientName = Bootstrap.class.getDeclaredMethod("getClientName");
		getClientName.setAccessible(true);
		validateRegionAttributesMetadata = Bootstrap.class.getDeclaredMethod("validateRegionAttributesMetadata");
		validateRegionAttributesMetadata.setAccessible(true);
	}
	
	@Before
	public void setUp() throws Exception {
		MockitoAnnotations.initMocks(this);

		// Mockito doesn't set static fields, and PowerMock is more trouble than value
		Field clientCacheField = Bootstrap.class.getDeclaredField("clientCache");
		clientCacheField.setAccessible(true);
		clientCacheField.set(null, clientCache);
	}


	// validateRegionAttributesMetadata() tests
	
	@Test
	public void testValidate_metadataMissing() {
		Mockito.when(clientCache.getRegion("__regionAttributesMetadata")).thenReturn(null);

		try {
			validateRegionAttributesMetadata.invoke(null, (Object[])null);
			fail("Exception expected");
		} catch (Exception e) {
			assertEquals(e.getCause().getMessage(),"Region '__regionAttributesMetadata' missing");
		}
	}
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testValidate_metadataWrongPool() {
		Region region = Mockito.mock(Region.class);
		RegionAttributes regionAttributes = Mockito.mock(RegionAttributes.class);
		
		Mockito.when(clientCache.getRegion("__regionAttributesMetadata")).thenReturn(region);
		Mockito.when(region.getAttributes()).thenReturn(regionAttributes);
		Mockito.when(regionAttributes.getPoolName()).thenReturn("DEFAULT");
	
		try {
			validateRegionAttributesMetadata.invoke(null, (Object[])null);
			fail("Exception expected");
		} catch (Exception e) {
			assertEquals(e.getCause().getMessage(),"Region '__regionAttributesMetadata' not using pool 'myPool'");
		}
	}
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testValidate_metadataNoListeners() {
		Region region = Mockito.mock(Region.class);
		RegionAttributes regionAttributes = Mockito.mock(RegionAttributes.class);
		
		Mockito.when(clientCache.getRegion("__regionAttributesMetadata")).thenReturn(region);
		Mockito.when(region.getAttributes()).thenReturn(regionAttributes);
		Mockito.when(regionAttributes.getPoolName()).thenReturn("myPool");
		Mockito.when(regionAttributes.getCacheListeners()).thenReturn(new CacheListener[0]);
	
		try {
			validateRegionAttributesMetadata.invoke(null, (Object[])null);
			fail("Exception expected");
		} catch (Exception e) {
			assertEquals(e.getCause().getMessage(),"Region '__regionAttributesMetadata' does not have a listener of type 'io.pivotal.adp_dynamic_region_management.MetadataRegionCacheListener'");
		}
	}
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testValidate_metadataMissingListener() {
		Region region = Mockito.mock(Region.class);
		RegionAttributes regionAttributes = Mockito.mock(RegionAttributes.class);

		// Mocked class will be an instance of the generic class.
		
		CacheListener cacheListener1 = Mockito.mock(CacheListener.class);
		CacheListener cacheListener2 = Mockito.mock(CacheListener.class);
		CacheListener cacheListener3 = Mockito.mock(CacheListener.class);
		CacheListener[] cacheListeners = { cacheListener1, cacheListener2, cacheListener3 } ;
		
		Mockito.when(clientCache.getRegion("__regionAttributesMetadata")).thenReturn(region);
		Mockito.when(region.getAttributes()).thenReturn(regionAttributes);
		Mockito.when(regionAttributes.getPoolName()).thenReturn("myPool");
		Mockito.when(regionAttributes.getCacheListeners()).thenReturn(cacheListeners);
	
		try {
			validateRegionAttributesMetadata.invoke(null, (Object[])null);
			fail("Exception expected");
		} catch (Exception e) {
			assertEquals(e.getCause().getMessage(),"Region '__regionAttributesMetadata' does not have a listener of type 'io.pivotal.adp_dynamic_region_management.MetadataRegionCacheListener'");
		}
	}
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testValidate_notPdxReadSerialized() {
		Region region = Mockito.mock(Region.class);
		RegionAttributes regionAttributes = Mockito.mock(RegionAttributes.class);

		// Mocked class will be an instance of the target class.
		CacheListener[] cacheListeners = { Mockito.mock(MetadataRegionCacheListener.class)};
		
		Mockito.when(clientCache.getRegion("__regionAttributesMetadata")).thenReturn(region);
		Mockito.when(region.getAttributes()).thenReturn(regionAttributes);
		Mockito.when(regionAttributes.getPoolName()).thenReturn("myPool");
		Mockito.when(regionAttributes.getCacheListeners()).thenReturn(cacheListeners);
		Mockito.when(clientCache.getPdxReadSerialized()).thenReturn(false);
		
		try {
			validateRegionAttributesMetadata.invoke(null, (Object[])null);
			fail("Exception expected");
		} catch (Exception e) {
			assertEquals(e.getCause().getMessage(),"Cache PDX option should be set for read-serialized");
		}
	}
	
	// getName() tests
	
	@Test
	public void testGetName() throws Exception {
		String junit = "junit";

		assertNull("System property not set prior to test", System.getProperty("gemfire.security-username"));
		
		String result1 = (String) getClientName.invoke(null, (Object[])null);
		System.setProperty("gemfire.security-username", junit);
		String result2 = (String) getClientName.invoke(null, (Object[])null);
		
		assertNotNull("result1", result1);
		assertEquals("result1", result1.length(), 0);
		assertNotNull("result2", result2);
		assertEquals("result2", result2, junit);
	}
}
