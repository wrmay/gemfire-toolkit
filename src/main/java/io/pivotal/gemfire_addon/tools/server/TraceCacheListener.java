package io.pivotal.gemfire_addon.tools.server;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.CacheEvent;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.CacheListener;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.RegionEvent;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.pdx.JSONFormatter;
import com.gemstone.gemfire.pdx.PdxInstance;


public class TraceCacheListener implements CacheListener<Object, Object> {

	private LogWriter log;
	private String regionName;
	
	
	public TraceCacheListener(String regionName){
		log = CacheFactory.getAnyInstance().getLogger();
		this.regionName = regionName;
	}
	
	@Override
	public void close() {
	}

	@Override
	public void afterCreate(EntryEvent<Object, Object> entryEvent) {
		message("created key " +  format(entryEvent.getKey()) + " in " + regionName , entryEvent );
	}

	@Override
	public void afterDestroy(EntryEvent<Object, Object> entryEvent) {
		message("destroyed key " +  format(entryEvent.getKey()) + " in " + regionName  , entryEvent );
	}

	@Override
	public void afterInvalidate(EntryEvent<Object, Object> entryEvent) {
		message("invalidated key " +  format(entryEvent.getKey()) + " in " + regionName  , entryEvent );
	}

	@Override
	public void afterRegionClear(RegionEvent<Object, Object> regionEvent) {
		message("cleared region " + regionName, regionEvent);
	}

	@Override
	public void afterRegionCreate(RegionEvent<Object, Object> regionEvent) {
		// will never see this
	}

	@Override
	public void afterRegionDestroy(RegionEvent<Object, Object> regionEvent) {		
		message("destroyed region " + regionName, regionEvent);
	}

	@Override
	public void afterRegionInvalidate(RegionEvent<Object, Object> regionEvent) {
		message("invalidated region " + regionName, regionEvent);
	}

	@Override
	public void afterRegionLive(RegionEvent<Object, Object> regionEvent) {
		// nothing to do here
	}

	@Override
	public void afterUpdate(EntryEvent<Object, Object> entryEvent) {
		message("updated key " +  format(entryEvent.getKey()) + " in " + regionName  , entryEvent );
	}

	private void message(String msg, CacheEvent event){
		DistributedMember member = event.getDistributedMember();
		log.info(msg);
	}
	
	
	private String format(Object obj){
		if (obj instanceof PdxInstance){
			return JSONFormatter.toJSON((PdxInstance) obj);
		} else{ 
			try {
				return obj.toString();
			} catch(Exception x){
				return "instance of " + obj.getClass().getName();
			}
		}
	}
}
