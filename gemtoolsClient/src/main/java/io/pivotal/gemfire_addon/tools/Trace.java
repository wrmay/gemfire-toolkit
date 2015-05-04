package io.pivotal.gemfire_addon.tools;

import io.pivotal.gemfire_addon.functions.FunctionCatalog;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.gemstone.gemfire.cache.client.Pool;
import com.gemstone.gemfire.cache.execute.Execution;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.cache.execute.ResultCollector;

public class Trace {
	private static String locatorString = null;
	private static String regionName = null;
	
	public static void main(String []args){
		int rc = 1;
		try {
			parseArgs(args);

			initCache(locatorString);
			
			Pool pool = ClientCacheFactory.getAnyInstance().getDefaultPool();
			
			Execution exec = FunctionService.onServers(pool)
					.withArgs(regionName)
					.withCollector(new PrintResultCollector());
			
			ResultCollector coll = exec.execute(FunctionCatalog.TRACE_FN.toString());
			coll.getResult();
			
			rc = 0;
			
		} catch(Exception x){
			x.printStackTrace(System.err);
		} finally {
			ClientCache cache = ClientCacheFactory.getAnyInstance();
			if (cache != null) cache.close();
		}
		
		System.exit(rc);
	}
	
	
	private static void initCache(String locatorString){
		ClientCacheFactory factory = new ClientCacheFactory();
		setupPools(factory, locatorString);
		factory.create();
		System.out.println("connected to distributed system  with locator " + locatorString);
	}
	
	
	// this will need to be enhanced to support server groups
	private static void setupPools(ClientCacheFactory ccf, String locator){
		Pattern pattern = Pattern.compile("(.*)\\[(.*)\\]");
		Matcher matcher = pattern.matcher(locator);
		
		if (!matcher.matches())
			throw new RuntimeException("could not parse locator string: " + locator);
		
		String host = matcher.group(1);
		int port = Integer.parseInt(matcher.group(2));
		
		ccf.addPoolLocator(host, port);
	}
	
	private static void parseArgs(String []args){
		if (args.length != 2) {
			printUsage();
			System.exit(1);
		}
	
		locatorString = args[0];
		regionName = args[1];
	}
	
	private static void printUsage(){
		System.err.println("usage: trace locator-host[port] region-name");
	}
 }
