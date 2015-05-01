package io.pivotal.gemfire_addon.functions;

/* Function names need to be available to clients, but not implementation
 * 
 *TODO CacheInitializer could autoload these ? Don't need in XML
 */
public enum FunctionCatalog {
	PARALLEL_EXPORT_FN ("ADPParallelExport"),
	PARALLEL_IMPORT_FN ("ADPParallelImport");
	
	private String s;
	
	FunctionCatalog(String s) {
		this.s = s;
	}
	
	public String toString() {
		return this.s;
	}
}
