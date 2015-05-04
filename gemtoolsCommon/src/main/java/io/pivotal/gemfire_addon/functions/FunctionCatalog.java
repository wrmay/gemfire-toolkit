package io.pivotal.gemfire_addon.functions;

/* Function names need to be available to clients, but not implementation
 * 
 *TODO CacheInitializer could autoload these ? Don't need in XML
 */
public enum FunctionCatalog {
	PARALLEL_EXPORT_FN ("ADPParallelExport"),
	PARALLEL_IMPORT_FN ("ADPParallelImport"),
	TOUCH_FN		   ("Touch"),
	TRACE_FN		   ("Trace"),
	UNTRACE_FN		   ("Untrace");
	
	private String s;
	
	FunctionCatalog(String s) {
		this.s = s;
	}
	
	public String toString() {
		return this.s;
	}
}
