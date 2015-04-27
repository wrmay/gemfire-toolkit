package io.pivotal.gemfire_addon.functions;

public class FunctionCatalog {

	/*TODO
	 * Server Initializer could autoload these ? Don't need in XML
	 */
	// Function names need to be available to clients, but not implementation
	public static final String	PARALLEL_EXPORT_FN = "ADPParallelExport";
	public static final String	PARALLEL_IMPORT_FN = "ADPParallelImport";
}
