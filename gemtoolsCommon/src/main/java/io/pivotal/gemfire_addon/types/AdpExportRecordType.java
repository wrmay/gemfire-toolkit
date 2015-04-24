package io.pivotal.gemfire_addon.types;

public enum AdpExportRecordType {
	HEADER ((byte)0)
	,HINT ((byte)1)
	,DATA ((byte)2)
	,FOOTER ((byte)4)
	;
	
	private final byte b;
	
	AdpExportRecordType(Byte b) {
		this.b = b;
	}
	
	public byte getB() {
		return this.b;
	}
	
	public String toString() {
		return this.b + "";
	}
}
