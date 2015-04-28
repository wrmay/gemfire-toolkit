package io.pivotal.gemfire_addon.types;

import java.io.File;
import java.io.Serializable;

public class ExportResult implements Serializable {
	private static final long serialVersionUID = 1L;
	
	private transient File file;
	private String  fileDir;
	private String	fileName;
	private int     recordsRead;
	private int		recordsWritten;
	
	public File getFile() {
		return file;
	}
	public void setFile(File file) {
		this.file = file;
	}
	public String getFileDir() {
		return fileDir;
	}
	public void setFileDir(String fileDir) {
		this.fileDir = fileDir;
	}
	public String getFileName() {
		return fileName;
	}
	public void setFileName(String fileName) {
		this.fileName = fileName;
	}
	public int getRecordsRead() {
		return recordsRead;
	}
	public void setRecordsRead(int recordsRead) {
		this.recordsRead = recordsRead;
	}
	public int getRecordsWritten() {
		return recordsWritten;
	}
	public void setRecordsWritten(int recordsWritten) {
		this.recordsWritten = recordsWritten;
	}
	
	@Override
	public String toString() {
		return "ExportResult [fileDir=" + fileDir + ", fileName=" + fileName
				+ ", recordsRead=" + recordsRead + ", recordsWritten="
				+ recordsWritten + "]";
	}

}
