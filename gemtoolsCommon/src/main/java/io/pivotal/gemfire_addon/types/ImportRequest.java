package io.pivotal.gemfire_addon.types;

import java.io.File;
import java.io.Serializable;

public class ImportRequest implements Serializable {
	private static final long serialVersionUID = 1L;
	
	private transient File file;
	private String  fileDir;
	private String	fileName;
	private String	member;
	
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
	public String getMember() {
		return member;
	}
	public void setMember(String member) {
		this.member = member;
	}
	@Override
	public String toString() {
		return "ImportRequest [fileDir=" + fileDir + ", fileName=" + fileName
				+ ", member=" + member + "]";
	}

}
