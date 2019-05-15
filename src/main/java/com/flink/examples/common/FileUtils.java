package com.flink.examples.common;

import java.io.File;

public class FileUtils {

	public static void deleteDirectory(String srcPath) throws Exception{
		File file = new File(srcPath);
		if(file.isDirectory()){
			File[] files = file.listFiles();
			for(File f:files){
				f.delete();
			}
		}
	}
	
	public static void deleteFile(String srcPath) throws Exception{
		File file = new File(srcPath);
		file.delete();
	} 
	
}
