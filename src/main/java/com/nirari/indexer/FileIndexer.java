package com.nirari.indexer;

import com.nirari.db.FileIndex;
import org.apache.commons.io.FilenameUtils;

import java.io.File;
import java.sql.SQLException;
import java.util.*;
import java.util.zip.Adler32;
import java.util.zip.Checksum;

public class FileIndexer {
	
	/**
	 * Represents a filesystem index
	 */
	private FileIndex fileIndex = new FileIndex();
	
	/**
	 * A {@link Queue<File>} which stores all the directories to be indexed.
	 * Initially, only contains root directories, but when indexing is in progress,
	 * directories will be automatically added.
	 */
	private Queue<File> directories;
	
	/**
	 * @param directories Root directories to begin indexing
	 * @throws SQLException If a read and/or write operation fails on the filesystem index.
	 */
	public FileIndexer(Collection<File> directories) throws SQLException {
		this.directories = new ArrayDeque<>(directories);
	}
	
	
	/**
	 * Creates a filesystem index for the specified {@link FileIndexer#directories}.
	 *
	 * @throws SQLException If the {@link FileIndex} is unable to add a file to the file index
	 */
	public void indexDirectories() throws SQLException {

		while (!directories.isEmpty()) {
			File current = directories.poll();
			File[] files = current.listFiles();

			if (files != null) {
				
				for (File file : files) {
					if (file.isDirectory()) {
						fileIndex.addDirectory(file.getAbsolutePath(), generateChecksum(file));
						fileIndex.commit(); //Commit changes every time a new directory is reached
						
						directories.offer(file); //Add file (or directory) to queue so it can be indexed
						
					}
					else {
						fileIndex.addFile(file.getAbsolutePath(), file.getName(),
								FilenameUtils.getExtension(file.getName()));
					}
				}
				
			}
		}
		
		fileIndex.commit(); //Ensure all files are written to the disk
	}
	
	
	/**
	 * Compares the filesystem index to the actual filesystem,
	 * and returns a {@link Set<File>} containing all files that differ from the filesystem index.
	 * @param root
	 * @return
	 * @throws SQLException
	 */
	public Set<File> detectChange(File root) throws SQLException {
		Set<File> changedFiles = new HashSet<>();
		ArrayDeque<File> folders = new ArrayDeque<>();
		
		folders.add(root);

		while (!folders.isEmpty()) {
			File currentFolder = folders.poll();
			File[] contents = currentFolder.listFiles();

			if (!checksumMatches(currentFolder)) { //Directory has been modified since last indexed
				changedFiles.add(currentFolder);
			}

			if (contents != null) {
				for (File content : contents) {
					if (content.isDirectory()) {
						if (!checksumMatches(content)) {
							folders.offer(content); //Need to recursively check contents to isolate changed subdirectory
						}
					}
					else {
						if (!fileIndex.containsFile(content.getAbsolutePath())) { //If file has not been indexed
							
							fileIndex.addFile(content.getAbsolutePath(), content.getName(),
									FilenameUtils.getExtension(content.getName()));
							
							changedFiles.add(content);
						}
					}
				}
			}
		}

		return changedFiles;
	}
	
	/**
	 * Determines if the checksum for a directory on the filesystem matches the stored checksum in the filesystem index
	 * @param path Path of the directory to check
	 * @return true if checksum matches, false otherwise
	 * @throws SQLException If the filesystem index cannot be accessed
	 */
	public boolean checksumMatches(File path) throws SQLException {
		return generateChecksum(path) == fileIndex.getChecksum(path.getAbsolutePath());
	}
	
	/**
	 * Generates an {@link Adler32} checksum for a given directory
	 *
	 * The generated checksum is based on the directory path, it's last modified date,
	 * as well as the file paths of all subfiles and subdirectories.
	 *
	 * @param dir File to generate a checksum for
	 * @return A checksum for the given file
	 */
	public static long generateChecksum(File dir) {
		Checksum checksum = new Adler32();
		File[] files = dir.listFiles();

		StringBuilder stringBuilder = new StringBuilder();
		stringBuilder.append(dir.getAbsolutePath());
		stringBuilder.append(dir.lastModified());

		if (files != null) {
			stringBuilder.append(files.length);

			for (File file : files) {
				stringBuilder.append(file.getName());
			}
		}

		checksum.update(stringBuilder.toString().getBytes(), 0, stringBuilder.length());
		return checksum.getValue();
	}
}
