package com.nirari.db;

import java.sql.*;

public class FileIndex {
	
	private static final String DB_FILE_NAME = "index.db";
	
	/**
	 * Connection string specifying an SQLite database stored in the file {@value DB_FILE_NAME}
	 */
	private static final String DB_URL = "jdbc:sqlite:" + DB_FILE_NAME;
	
	/**
	 * Connection to the SQLite database specified in {@link FileIndex#DB_URL}
	 */
	private Connection connection;

	private PreparedStatement addFileStmt;
	private PreparedStatement addDirStmt;
	private PreparedStatement getFileStmt;

	public FileIndex() {
		try {
			this.connection = DriverManager.getConnection(DB_URL); //Establish connection
			
			//SQL statement to create a table for storing file details
			String createFileTable =
					"CREATE TABLE IF NOT EXISTS `files` (" +
						"`directory` VARCHAR(30000)," +
						"`name` VARCHAR(256), " +
						"`type` VARCHAR(256)," +
						"PRIMARY KEY (`directory`, `name`)" +
					");";
			
			//SQL statement to create a table for storing folder details
			String createFoldersTable =
					"CREATE TABLE IF NOT EXISTS `folders` (" +
							"`directory` VARCHAR(30000)," +
							"`checksum` BIGINT," +
							"PRIMARY KEY (`directory`)" +
					");";
			
			//Create the files and folders table if they do not exist
			connection.createStatement().execute(createFileTable);
			connection.createStatement().execute(createFoldersTable);
			
			//Generate the prepared statements for adding files/folders to the respective tables
			//Replace into is used in order to update existing columns if the file/folder is already indexed,
			//or create it othgerwise
			this.addFileStmt = this.connection.prepareStatement("REPLACE INTO files VALUES (?,?,?);");
			this.addDirStmt = this.connection.prepareStatement("REPLACE INTO folders VALUES (?,?);");
			
			//Returns count of rows matching specified path
			//Given the architecture of the system, will either return 0 or 1
			//Used to determine if a directory has been indexed before
			this.getFileStmt = this.connection.prepareStatement("SELECT COUNT(*) FROM files WHERE directory = ?;");

			this.connection.setAutoCommit(false); //Don't auto-commit after every query - manual commits will increase speed
		}
		catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Adds a given file to the database, in the files table
	 * @param dir Directory of the file
	 * @param name Name of the file
	 * @param type Extension of the file such as {@code jpg}, or {@code png}
	 * @throws SQLException If the database cannot be accessed
	 */
	public void addFile(String dir, String name, String type) throws SQLException {
		addFileStmt.clearParameters();
		addFileStmt.setString(1, dir);
		addFileStmt.setString(2, name);
		addFileStmt.setString(3, type);
		addFileStmt.executeUpdate();
	}
	
	/**
	 * Determines if the database contains an entry corresponding to the specific directory
	 * @param dir Directory to search the database for
	 * @return true if database contains directory, false otherwise
	 * @throws SQLException If database cannot be communicated with
	 */
	public boolean containsFile(String dir) throws SQLException {
		getFileStmt.setString(1, dir);
		ResultSet resultSet = getFileStmt.executeQuery();

		//If the ResultSet is closed, or the query returns an integer less than zero, then
		//the database does not contain the given directory
		return !resultSet.isClosed() && resultSet.getLong(1) > 0;
	}
	
	/**
	 * Adds a given directory to the database
	 * @param dir Path of directory to add to database
	 * @param checksum Checksum for the given directory
	 * @throws SQLException If the database cannot be communicated with
	 */
	public void addDirectory(String dir, long checksum) throws SQLException {
		addDirStmt.clearParameters();
		addDirStmt.setString(1, dir);
		addDirStmt.setLong(2, checksum);
		addDirStmt.executeUpdate();
	}
	
	/**
	 * Commits all pending queries, writing them to the database on disk
	 * @throws SQLException
	 */
	public void commit() throws SQLException {
		this.connection.commit();
	}
	
	/**
	 * Terminate the connection
	 * @throws SQLException
	 */
	public void close() throws SQLException {
		this.connection.close();
	}
	
	/**
	 * Gets the stored checksum for the given directory
	 *
	 * @param path Path of directory to get stored checksum for
	 * @return Stored checksum of the given directory, else -1 if database does not contain an entry for it
	 * @throws SQLException
	 */
	public long getChecksum(String path) throws SQLException {
		PreparedStatement preparedStatement =  this.connection.prepareStatement("SELECT checksum FROM folders WHERE directory = ?;");
		preparedStatement.setString(1, path);
		
		ResultSet resultSet = preparedStatement.executeQuery();

		return resultSet.isClosed() ? -1L : resultSet.getLong(1);
	}


}
