package model;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;

import util.MatchCase;

public class FileSystemTableLoader implements TableLoader
{
	String dir = null;
	public FileSystemTableLoader(String dir) throws IOException
	{
		this.dir = new File(dir).getCanonicalPath() + File.separator;
	}
	
	@Override
	public Table loadTable(MatchCase triple) throws SQLException
	{
		String path = triple.tableID;
		Table t = new Table(dir + path);
		return t;
	}
	
}
