package util;

public class MatchCase
{
	public String tableID;
	public int col1;
	public int col2;
	
	public MatchCase(String tableID, int col1, int col2)
	{
		this.tableID = tableID;
		this.col1 = col1;
		this.col2 = col2;
	}

	
	
	@Override
	public String toString()
	{
		return "(" + tableID + ", " + col1 + ", " + col2 + ")";
	}



	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + col1;
		result = prime * result + col2;
		result = prime * result + ((tableID == null) ? 0 : tableID.hashCode());
		return result;
	}



	@Override
	public boolean equals(Object obj)
	{
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		MatchCase other = (MatchCase) obj;
		if (col1 != other.col1)
			return false;
		if (col2 != other.col2)
			return false;
		if (tableID == null)
		{
			if (other.tableID != null)
				return false;
		}
		else if (!tableID.equals(other.tableID))
			return false;
		return true;
	}



	public String getTableID()
	{
		return tableID;
	}



	public void setTableID(String tableID)
	{
		this.tableID = tableID;
	}



	public int getCol1()
	{
		return col1;
	}



	public void setCol1(int col1)
	{
		this.col1 = col1;
	}



	public int getCol2()
	{
		return col2;
	}



	public void setCol2(int col2)
	{
		this.col2 = col2;
	}
	
	
	
}