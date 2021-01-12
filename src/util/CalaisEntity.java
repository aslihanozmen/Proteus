package util;

import java.util.ArrayList;

public class CalaisEntity
{
	public String uri = null;
	public ArrayList<String> resolvedURIs = new ArrayList<String>();
	public String typeURI = null;
	
	
	@Override
	public boolean equals(Object obj)
	{
		if(obj == null)
		{
			return false;
		}
		if(! (obj instanceof CalaisEntity))
		{
			return false;
		}
		CalaisEntity e2 = (CalaisEntity) obj;
		
		if(uri.equals(e2.uri))
		{
			return true;
		}
		
		for(String resolvedURI : resolvedURIs)
		{
			if(e2.resolvedURIs.contains(resolvedURI))
			{
				return true;
			}
		}
		
		//TODO: type? should we make the return similarity score instead of boolean equals()?
		
		
		return false;
	}
	
}
