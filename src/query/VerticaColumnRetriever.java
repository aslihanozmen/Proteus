package query;

import gnu.trove.map.hash.TObjectIntHashMap;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;

import com.vertica.jdbc.VerticaConnection;

import main.WTTransformerMultiColSets;
import util.StemHistogram;
import util.StemMap;
import util.MatchCase;

public class VerticaColumnRetriever implements TableQuerier {
	VerticaConnection con;

	public VerticaColumnRetriever(VerticaConnection con) {
		this.con = con;
	}


	@Override
	public ArrayList<MatchCase> findTables(
			StemMap<StemMap<HashSet<MatchCase>>> keyToImages,
			StemMap<StemHistogram> knownExamples, int tau)
	{
		ArrayList<String> tablesMatched = new ArrayList<String>();
		
		if (tau > knownExamples.size()) {
			throw new IllegalStateException();
		}

		ArrayList<String> xs = new ArrayList<>();
		ArrayList<String> ys = new ArrayList<>();
		int m = 0;
		for (String x : knownExamples.keySet()) {
			xs.add(x);
			m++;

			for (String y : knownExamples.get(x).getCountsUnsorted().keySet()) {
				ys.add(y);
			}
			if (ys.size()>m) {
				for (;m<ys.size();m++) {
					xs.add(x);
				}
			}
		}

		TObjectIntHashMap <MatchCase> candidates = new TObjectIntHashMap<MatchCase>();
		ArrayList<MatchCase> results = new ArrayList<MatchCase>();

		try {
			PreparedStatement ps = con.prepareStatement("SELECT col1.tableid, col1.colid, col2.colid " +
                    "FROM (SELECT tableid, colid, rowid from tokenized_to_col where tokenized = ?) AS col1," +
                    "(SELECT tableid, colid, rowid from tokenized_to_col where tokenized = ?) AS col2 " +
                    "WHERE col1.rowid = col2.rowid AND col1.colid <> col2.colid AND col1.tableid = col2.tableid");
			for (int j = 0; j < xs.size(); j++) {
				ps.setObject(1, xs.get(j));
				ps.setObject(2, ys.get(j));
				ResultSet rs = ps.executeQuery();

				while (rs.next()) {
					
					int id  = rs.getInt(1);
					String externalid = WTTransformerMultiColSets.idToExternalId.get(id);
					MatchCase triple = new MatchCase(
							externalid, rs.getInt(2), rs.getInt(3));
					if (candidates.contains(triple)) {
						candidates.put(triple, candidates.get(triple)+1);
					}else {
						candidates.put(triple,1);
					}
				}
				rs.close();			
			}
			ps.close();
			for ( MatchCase candidate : candidates.keySet()) {
				if (candidates.get(candidate) >=tau)
				results.add(candidate);
				tablesMatched.add(candidate.tableID);
				
			}

		} catch (SQLException e) {
			e.printStackTrace();
		}

		return results;
	}

}
