package enrichment;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import au.com.bytecode.opencsv.CSVWriter;
import gnu.trove.map.hash.TObjectIntHashMap;
import model.Table;
/**
 * This class maintains a grid that has pointers to alternative cell values and their frequency scores.
 * @author abedjan
 *
 */
public class ValueAlternatives {
	private Table inputTable;
	private Map<String, TObjectIntHashMap<String>> alternativeCellValues = new HashMap<>(); 
	
	public ValueAlternatives(Table inputTable) {
		this.inputTable = inputTable;
	}
	
	public void addAlternativeCellValue(int row,int column, String value){
		String cell = row+","+column;
		if (alternativeCellValues.containsKey(cell)) {
			TObjectIntHashMap<String> values = alternativeCellValues.get(cell);
			if (values.contains(value)) {
				values.increment(value);
			}else {
				values.put(value, 1);
			}
		} else {
			TObjectIntHashMap<String> values = new TObjectIntHashMap<>();
			values.put(inputTable.getRow(row).getCell(column).getValue(),1);
			values.put(value, 1);
			alternativeCellValues.put(cell, values);
			
		}
	}

	public void printAlternatives() {
		try {
			CSVWriter writer = new CSVWriter(new FileWriter(new File("errorCells.csv")));
			for (String cell: alternativeCellValues.keySet()) {
				TObjectIntHashMap<String> cellValues = alternativeCellValues.get(cell);
				for (String value :cellValues.keySet()) {
					writer.writeNext(new String[]{cell, value, String.valueOf(cellValues.get(value))});
				}// ENDFOR cellValues
			}// ENDFOR alternativeCellValues
			writer.flush();
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		
	}
	
	

}
