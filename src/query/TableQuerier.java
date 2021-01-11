package query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.concurrent.ExecutionException;

import org.apache.lucene.queryparser.classic.ParseException;

import util.StemHistogram;
import util.StemMap;
import util.MatchCase;

public interface TableQuerier {

    public abstract ArrayList<MatchCase> findTables(
            StemMap<StemMap<HashSet<MatchCase>>> keyToImages,
            StemMap<StemHistogram> knownExamples, int examplesPerQuery) throws IOException, ParseException, InterruptedException, ExecutionException;

}