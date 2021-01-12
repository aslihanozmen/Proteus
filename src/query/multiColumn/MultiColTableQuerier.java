package query.multiColumn;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ExecutionException;

import org.apache.lucene.queryparser.classic.ParseException;

import util.MultiColMatchCase;
import util.StemMultiColHistogram;
import util.StemMultiColMap;

public interface MultiColTableQuerier {

    public abstract ArrayList<MultiColMatchCase> findTables(
            StemMultiColMap<StemMultiColHistogram> knownExamples, int examplesPerQuery) throws IOException, ParseException, InterruptedException, ExecutionException;

    ArrayList<MultiColMatchCase> findTables(Collection<String>[] xs,
                                            int examplesPerQuery) throws IOException, ParseException,
            InterruptedException, ExecutionException;

}