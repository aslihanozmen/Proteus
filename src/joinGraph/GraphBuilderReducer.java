package joinGraph;

import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class GraphBuilderReducer extends Reducer<Text, BooleanWritable, Text, BooleanWritable> {
    private final static BooleanWritable tru = new BooleanWritable(true);

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        context.write(key, tru);
    }
}