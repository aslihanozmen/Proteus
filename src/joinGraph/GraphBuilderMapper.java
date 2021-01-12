package joinGraph;

import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class GraphBuilderMapper extends Mapper<LongWritable, Text, Text, BooleanWritable> {
    private Text pair = new Text();
    private final static BooleanWritable tru = new BooleanWritable(true);

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        String[] ids = line.split(",");

        if (ids.length <= 1) {
            return;
        }
        for (int i = 0; i < ids.length - 1; i++) {
            String id1 = ids[i];

            for (int j = i + 1; j < ids.length; j++) {
                String id2 = ids[j];


                if (id1.compareTo(id2) > 0) {
                    pair.set(id2 + "," + id1);
                } else {
                    pair.set(id1 + "," + id2);
                }

                context.write(pair, tru);
            }
        }
    }
}
