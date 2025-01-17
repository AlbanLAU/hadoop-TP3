package org.epf.hadoop.colfil1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.StringJoiner;

public class RelationshipReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        StringJoiner joiner = new StringJoiner(",");

        for (Text value : values) {
            joiner.add(value.toString());
        }
        context.write(key, new Text(joiner.toString()));
    }
}