package org.epf.hadoop.colfil1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class RelationshipMapper extends Mapper<LongWritable, Relationship, Text, Text> {
    private Text user = new Text();
    private Text friend = new Text();

    @Override
    public void map(LongWritable key, Relationship value, Context context)
            throws IOException, InterruptedException {
        // Emit A -> B
        user.set(value.getId1());
        friend.set(value.getId2());
        context.write(user, friend);

        // Emit B -> A
        user.set(value.getId2());
        friend.set(value.getId1());
        context.write(user, friend);
    }
}