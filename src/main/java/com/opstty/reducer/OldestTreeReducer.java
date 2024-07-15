package com.opstty.reducer;

import com.opstty.writable.TreeInfoWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class OldestTreeReducer extends Reducer<Text, TreeInfoWritable, Text, Text> {
    public void reduce(Text key, Iterable<TreeInfoWritable> values, Context context) throws IOException, InterruptedException {
        int oldestAge = Integer.MIN_VALUE;
        String oldestDistrict = "";

        for (TreeInfoWritable value : values) {
            if (value.getAge().get() > oldestAge) {
                oldestAge = value.getAge().get();
                oldestDistrict = value.getDistrict().toString();
            }
        }
        context.write(new Text("Oldest tree district"), new Text(oldestDistrict));
    }
}
