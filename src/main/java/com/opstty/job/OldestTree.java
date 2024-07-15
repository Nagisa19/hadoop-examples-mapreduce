package com.opstty.job;

import com.opstty.mapper.OldestTreeMapper;
import com.opstty.reducer.OldestTreeReducer;
import com.opstty.writable.TreeInfoWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class OldestTree {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: OldestTreeJob <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Oldest Tree");

        job.setJarByClass(OldestTree.class);
        job.setMapperClass(OldestTreeMapper.class);
        job.setReducerClass(OldestTreeReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TreeInfoWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
