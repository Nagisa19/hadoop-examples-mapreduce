package com.opstty.job;

import com.opstty.mapper.TreeHeightSortMapper;
import com.opstty.reducer.TreeHeightSortReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TreeHeightSort {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: TreeHeightSortJob <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Tree Height Sort");

        job.setJarByClass(TreeHeightSort.class);
        job.setMapperClass(TreeHeightSortMapper.class);
        job.setReducerClass(TreeHeightSortReducer.class);

        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
