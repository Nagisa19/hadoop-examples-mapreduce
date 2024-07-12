package com.opstty.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.util.Arrays;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class TreeKindReducerTest {
    @Mock
    private Reducer.Context context;
    private TreeKindReducer treeKindReducer;

    @Before
    public void setup() {
        this.treeKindReducer = new TreeKindReducer();
    }

    @Test
    public void testReduce() throws IOException, InterruptedException {
        Text key = new Text("Acer");
        Iterable<IntWritable> values = Arrays.asList(new IntWritable(1), new IntWritable(1), new IntWritable(1));

        this.treeKindReducer.reduce(key, values, this.context);
        verify(this.context, times(1)).write(key, new IntWritable(3));
    }
}
