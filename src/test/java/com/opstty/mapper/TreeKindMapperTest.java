package com.opstty.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;

import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class TreeKindMapperTest {
    @Mock
    private Mapper.Context context;
    private TreeKindMapper treeKindMapper;

    @Before
    public void setup() {
        this.treeKindMapper = new TreeKindMapper();
    }

    @Test
    public void testMap() throws IOException, InterruptedException {
        // Load a sample from the CSV file using a relative path
        URL resource = getClass().getResource("/data/trees.csv");
        BufferedReader reader = new BufferedReader(new FileReader(resource.getFile()));
        String header = reader.readLine();
        String value1 = reader.readLine();
        String value2 = reader.readLine();
        reader.close();

        // Simulate reading the header line
        this.treeKindMapper.map(null, new Text(header), this.context);

        // First data line
        this.treeKindMapper.map(null, new Text(value1), this.context);
        String[] fields1 = value1.split(";");
        verify(this.context, times(1)).write(new Text(fields1[2]), new IntWritable(1));

        // Second data line
        this.treeKindMapper.map(null, new Text(value2), this.context);
        String[] fields2 = value2.split(";");
        verify(this.context, times(1)).write(new Text(fields2[2]), new IntWritable(1));
    }
}
