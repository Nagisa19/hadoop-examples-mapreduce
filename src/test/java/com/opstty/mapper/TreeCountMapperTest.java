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

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class TreeCountMapperTest {
    @Mock
    private Mapper.Context context;
    private TreeCountMapper treeCountMapper;

    @Before
    public void setup() {
        this.treeCountMapper = new TreeCountMapper();
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
        this.treeCountMapper.map(null, new Text(header), this.context);

        // First data line
        this.treeCountMapper.map(null, new Text(value1), this.context);
        String[] fields1 = value1.split(";");
        String district1 = fields1[1];
        System.out.println(district1);
        verify(this.context, times(1)).write(new Text(district1), new IntWritable(1));
    }
}
