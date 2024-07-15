package com.opstty.mapper;

import org.apache.hadoop.io.DoubleWritable;
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
public class TreeHeightSortMapperTest {
    @Mock
    private Mapper.Context context;
    private TreeHeightSortMapper treeHeightSortMapper;

    @Before
    public void setup() {
        this.treeHeightSortMapper = new TreeHeightSortMapper();
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
        this.treeHeightSortMapper.map(null, new Text(header), this.context);

        // First data line
        this.treeHeightSortMapper.map(null, new Text(value1), this.context);
        String[] fields1 = value1.split(";");
        double height1 = Double.parseDouble(fields1[6]);
        String address1 = fields1[8];
        verify(this.context, times(1)).write(new DoubleWritable(height1), new Text(address1));

        // Second data line
        this.treeHeightSortMapper.map(null, new Text(value2), this.context);
        String[] fields2 = value2.split(";");
        double height2 = Double.parseDouble(fields2[6]);
        String address2 = fields2[8];
        verify(this.context, times(1)).write(new DoubleWritable(height2), new Text(address2));
    }
}
