package com.opstty.writable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class TreeInfoWritable implements Writable {
    private IntWritable age;
    private Text district;

    public TreeInfoWritable() {
        this.age = new IntWritable();
        this.district = new Text();
    }

    public TreeInfoWritable(IntWritable age, Text district) {
        this.age = age;
        this.district = district;
    }

    public IntWritable getAge() {
        return age;
    }

    public Text getDistrict() {
        return district;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        age.write(out);
        district.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        age.readFields(in);
        district.readFields(in);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TreeInfoWritable that = (TreeInfoWritable) o;
        return Objects.equals(age, that.age) && Objects.equals(district, that.district);
    }

    @Override
    public int hashCode() {
        return Objects.hash(age, district);
    }

    @Override
    public String toString() {
        return age + "\t" + district;
    }
}
