package com.example.hadoophomework;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SortReducer extends Reducer<DoubleWritable, Text, Text, Text> {
    private Text dateOut = new Text();
    private Text flowOut = new Text();

    public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text val : values) {
            String[] columns = val.toString().split(",");
            if (columns.length != 3) {
                continue;
            }
            dateOut.set(columns[0]);
            flowOut.set(columns[1] + "," + columns[2]);
            context.write(dateOut, flowOut);
        }
    }
}
