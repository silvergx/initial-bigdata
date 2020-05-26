package com.initialbigdata.mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * @author silvergx on 2020/5/26.
 */
public class InvertedIndex {

    public static class Map extends Mapper<Object, Text, Text, Text> {

        //key:单词 + 所在文件名
        private Text keyInfo = new Text();
        //value：词频
        private Text valueInfo = new Text();
        //文件分割
        private FileSplit split;

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            split = (FileSplit) context.getInputSplit();
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                int splitIndex = split.getPath().toString().indexOf("file");
                keyInfo.set(itr.nextToken() + "," + split.getPath().toString().substring(splitIndex));
                valueInfo.set("1");
                context.write(keyInfo, valueInfo);
            }
        }
    }
}
