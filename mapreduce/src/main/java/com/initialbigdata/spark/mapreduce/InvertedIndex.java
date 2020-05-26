package com.initialbigdata.spark.mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * 倒排索引
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

        /**
         * input: fileA(hello world world) fileB(hello) fileC(world) fileD(hello hello world)
         * output: <hello_fileA,1> <world_fileA,1> <world_fileA,1> <hello_fileB,1> <world_fileC,1> <hello_fileD,1> <hello_fileD,1> <world_fileD,1>
         */
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            split = (FileSplit) context.getInputSplit();
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                int splitIndex = split.getPath().toString().indexOf("file");
                keyInfo.set(itr.nextToken() + "_" + split.getPath().toString().substring(splitIndex));
                valueInfo.set("1");
                context.write(keyInfo, valueInfo);
            }
        }
    }

    public static class Combine extends Reducer<Text, Text, Text, Text> {

        private Text info = new Text();

        /**
         * input: <hello_fileA,1> <world_fileA,1> <world_fileA,1> <hello_fileB,1> <world_fileC,1> <hello_fileD,1> <hello_fileD,1> <world_fileD,1>
         * output: <hello,fileA_1> <world,fileA_2> <hello,fileB_1> <world,fileC_1> <hello,fileD_2> <world,fileD_1>
         */
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (Text value : values) {
                sum += Integer.parseInt(value.toString());
            }
            int splitIndex = key.toString().indexOf("_");
            info.set(key.toString().substring(splitIndex + 1) + "_" + sum);
            key.set(key.toString().substring(0, splitIndex));
            context.write(key, info);
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();

        /**
         * input: <hello,fileA_1> <world,fileA_2> <hello,fileB_1> <world,fileC_1> <hello,fileD_2> <world,fileD_1>
         * output: <hello,fileA_1;fileB_1;fileD_2> <world,fileA_2;fileC_1;fileD_1>
         */
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String fileList = new String();
            for (Text value : values) {
                fileList += value.toString() + ";";
            }
            result.set(fileList);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "InvertedIndex");
        job.setJarByClass(InvertedIndex.class);

        job.setMapperClass(Map.class);
        job.setCombinerClass(Combine.class);
        job.setReducerClass(Reduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // 设置Reduce输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.waitForCompletion(true);
    }
}
