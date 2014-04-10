package me.hunsoo.wikipedia.job;

import me.hunsoo.wikipedia.PageRank;
import me.hunsoo.wikipedia.mapper.PageRankIterationMapper;
import me.hunsoo.wikipedia.reducer.PageRankIterationReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class PageRankCalculationJob {
    /**
     * Represents a single iteration of PageRank calculation.
     * Typically 20 iterations are enough to stabilize PageRank output.
     * @param inputPath Input file path which is from the previous iteration of pagerank
     * @param outputPath Input file path which is from the current iteration of pagerank
     * @throws java.io.IOException
     */
    public void calculatePageRank(String inputPath, String outputPath)
            throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration(), "Calculate PageRank");

        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.setMapperClass(PageRankIterationMapper.class);
        job.setReducerClass(PageRankIterationReducer.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        job.setJarByClass(PageRank.class);
        job.waitForCompletion(true); // set parameter to true for verbose progress logs
    }
}
