package wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * <p>Title:WordCountSortJob</p>
 * <p>Description: </p>
 *
 * @author zhuxl
 * @version v1.0
 * @date 2019/6/30 15:19
 */
public class WordCountCommonJob {

    public static void main(String[] args) throws Exception {

        // 1. 创建job
        Job job = Job.getInstance(new Configuration());
        job.setJobName("wordCountNormal");
        job.setJarByClass(WordCountCommonJob.class);

        // 2. 设置mapper及其输出类型
        job.setMapperClass(WordCountCommonMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //设置排序规则
        job.setSortComparatorClass(WordCountComparator.class);

        /*添加combiner*/
        // job.setCombinerClass(WordCountCommonReducer.class);

        // 3. 设置reduce及其输出类型
        job.setReducerClass(WordCountCommonReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 4. 设置输入输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 5. 运行job
        job.waitForCompletion(true);
    }
}
