package partition;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * <p>Title:MyPartitioner</p>
 * <p>Description: </p>
 *
 * @author zhuxl
 * @version v1.0
 * @date 2019/7/1 9:39
 */
public class MyPartitioner extends Partitioner<IntWritable, Employee> {

    //分3个分区
    public int getPartition(IntWritable k2, Employee v2, int partitionNum) {

        if (v2.getDeptno() == 10) {
            //放入1号分区
            return 1 % partitionNum;
        } else if (v2.getDeptno() == 20) {
            //放入2号分区
            return 2 % partitionNum;
        } else
            //放入3号分区
            return 3 % partitionNum;
    }
}
