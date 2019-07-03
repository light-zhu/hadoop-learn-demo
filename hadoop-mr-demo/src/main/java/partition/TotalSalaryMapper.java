package partition;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * <p>Title:TotalSalaryMapper</p>
 * <p>Description: </p>
 *
 * @author zhuxl
 * @version v1.0
 * @date 2019/6/30 21:18
 */
public class TotalSalaryMapper extends Mapper<LongWritable, Text, IntWritable, Employee> {

    @Override
    protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {
        //7369,SMITH,CLERK,7902,1980/12/17,800,,20
        String data = v1.toString();

        String[] words = data.split(",");

        Employee employee  = new Employee();
        employee.setEmpno(Integer.parseInt(words[0]));
        employee.setEname(words[1]);
        employee.setJob(words[2]);
        try {
            employee.setMgr(Integer.parseInt(words[3]));
        }catch (Exception ex){
            employee.setMgr(-1);
        }

        employee.setHiredate(words[4]);
        employee.setSal(Integer.parseInt(words[5]));
        try {
            employee.setComm(Integer.parseInt(words[6]));
        }catch (Exception ex){
            employee.setComm(-1);
        }
        employee.setDeptno(Integer.parseInt(words[7]));


        context.write(new IntWritable(Integer.parseInt(words[7])),employee);
    }
}
