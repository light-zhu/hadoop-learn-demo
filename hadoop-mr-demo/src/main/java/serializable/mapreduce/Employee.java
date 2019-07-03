package serializable.mapreduce;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * <p>Title:Employee</p>
 * <p>Description: </p>
 *
 * @author zhuxl
 * @version v1.0
 * @date 2019/6/30 21:40
 */
public class Employee implements Writable {

    private int empno;
    private String ename;
    private String job;
    private int mgr;
    private String hiredate;
    private int sal;
    private int comm;
    private int deptno;


    public void readFields(DataInput dataInput) throws IOException {
        //反序列化
        this.empno = dataInput.readInt();
        this.ename = dataInput.readUTF();
        this.job = dataInput.readUTF();
        this.mgr = dataInput.readInt();
        this.hiredate = dataInput.readUTF();
        this.sal = dataInput.readInt();
        this.comm = dataInput.readInt();
        this.deptno = dataInput.readInt();
    }

    public void write(DataOutput dataOutput) throws IOException {
        //序列化
        dataOutput.writeInt(empno);
        dataOutput.writeUTF(ename);
        dataOutput.writeUTF(job);
        dataOutput.writeInt(mgr);
        dataOutput.writeUTF(hiredate);
        dataOutput.writeInt(sal);
        dataOutput.writeInt(comm);
        dataOutput.writeInt(deptno);

    }
    public int getEmpno() {
        return empno;
    }

    public void setEmpno(int empno) {
        this.empno = empno;
    }

    public String getEname() {
        return ename;
    }

    public void setEname(String ename) {
        this.ename = ename;
    }

    public String getJob() {
        return job;
    }

    public void setJob(String job) {
        this.job = job;
    }

    public int getMgr() {
        return mgr;
    }

    public void setMgr(int mgr) {
        this.mgr = mgr;
    }

    public String getHiredate() {
        return hiredate;
    }

    public void setHiredate(String hiredate) {
        this.hiredate = hiredate;
    }

    public int getSal() {
        return sal;
    }

    public void setSal(int sal) {
        this.sal = sal;
    }

    public int getComm() {
        return comm;
    }

    public void setComm(int comm) {
        this.comm = comm;
    }

    public int getDeptno() {
        return deptno;
    }

    public void setDeptno(int deptno) {
        this.deptno = deptno;
    }
}
