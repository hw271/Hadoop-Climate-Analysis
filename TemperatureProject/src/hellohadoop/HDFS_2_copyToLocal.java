package hellohadoop;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

class HDFS_2_copyToLocal{

	public static void main(String[] args) throws IOException{
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:9000/user/hw271/testHDFS/");
        FileSystem fs = FileSystem.get(conf);
        FileStatus[] status = fs.listStatus(new Path("./testHDFS"));
        for(int i=0;i<status.length;i++){
            System.out.println(status[i].getPath());
            fs.copyToLocalFile(false, status[i].getPath(), new Path("localdir"));
        }
	}	

}