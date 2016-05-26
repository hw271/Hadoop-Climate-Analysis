import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HDFS_3_copyToLocal{

	public static void main(String[] args) throws IOException{

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", args[0]);
        FileSystem fs = FileSystem.get(conf);
        FileStatus[] status = fs.listStatus(new Path(args[0]));
        for(int i=0;i<status.length;i++){
            System.out.println(status[i].getPath());
            fs.copyToLocalFile(false, status[i].getPath(), new Path("localdir"));
        }



	}	

}
