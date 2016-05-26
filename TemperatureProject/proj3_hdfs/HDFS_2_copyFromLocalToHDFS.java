import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;

class HDFS_2_copyFromLocalToHDFS{

	public static void main(String[] args) throws IOException{
		Configuration conf = new Configuration();
 
		FileSystem fileSystem = FileSystem.get(conf);

		HDFS_2_copyFromLocalToHDFS hdfs = new HDFS_2_copyFromLocalToHDFS();
		hdfs.copyFromLocal(args[0], args[1]);		
	}

public void copyFromLocal (String source, String dest) throws IOException {
 
Configuration conf = new Configuration();
conf.addResource(new Path("/home/hadoop/hadoop/conf/core-site.xml"));
conf.addResource(new Path("/home/hadoop/hadoop/conf/hdfs-site.xml"));
conf.addResource(new Path("/home/hadoop/hadoop/conf/mapred-site.xml"));
 
FileSystem fileSystem = FileSystem.get(conf);
Path srcPath = new Path(source);
 
Path dstPath = new Path(dest);
// Check if the file already exists
if (!(fileSystem.exists(dstPath))) {
System.out.println("No such destination " + dstPath);
return;
}
 
// Get the filename out of the file path
String filename = source.substring(source.lastIndexOf('/') + 1, source.length());
 
try{
fileSystem.copyFromLocalFile(srcPath, dstPath);
System.out.println("File " + filename + "copied to " + dest);
}catch(Exception e){
System.err.println("Exception caught! :" + e);
System.exit(1);
}finally{
fileSystem.close();
}
}

}
