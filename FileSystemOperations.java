
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.logging.Logger;

public class FileSystemOperations {

    private static final Logger logger = Logger.getLogger("..");
    private static final String hdfsuri = "hdfs://xxx:8020/";
    private static final String local = "..";

    /**
     * call recursive method
     * @param hdfs_dir hdfs directory
     * @param conf hdfs configuration
     * @return all file status as an array
     * @throws IOException input exception
     */
    public ArrayList<FileStatus> scanDir(String hdfs_dir, Configuration conf) throws IOException {
        FileSystem fileSystem = FileSystem.get(URI.create(hdfsuri), conf);
        Path mssPath = new Path(hdfs_dir);

        if(!fileSystem.exists(mssPath)) {
            logger.info("Path "+ hdfs_dir +" does not exist.");
            System.exit(1);
        }

        ArrayList<FileStatus> allDirFiles = new ArrayList<>();
        dirFiles(fileSystem, mssPath, allDirFiles);
        return allDirFiles;
    

    /**
     * recursively get all files in a hdfs directory
     * @param fileSystem FileSystem
     * @param path hdfs path
     * @param outArray store all files in the ArrayList
     * @throws IOException input exception
     */
    private void dirFiles(FileSystem fileSystem, Path path, ArrayList<FileStatus> outArray) throws IOException {
        FileStatus[] status = fileSystem.listStatus(path);
        for (FileStatus fs: status) {
            if (fs.isDirectory()) {
                dirFiles(fileSystem, fs.getPath(), outArray);
            } else {
                outArray.add(fs);
            }
        }
    }

    /**
     * add a file from local filesystem to hdfs
     * @param source file path in local filesystem
     * @param dest hdfs destination
     * @param conf hdfs configuration
     * @throws IOException input exception
     */
    public void addFile(String source, String dest, Configuration conf) throws IOException{

        FileSystem fileSystem = FileSystem.get(URI.create(hdfsuri), conf);

        // Get file name from file path
        String filename = source.substring(source.lastIndexOf('/') + 1);

        // HDFS path to put
        if (dest.charAt(dest.length() - 1) != '/') {
            dest = dest + '/' + filename;
        } else {
            dest = dest + filename;
        }
        Path path = new Path(dest);
        logger.info("Adding file to " + dest);

        // Check file exist or not in HDFS
        if (fileSystem.exists(path)) {
            logger.info("File " + dest + " already exists");
        }

        // Open file as input stream
        File file = new File(source);
        InputStream in = new BufferedInputStream(new FileInputStream(file));
        String fileContent = org.apache.commons.io.IOUtils.toString(in);

        // Create new file and write data to it
        FSDataOutputStream out = fileSystem.create(path);
        out.writeBytes(fileContent);

        // Optional way addFile to HDFS
//        int buffer_Size = (int)file.length();
//        byte[] b = new byte[buffer_Size];
//        int numBytes = 0;
//        while ((numBytes = in.read(b)) > 0) {
//            out.write(b, 0, numBytes);
//        }

        // Close all the file descriptors
        in.close();
        out.close();
        fileSystem.close();
    }

    /**
     * read a file from hdfs to local.
     * @param file file path to read in hdfs
     * @param conf hdfs configuration
     * @return MicroStackShots binary file name, excludes directory path
     * @throws IOException input exception
     */
    public String readFile(String file, Configuration conf) throws IOException{
        FileSystem fileSystem = FileSystem.get(URI.create(hdfsuri), conf);

        // Get the filename out of file path
        String filename = file.substring(file.lastIndexOf('/') + 1);

        // HDFS file path to read
        Path path = new Path(file);

        // Local output path to put
        String output_path_str = local + filename;
        Path output_path = new Path(output_path_str);

        if (!fileSystem.exists(path)) {
            System.out.println("File" + file + " does not exists");
            logger.info("File" + file + " does not exists");
            return null;
        }

        // Open the file to input stream
        FSDataInputStream in = fileSystem.open(path);
        FileStatus status =fileSystem.getFileStatus(path);

        // Output Stream
        OutputStream out = new FileOutputStream(output_path_str);
        int buffer_size = Integer.parseInt(String.valueOf(status.getLen()));
        IOUtils.copyBytes(in, out, buffer_size,true);

        // Close all the file descriptors
        in.close();
        out.close();
        fileSystem.close();

        return filename;
    }

    public void copyFile() {

    }

    public void deleteFile() {

    }

    public void mkdir(String dir, Configuration conf) throws IOException {
        FileSystem fileSystem = FileSystem.get(URI.create(hdfsuri), conf);
        Path path = new Path(dir);
        if (fileSystem.exists(path)) {
            logger.info("Dir " + dir + " already exists");
            return;
        }

        fileSystem.mkdirs(path);

        fileSystem.close();
    }
}
