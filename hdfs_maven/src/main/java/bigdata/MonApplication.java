//=====================================================================
/**
 * Squelette minimal d'une application Hadoop
 * A exporter dans un jar sans les librairies externes
 * A exécuter avec la commande ./hadoop jar NOMDUFICHER.jar ARGUMENTS....
 */
package bigdata;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MonApplication {

    public static class MonProg extends Configured implements Tool {

        public int run(String[] args) throws Exception {
            System.out.println("Hello World");
            //CODE DE VOTRE PROGRAMME ICI
            String src = "";
            URI dst = new URI("user/root/input/ouabid/newFile.txt");
            dst = dst.normalize();
            Configuration conf = getConf();
            FileSystem fs = FileSystem.get(dst, conf, "hadoop");
            Path dstPath = new Path(dst.getPath());

            OutputStream outputStream = fs.create(dstPath);
            InputStream inputStream=  new BufferedInputStream(new FileInputStream(src));
            IOUtils.copyBytes(inputStream, outputStream, conf);

            outputStream.close();
            inputStream.close();
            return 0;
        }
    }

    public static void main(String[] args) throws Exception {
        int returnCode = ToolRunner.run(new MonApplication.MonProg(), args);
        System.exit(returnCode);
    }
}
//=====================================================================

