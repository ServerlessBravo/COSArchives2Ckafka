/*
 * This Java source file was generated by the Gradle 'init' task.
 */
package Java8.ETL;

import com.alibaba.fastjson.JSONObject;
import com.qcloud.cos.COSClient;
import com.qcloud.cos.ClientConfig;
import com.qcloud.cos.auth.BasicCOSCredentials;
import com.qcloud.cos.auth.COSCredentials;
import com.qcloud.cos.model.GetObjectRequest;
import com.qcloud.cos.model.PutObjectRequest;
import com.qcloud.cos.region.Region;
import com.qcloud.services.scf.runtime.events.CosEvent;
import net.ipip.ipdb.City;
import net.ipip.ipdb.CityInfo;
import net.ipip.ipdb.IPFormatException;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.config.SaslConfigs;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.concurrent.Future;
import java.util.zip.GZIPInputStream;

public class App {
  public static final String GZ_SUFFIX = ".gz";
  public static final String TAR_GZ_SUFFIX = ".tar.gz";
  private static final String REGION_NAME = "ap-guangzhou";
  private static final String SECRET_ID = "<replace by secret id>";
  private static final String SECRET_KEY = "<replace by secret key>";
  private static final String TARGET_BUCKET = "<replace_by_bucket_name>";
  private static final String APP_ID = "<replace by account appid>";

  private final COSClient client;
  private final City cityDb;

  public String mainHandler(CosEvent event) throws IOException {
    List<String> logFiles = downloadLogGz(event);
    List<String> logFilenames = unzipLogFile(logFiles);
    List<String> newLogFilenames = transformLogs(logFilenames);
    saveToCos(newLogFilenames);

    return "FUNCTION END --- " + new Date();
  }

  public App() throws IOException {
    COSCredentials cred = new BasicCOSCredentials(SECRET_ID, SECRET_KEY);
    Region region = new Region(REGION_NAME);
    ClientConfig clientConfig = new ClientConfig(region);
    client = new COSClient(cred, clientConfig);

    InputStream in = App.class.getClassLoader().getResourceAsStream("ip.ipdb");
    cityDb = new City(in);
  }

  private void sendToCKafka(List<String> logs) {
    System.out.println("?????????????????? --- " + new Date());
    // ??????JAAS????????????????????????
    CKafkaConfigurer.configureSaslPlain();
    // ??????kafka.properties???
    Properties kafkaProperties = CKafkaConfigurer.getCKafkaProperties();
    Properties props = new Properties();
    // ????????????????????????????????????????????????Topic???????????????
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getProperty("bootstrap.servers"));
    // ???????????????
    props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
    // Plain?????????
    props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
    // ????????????Kafka??????????????????????????????
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    // ??????????????????????????????
    props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 30 * 1000);
    // ????????????????????????????????????
    props.put(ProducerConfig.RETRIES_CONFIG, 5);
    // ????????????????????????????????????
    props.put(ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG, 3000);
    // ???????????????????????????15M
    props.put("message.max.bytes", "" + 1024 * 1024 * 15);
    // ??????Producer????????????????????????????????????????????????????????????????????????????????????Producer???????????????
    KafkaProducer<String, String> producer = new KafkaProducer<>(props);
    // ????????????????????????Kafka????????????
    String topic = kafkaProperties.getProperty("topic"); // ???????????????Topic???????????????????????????????????????????????????
    int messageCount = 1;
    for (String log : logs) {
      try {
        // ????????????Future????????????????????????????????????????????????????????????
        List<Future<RecordMetadata>> futures = new ArrayList<>(200);
        // ??????????????????????????????Future?????????

        ProducerRecord<String, String> kafkaMessage = new ProducerRecord<>(topic, log);
        Future<RecordMetadata> metadataFuture = producer.send(kafkaMessage);
        futures.add(metadataFuture);

        if (messageCount == 200) {
          producer.flush();
          for (Future<RecordMetadata> future : futures) {
            // ????????????Future??????????????????
            RecordMetadata recordMetadata = future.get();
            System.out.println("Produce ok:" + recordMetadata.toString());
          }
          messageCount = 1;
        } else {
          messageCount++;
        }
      } catch (Exception e) {
        // ?????????????????????????????????????????????????????????????????????????????????
        System.out.println("error occurred");
      }
    }
    System.out.println("?????????????????? --- " + new Date());
  }

  private void saveToCos(List<String> filenames) {
    System.out.println("?????????????????? --- " + new Date());
    String targetFolder = "logs/";
    for (String filename : filenames) {
      File localFile = new File(filename);
      // ????????????????????? COS ????????????????????????????????????????????????folder/picture.jpg????????????????????? picture.jpg ????????? folder
      // ?????????
      String key = targetFolder + localFile.getName();
      PutObjectRequest putObjectRequest = new PutObjectRequest(TARGET_BUCKET, key, localFile);
      client.putObject(putObjectRequest);
    }
    System.out.println("?????????????????? --- " + new Date());
  }

  private List<String> unzipLogFile(List<String> filenames) throws IOException {
    System.out.println("?????????????????? --- " + new Date());
    List<String> logFilenames = new ArrayList<>();
    for (String filename : filenames) {
      if (filename.endsWith(TAR_GZ_SUFFIX)) {
        List<String> logs = deCompressTarGzip(Paths.get(filename), Paths.get(filename).getParent());
        String firstLog = logs.get(0);
        if (firstLog.endsWith(GZ_SUFFIX)) {
          for (String log : logs) {
            Path target = Paths.get(log.substring(0, log.lastIndexOf(GZ_SUFFIX)));
            extracted(new File(log), target.toFile());
            logFilenames.add(target.toString());
          }
        } else {
          logFilenames.addAll(logs);
        }
      } else if (filename.endsWith(GZ_SUFFIX)) {
        Path source = Paths.get(filename);
        Path target = Paths.get(filename.substring(0, filename.lastIndexOf(GZ_SUFFIX)));
        File targetFile = target.toFile();
        extracted(source.toFile(), targetFile);
        logFilenames.add(targetFile.getAbsolutePath());
      }
    }
    System.out.println("?????????????????? --- " + new Date());
    return logFilenames;
  }

  private List<String> deCompressTarGzip(Path source, Path target) throws IOException {
    if (Files.notExists(source)) {
      throw new IOException("??????????????????????????????");
    }

    List<String> newLogFiles = new ArrayList<>();

    // InputStream??????????????????????????????tar.gz????????????????????????
    // BufferedInputStream???????????????
    // GzipCompressorInputStream???????????????
    // TarArchiveInputStream???tar????????????
    try (InputStream fi = Files.newInputStream(source);
        BufferedInputStream bi = new BufferedInputStream(fi);
        GzipCompressorInputStream gzi = new GzipCompressorInputStream(bi);
        TarArchiveInputStream ti = new TarArchiveInputStream(gzi)) {

      ArchiveEntry entry;
      while ((entry = ti.getNextEntry()) != null) {

        // ??????????????????????????????????????????????????????
        Path newPath = zipSlipProtect(entry, target);
        newLogFiles.add(newPath.toString());

        if (entry.isDirectory()) {
          // ????????????????????????
          Files.createDirectories(newPath);
        } else {
          // ??????????????????????????????????????????
          Path parent = newPath.getParent();
          if (parent != null) {
            if (Files.notExists(parent)) {
              Files.createDirectories(parent);
            }
          }
          // ????????????????????????TarArchiveInputStream??????????????????newPath??????
          Files.copy(ti, newPath, StandardCopyOption.REPLACE_EXISTING);
        }
      }
    }
    return newLogFiles;
  }

  // ?????????????????????????????????????????????????????????????????????
  private Path zipSlipProtect(ArchiveEntry entry, Path targetDir)
      throws IOException {

    Path targetDirResolved = targetDir.resolve(entry.getName());
    Path normalizePath = targetDirResolved.normalize();

    if (!normalizePath.startsWith(targetDir)) {
      throw new IOException("????????????????????????: " + entry.getName());
    }
    return normalizePath;
  }

  private void extracted(File sourceFile, File targetFile) throws IOException {
    try (GZIPInputStream gis = new GZIPInputStream(new FileInputStream(sourceFile));
        FileOutputStream fos = new FileOutputStream(targetFile)) {
      // copy GZIPInputStream to FileOutputStream
      byte[] buffer = new byte[1024];
      int len;
      while ((len = gis.read(buffer)) > 0) {
        fos.write(buffer, 0, len);
      }
    }
  }

  private List<String> transformLogs(List<String> logFilenames) {
    System.out.println("?????????????????? --- " + new Date());
    List<String> newLogFilenames = new ArrayList<>();
    if (!logFilenames.isEmpty()) {
      for (String filename : logFilenames) {
        System.out.println("?????????????????? --- " + filename + " -- " + new Date());
        String newFilename = filename.replace(".log", "-new.log");
        List<String> jsonLogs = new ArrayList<>();
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(newFilename))) {
          try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
            String line;
            while ((line = br.readLine()) != null) {
              String[] items = line.split("\\|");
              String date = items[0];
              String log_host = items[1];
              String log_remote_addr = items[2];
              log_remote_addr = transformIP(log_remote_addr);
              String log_request_method = items[3];
              String log_request_uri = items[4];
              String log_scheme = items[5];
              String log_response_status = items[6];
              String log_bytes_send = items[7];
              String log_request_time = items[8];
              String log_referer = items[9];
              Map<String, String> map = new HashMap<>();
              map.put("date", date);
              map.put("log_host", log_host);
              map.put("log_remote_addr", log_remote_addr);
              map.put("log_request_method", log_request_method);
              map.put("log_request_uri", log_request_uri);
              map.put("log_scheme", log_scheme);
              map.put("log_response_status", log_response_status);
              map.put("log_bytes_send", log_bytes_send);
              map.put("log_request_time", log_request_time);
              map.put("log_referer", log_referer);
              String jsonLog = JSONObject.toJSONString(map);
              bw.write(jsonLog);
              bw.newLine();
              jsonLogs.add(jsonLog);
            }
            System.out.println("?????????????????? --- " + filename + " -- " + new Date());
          } catch (IOException e) {
            e.printStackTrace();
          }
        } catch (Exception e) {
          e.printStackTrace();
          System.out.println(e.getMessage());
        }
        // ?????????KAFKA
        sendToCKafka(jsonLogs);
        newLogFilenames.add(newFilename);
      }
    }
    System.out.println("?????????????????? --- " + new Date());
    return newLogFilenames;
  }

  private String transformIP(String log_remote_addr) throws IOException {
    try {
      CityInfo info = cityDb.findInfo(log_remote_addr, "CN");
      return String.format("%s-%s-%s-%s", info.getCountryName(), info.getRegionName(), info.getCityName(),
          info.getIspDomain());
    } catch (IPFormatException e) {
      System.out.println(e.getMessage());
      return log_remote_addr;
    }
  }

  private List<String> downloadLogGz(CosEvent event) {
    System.out.println("?????????????????? --- " + new Date());
    List<String> files = new ArrayList<>();
    List<CosEvent.Record> records = event.getRecords();
    for (CosEvent.Record record : records) {
      CosEvent.CosInfo cosInfo = record.getCos();
      System.out.println(cosInfo);
      String bucket = cosInfo.getCosBucket().getName() + "-" + APP_ID;
      String key = cosInfo.getCosObject().getKey();
      key = key.replace('/' + APP_ID + '/' + cosInfo.getCosBucket().getName() + '/', "");
      String outputFilePath = "/tmp/";
      File downloadFile = new File(outputFilePath + key);
      GetObjectRequest getObjectRequest = new GetObjectRequest(bucket, key);
      client.getObject(getObjectRequest, downloadFile);
      files.add(downloadFile.getAbsolutePath());
    }
    System.out.println("?????????????????? --- " + new Date());
    return files;
  }

  public static void main(String[] args) {
    // code to test the mainHadnler
  }
}
