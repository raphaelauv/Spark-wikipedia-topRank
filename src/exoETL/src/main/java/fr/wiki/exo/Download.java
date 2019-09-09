package fr.wiki.exo;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;

import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Download {

  private static final Logger logger         = LogManager.getLogger(Download.class);
  private static       String BLACK_LIST     = "blacklist_domains_and_pages";
  private static       String BLACK_LIST_URL = "https://s3.amazonaws.com/dd-interview-data/data_engineer/wikipedia/blacklist_domains_and_pages";

  static String computeWikiFileName(String[] dateHour) {
    String date = dateHour[0];
    String hour = dateHour[1];
    return "pageviews-" + date + "-" + hour + "0000";
  }

  private static String computeWikiUrl(String[] dateHour) {
    String year = dateHour[0].substring(0, 4);
    String month = dateHour[0].substring(4, 6);

    String fileName = computeWikiFileName(dateHour);

    return "https://dumps.wikimedia.org/other/pageviews/" + year + "/" + year + "-" + month + "/" + fileName + ".gz";

  }

  /**
   * //TODO
   *
   * @param dateHour
   * @return
   * @throws IOException
   */
  static String downloadPageView(String[] dateHour) throws IOException {

    String fileName = computeWikiFileName(dateHour);
    String url = computeWikiUrl(dateHour);
    download(url, fileName + ".gz");

    decompressGZIP(new File(fileName + ".gz"), new File(fileName));

    return fileName;
  }

  static String downloadBlackList() throws IOException {

    download(BLACK_LIST_URL, BLACK_LIST);

    return BLACK_LIST;
  }

  static void download(String url, String path) throws IOException {

    logger.info("Download " + url + " to " + path);

    FileUtils.copyURLToFile(
      new URL(url),
      new File(path),
      10000,
      10000);

  }

  static boolean fileExist(String pathFile) {
    File tempFile = new File(pathFile);
    return tempFile.exists();
  }

  private static void decompressGZIP(File input, File output) throws IOException {
    logger.info("decompress " + input + " to " + output);

    try (GzipCompressorInputStream in = new GzipCompressorInputStream(new FileInputStream(input))) {
      IOUtils.copy(in, new FileOutputStream(output));
    }
  }
}
