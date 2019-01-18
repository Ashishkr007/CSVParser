/**
 * 
 */
package CSVDataValidator.CSVParser;

import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;


/**
 * @author Ashish kumar
 * @email niits007@gmail.com
 * @website riveriq.com
 * @since Dec 22, 2018
 * @version 1.0
 */
public class Parser {
	private char ext;
	private String dff;
	FileReader fileReader = null;
	CSVParser csvFileParser = null;
	private boolean success;
	private String data;
	private String Gdata;
	private String Bdata;
	private String fname;
	private String tempPath;
	private String tempgdpath;
	private String tempbdpath;
	private long rowcount = 0;
	CSVFormat csvFileFormat = null;
	ArrayList<Long> Lines = new ArrayList<Long>();
	HashMap<String, Long> gdbd = new HashMap<String, Long>();
	List<CSVRecord> csvRecords;
	List<CSVRecord> csvRecords1;
	long gcount = 0;
	long bcount = 0;

	public void CSVParser() {
		try {
			String datafilepath = "E:\\StudyDoc\\ProjectsWS\\JavaWS\\CSVParser\\testDir\\SourceData\\Source001\\Feed001\\Feed001TestData_20180202.CSV";
			String datafilename = "Feed001TestData_20180202.CSV";
			String tempdatafilepath = "E:\\StudyDoc\\ProjectsWS\\JavaWS\\CSVParser\\testDir\\SourceData\\Source001\\Feed001\\temp";
			String gooddatapath = "E:\\StudyDoc\\ProjectsWS\\JavaWS\\CSVParser\\testDir\\ValidatedData\\Source001\\Feed001\\data";
			String baddatapath = "E:\\StudyDoc\\ProjectsWS\\JavaWS\\CSVParser\\testDir\\ValidatedData\\Source001\\Feed001\\badrecords";
			ext = ',';
			dff = ".CSV";
			int delimetercount = 36;

			fileReader = new FileReader(datafilepath);
			fname = datafilename.substring(0, datafilename.lastIndexOf("."));

			CsvParserSettings parserSettings = new CsvParserSettings();
			parserSettings.setLineSeparatorDetectionEnabled(true);
			// parserSettings.setHeaderExtractionEnabled(true);
			parserSettings.setAutoConfigurationEnabled(true);
			parserSettings.setQuoteDetectionEnabled(true);
			// parserSettings.detectFormatAutomatically();
			parserSettings.setIgnoreLeadingWhitespaces(true);
			parserSettings.setIgnoreTrailingWhitespaces(true);
			parserSettings.getFormat().setDelimiter(ext);
			// parserSettings.setSkipEmptyLines(true);
			parserSettings.setKeepQuotes(true);
			// parserSettings.setParseUnescapedQuotes(false);
			CsvParser parser = new CsvParser(parserSettings);

			parser.beginParsing(fileReader);
			List<String[]> goodrows = new ArrayList<String[]>();
			List<String[]> badrows = new ArrayList<String[]>();

			String[] row;
			long i = 1;
			String[] headers = null;

			while ((row = parser.parseNext()) != null) {
				if (row.length - 1 == delimetercount) {
					goodrows.add(row);
				} else {
					Lines.add(Long.parseLong(String.valueOf(i)));
					badrows.add(row);
				}
				rowcount = i;
				i++;
			}
			parser.stopParsing();

			if (Lines.size() > 0) {
				tempgdpath = tempdatafilepath + "\\" + fname + "_GoodRecords" + dff;
				tempbdpath = tempdatafilepath + "\\" + fname + "_BadRecords" + dff;

				File fgd = new File(tempgdpath);
				File fbd = new File(tempbdpath);

				if (fgd.exists()) {
					fgd.delete();
					System.out.println("deleted " + fgd.getPath());
				}
				if (fbd.exists()) {
					System.out.println("deleted " + fbd.getPath());
					fbd.delete();
				}

				gdbd = UnivocityCleanWithGDBD(tempgdpath, tempbdpath, goodrows, badrows, headers);

				try {
					Bdata = baddatapath + "\\" + fname + "_BadRecords" + dff;
					// Odata = gooddatapath + "\\" + datafilename;
					Gdata = gooddatapath + "\\" + fname + "_GoodRecords" + dff;

					fileReader = new FileReader(tempgdpath);
					ValidateFiles(tempgdpath, delimetercount);
					fileCopy(tempbdpath, Bdata, null);
					fileCopy(tempgdpath, Gdata, null);
					// fileCopy(datafilepath, Odata, rp);

					if (fgd.exists()) {
						fgd.delete();
						System.out.println("deleted " + fgd.getPath());
					} else {
						System.out.println("File does not exist " + fgd.getPath());
					}
					if (fbd.exists()) {
						System.out.println("deleted " + fbd.getPath());
						fbd.delete();
					} else {
						System.out.println("File does not exist " + fbd.getPath());
					}

				} catch (Exception ex) {
					System.out.println("delimeter good data Exception" + ex);
				} finally {
					if (csvFileParser != null)
						csvFileParser.close();
					if (fileReader != null)
						fileReader.close();
				}
				Lines.clear();
			} else {

				String temppath = tempdatafilepath + "\\" + fname + dff;
				gdbd = UnivocityCleanWithoutGDBD(temppath, goodrows, headers);
				ValidateFiles(temppath, delimetercount);

				data = gooddatapath + "\\" + fname + dff;
				fileCopy(temppath, data, null);
				File fds = new File(temppath);

				if (fds.exists()) {
					fds.delete();
					System.out.println("deleted " + fds.getPath());
				} else {
					System.out.println("File does not exist " + fds.getPath());
				}
			}
		} catch (Exception e) {
		}
	}

	public void ValidateFiles(String file, int delimetercount) throws IOException {
		ArrayList<Long> lines = new ArrayList<Long>();
		File Gdata = new File(file);
		if (!Gdata.exists()) {
			System.out.println("temp file not found");
		}
		CSVParser csvFileParser = null;
		FileReader fileReader = null;
		try {
			fileReader = new FileReader(Gdata);
			csvFileFormat = CSVFormat.DEFAULT.withQuote('"').withDelimiter(ext);
			csvFileParser = new CSVParser(fileReader, csvFileFormat);
			long i = 1;
			for (CSVRecord csvRecord : csvFileParser) {
				if (i % 10000 == 0) {
					System.out.println("Records Processed:-" + i);
				}
				if (csvRecord.size() - 1 == delimetercount) {
					success = true;
				} else {
					lines.add(Long.parseLong(String.valueOf(i)));
				}
				rowcount = i;
				i++;
			}

			if (lines.size() > 0) {
				success = false;
			} else {
				success = true;
				System.out.println("validated with apache csv");
			}

		} catch (Exception e) {
		} finally {
			try {
				if (csvFileParser != null)
					csvFileParser.close();
				if (fileReader != null)
					fileReader.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}
	}

	public HashMap<String, Long> UnivocityCleanWithGDBD(String gpath, String bpath, List<String[]> goodrows,
			List<String[]> badrows, String[] headers) throws IOException {

		final String NEW_LINE_SEPARATOR = "\r\n";
		File f = new File(gpath);
		if (!f.getParentFile().exists()) {
			f.getParentFile().mkdirs();
		}
		// Remove if clause if you want to overwrite file
		if (!f.exists()) {
			try {
				f.createNewFile();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		FileWriter fileWriter = null;
		FileWriter fileWriter1 = null;

		try {
			fileWriter = new FileWriter(f);
			fileWriter1 = new FileWriter(bpath);

			if (headers != null) {
				long count0 = 1;
				for (String hdata : headers) {
					if (hdata != null) {
						fileWriter.append("\"");
						fileWriter.append(hdata.replaceAll("\r\n", " ").replaceAll("\n", " "));
						fileWriter.append("\"");
					} else {
						fileWriter.append("");
					}
					if (count0 < headers.length) {
						fileWriter.append(ext);
					}
					count0++;
				}
				fileWriter.append(NEW_LINE_SEPARATOR);
			}
			long count = 1;

			for (String[] gdata : goodrows) {
				long count1 = 1;

				for (String data : gdata) {
					if (data != null) {
						fileWriter.append("\"");
						fileWriter.append(data.replaceAll("\r\n", " ").replaceAll("\n", " ").replaceAll("\"", ""));
						fileWriter.append("\"");
					} else {
						fileWriter.append("");
					}
					if (count1 < gdata.length) {
						fileWriter.append(ext);
					}
					count1++;
				}
				fileWriter.append(NEW_LINE_SEPARATOR);
				gcount++;
			}

			for (String[] bdata : badrows) {
				long count1 = 1;
				for (String data : bdata) {
					if (data != null) {
						fileWriter1.append(data.replaceAll("\r\n", " ").replaceAll("\n", " "));
					} else {
						fileWriter1.append("");
					}
					if (count1 < bdata.length) {
						fileWriter1.append(ext);
					} else {
						fileWriter1.append("\"");
					}
					count1++;
				}
				fileWriter1.append(NEW_LINE_SEPARATOR);
				bcount++;
			}

			gdbd.put("gcount", gcount);
			gdbd.put("bcount", bcount);

			System.out.println(count);
			System.out.println("CSV file was created successfully !!!");

		} catch (Exception e) {
			System.out.println("Error in ReportWriter !!!");
			e.printStackTrace();
		} finally {
			try {
				fileWriter.flush();
				fileWriter.close();
				fileWriter1.flush();
				fileWriter1.close();
			} catch (IOException e) {
				System.out.println("Error while flushing/closing fileWriter !!!");
				e.printStackTrace();
			}
		}
		return gdbd;
	}

	public void fileCopy(String s, String d, HashMap<String, String> data) throws IOException {
		File source = new File(s);
		if (!source.getParentFile().exists()) {
			source.getParentFile().mkdirs();
		}

		File dest = new File(d);
		if (!dest.getParentFile().exists()) {
			dest.getParentFile().mkdirs();
		}

		if (dest.exists()) {
			try {
				dest.delete();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		copyFileUsingJava7Files(source, dest); // copy using java7
	}

	private static void copyFileUsingJava7Files(File source, File dest) throws IOException {
		Files.copy(source.toPath(), dest.toPath());
	}

	public HashMap<String, Long> UnivocityCleanWithoutGDBD(String path, List<String[]> rows, String[] headers)
			throws IOException {
		final String NEW_LINE_SEPARATOR = "\r\n";
		File f = new File(path);
		if (!f.getParentFile().exists()) {
			f.getParentFile().mkdirs();
		}
		// Remove if clause if you want to overwrite file
		if (!f.exists()) {
			try {
				f.createNewFile();
			} catch (Exception e) {
				e.printStackTrace();
			}

		}
		FileWriter fileWriter = null;
		try {
			fileWriter = new FileWriter(path);
			if (headers != null) {
				long count0 = 1;
				for (String hdata : headers) {
					if (hdata != null) {
						fileWriter.append("\"");
						fileWriter.append(hdata.replaceAll("\r\n", " ").replaceAll("\n", " "));
						fileWriter.append("\"");
					} else {
						fileWriter.append("");
					}
					if (count0 < headers.length) {
						fileWriter.append(ext);
					}
					count0++;
				}
				fileWriter.append(NEW_LINE_SEPARATOR);
			}
			long count = 0;

			for (String[] gdata : rows) {
				long count1 = 1;
				for (String data : gdata) {
					if (data != null) {
						fileWriter.append("\"");
						fileWriter.append(data.replaceAll("\r\n", " ").replaceAll("\n", " ").replaceAll("\"", ""));
						fileWriter.append("\"");
					} else {
						fileWriter.append("");
					}
					if (count1 < gdata.length) {
						fileWriter.append(ext);
					}
					count1++;
				}
				fileWriter.append(NEW_LINE_SEPARATOR);
				count++;
			}

			gdbd.put("count", count);
			System.out.println("CSV file was created successfully !!!");

		} catch (Exception e) {
			System.out.println("Error in ReportWriter !!!");
			e.printStackTrace();
		} finally {

			try {
				fileWriter.flush();
				fileWriter.close();
			} catch (IOException e) {
				System.out.println("Error while flushing/closing fileWriter !!!");
				e.printStackTrace();
			}
		}
		return gdbd;
	}

	public static void main(String[] args) {
		Parser obj_prs = new Parser();
		obj_prs.CSVParser();
	}
}
