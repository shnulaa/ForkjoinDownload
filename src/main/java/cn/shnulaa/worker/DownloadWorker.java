package cn.shnulaa.worker;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.RandomAccessFile;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.atomic.AtomicLong;

import cn.shnulaa.manager.Manager;

public class DownloadWorker extends RecursiveAction {

	/** the default forkJoin thresholds **/
	private static final long THRESHOLDS = (1024 * 1024 * 10); // 1M
	/** one thread download connection timeout **/
	private static final int THREAD_DOWNLOAD_TIMEOUT = 30000;
	/** one thread download max retry count **/
	private static final int THREAD_MAX_RETRY_COUNT = 10;
	/** the instance of Manager **/
	private static final Manager m = Manager.getInstance();

	/**
	 * serialVersionUID
	 */
	private static final long serialVersionUID = -8469875906879091144L;

	private String key;
	private long start;
	private long end;
	private URL url;
	private File dFile;
	private AtomicLong current;

	public DownloadWorker(long start, long end, URL url, File dFile) {
		this.start = start;
		this.current = new AtomicLong(start);
		this.end = end;
		this.url = url;
		this.dFile = dFile;
		this.key = (String.valueOf(this.start) + "-" + String.valueOf(this.end));
	}

	@Override
	public String toString() {
		return String.format("start: %s, end: %s, current: %s", start, end, current);
	}

	@Override
	protected void compute() {
		if (end - start <= THRESHOLDS) {
			try {
				DownloadWorker reTask = null;
				current.set((m.recovery && (reTask = m.get(getKey())) != null) ? reTask.getCurrent() : start);
				m.add(this);
				execute();
			} catch (Exception ex) {
				System.err.println("exception occurred when exec task..");
				ex.printStackTrace();
			} finally {
				// m.remove(this);
			}
		} else {
			long middle = (start + end) / 2;
			ForkJoinTask<?> childTask1 = new DownloadWorker(start, middle, url, dFile);
			ForkJoinTask<?> childTask2 = new DownloadWorker(middle + 1, end, url, dFile);
			invokeAll(childTask1, childTask2);
		}
	}

	private void execute() {
		int retryCount = 0;
		while (retryCount++ < THREAD_MAX_RETRY_COUNT) {
			HttpURLConnection con = null;
			HttpURLConnection.setFollowRedirects(true);
			try {

				con = (HttpURLConnection) url.openConnection();

				// if (useHeader) {
				// addHeader(con);
				// }
				con.setReadTimeout(THREAD_DOWNLOAD_TIMEOUT);
				con.setConnectTimeout(THREAD_DOWNLOAD_TIMEOUT);

				if (getCurrent() >= end) {
					// System.err.println("Already complete..");
					return;
				}
				// System.out.println("bytes=" + getCurrent() + "-" + end);
				con.setRequestProperty("Range", "bytes=" + getCurrent() + "-" + end);
				// System.out.println("Thread name:" +
				// Thread.currentThread().getName() + ", Ready to get
				// bytes="
				// + getCurrent() + "-" + end);
				try (BufferedInputStream bis = new BufferedInputStream(con.getInputStream());
						RandomAccessFile file = new RandomAccessFile(dFile, "rw");) {
					file.seek(getCurrent());
					final byte[] bytes = new byte[1024];

					int readed = 0;
					while ((readed = bis.read(bytes)) != -1) {
						file.write(bytes, 0, readed);
						current.getAndAdd(readed);
						m.alreadyRead.getAndAdd(readed);
					}
					// System.out.println("Thread name:" +
					// Thread.currentThread().getName() + ", end to get
					// bytes="
					// + getCurrent() + "-" + end);
					// if write file successfully
					break;
				} catch (Exception e) {
					System.err.println("exception occurred while download..");
					e.printStackTrace();
					Thread.sleep(1000);
					continue; // write exception or read timeout, retry
				}
			} catch (Exception ex) {
				System.err.println("exception occurred while download..");
				ex.printStackTrace();
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					System.err.println("InterruptedException occurred while download..");
					e.printStackTrace();
				}
				continue;
			} finally {
				if (con != null) {
					con.disconnect();
				}
			}
		}
	}

	public long getStart() {
		return start;
	}

	public long getEnd() {
		return end;
	}

	public long getCurrent() {
		return current.get();
	}

	public String getKey() {
		return key;
	}

	private static void addHeader(URLConnection connection) {
		connection.setRequestProperty("User-Agent",
				"Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.106 Safari/537.36");
		connection.setRequestProperty("Upgrade-Insecure-Requests", "100");
		connection.setRequestProperty("Accept",
				"text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8");
		connection.setRequestProperty("Connection", "keep-alive");
		connection.setRequestProperty("Accept-Encoding", "gzip, deflate, sdch");
		connection.setRequestProperty("Accept-Language", "zh-CN,zh;q=0.8");
		connection.setRequestProperty("Cookie", "kuaichuanid=7737D93877EE6FEA2110BB086960418B"); // thunder

	}
}