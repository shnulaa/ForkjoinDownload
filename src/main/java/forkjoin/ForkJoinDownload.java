package forkjoin;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * "https://github.com/hashem78/android_hardware_qcom_gps/archive/msm8x94.zip");
 * "http://184.164.76.104/pukiwiki.20150612.tar.gz");
 * "http://speed.myzone.cn/pc_elive_1.1.rar");
 * "http://down.360safe.com/cse/360cse_8.5.0.126.exe"); <br>
 * 
 * ForkJoin download
 * 
 * @author liuyq
 * 
 */
public final class ForkJoinDownload {

	/** the file destination folder **/
	private static final String FOLDER = "./";
	/** ForkJoinPool pool size **/
	private static final int POOL_SIZE = 15;
	/** the default forkJoin thresholds **/
	private static final long THRESHOLDS = (1024 * 1024 * 10); // 1M
	/** one thread download connection timeout **/
	private static final int THREAD_DOWNLOAD_TIMEOUT = 30000;
	/** one thread download max retry count **/
	private static final int THREAD_MAX_RETRY_COUNT = 10;
	/** the instance of Manager **/
	private static final Manager m = Manager.getInstance();
	/** is the mode recovery **/
	private static volatile boolean recovery = false;
	private static boolean useHeader = false;

	/**
	 * main
	 * 
	 * @param args
	 * @throws Throwable
	 */
	public static void main(String[] args) throws Throwable {
		if (args == null || args.length < 3) {
			System.err.println("argument error..");
			System.err.println(
					"usage: java -jar [XX.jar] [downloadUrl] [threadNumber] [savedPath] [savedName] [use header]");
			return;
		}

		if (args[0] == null || args[0].isEmpty()) {
			System.err.println("the download url must specified..");
			return;
		}
		final String downloadURL = args[0];

		int threadNumber = POOL_SIZE;
		try {
			threadNumber = Integer.valueOf(args[1]);
		} catch (Exception ex) {
			System.err.println("threadNumber error, use default..");
		}

		String savedPath = FOLDER;
		if (args[2] != null || !args[2].isEmpty()) {
			savedPath = args[2];
		}

		String fileName = downloadURL.substring(downloadURL.lastIndexOf("/") + 1);
		if (args[3] != null || !args[3].isEmpty()) {
			fileName = args[3];
		}

		if (args.length > 3 && (args[4] != null || !args[4].isEmpty())) {
			useHeader = true;  
		}

		final URL url = new URL(downloadURL);
		HttpURLConnection.setFollowRedirects(true);

		URLConnection connection = null;
		try {

			connection = url.openConnection();
			if (connection instanceof HttpURLConnection) {
				if (useHeader) {
					addHeader(connection);
				}

				int code = ((HttpURLConnection) connection).getResponseCode();
				System.out.println("response code: " + code);

				final long size = ((HttpURLConnection) connection).getContentLength();
				System.out.println("remote file content size:" + size);

				if (size <= 0) {
					System.err.println("remote file size is negative, skip download...");
					return;
				}

				String fullPath = savedPath + fileName;
				if (!savedPath.endsWith(File.separator)) {
					fullPath = savedPath + File.separator + fileName;
				}

				final String sFilePath = fullPath + ".s";
				final File sFile = new File(sFilePath);

				long start = System.currentTimeMillis();
				ScheduledExecutorService s = null;
				ForkJoinPool pool = null;
				try {
					s = Executors.newSingleThreadScheduledExecutor();
					pool = new ForkJoinPool(threadNumber);

					recovery(sFile);
					pool.submit(new ExecTask(0, size, url, new File(fullPath)));
					s.scheduleAtFixedRate(new SnapshotWorker(sFile, size), 0, 1, TimeUnit.SECONDS);
				} finally {
					if (pool != null) {
						pool.shutdown();
					}
					pool.awaitTermination(30, TimeUnit.HOURS);

					if (s != null) {
						s.shutdown();
					}
					s.awaitTermination(30, TimeUnit.HOURS);

					if (sFile.exists()) {
						sFile.deleteOnExit();
					}
				}

				System.out.print(ProgressBar.showBarByPoint(100, 100, 70, getPerSecondSpeed(), true));
				long end = System.currentTimeMillis();
				System.out.println("cost time: " + (end - start) / 1000 + "s");
			} else {
				System.err.println("The destination url http connection is not support.");
			}
		} finally {
			if (connection instanceof HttpURLConnection) {
				if (connection != null)
					((HttpURLConnection) connection).disconnect();
			} else {
				System.err.println("connection is not the instance of HttpURLConnection..");
			}
		}
	}

	private static void recovery(final File sFile) {
		if (sFile.exists()) {
			Manager re = readObject(sFile);
			m.recovry(re);
			recovery = true;
		}
	}

	private static long getPerSecondSpeed() {
		long speed = m.alreadyRead.get() - m.preAlreadyRead.get();
		m.preAlreadyRead.set(m.alreadyRead.get());
		return (speed / 1000);
	}

	static class ExecTask extends RecursiveAction {
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

		public ExecTask(long start, long end, URL url, File dFile) {
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
					ExecTask reTask = null;
					current.set((recovery && (reTask = m.get(getKey())) != null) ? reTask.getCurrent() : start);
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
				ForkJoinTask<?> childTask1 = new ExecTask(start, middle, url, dFile);
				ForkJoinTask<?> childTask2 = new ExecTask(middle + 1, end, url, dFile);
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

					if (useHeader) {
						addHeader(con);
					}
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
	}

	static class SnapshotWorker implements Runnable {
		private File sFile;
		private long size;

		public SnapshotWorker(File sFile, long size) {
			this.sFile = sFile;
			this.size = size;
		}

		@Override
		public void run() {
			try {
				double current = 100 * (Double.valueOf(m.alreadyRead.get()) / size);
				System.out.print(ProgressBar.showBarByPoint(current, 100, 70, getPerSecondSpeed(), true));
				writeObject(m, sFile);
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}
	}

	/**
	 * Manager
	 * 
	 * @author liuyq
	 * 
	 */
	static class Manager implements Serializable {
		/**
		 * serialVersionUID
		 */
		private static final long serialVersionUID = 3879351808960296777L;
		/** key(start-end) value(task info) **/
		private final Map<String, ExecTask> map;

		public final AtomicLong alreadyRead = new AtomicLong(0);
		public final AtomicLong preAlreadyRead = new AtomicLong(0);

		private Manager() {
			this.map = new ConcurrentHashMap<>();
		}

		public Map<String, ExecTask> getMap() {
			return this.map;
		}

		public ExecTask get(String key) {
			return map.get(key);
		}

		public Collection<ExecTask> getCollections() {
			return map.values();
		}

		public Manager add(ExecTask task) {
			final String key = task.getKey();
			map.put(key, task); // overwrite
			return this;
		}

		public void remove(ExecTask task) {
			final String key = task.getKey();
			this.map.remove(key);
		}

		static class SingletonHolder {
			public static final Manager Manager = new Manager();
		}

		public static Manager getInstance() {
			return SingletonHolder.Manager;
		}

		public void recovry(Manager re) {
			alreadyRead.set(re.alreadyRead.get());
			preAlreadyRead.set(re.preAlreadyRead.get());
			map.putAll(re.getMap());
		}

	}

	/**
	 * writeObject
	 * 
	 * @param object
	 */
	static void writeObject(Object object, File path) {
		try (OutputStream os = new FileOutputStream(path, false);
				ObjectOutputStream oos = new ObjectOutputStream(os);) {
			oos.writeObject(object);
			oos.flush();
		} catch (Exception e) {
			System.err.println("exception occurred when write object.");
			e.printStackTrace();
		}
	}

	/**
	 * readObject
	 * 
	 * @param path
	 * @return
	 */
	@SuppressWarnings("unchecked")
	static <T> T readObject(File path) {
		try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(path))) {
			return (T) ois.readObject();
		} catch (Exception e) {
			System.err.println("exception occurred when read object.");
			e.printStackTrace();
		}
		return null;
	}

	static class ProgressBar {
		public static String showBarByPoint(double currentPoint, double finishPoint, int barLength, long perSecond,
				boolean withR) {
			double rate = currentPoint / finishPoint;
			int barSign = (int) (rate * barLength);
			return makeBarBySignAndLength(barSign, barLength) + String.format(" %.2f%%", rate * 100)
					+ String.format(" %sK/S", perSecond) + (withR ? "\r" : "\n");
		}

		private static String makeBarBySignAndLength(int barSign, int barLength) {
			StringBuilder bar = new StringBuilder();
			bar.append("[");
			for (int i = 1; i <= barLength; i++) {
				if (i < barSign) {
					bar.append("=");
				} else if (i == barSign) {
					bar.append(">");
				} else {
					bar.append(" ");
				}
			}
			bar.append("]");
			return bar.toString();
		}
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

	// log.info("!!!!!!!Thread name: {}, start: {}, end:
	// {}, current: {}",
	// Thread.currentThread().getName(), getStart(),
	// end, current.get());

	// only for test
	// final byte[] readBytes = new byte[10];
	// file.read(readBytes);
	// for (byte b : readBytes) {
	// if (b != 0) {
	// System.out.println("file already been written,
	// position:" + getCurrent());
	// }
	// }

}
