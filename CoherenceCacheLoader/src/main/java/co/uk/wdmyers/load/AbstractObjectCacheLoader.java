package co.uk.wdmyers.load;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import org.joda.time.LocalDate;
import com.tangosol.net.CacheFactory;
import com.tangosol.net.InvocationObserver;
import com.tangosol.net.InvocationService;
import com.tangosol.net.Member;
import com.tangosol.net.NamedCache;
import com.tangosol.net.cache.AbstractCacheLoader;

import co.uk.wdmyers.dao.LoaderDao;
import co.uk.wdmyers.task.AbstractPreloadTask;
import co.uk.wdmyers.util.CacheLoaderUtilities;

public abstract class AbstractObjectCacheLoader<T> extends AbstractCacheLoader implements Runnable {

	protected static int fetchSize = 1000;
	private final static CacheLoaderUtilities utils = new CacheLoaderUtilities();
	private String startDate;
	private String endDate;
	public AbstractObjectCacheLoader() {
	}

	public boolean preloadCache() {
		String cName = getCacheName();
		NamedCache cache = CacheFactory.getCache(cName);
		log(String.format("%s: start cache size is %d", cName, cache.size()));
		if (cache.size() != 0) {
			System.out.println("$$$$$$$$$$$$$$$$$$$ " + cName
					+ " is not empty, cannot continue or data will be corrupted! $$$$$$$$$$$$$$$$$$$");
			return false;
		}
		startDate = getStartDate();
		endDate = getEndDate();
		Boolean run = utils.shouldRun(getControlName());
		if (run == null || run) {
			// stop another process from running a load
			utils.setControlValueToRunning(getControlName());
			// disable the cache store
			disableStore();
			long rows = getRowCount();
			log("**************************************************************************");
			log("******** Date range to load: " + startDate + " to " + endDate + " ********");
			log("******** Number of rows to load in the date range is " + rows + " ********");
			log("**************************************************************************");
			InvocationService serviceInv = (InvocationService) CacheFactory.getService("InvocationService");
			long start = System.currentTimeMillis();
			Set<Member> setLoadingMembers = getLoadingMembers(serviceInv);
			Map<Member, AbstractPreloadTask> mapMemberTasks = generateTasks(setLoadingMembers, rows);
			int tasks = mapMemberTasks.size();
			final CountDownLatch latch = new CountDownLatch(tasks);
			InvocationObserver observer = new CacheLoaderObserver(getCacheName(), latch);
			for (Map.Entry<Member, AbstractPreloadTask> entry : mapMemberTasks.entrySet()) {
				Member member = entry.getKey();
				Set<Member> setTaskMembers = Collections.singleton(member);
				AbstractPreloadTask task = entry.getValue();
				serviceInv.execute(task, setTaskMembers, observer);
				log(String.format("%s: rows %d-%d sent to %s", getCacheName(), task.getFirstKey(), task.getLastKey(),
						member.toString()));
			}
			try {
				latch.await();
			} catch (InterruptedException e) {
			}
			long durationMillis = System.currentTimeMillis() - start;
			log(String.format("%s: pre-loaded %d rows in %.3f secs (%.3f rows/sec)", getCacheName(), rows,
					durationMillis / 1000.0, rows / (durationMillis / 1000.0)));
			log(String.format("%s: final size is %d", getCacheName(), cache.size()));
			try {
				// do this to stop the store from firing after we re-enable it
				Thread.sleep(10000);
			} catch (InterruptedException e) {
			}

			// re-enable the cache store
			enableStore();
			utils.setControlValueToCompleted(getControlName());
			return true;
		}

		// wait for the control cache to be set to completed before returning
		while (utils.isRunning(getControlName())) {
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
			}
		}
		return false;
	}

	protected Map<Member, AbstractPreloadTask> generateTasks(Set<Member> setMembers, long rows) {
		Map<Member, AbstractPreloadTask> mapTasks = new HashMap<Member, AbstractPreloadTask>(setMembers.size());
		if (rows <= getFetchSize()) { // for small number of rows, just send the							// load
			// to one member
			Member member = setMembers.iterator().next();
			AbstractPreloadTask<T> task = getNewTask(getCacheName(), 1, rows, getFetchSize(), startDate, endDate);
			mapTasks.put(member, task);
		} else {
			int members = setMembers.size();
			long minRowsPerMember = rows / members;
			long remainingRows = rows % members;
			long firstRow;
			long lastRow = 0;
			for (Member member : setMembers) {
				firstRow = lastRow + 1;
				lastRow = firstRow + minRowsPerMember + (remainingRows-- > 0 ? 1 : 0) - 1;
				AbstractPreloadTask task = getNewTask(getCacheName(), firstRow, lastRow, getFetchSize(), startDate,
						endDate);
				mapTasks.put(member, task);
			}
		}
		return mapTasks;
	}

	public abstract AbstractPreloadTask<T> getNewTask(String cacheName, long firstRow, long rows, int fetchSize,
			String startDate, String endDate);

	public abstract String getCacheName();

	public boolean enableStore() {
		return false;
	}

	public boolean disableStore() {
		return false;
	}

	public long getRowCount() {
		return getRepo().getRowCount(startDate, endDate);
	}

	public abstract LoaderDao getRepo();

	public abstract String getControlName();

	@Override
	public Object load(Object arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void run() {
		try {
			this.preloadCache();
		} catch (Exception e) {
			log(e.getCause());
			utils.setControlValueToFailed(getControlName());
			log(e);
		}
	}

	@SuppressWarnings("unchecked")
	public Set<Member> getLoadingMembers(InvocationService serviceInv) {
		Set<Member> members = serviceInv.getInfo().getServiceMembers();
		members.remove(serviceInv.getCluster().getLocalMember());
		if (members.size() == 0) {
			throw new IllegalStateException("No other members are running InvocationService. Is the cluster up?");
		}

		for (Member member : members) {
			System.out.println("InvocationService Member: [ID:" + member.getId() + "][NAME:" + member.getMemberName()
					+ "][PROCESS NAME:" + member.getProcessName() + "][ROLE NAME" + member.getRoleName() + "]");
		}
		return members;
	}

	public static String getEndDate() {
		if (System.getProperty("load.end.date") == null || System.getProperty("load.end.date").isEmpty()) {
			LocalDate endDate = (new LocalDate()).plusYears(1);
			System.out.println("## No End Date specified so using default of 1 year....endDate set to "
					+ endDate.toString("dd/MM/yy") + " ##");
			return endDate.toString("dd/MM/yy");
		} else {
			return System.getProperty("load.end.date");
		}

	}

	public static String getStartDate() {
		if (System.getProperty("load.start.date") == null || System.getProperty("load.start.date").isEmpty()) {
			LocalDate startDate = new LocalDate();
			String offsetString = System.getProperty("tangosol.coherence.eviction.offset");
			if (offsetString != null && !offsetString.isEmpty()) {
				startDate = (new LocalDate()).minusDays(new Integer(offsetString));
				System.out.println("## No Start Date specified so using offset(" + offsetString
						+ " days)...startDate set to " + startDate.toString("dd/MM/yy") + " ##");
			} else {
				startDate = (new LocalDate()).minusDays(10);
				System.out.println(
						"## No Start Date or offset specified so using default offset of 10 days...startDate set to "
								+ startDate.toString("dd/MM/yy") + " ##");
			}
			return startDate.toString("dd/MM/yy");
		} else {
			return System.getProperty("load.start.date");
		}
	}

	public static int getFetchSize() {
		if (System.getProperty("load.fetch.size") != null) {
			int fSize = new Integer(System.getProperty("load.fetch.size")).intValue();
			System.out.println("## load.fetch.size set to " + fSize + " ##");
			return fSize;
		} else {
			System.out.println("## No load.fetch.size specified so using default of 1000 ##");
			return fetchSize;
		}
	}
}