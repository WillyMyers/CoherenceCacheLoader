package co.uk.wdmyers.task;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.tangosol.io.pof.annotation.Portable;
import com.tangosol.io.pof.annotation.PortableProperty;
import com.tangosol.net.AbstractInvocable;
import com.tangosol.net.CacheFactory;
import com.tangosol.net.NamedCache;

import co.uk.wdmyers.dao.LoaderDao;

@SuppressWarnings("serial")
@Portable
public abstract class AbstractPreloadTask<T> extends AbstractInvocable {
	
	@PortableProperty(0)
	protected String cacheName;

	@PortableProperty(1)
	protected long firstRow;

	@PortableProperty(2)
	protected long lastRow;

	@PortableProperty(3)
	protected int fetchSize;

	@PortableProperty(4)
	protected String startDate;

	@PortableProperty(5)
	protected String endDate;

	@PortableProperty(6)
	protected String url;

	@PortableProperty(7)
	protected String username;

	@PortableProperty(8)
	protected String password;

	public AbstractPreloadTask() {
		super();
	}

	public AbstractPreloadTask(String cacheName, long firstRow, long rows, int fetchSize, String startDate,
			String endDate, String url, String username, String password) {
		this.firstRow = firstRow;
		this.lastRow = rows;
		this.fetchSize = fetchSize;
		this.startDate = startDate;
		this.endDate = endDate;
		this.cacheName = cacheName;
		this.url = url;
		this.username = username;
		this.password = password;
	}

	@Override
	public void run() {
		preload();
	}

	public long getFirstKey() {
		return firstRow;
	}

	public long getLastKey() {
		return lastRow;
	}

	public void preload() {
		System.out.println("Cache name = " + this.cacheName);
		NamedCache cache = CacheFactory.getCache(cacheName);
		Map<Object, Object> mapBuffer = new HashMap<Object, Object>(fetchSize);
		long rows = (lastRow - firstRow) + 1;
		long batches = rows / fetchSize;
		long remaining = rows % fetchSize;
		batches += remaining == 0 ? 0 : 1;
		long batchFirstRow;
		long batchLastRow = firstRow - 1;
		log("Executing preload query in " + batches + " batches");
		for (int i = 0; i < batches; ++i) {
			batchFirstRow = batchLastRow + 1;
			batchLastRow = Math.min(lastRow, batchFirstRow + (fetchSize - 1));
			List<T> candidates = getRepo().selectInBatches(batchFirstRow, batchLastRow, startDate, endDate);
			processResults(candidates, mapBuffer);
			log("Loaded rows " + batchFirstRow + " to " + batchLastRow + " into " + this.cacheName + "...");
			cache.putAll(mapBuffer);
			mapBuffer.clear();
		}
		log("****** Finished loading rows " + firstRow + " to " + lastRow + " into " + this.cacheName + " ******");
	}

	public abstract boolean processResults(List<T> candidates, Map<Object, Object> mapResults);

	public abstract LoaderDao getRepo();

}