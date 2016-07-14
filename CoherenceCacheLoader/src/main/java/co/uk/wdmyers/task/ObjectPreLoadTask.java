package co.uk.wdmyers.task;

import java.util.List;
import java.util.Map;

import com.tangosol.io.pof.annotation.Portable;

@SuppressWarnings("serial")
@Portable
public class MatchablePreloadTask extends AbstractPreloadTask<Matchable> {

	public MatchablePreloadTask(String cacheName, long firstRow, long rows, int fetchSize, String startDate,
			String endDate, String url, String username, String password) {
		super(cacheName, firstRow, rows, fetchSize, startDate, endDate, url, username, password);
	}

	public MatchablePreloadTask() {
	}

	@Override
	public boolean processResults(List<Object> candidates, Map<Object, Object> mapResults) {
		for (Object o : candidates) {
			Object c = (Object) o;
			//mapResults.put(c.getId(), c);
		}
		return mapResults.size() > 0;
	}

	public LoaderDAO<Object> getRepo() {
		return DAOFactory.getInstance(url, username, password).getObjectDAO();
	}

}