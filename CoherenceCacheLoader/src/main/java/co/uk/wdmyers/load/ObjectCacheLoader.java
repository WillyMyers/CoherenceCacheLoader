package co.uk.wdmyers.load;

public class ObjectCacheLoader extends AbstractObjectCacheLoader<Object> implements Runnable {

	public ObjectCacheLoader() {
	}

	@Override
	public boolean enableStore() {
		//return new MatchableCacheStore().enable();
	}

	@Override
	public boolean disableStore() {
		//return new MatchableCacheStore().disable();
	}

	@Override
	public MatchablePreloadTask getNewTask(String cacheName, long firstRow, long rows, int fetchSize, String startDate,
			String endDate) {
		return new MatchablePreloadTask(cacheName, firstRow, rows, fetchSize, startDate, endDate,
				PropertyConfig.getDbUrl(), PropertyConfig.getdbUser(), PropertyConfig.getdbPassword());
	}

	@Override
	public String getCacheName() {
		log("Cache Name is " + CacheConstants.MATCHABLE_CACHE);
		return CacheConstants.MATCHABLE_CACHE;
	}

	@Override
	public String getControlName() {
		return CacheConstants.MATCHABLE_CACHE;
	}

	@Override
	public LoaderDAO<Matchable> getRepo() {
		return DAOFactory
				.getInstance(PropertyConfig.getDbUrl(), PropertyConfig.getdbUser(), PropertyConfig.getdbPassword())
				.getMatchableDAO();
	}

}