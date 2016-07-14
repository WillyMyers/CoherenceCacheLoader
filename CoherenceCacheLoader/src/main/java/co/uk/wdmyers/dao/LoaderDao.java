package co.uk.wdmyers.dao;

import java.util.List;
import javax.sql.DataSource;


public abstract class LoaderDao<T> {


	public LoaderDao(DataSource datasource) {
		//return the datasource
	}

	public abstract List<T> selectInBatches(long batchFirstRow,	long batchLastRow, String startDate, String endDate);
	public abstract Long getRowCount(String startDate, String endDate);
	public abstract List<T> processResults(List<Object[]> list);

	public List<T> selectAll(String startDate, String endDate) {
		throw new UnsupportedOperationException("Method not implemented!");
	}
}
