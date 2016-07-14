package co.uk.wdmyers.dao;

import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;


public class ObjectDao extends LoaderDao<Object> {
	private static final String ROWCOUNT = "SELECT COUNT(*) FROM TABLE WHERE X = Y";
	private static final String SELECT = "SELECT * FROM  (SELECT a.*, ROWNUM r FROM (SELECT x, y, z FROM TABLE WHERE X = Y) a   WHERE ROWNUM <= ?) WHERE r >= ?";

	public ObjectDao(DataSource datasource) {
		super(datasource);
	}

	@Override
	public List<Object> selectInBatches(long batchFirstRow, long batchLastRow, String startDate, String endDate) {
		//call thedb with the SELECT sql and then process the results and return
		return null;
	}

	@Override
	public Long getRowCount(String startDate, String endDate) {
		//call the db with the ROWCOUNT sql and return the result
		return -1L;
	}

	@Override
	public List<Object> processResults(List<Object[]> list) {
		List<Object> objects = new ArrayList<Object>();
		//process the list and return them
		return objects;

	}

}