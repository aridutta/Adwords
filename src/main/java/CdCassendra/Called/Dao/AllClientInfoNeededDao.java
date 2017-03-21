package CdCassendra.Called.Dao;

import java.util.List;
import java.util.Map;

public interface AllClientInfoNeededDao {

	List<Map<String, Object>> getAllClientInfo();
	
	List<Map<String, Object>> createURLFromAdwordsData(List<Map<String, Object>> listAdwordsId);
}
