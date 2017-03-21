package CdCassendra.Called.Dao;

import java.util.List;
import java.util.Map;

public interface FetchDataFromAdwordsDao {

	void get_Weekly_CampaignPerformance_Data_From_Adwords(List<Map<String, Object>> urlList);

	void get_Weekly_AdNetworkPerformance_Data_From_Adwords(List<Map<String, Object>> urlList);

	void get_Daily_AccountPerformance_Data_From_Adwords(List<Map<String, Object>> urlList);

	void get_Daily_CampaignPerformance_Data_From_Adwords(List<Map<String, Object>> urlList);

	void get_Daily_AdNetworkPerformance_Data_From_Adwords(List<Map<String, Object>> urlList);

	void getAllDataFromAdwords(List<String> dataList);

	void get_Weekly_AccountPerformance_Data_From_Adwords(List<Map<String, Object>> urlList);

}
