package CdCassendra.Called.Dao;

import java.util.List;
import java.util.Map;

public interface CassandraDao {

	public void writeDataToAdNetworkAccountPerformanceCassandraDaily(
			List<Map<String, Object>> account_performance_weekly_data, String customerId,String adwordsId);

	public void writeDataToAccountPerformanceToCassandraDaily(List<Map<String, Object>> account_performance_weekly_data,
			String customerId,String adwordsId,Long budget);

	public void writeDataToCampaignAccountPerformanceCassandraDaily(
			List<Map<String, Object>> account_performance_weekly_data, String customerId,String adwordsIds);

	public void writeDataToAdNetworkAccountPerformanceCassandraWeekly(
			List<Map<String, Object>> account_performance_weekly_data, String customerId,String adwordsId);

	public void writeDataToAccountPerformanceToCassandraWeekly(
			List<Map<String, Object>> account_performance_weekly_data, String customerId, String adwordsId,Long budget);

	public void writeDataToCampaignAccountPerformanceCassandraWeekly(
			List<Map<String, Object>> account_performance_weekly_data, String customerId,String adwordsId);

	public void writeDataToCampaignAccountPerformanceCassandraMonthly(
			List<Map<String, Object>> account_performance_monthly_data, String customerId,String adwordsId);

	public void writeDataToAccountPerformanceToCassandraMonthly(
			List<Map<String, Object>> account_performance_monthly_data, String customerId,String adwordsId,Long budget);

	public void writeDataToAdNetworkAccountPerformanceCassandraMonthly(
			List<Map<String, Object>> account_performance_monthly_data, String customerId,String adwordsId);

}
