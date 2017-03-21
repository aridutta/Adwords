package CdCassendra.Called.DaoImpl;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;

import CdCassendra.Called.Bean.CassandraCredentials;
import CdCassendra.Called.Dao.CassandraDao;

public class CassandraCRUDImpl implements CassandraDao {

	Cluster cluster = null;

	private Session session;

	String weekly_account_performance_Query = "";

	String weekly_account_ad_network_performance_Query = "";

	String weekly_campaign_performance_Query = "";

	String daily_account_performance_Query = "";

	String daily_account_ad_network_performance_Query = "";

	String daily_campaign_performance_Query = "";

	String dailyQuery = "";

	CassandraCredentials cassandraCredentials;
	List<InetSocketAddress> address;

	synchronized public Session getConection() {
		if (session != null) {
			return session;
		} else {
			address = new ArrayList<InetSocketAddress>();
			address.add(new InetSocketAddress(cassandraCredentials.getContactpoint(),
					Integer.parseInt(cassandraCredentials.getPort())));
			cluster = Cluster.builder().addContactPointsWithPorts(address).withCredentials("root", "Dig$dev%").build(); // needed
			// cluster=Cluster.builder().addContactPointsWithPorts(address).build();
			try {
				session = cluster.connect(cassandraCredentials.getDb());
			} catch (Exception e) {
				e.printStackTrace();
			}

			return session;
		}
	}

	public CassandraCRUDImpl(CassandraCredentials cassandraCredentials) {
		this.cassandraCredentials = cassandraCredentials;
		session = getConection();
	}

	public CassandraCRUDImpl() {
	}

	public void writeDataToAccountPerformanceToCassandraWeekly(
			List<Map<String, Object>> account_performance_weekly_data, String customerId, String adwordsId,Long budget) {
		int size = account_performance_weekly_data.size();

		try {
			for (int i = 0; i < size; i++) {

				Map<String, Object> weeklyData = account_performance_weekly_data.get(i);

				StringBuffer preparedStatementQuery = new StringBuffer("Insert into adwords_weekly_account_performance "
						+ "(client_stamp," + " week," + " external_customer_id," + " account_descriptive_name,"
						+ " account_currency_code," + " account_timezone_id," + " average_position," + " conversions,"
						+ " impressions," + " clicks," + " ctr," + " cost," + " cost_per_conversion,"
						+ " all_conversion_rate," + " video_views," + " video_view_rate," + " average_cpv,"
						+ " view_through_conversions," + " search_budget_lost_impression_share,"
						+ " search_exactmatch_impression_share," + " search_impression_share,"
						+ " search_rank_lost_impression_share," + " invalid_clicks," + " invalid_click_rate,"
						+ " all_conversion_value, time, client_customer_id,weekly_budget) values (?, ?, ?, ?, ?, ?, ?, ?"
						+ ", ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
				session = getConection();
				PreparedStatement preparedStmt = session.prepare(preparedStatementQuery.toString());

				BoundStatement boundStmt = preparedStmt.bind().setString(0, customerId)
						.setString(1, (String) weeklyData.get("Week"))
						.setString(2, String.valueOf(weeklyData.get("ExternalCustomerid")))
						.setString(3, (String) weeklyData.get("AccountDescriptiveName"))
						.setString(4, (String) weeklyData.get("AccountCurrencyCode"))
						.setString(5, (String) weeklyData.get("AccountTimeZoneId"))
						.setFloat(6, Float.parseFloat(String.valueOf(weeklyData.get("AveragePosition"))))
						.setFloat(7, Float.parseFloat(String.valueOf(weeklyData.get("Conversions"))))
						.setLong(8, (Long) weeklyData.get("Impressions"))
						.setInt(9, Integer.parseInt(String.valueOf(weeklyData.get("Clicks"))))
						.setFloat(10, Float.parseFloat(String.valueOf(weeklyData.get("Ctr"))))
						.setFloat(11, Float.parseFloat(String.valueOf(weeklyData.get("Cost"))))
						.setFloat(12, Float.parseFloat(String.valueOf(weeklyData.get("CostPerConversion"))))
						.setFloat(13, Float.parseFloat(String.valueOf(weeklyData.get("AllConversionRate"))))
						.setInt(14, Integer.parseInt(String.valueOf(weeklyData.get("VideoViews"))))
						.setFloat(15, Float.parseFloat(String.valueOf(weeklyData.get("VideoViewRate"))))
						.setFloat(16, Float.parseFloat(String.valueOf(weeklyData.get("AverageCpv"))))
						.setInt(17, Integer.parseInt(String.valueOf(weeklyData.get("ViewThroughConversions"))))
						.setFloat(18, Float.parseFloat(
								String.valueOf(weeklyData.get("SearchBudgetLostImpressionShare") == null ? "0.0"
										: weeklyData.get("SearchBudgetLostImpressionShare"))))
						.setFloat(19, Float.parseFloat(
								String.valueOf(weeklyData.get("SearchExactMatchImpressionShare") == null ? "0.0"
										: weeklyData.get("SearchExactMatchImpressionShare"))))
						.setFloat(20,
								Float.parseFloat(String.valueOf(weeklyData.get("SearchImpressionShare") == null ? "0.0"
										: weeklyData.get("SearchImpressionShare"))))
						.setFloat(21, Float.parseFloat(
								String.valueOf(weeklyData.get("SearchRankLostImpressionShare") == null ? "0.0"
										: weeklyData.get("SearchRankLostImpressionShare"))))
						.setInt(22, Integer.parseInt(String.valueOf(weeklyData.get("InvalidClicks"))))
						.setFloat(23, Float.parseFloat(String.valueOf(weeklyData.get("InvalidClickRate"))))
						.setFloat(24, Float.parseFloat(String.valueOf(weeklyData.get("AllConversionValue"))))
						.setLong(25, System.currentTimeMillis())
						.setString(26, adwordsId)
						.setLong(27, budget);

				session.execute(boundStmt);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {

		}

	}

	public void writeDataToAdNetworkAccountPerformanceCassandraWeekly(
			List<Map<String, Object>> account_performance_weekly_data, String customerId, String adwordsId) {
		int size = account_performance_weekly_data.size();
		try {
			for (int i = 0; i < size; i++) {

				Map<String, Object> weeklyData = account_performance_weekly_data.get(i);

				StringBuffer preparedStatementQuery = new StringBuffer(
						"Insert into adwords_weekly_ad_network_account_performance " + "(client_stamp," + " week,"
								+ " external_customer_id," + " ad_network_type1," + " account_descriptive_name,"
								+ " account_currency_code," + " account_timezone_id," + " average_position,"
								+ " conversions," + " impressions," + " clicks," + " ctr," + " cost,"
								+ " cost_per_conversion," + " all_conversion_rate," + " video_views,"
								+ " video_view_rate," + " average_cpv," + " view_through_conversions,"
								+ " search_budget_lost_impression_share," + " search_exactmatch_impression_share,"
								+ " search_impression_share," + " search_rank_lost_impression_share,"
								+ " invalid_clicks," + " invalid_click_rate,"
								+ " all_conversion_value, time, client_customer_id) values (?, ?, ?, ?, ?, ?, ?, ?"
								+ ", ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");

				PreparedStatement preparedStmt = session.prepare(preparedStatementQuery.toString());

				BoundStatement boundStmt = preparedStmt.bind().setString(0, customerId)
						.setString(1, (String) weeklyData.get("Week"))
						.setString(2, String.valueOf(weeklyData.get("ExternalCustomerid")))
						.setString(3, (String) weeklyData.get("AdNetworkType1"))
						.setString(4, (String) weeklyData.get("AccountDescriptiveName"))
						.setString(5, (String) weeklyData.get("AccountCurrencyCode"))
						.setString(6, (String) weeklyData.get("AccountTimeZoneId"))
						.setFloat(7, Float.parseFloat(String.valueOf(weeklyData.get("AveragePosition"))))
						.setFloat(8, Float.parseFloat(String.valueOf(weeklyData.get("Conversions"))))
						.setLong(9, (Long) weeklyData.get("Impressions"))
						.setInt(10, Integer.parseInt(String.valueOf(weeklyData.get("Clicks"))))
						.setFloat(11, Float.parseFloat(String.valueOf(weeklyData.get("Ctr"))))
						.setFloat(12, Float.parseFloat(String.valueOf(weeklyData.get("Cost"))))
						.setFloat(13, Float.parseFloat(String.valueOf(weeklyData.get("CostPerConversion"))))
						.setFloat(14, Float.parseFloat(String.valueOf(weeklyData.get("AllConversionRate"))))
						.setInt(15, Integer.parseInt(String.valueOf(weeklyData.get("VideoViews"))))
						.setFloat(16, Float.parseFloat(String.valueOf(weeklyData.get("VideoViewRate"))))
						.setFloat(17, Float.parseFloat(String.valueOf(weeklyData.get("AverageCpv"))))
						.setInt(18, Integer.parseInt(String.valueOf(weeklyData.get("ViewThroughConversions"))))
						.setFloat(19, Float.parseFloat(
								String.valueOf(weeklyData.get("SearchBudgetLostImpressionShare") == null ? "0.0"
										: weeklyData.get("SearchBudgetLostImpressionShare"))))
						.setFloat(20, Float.parseFloat(
								String.valueOf(weeklyData.get("SearchExactMatchImpressionShare") == null ? "0.0"
										: weeklyData.get("SearchExactMatchImpressionShare"))))
						.setFloat(21,
								Float.parseFloat(String.valueOf(weeklyData.get("SearchImpressionShare") == null ? "0.0"
										: weeklyData.get("SearchImpressionShare"))))
						.setFloat(22, Float.parseFloat(
								String.valueOf(weeklyData.get("SearchRankLostImpressionShare") == null ? "0.0"
										: weeklyData.get("SearchRankLostImpressionShare"))))
						.setInt(23, Integer.parseInt(String.valueOf(weeklyData.get("InvalidClicks"))))
						.setFloat(24, Float.parseFloat(String.valueOf(weeklyData.get("InvalidClickRate"))))
						.setFloat(25, Float.parseFloat(String.valueOf(weeklyData.get("AllConversionValue"))))
						.setLong(26, System.currentTimeMillis()).setString(27, adwordsId);
				session.execute(boundStmt);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {

		}

	}

	public void writeDataToAccountPerformanceToCassandraDaily(List<Map<String, Object>> account_performance_weekly_data,
			String customerId, String adwordsId, Long budget) {
		int size = account_performance_weekly_data.size();
		try {
			for (int i = 0; i < size; i++) {

				Map<String, Object> weeklyData = account_performance_weekly_data.get(i);

				StringBuffer preparedStatementQuery = new StringBuffer(
						"Insert into adwords_daily_account_performance " + "(client_stamp," + " week,"
								+ " external_customer_id," + " account_descriptive_name," + " account_currency_code,"
								+ " account_timezone_id," + " average_position," + " conversions," + " impressions,"
								+ " clicks," + " ctr," + " cost," + " cost_per_conversion," + " all_conversion_rate,"
								+ " video_views," + " video_view_rate," + " average_cpv," + " view_through_conversions,"
								+ " search_budget_lost_impression_share," + " search_exactmatch_impression_share,"
								+ " search_impression_share," + " search_rank_lost_impression_share,"
								+ " invalid_clicks," + " invalid_click_rate," + " all_conversion_value,"
								+ " date, time, client_customer_id,daily_budget) values (?, ?, ?, ?, ?, ?, ?, ?"
								+ ", ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");

				PreparedStatement preparedStmt = session.prepare(preparedStatementQuery.toString());

				BoundStatement boundStmt = preparedStmt.bind().setString(0, customerId)
						.setString(1, (String) weeklyData.get("Week"))
						.setString(2, String.valueOf(weeklyData.get("ExternalCustomerid")))

						.setString(3, (String) weeklyData.get("AccountDescriptiveName"))
						.setString(4, (String) weeklyData.get("AccountCurrencyCode"))
						.setString(5, (String) weeklyData.get("AccountTimeZoneId"))
						.setFloat(6, Float.parseFloat(String.valueOf(weeklyData.get("AveragePosition"))))
						.setFloat(7, Float.parseFloat(String.valueOf(weeklyData.get("Conversions"))))
						.setLong(8, (Long) weeklyData.get("Impressions"))
						.setInt(9, Integer.parseInt(String.valueOf(weeklyData.get("Clicks"))))
						.setFloat(10, Float.parseFloat(String.valueOf(weeklyData.get("Ctr"))))
						.setFloat(11, Float.parseFloat(String.valueOf(weeklyData.get("Cost"))))
						.setFloat(12, Float.parseFloat(String.valueOf(weeklyData.get("CostPerConversion"))))
						.setFloat(13, Float.parseFloat(String.valueOf(weeklyData.get("AllConversionRate"))))
						.setInt(14, Integer.parseInt(String.valueOf(weeklyData.get("VideoViews"))))
						.setFloat(15, Float.parseFloat(String.valueOf(weeklyData.get("VideoViewRate"))))
						.setFloat(16, Float.parseFloat(String.valueOf(weeklyData.get("AverageCpv"))))
						.setInt(17, Integer.parseInt(String.valueOf(weeklyData.get("ViewThroughConversions"))))
						.setFloat(18, Float.parseFloat(
								String.valueOf(weeklyData.get("SearchBudgetLostImpressionShare") == null ? "0.0"
										: weeklyData.get("SearchBudgetLostImpressionShare"))))
						.setFloat(19, Float.parseFloat(
								String.valueOf(weeklyData.get("SearchExactMatchImpressionShare") == null ? "0.0"
										: weeklyData.get("SearchExactMatchImpressionShare"))))
						.setFloat(20,
								Float.parseFloat(String.valueOf(weeklyData.get("SearchImpressionShare") == null ? "0.0"
										: weeklyData.get("SearchImpressionShare"))))
						.setFloat(21, Float.parseFloat(
								String.valueOf(weeklyData.get("SearchRankLostImpressionShare") == null ? "0.0"
										: weeklyData.get("SearchRankLostImpressionShare"))))
						.setInt(22, Integer.parseInt(String.valueOf(weeklyData.get("InvalidClicks"))))
						.setFloat(23, Float.parseFloat(String.valueOf(weeklyData.get("InvalidClickRate"))))
						.setFloat(24, Float.parseFloat(String.valueOf(weeklyData.get("AllConversionValue"))))
						.setString(25, (String) weeklyData.get("Date")).setLong(26, System.currentTimeMillis())
						.setString(27, adwordsId)
						.setLong(28, budget);

				session.execute(boundStmt);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {

		}
	}

	public void writeDataToAdNetworkAccountPerformanceCassandraDaily(
			List<Map<String, Object>> account_performance_weekly_data, String customerId, String adwordsId) {
		int size = account_performance_weekly_data.size();

		try {

			for (int i = 0; i < size; i++) {

				Map<String, Object> weeklyData = account_performance_weekly_data.get(i);

				StringBuffer preparedStatementQuery = new StringBuffer(
						"Insert into adwords_daily_ad_network_account_performance " + "(client_stamp," + " week,"
								+ " external_customer_id," + " ad_network_type1," + " account_descriptive_name,"
								+ " account_currency_code," + " account_timezone_id," + " average_position,"
								+ " conversions," + " impressions," + " clicks," + " ctr," + " cost,"
								+ " cost_per_conversion," + " all_conversion_rate," + " video_views,"
								+ " video_view_rate," + " average_cpv," + " view_through_conversions,"
								+ " search_budget_lost_impression_share," + " search_exactmatch_impression_share,"
								+ " search_impression_share," + " search_rank_lost_impression_share,"
								+ " invalid_clicks," + " invalid_click_rate," + " all_conversion_value,"
								+ " date,time, client_customer_id) values (?, ?, ?, ?, ?, ?, ?, ?"
								+ ", ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");

				PreparedStatement preparedStmt = session.prepare(preparedStatementQuery.toString());

				BoundStatement boundStmt = preparedStmt.bind().setString(0, customerId)
						.setString(1, (String) weeklyData.get("Week"))
						.setString(2, String.valueOf(weeklyData.get("ExternalCustomerid")))
						.setString(3, (String) weeklyData.get("AdNetworkType1"))
						.setString(4, (String) weeklyData.get("AccountDescriptiveName"))
						.setString(5, (String) weeklyData.get("AccountCurrencyCode"))
						.setString(6, (String) weeklyData.get("AccountTimeZoneId"))
						.setFloat(7, Float.parseFloat(String.valueOf(weeklyData.get("AveragePosition"))))
						.setFloat(8, Float.parseFloat(String.valueOf(weeklyData.get("Conversions"))))
						.setLong(9, (Long) weeklyData.get("Impressions"))
						.setInt(10, Integer.parseInt(String.valueOf(weeklyData.get("Clicks"))))
						.setFloat(11, Float.parseFloat(String.valueOf(weeklyData.get("Ctr"))))
						.setFloat(12, Float.parseFloat(String.valueOf(weeklyData.get("Cost"))))
						.setFloat(13, Float.parseFloat(String.valueOf(weeklyData.get("CostPerConversion"))))
						.setFloat(14, Float.parseFloat(String.valueOf(weeklyData.get("AllConversionRate"))))
						.setInt(15, Integer.parseInt(String.valueOf(weeklyData.get("VideoViews"))))
						.setFloat(16, Float.parseFloat(String.valueOf(weeklyData.get("VideoViewRate"))))
						.setFloat(17, Float.parseFloat(String.valueOf(weeklyData.get("AverageCpv"))))
						.setInt(18, Integer.parseInt(String.valueOf(weeklyData.get("ViewThroughConversions"))))
						.setFloat(19, Float.parseFloat(
								String.valueOf(weeklyData.get("SearchBudgetLostImpressionShare") == null ? "0.0"
										: weeklyData.get("SearchBudgetLostImpressionShare"))))
						.setFloat(20, Float.parseFloat(
								String.valueOf(weeklyData.get("SearchExactMatchImpressionShare") == null ? "0.0"
										: weeklyData.get("SearchExactMatchImpressionShare"))))
						.setFloat(21,
								Float.parseFloat(String.valueOf(weeklyData.get("SearchImpressionShare") == null ? "0.0"
										: weeklyData.get("SearchImpressionShare"))))
						.setFloat(22, Float.parseFloat(
								String.valueOf(weeklyData.get("SearchRankLostImpressionShare") == null ? "0.0"
										: weeklyData.get("SearchRankLostImpressionShare"))))
						.setInt(23, Integer.parseInt(String.valueOf(weeklyData.get("InvalidClicks"))))
						.setFloat(24, Float.parseFloat(String.valueOf(weeklyData.get("InvalidClickRate"))))
						.setFloat(25, Float.parseFloat(String.valueOf(weeklyData.get("AllConversionValue"))))
						.setString(26, (String) weeklyData.get("Date")).setLong(27, System.currentTimeMillis())
						.setString(28, adwordsId);

				session.execute(boundStmt);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {

		}

	}

	public void writeDataToCampaignAccountPerformanceCassandraDaily(
			List<Map<String, Object>> account_performance_weekly_data, String customerId, String adwordsId) {
		int size = account_performance_weekly_data.size();

		try {
			for (int i = 0; i < size; i++) {
				Map<String, Object> weeklyData = account_performance_weekly_data.get(i);

				StringBuffer preparedStatementQuery = new StringBuffer(
						"Insert into adwords_daily_account_campaign_performance " + "(client_stamp," + " week,"
								+ " account_descriptive_name," + " account_currency_code," + " account_timezone_id,"
								+ " average_position," + " conversions," + " impressions," + " clicks," + " ctr,"
								+ " cost," + " cost_per_conversion," + " video_views," + " video_view_rate,"
								+ " view_through_conversions," + " search_budget_lost_impression_share,"
								+ " search_exact_match_impression_share," + " search_impression_share,"
								+ " search_rank_lost_impression_share," + " invalid_clicks," + " invalid_click_rate,"
								+ " all_conversion_value," + " date," + " advertising_channel_type,"
								+ " average_time_on_site," + " bounce_rate," + " campaign_id," + " campaign_name,"
								+ " campaign_status," + " click_assisted_conversions," + " conversion_rate,"
								+ " impression_reach, time, client_customer_id" + ") values (?, ?, ?, ?, ?, ?, ?, ?"
								+ ", ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? ,? ,? ,? ,?, ?, ?)");

				PreparedStatement preparedStmt = session.prepare(preparedStatementQuery.toString());

				BoundStatement boundStmt = preparedStmt.bind().setString(0, customerId)
						.setString(1, (String) weeklyData.get("Week"))
						.setString(2, (String) weeklyData.get("AccountDescriptiveName"))
						.setString(3, (String) weeklyData.get("AccountCurrencyCode"))
						.setString(4, (String) weeklyData.get("AccountTimeZoneId"))
						.setFloat(5, Float.parseFloat(String.valueOf(weeklyData.get("AveragePosition"))))
						.setFloat(6, Float.parseFloat(String.valueOf(weeklyData.get("Conversions"))))
						.setLong(7, (Long) weeklyData.get("Impressions"))
						.setInt(8, Integer.parseInt(String.valueOf(weeklyData.get("Clicks"))))
						.setFloat(9, Float.parseFloat(String.valueOf(weeklyData.get("Ctr"))))
						.setFloat(10, Float.parseFloat(String.valueOf(weeklyData.get("Cost"))))
						.setFloat(11, Float.parseFloat(String.valueOf(weeklyData.get("CostPerConversion"))))
						.setInt(12, Integer.parseInt(String.valueOf(weeklyData.get("VideoViews"))))
						.setFloat(13, Float.parseFloat(String.valueOf(weeklyData.get("VideoViewRate"))))
						.setInt(14, Integer.parseInt(String.valueOf(weeklyData.get("ViewThroughConversions"))))
						.setFloat(15, Float.parseFloat(
								String.valueOf(weeklyData.get("SearchBudgetLostImpressionShare") == null ? "0.0"
										: weeklyData.get("SearchBudgetLostImpressionShare"))))
						.setFloat(16, Float.parseFloat(
								String.valueOf(weeklyData.get("SearchExactMatchImpressionShare") == null ? "0.0"
										: weeklyData.get("SearchExactMatchImpressionShare"))))
						.setFloat(17,
								Float.parseFloat(String.valueOf(weeklyData.get("SearchImpressionShare") == null ? "0.0"
										: weeklyData.get("SearchImpressionShare"))))
						.setFloat(18, Float.parseFloat(
								String.valueOf(weeklyData.get("SearchRankLostImpressionShare") == null ? "0.0"
										: weeklyData.get("SearchRankLostImpressionShare"))))
						.setInt(19, Integer.parseInt(String.valueOf(weeklyData.get("InvalidClicks"))))
						.setFloat(20, Float.parseFloat(String.valueOf(weeklyData.get("InvalidClickRate"))))
						.setFloat(21, Float.parseFloat(String.valueOf(weeklyData.get("AllConversionValue"))))
						.setString(22, (String) weeklyData.get("Date"))
						.setString(23, (String) weeklyData.get("AdvertisingChannelType"))
						.setFloat(24, Float.parseFloat(String.valueOf(weeklyData.get("AverageTimeOnSite"))))
						.setFloat(25, Float.parseFloat(String.valueOf(weeklyData.get("BounceRate"))))
						.setString(26, String.valueOf(weeklyData.get("CampaignId")))
						.setString(27, String.valueOf(weeklyData.get("CampaignName")))
						.setString(28, String.valueOf(weeklyData.get("CampaignStatus")))
						.setFloat(29, Float.parseFloat(String.valueOf(weeklyData.get("ClickAssistedConversions"))))
						.setFloat(30, Float.parseFloat(String.valueOf(weeklyData.get("ConversionRate"))))
						.setString(31, String.valueOf(weeklyData.get("ImpressionReach")))
						.setLong(32, System.currentTimeMillis()).setString(33, adwordsId);
				session.execute(boundStmt);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {

		}
	}

	public void writeDataToCampaignAccountPerformanceCassandraWeekly(
			List<Map<String, Object>> account_performance_weekly_data, String customerId, String adwordsId) {
		int size = account_performance_weekly_data.size();

		try {

			for (int i = 0; i < size; i++) {
				Map<String, Object> weeklyData = account_performance_weekly_data.get(i);

				StringBuffer preparedStatementQuery = new StringBuffer(
						"Insert into adwords_weekly_account_campaign_performance " + "(client_stamp," + " week,"
								+ " account_descriptive_name," + " account_currency_code," + " account_timezone_id,"
								+ " average_position," + " conversions," + " impressions," + " clicks," + " ctr,"
								+ " cost," + " cost_per_conversion," + " video_views," + " video_view_rate,"
								+ " view_through_conversions," + " search_budget_lost_impression_share,"
								+ " search_exact_match_impression_share," + " search_impression_share,"
								+ " search_rank_lost_impression_share," + " invalid_clicks," + " invalid_click_rate,"
								+ " all_conversion_value," + " advertising_channel_type," + " average_time_on_site,"
								+ " bounce_rate," + " campaign_id," + " campaign_name," + " campaign_status,"
								+ " click_assisted_conversions," + " conversion_rate,"
								+ " impression_reach, time, client_customer_id" + ") values (?, ?, ?, ?, ?, ?, ?, ?"
								+ ", ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? ,? ,? ,?, ?, ?)");

				PreparedStatement preparedStmt = session.prepare(preparedStatementQuery.toString());

				BoundStatement boundStmt = preparedStmt.bind().setString(0, customerId)
						.setString(1, (String) weeklyData.get("Week"))
						.setString(2, (String) weeklyData.get("AccountDescriptiveName"))
						.setString(3, (String) weeklyData.get("AccountCurrencyCode"))
						.setString(4, (String) weeklyData.get("AccountTimeZoneId"))
						.setFloat(5, Float.parseFloat(String.valueOf(weeklyData.get("AveragePosition"))))
						.setFloat(6, Float.parseFloat(String.valueOf(weeklyData.get("Conversions"))))
						.setLong(7, (Long) weeklyData.get("Impressions"))
						.setInt(8, Integer.parseInt(String.valueOf(weeklyData.get("Clicks"))))
						.setFloat(9, Float.parseFloat(String.valueOf(weeklyData.get("Ctr"))))
						.setFloat(10, Float.parseFloat(String.valueOf(weeklyData.get("Cost"))))
						.setFloat(11, Float.parseFloat(String.valueOf(weeklyData.get("CostPerConversion"))))
						.setInt(12, Integer.parseInt(String.valueOf(weeklyData.get("VideoViews"))))
						.setFloat(13, Float.parseFloat(String.valueOf(weeklyData.get("VideoViewRate"))))
						.setInt(14, Integer.parseInt(String.valueOf(weeklyData.get("ViewThroughConversions"))))
						.setFloat(15, Float.parseFloat(
								String.valueOf(weeklyData.get("SearchBudgetLostImpressionShare") == null ? "0.0"
										: weeklyData.get("SearchBudgetLostImpressionShare"))))
						.setFloat(16, Float.parseFloat(
								String.valueOf(weeklyData.get("SearchExactMatchImpressionShare") == null ? "0.0"
										: weeklyData.get("SearchExactMatchImpressionShare"))))
						.setFloat(17,
								Float.parseFloat(String.valueOf(weeklyData.get("SearchImpressionShare") == null ? "0.0"
										: weeklyData.get("SearchImpressionShare"))))
						.setFloat(18, Float.parseFloat(
								String.valueOf(weeklyData.get("SearchRankLostImpressionShare") == null ? "0.0"
										: weeklyData.get("SearchRankLostImpressionShare"))))
						.setInt(19, Integer.parseInt(String.valueOf(weeklyData.get("InvalidClicks"))))
						.setFloat(20, Float.parseFloat(String.valueOf(weeklyData.get("InvalidClickRate"))))
						.setFloat(21, Float.parseFloat(String.valueOf(weeklyData.get("AllConversionValue"))))
						.setString(22, (String) weeklyData.get("AdvertisingChannelType"))
						.setFloat(23, Float.parseFloat(String.valueOf(weeklyData.get("AverageTimeOnSite"))))
						.setFloat(24, Float.parseFloat(String.valueOf(weeklyData.get("BounceRate"))))
						.setString(25, String.valueOf(weeklyData.get("CampaignId")))
						.setString(26, String.valueOf(weeklyData.get("CampaignName")))
						.setString(27, String.valueOf(weeklyData.get("CampaignStatus")))
						.setFloat(28, Float.parseFloat(String.valueOf(weeklyData.get("ClickAssistedConversions"))))
						.setFloat(29, Float.parseFloat(String.valueOf(weeklyData.get("ConversionRate"))))
						.setString(30, String.valueOf(weeklyData.get("ImpressionReach")))
						.setLong(31, System.currentTimeMillis()).setString(32, adwordsId);

				session.execute(boundStmt);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {

		}
	}

	public void writeDataToCampaignAccountPerformanceCassandraMonthly(
			List<Map<String, Object>> account_performance_monthly_data, String customerId, String adwordsId) {
		int size = account_performance_monthly_data.size();

		try {

			for (int i = 0; i < size; i++) {
				Map<String, Object> monthlyData = account_performance_monthly_data.get(i);

				StringBuffer preparedStatementQuery = new StringBuffer(
						"Insert into adwords_monthly_account_campaign_performance " + "(client_stamp," + " month,"
								+ " account_descriptive_name," + " account_currency_code," + " account_timezone_id,"
								+ " average_position," + " conversions," + " impressions," + " clicks," + " ctr,"
								+ " cost," + " cost_per_conversion," + " video_views," + " video_view_rate,"
								+ " view_through_conversions," + " search_budget_lost_impression_share,"
								+ " search_exact_match_impression_share," + " search_impression_share,"
								+ " search_rank_lost_impression_share," + " invalid_clicks," + " invalid_click_rate,"
								+ " all_conversion_value," + " advertising_channel_type," + " average_time_on_site,"
								+ " bounce_rate," + " campaign_id," + " campaign_name," + " campaign_status,"
								+ " click_assisted_conversions," + " conversion_rate,"
								+ " impression_reach, time, client_customer_id" + ") values (?, ?, ?, ?, ?, ?, ?, ?"
								+ ", ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? ,? ,? ,?, ?, ?)");

				PreparedStatement preparedStmt = session.prepare(preparedStatementQuery.toString());

				BoundStatement boundStmt = preparedStmt.bind().setString(0, customerId)
						.setString(1, (String) monthlyData.get("Month"))
						.setString(2, (String) monthlyData.get("AccountDescriptiveName"))
						.setString(3, (String) monthlyData.get("AccountCurrencyCode"))
						.setString(4, (String) monthlyData.get("AccountTimeZoneId"))
						.setFloat(5, Float.parseFloat(String.valueOf(monthlyData.get("AveragePosition"))))
						.setFloat(6, Float.parseFloat(String.valueOf(monthlyData.get("Conversions"))))
						.setLong(7, (Long) monthlyData.get("Impressions"))
						.setInt(8, Integer.parseInt(String.valueOf(monthlyData.get("Clicks"))))
						.setFloat(9, Float.parseFloat(String.valueOf(monthlyData.get("Ctr"))))
						.setFloat(10, Float.parseFloat(String.valueOf(monthlyData.get("Cost"))))
						.setFloat(11, Float.parseFloat(String.valueOf(monthlyData.get("CostPerConversion"))))
						.setInt(12, Integer.parseInt(String.valueOf(monthlyData.get("VideoViews"))))
						.setFloat(13, Float.parseFloat(String.valueOf(monthlyData.get("VideoViewRate"))))
						.setInt(14, Integer.parseInt(String.valueOf(monthlyData.get("ViewThroughConversions"))))
						.setFloat(15, Float.parseFloat(
								String.valueOf(monthlyData.get("SearchBudgetLostImpressionShare") == null ? "0.0"
										: monthlyData.get("SearchBudgetLostImpressionShare"))))
						.setFloat(16, Float.parseFloat(
								String.valueOf(monthlyData.get("SearchExactMatchImpressionShare") == null ? "0.0"
										: monthlyData.get("SearchExactMatchImpressionShare"))))
						.setFloat(17,
								Float.parseFloat(String.valueOf(monthlyData.get("SearchImpressionShare") == null ? "0.0"
										: monthlyData.get("SearchImpressionShare"))))
						.setFloat(18, Float.parseFloat(
								String.valueOf(monthlyData.get("SearchRankLostImpressionShare") == null ? "0.0"
										: monthlyData.get("SearchRankLostImpressionShare"))))
						.setInt(19, Integer.parseInt(String.valueOf(monthlyData.get("InvalidClicks"))))
						.setFloat(20, Float.parseFloat(String.valueOf(monthlyData.get("InvalidClickRate"))))
						.setFloat(21, Float.parseFloat(String.valueOf(monthlyData.get("AllConversionValue"))))
						.setString(22, (String) monthlyData.get("AdvertisingChannelType"))
						.setFloat(23, Float.parseFloat(String.valueOf(monthlyData.get("AverageTimeOnSite"))))
						.setFloat(24, Float.parseFloat(String.valueOf(monthlyData.get("BounceRate"))))
						.setString(25, String.valueOf(monthlyData.get("CampaignId")))
						.setString(26, String.valueOf(monthlyData.get("CampaignName")))
						.setString(27, String.valueOf(monthlyData.get("CampaignStatus")))
						.setFloat(28, Float.parseFloat(String.valueOf(monthlyData.get("ClickAssistedConversions"))))
						.setFloat(29, Float.parseFloat(String.valueOf(monthlyData.get("ConversionRate"))))
						.setString(30, String.valueOf(monthlyData.get("ImpressionReach")))
						.setLong(31, System.currentTimeMillis()).setString(32, adwordsId);

				session.execute(boundStmt);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {

		}
	}

	public void writeDataToAccountPerformanceToCassandraMonthly(
			List<Map<String, Object>> account_performance_monthly_data, String customerId, String adwordsId,
			Long budget) {

		int size = account_performance_monthly_data.size();

		try {
			for (int i = 0; i < size; i++) {

				Map<String, Object> monthlyData = account_performance_monthly_data.get(i);

				StringBuffer preparedStatementQuery = new StringBuffer(
						"Insert into adwords_monthly_account_performance " + "(client_stamp," + " month,"
								+ " external_customer_id," + " account_descriptive_name," + " account_currency_code,"
								+ " account_timezone_id," + " average_position," + " conversions," + " impressions,"
								+ " clicks," + " ctr," + " cost," + " cost_per_conversion," + " all_conversion_rate,"
								+ " video_views," + " video_view_rate," + " average_cpv," + " view_through_conversions,"
								+ " search_budget_lost_impression_share," + " search_exactmatch_impression_share,"
								+ " search_impression_share," + " search_rank_lost_impression_share,"
								+ " invalid_clicks," + " invalid_click_rate,"
								+ " all_conversion_value, time, client_customer_id,monthly_budget) values (?, ?, ?, ?, ?, ?, ?, ?"
								+ ", ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
				session = getConection();
				PreparedStatement preparedStmt = session.prepare(preparedStatementQuery.toString());

				BoundStatement boundStmt = preparedStmt.bind().setString(0, customerId)
						.setString(1, (String) monthlyData.get("Month"))
						.setString(2, String.valueOf(monthlyData.get("ExternalCustomerid")))
						.setString(3, (String) monthlyData.get("AccountDescriptiveName"))
						.setString(4, (String) monthlyData.get("AccountCurrencyCode"))
						.setString(5, (String) monthlyData.get("AccountTimeZoneId"))
						.setFloat(6, Float.parseFloat(String.valueOf(monthlyData.get("AveragePosition"))))
						.setFloat(7, Float.parseFloat(String.valueOf(monthlyData.get("Conversions"))))
						.setLong(8, (Long) monthlyData.get("Impressions"))
						.setInt(9, Integer.parseInt(String.valueOf(monthlyData.get("Clicks"))))
						.setFloat(10, Float.parseFloat(String.valueOf(monthlyData.get("Ctr"))))
						.setFloat(11, Float.parseFloat(String.valueOf(monthlyData.get("Cost"))))
						.setFloat(12, Float.parseFloat(String.valueOf(monthlyData.get("CostPerConversion"))))
						.setFloat(13, Float.parseFloat(String.valueOf(monthlyData.get("AllConversionRate"))))
						.setInt(14, Integer.parseInt(String.valueOf(monthlyData.get("VideoViews"))))
						.setFloat(15, Float.parseFloat(String.valueOf(monthlyData.get("VideoViewRate"))))
						.setFloat(16, Float.parseFloat(String.valueOf(monthlyData.get("AverageCpv"))))
						.setInt(17, Integer.parseInt(String.valueOf(monthlyData.get("ViewThroughConversions"))))
						.setFloat(18, Float.parseFloat(
								String.valueOf(monthlyData.get("SearchBudgetLostImpressionShare") == null ? "0.0"
										: monthlyData.get("SearchBudgetLostImpressionShare"))))
						.setFloat(19, Float.parseFloat(
								String.valueOf(monthlyData.get("SearchExactMatchImpressionShare") == null ? "0.0"
										: monthlyData.get("SearchExactMatchImpressionShare"))))
						.setFloat(20,
								Float.parseFloat(String.valueOf(monthlyData.get("SearchImpressionShare") == null ? "0.0"
										: monthlyData.get("SearchImpressionShare"))))
						.setFloat(21, Float.parseFloat(
								String.valueOf(monthlyData.get("SearchRankLostImpressionShare") == null ? "0.0"
										: monthlyData.get("SearchRankLostImpressionShare"))))
						.setInt(22, Integer.parseInt(String.valueOf(monthlyData.get("InvalidClicks"))))
						.setFloat(23, Float.parseFloat(String.valueOf(monthlyData.get("InvalidClickRate"))))
						.setFloat(24, Float.parseFloat(String.valueOf(monthlyData.get("AllConversionValue"))))
						.setLong(25, System.currentTimeMillis()).setString(26, adwordsId).setLong(27, budget);

				session.execute(boundStmt);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {

		}

	}

	public void writeDataToAdNetworkAccountPerformanceCassandraMonthly(
			List<Map<String, Object>> account_performance_monthly_data, String customerId, String adwordsId) {
		int size = account_performance_monthly_data.size();
		try {
			for (int i = 0; i < size; i++) {

				Map<String, Object> monthlyData = account_performance_monthly_data.get(i);

				StringBuffer preparedStatementQuery = new StringBuffer(
						"Insert into adwords_monthly_ad_network_account_performance " + "(client_stamp," + " month,"
								+ " external_customer_id," + " ad_network_type1," + " account_descriptive_name,"
								+ " account_currency_code," + " account_timezone_id," + " average_position,"
								+ " conversions," + " impressions," + " clicks," + " ctr," + " cost,"
								+ " cost_per_conversion," + " all_conversion_rate," + " video_views,"
								+ " video_view_rate," + " average_cpv," + " view_through_conversions,"
								+ " search_budget_lost_impression_share," + " search_exactmatch_impression_share,"
								+ " search_impression_share," + " search_rank_lost_impression_share,"
								+ " invalid_clicks," + " invalid_click_rate,"
								+ " all_conversion_value, time, client_customer_id) values (?, ?, ?, ?, ?, ?, ?, ?"
								+ ", ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");

				PreparedStatement preparedStmt = session.prepare(preparedStatementQuery.toString());

				BoundStatement boundStmt = preparedStmt.bind().setString(0, customerId)
						.setString(1, (String) monthlyData.get("Month"))
						.setString(2, String.valueOf(monthlyData.get("ExternalCustomerid")))
						.setString(3, (String) monthlyData.get("AdNetworkType1"))
						.setString(4, (String) monthlyData.get("AccountDescriptiveName"))
						.setString(5, (String) monthlyData.get("AccountCurrencyCode"))
						.setString(6, (String) monthlyData.get("AccountTimeZoneId"))
						.setFloat(7, Float.parseFloat(String.valueOf(monthlyData.get("AveragePosition"))))
						.setFloat(8, Float.parseFloat(String.valueOf(monthlyData.get("Conversions"))))
						.setLong(9, (Long) monthlyData.get("Impressions"))
						.setInt(10, Integer.parseInt(String.valueOf(monthlyData.get("Clicks"))))
						.setFloat(11, Float.parseFloat(String.valueOf(monthlyData.get("Ctr"))))
						.setFloat(12, Float.parseFloat(String.valueOf(monthlyData.get("Cost"))))
						.setFloat(13, Float.parseFloat(String.valueOf(monthlyData.get("CostPerConversion"))))
						.setFloat(14, Float.parseFloat(String.valueOf(monthlyData.get("AllConversionRate"))))
						.setInt(15, Integer.parseInt(String.valueOf(monthlyData.get("VideoViews"))))
						.setFloat(16, Float.parseFloat(String.valueOf(monthlyData.get("VideoViewRate"))))
						.setFloat(17, Float.parseFloat(String.valueOf(monthlyData.get("AverageCpv"))))
						.setInt(18, Integer.parseInt(String.valueOf(monthlyData.get("ViewThroughConversions"))))
						.setFloat(19, Float.parseFloat(
								String.valueOf(monthlyData.get("SearchBudgetLostImpressionShare") == null ? "0.0"
										: monthlyData.get("SearchBudgetLostImpressionShare"))))
						.setFloat(20, Float.parseFloat(
								String.valueOf(monthlyData.get("SearchExactMatchImpressionShare") == null ? "0.0"
										: monthlyData.get("SearchExactMatchImpressionShare"))))
						.setFloat(21,
								Float.parseFloat(String.valueOf(monthlyData.get("SearchImpressionShare") == null ? "0.0"
										: monthlyData.get("SearchImpressionShare"))))
						.setFloat(22, Float.parseFloat(
								String.valueOf(monthlyData.get("SearchRankLostImpressionShare") == null ? "0.0"
										: monthlyData.get("SearchRankLostImpressionShare"))))
						.setInt(23, Integer.parseInt(String.valueOf(monthlyData.get("InvalidClicks"))))
						.setFloat(24, Float.parseFloat(String.valueOf(monthlyData.get("InvalidClickRate"))))
						.setFloat(25, Float.parseFloat(String.valueOf(monthlyData.get("AllConversionValue"))))
						.setLong(26, System.currentTimeMillis()).setString(27, adwordsId);
				session.execute(boundStmt);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {

		}

	}

}
