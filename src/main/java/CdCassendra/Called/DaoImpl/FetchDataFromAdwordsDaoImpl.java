package CdCassendra.Called.DaoImpl;

import java.sql.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;
import java.util.TimerTask;

import javax.sql.DataSource;

import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import CdCassandra.Called.sub.ServiceBudgetImpl;
import CdCassendra.Called.Dao.FetchDataFromAdwordsDao;
import CdCassendra.Called.Utility.UtilityTimeHelper;

public class FetchDataFromAdwordsDaoImpl implements FetchDataFromAdwordsDao {
	JdbcTemplate jdbc;
	CassandraCRUDImpl cassandraDao;
	String startDate1 = "";
	String endDate1 = "";
	String date = "";
	DataSource source;
	ServiceBudgetImpl service;

	public FetchDataFromAdwordsDaoImpl(DataSource source, CassandraCRUDImpl cassandraDao, ServiceBudgetImpl service) {
		this.source = source;
		jdbc = new JdbcTemplate(source);
		this.cassandraDao = cassandraDao;
		startDate1 = getDateStart();
		endDate1 = getDateEnd();
		date = getDailyDateFromUtilityHelper();
		this.service = service;
	}

	@Bean
	public DataSource getDataSource(String url) {
		DriverManagerDataSource dataSource = new DriverManagerDataSource();
		dataSource.setDriverClassName("cdata.jdbc.googleadwords.GoogleAdWordsDriver");
		dataSource.setUrl(url);
		dataSource.setUsername("root");
		dataSource.setPassword("password");
		return dataSource;
	}

	public void getAllDataFromAdwords(List<String> dataList) {

		for (int i = 0; i < dataList.size(); i++) {
			String url = dataList.get(i);
			DataSource source = getDataSource(url);
			jdbc = new JdbcTemplate(source);
		}

	}

	public String getDateEnd() {
		String s = new UtilityTimeHelper().getYesterdayDateString();
		return s;
	}

	public String getDateStart() {
		String s = new UtilityTimeHelper().getWeeklyDateString();
		return s;
	}

	public String getDailyDateFromUtilityHelper() {
		String s = new UtilityTimeHelper().getYesterdayDateString();
		return s;
	}

	private String getDateEndMonthly() {
		String s = new UtilityTimeHelper().getYesterdayDateString();
		return s;
	}

	private String getDateStartMonthly() {
		String s = new UtilityTimeHelper().getWeeklyDateString();
		return s;
	}

	public void get_Weekly_AccountPerformance_Data_From_Adwords(final List<Map<String, Object>> urlList) {

		try {
			TimerTask t = new TimerTask() {

				@Override
				public void run() {

					String startDate1acc = getDateStart();
					startDate1acc = startDate1acc.replaceAll("/", "-");
					String endDate1acc = getDateEnd();
					endDate1acc = endDate1acc.replaceAll("/", "-");

					int size = urlList.size();

					for (int i = 0; i < size; i++) {
						final String weekly_account_performance_query = "SELECT AccountPerformance.Week,AccountPerformance.ExternalCustomerId,AccountPerformance.AccountDescriptiveName,"
								+ "AccountPerformance.AccountCurrencyCode,AccountPerformance.AccountTimeZoneId,AccountPerformance.AveragePosition,AccountPerformance.Conversions,"
								+ "AccountPerformance.Impressions,AccountPerformance.Clicks,AccountPerformance.Ctr,AccountPerformance.Cost,AccountPerformance.CostPerConversion,"
								+ "AccountPerformance.AllConversionRate,AccountPerformance.VideoViews,AccountPerformance.VideoViewRate,AccountPerformance.AverageCpv,AccountPerformance.ViewThroughConversions,"
								+ "AccountPerformance.SearchBudgetLostImpressionShare,AccountPerformance.SearchExactMatchImpressionShare,AccountPerformance.SearchImpressionShare,"
								+ "AccountPerformance.SearchRankLostImpressionShare,AccountPerformance.InvalidClicks,AccountPerformance.InvalidClickRate,AccountPerformance.AllConversionValue"
								+ " FROM AccountPerformance WHERE " + "StartDate='" + startDate1acc + "' and EndDate='"
								+ endDate1acc + "'";

						source = getDataSource((String) urlList.get(i).get("finalUrl" + i));
						jdbc = new JdbcTemplate(source);

						List<Map<String, Object>> account_performance_weekly_data = jdbc
								.queryForList(weekly_account_performance_query);
						// System.out.println(account_performance_weekly_data);
						System.out.println("Ok");
						String customerId = (String) urlList.get(i).get("client_stamp" + i);

						String adwordsId = (String) urlList.get(i).get("client_customer_id" + i);

						String clientMonthlyBudget = String.valueOf(urlList.get(i).get("clientMonthlyBudget" + i));

						String clientWeeklyBudget = String.valueOf(urlList.get(i).get("clientWeeklyBudget" + i));

						String dates = "";
						if (account_performance_weekly_data.size() > 0) {
							Map<String, Object> weeklyData = account_performance_weekly_data.get(0);
							dates = (String) weeklyData.get("Week");
						} else {
							DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
							Date date = new Date(System.currentTimeMillis());
							dates = dateFormat.format(date).toString();
						}

						Long budget = service.getweeklyClientBudget(clientMonthlyBudget, clientWeeklyBudget, dates);

						cassandraDao.writeDataToAccountPerformanceToCassandraWeekly(account_performance_weekly_data,
								customerId, adwordsId, budget);
					}
				}
			};

			UtilityTimeHelper u = new UtilityTimeHelper();
			u.method(t);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {

		}

	}

	public void get_Weekly_CampaignPerformance_Data_From_Adwords(final List<Map<String, Object>> urlList) {

		try {
			TimerTask t = new TimerTask() {

				@Override
				public void run() {

					String startDate1camp = getDateStart();
					startDate1camp = startDate1camp.replaceAll("/", "-");
					String endDate1camp = getDateEnd();
					endDate1camp = endDate1camp.replaceAll("/", "-");

					int size = urlList.size();

					for (int i = 0; i < size; i++) {
						final String campaign_performance = "SELECT CampaignPerformance.Week,CampaignPerformance.AccountCurrencyCode,CampaignPerformance.AdvertisingChannelType,"
								+ "CampaignPerformance.AccountDescriptiveName,CampaignPerformance.AccountTimeZoneId,CampaignPerformance.CampaignId,CampaignPerformance.CampaignName,"
								+ "CampaignPerformance.CampaignStatus,CampaignPerformance.Cost,CampaignPerformance.Impressions,CampaignPerformance.Clicks,CampaignPerformance.Ctr,"
								+ "CampaignPerformance.Conversions,CampaignPerformance.ConversionRate,CampaignPerformance.CostPerConversion,CampaignPerformance.VideoViews,"
								+ "CampaignPerformance.VideoViewRate,CampaignPerformance.ViewThroughConversions,CampaignPerformance.InvalidClicks,CampaignPerformance.InvalidClickRate,"
								+ "CampaignPerformance.ImpressionReach,CampaignPerformance.SearchBudgetLostImpressionShare,CampaignPerformance.SearchExactMatchImpressionShare,"
								+ "CampaignPerformance.SearchImpressionShare,CampaignPerformance.SearchRankLostImpressionShare,CampaignPerformance.AveragePosition,"
								+ "CampaignPerformance.AverageTimeOnSite,CampaignPerformance.BounceRate,CampaignPerformance.ClickAssistedConversions,"
								+ "CampaignPerformance.AllConversionValue FROM CampaignPerformance WHERE "
								+ "StartDate='" + startDate1camp + "' and EndDate='" + endDate1camp + "'";

						source = getDataSource((String) urlList.get(i).get("finalUrl" + i));
						jdbc = new JdbcTemplate(source);

						List<Map<String, Object>> weekly_campaign_performance_data = jdbc
								.queryForList(campaign_performance);
						// System.out.println(weekly_campaign_performance_data);
						System.out.println("Ok");
						String customerId = (String) urlList.get(i).get("client_stamp" + i);

						String adwordsId = (String) urlList.get(i).get("client_customer_id" + i);

						cassandraDao.writeDataToCampaignAccountPerformanceCassandraWeekly(
								weekly_campaign_performance_data, customerId, adwordsId);
					}
				}
			};

			UtilityTimeHelper u = new UtilityTimeHelper();
			u.method(t);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public void get_Weekly_AdNetworkPerformance_Data_From_Adwords(final List<Map<String, Object>> urlList) {

		try {
			TimerTask t = new TimerTask() {

				@Override
				public void run() {

					String startDate1net = getDateStart();
					startDate1net = startDate1net.replaceAll("/", "-");
					String endDate1net = getDateEnd();
					endDate1net = endDate1net.replaceAll("/", "-");

					int size = urlList.size();

					for (int i = 0; i < size; i++) {
						final String weekly_account_ad_network_performance = "SELECT AccountPerformance.Week,AccountPerformance.ExternalCustomerId,AccountPerformance.AdNetworkType1,AccountPerformance.AccountDescriptiveName,"
								+ "AccountPerformance.AccountCurrencyCode,AccountPerformance.AccountTimeZoneId,AccountPerformance.AveragePosition,AccountPerformance.Conversions,"
								+ "AccountPerformance.Impressions,AccountPerformance.Clicks,AccountPerformance.Ctr,AccountPerformance.Cost,AccountPerformance.CostPerConversion,"
								+ "AccountPerformance.AllConversionRate,AccountPerformance.VideoViews,AccountPerformance.VideoViewRate,AccountPerformance.AverageCpv,AccountPerformance.ViewThroughConversions,"
								+ "AccountPerformance.SearchBudgetLostImpressionShare,AccountPerformance.SearchExactMatchImpressionShare,AccountPerformance.SearchImpressionShare,"
								+ "AccountPerformance.SearchRankLostImpressionShare,AccountPerformance.InvalidClicks,AccountPerformance.InvalidClickRate,AccountPerformance.AllConversionValue"
								+ " FROM AccountPerformance WHERE " + "StartDate='" + startDate1net + "' and EndDate='"
								+ endDate1net + "'";

						source = getDataSource((String) urlList.get(i).get("finalUrl" + i));
						jdbc = new JdbcTemplate(source);

						List<Map<String, Object>> weekly_account_ad_network_performance_data = jdbc
								.queryForList(weekly_account_ad_network_performance);
						// System.out.println(weekly_account_ad_network_performance_data);
						System.out.println("Ok");

						String customerId = (String) urlList.get(i).get("client_stamp" + i);

						String adwordsId = (String) urlList.get(i).get("client_customer_id" + i);

						cassandraDao.writeDataToAdNetworkAccountPerformanceCassandraWeekly(
								weekly_account_ad_network_performance_data, customerId, adwordsId);
					}
				}
			};

			UtilityTimeHelper u = new UtilityTimeHelper();
			u.method(t);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public void get_Daily_AccountPerformance_Data_From_Adwords(final List<Map<String, Object>> urlList) {

		try {
			TimerTask t = new TimerTask() {

				@Override
				public void run() {

					String date = getDailyDateFromUtilityHelper();
					date = date.replaceAll("/", "-");

					int size = urlList.size();

					for (int i = 0; i < size; i++) {
						final String daily_account_performance = "SELECT AccountPerformance.Date,AccountPerformance.Week,AccountPerformance.ExternalCustomerId,"
								+ "AccountPerformance.AccountDescriptiveName,AccountPerformance.AccountCurrencyCode,AccountPerformance.AccountTimeZoneId,AccountPerformance.AveragePosition,"
								+ "AccountPerformance.Conversions,AccountPerformance.Impressions,AccountPerformance.Clicks,AccountPerformance.Ctr,AccountPerformance.Cost,"
								+ "AccountPerformance.CostPerConversion,AccountPerformance.AllConversionRate,AccountPerformance.VideoViews,AccountPerformance.VideoViewRate,"
								+ "AccountPerformance.AverageCpv,AccountPerformance.ViewThroughConversions,AccountPerformance.SearchBudgetLostImpressionShare,"
								+ "AccountPerformance.SearchExactMatchImpressionShare,AccountPerformance.SearchImpressionShare,AccountPerformance.SearchRankLostImpressionShare,"
								+ "AccountPerformance.InvalidClicks,AccountPerformance.InvalidClickRate,"
								+ "AccountPerformance.AllConversionValue FROM AccountPerformance WHERE StartDate='"
								+ date + "' AND EndDate='" + date + "'";

						source = getDataSource((String) urlList.get(i).get("finalUrl" + i));
						jdbc = new JdbcTemplate(source);

						List<Map<String, Object>> daily_account_performance_data = jdbc
								.queryForList(daily_account_performance);

						// System.out.println(daily_account_performance_data);
						System.out.println("Ok");
						String customerId = (String) urlList.get(i).get("client_stamp" + i);

						String adwordsId = (String) urlList.get(i).get("client_customer_id" + i);
						String clientMonthlyBudget = String.valueOf(urlList.get(i).get("clientMonthlyBudget" + i));

						String clientWeeklyBudget = String.valueOf(urlList.get(i).get("clientWeeklyBudget" + i));

						String dates = "";
						if (daily_account_performance_data.size() > 0) {
							Map<String, Object> weeklyData = daily_account_performance_data.get(0);
							dates = (String) weeklyData.get("Date");
						} else {
							DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
							Date date1 = new Date(System.currentTimeMillis());
							dates = dateFormat.format(date1).toString();
						}

						Long budget = service.getdailyClientBudget(clientMonthlyBudget, clientWeeklyBudget, dates);

						cassandraDao.writeDataToAccountPerformanceToCassandraDaily(daily_account_performance_data,
								customerId, adwordsId, budget);
					}
				}
			};

			UtilityTimeHelper u = new UtilityTimeHelper();
			u.method(t);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public void get_Daily_CampaignPerformance_Data_From_Adwords(final List<Map<String, Object>> urlList) {

		try {
			TimerTask t = new TimerTask() {

				@Override
				public void run() {

					int size = urlList.size();

					String date = getDailyDateFromUtilityHelper();
					date = date.replaceAll("/", "-");

					for (int i = 0; i < size; i++) {
						final String daily_campaign_performance = "SELECT CampaignPerformance.Date,CampaignPerformance.Week,CampaignPerformance.AccountCurrencyCode,"
								+ "CampaignPerformance.AdvertisingChannelType,CampaignPerformance.AccountDescriptiveName,CampaignPerformance.AccountTimeZoneId,"
								+ "CampaignPerformance.CampaignId,CampaignPerformance.CampaignName,CampaignPerformance.CampaignStatus,CampaignPerformance.Cost,"
								+ "CampaignPerformance.Impressions,CampaignPerformance.Clicks,CampaignPerformance.Ctr,CampaignPerformance.Conversions,"
								+ "CampaignPerformance.ConversionRate,CampaignPerformance.CostPerConversion,CampaignPerformance.VideoViews,CampaignPerformance.VideoViewRate,"
								+ "CampaignPerformance.ViewThroughConversions,CampaignPerformance.InvalidClicks,CampaignPerformance.InvalidClickRate,CampaignPerformance.ImpressionReach,"
								+ "CampaignPerformance.SearchBudgetLostImpressionShare,CampaignPerformance.SearchExactMatchImpressionShare,CampaignPerformance.SearchImpressionShare,"
								+ "CampaignPerformance.SearchRankLostImpressionShare,CampaignPerformance.AveragePosition,CampaignPerformance.AverageTimeOnSite,CampaignPerformance.BounceRate,"
								+ "CampaignPerformance.ClickAssistedConversions,CampaignPerformance.AllConversionValue FROM CampaignPerformance WHERE StartDate='"
								+ date + "' AND EndDate='" + date + "'";

						source = getDataSource((String) urlList.get(i).get("finalUrl" + i));
						jdbc = new JdbcTemplate(source);

						List<Map<String, Object>> daily_campaign_performance_data = jdbc
								.queryForList(daily_campaign_performance);
						// System.out.println(daily_campaign_performance_data);

						System.out.println("Ok");

						String customerId = (String) urlList.get(i).get("client_stamp" + i);

						String adwordsId = (String) urlList.get(i).get("client_customer_id" + i);

						cassandraDao.writeDataToCampaignAccountPerformanceCassandraDaily(
								daily_campaign_performance_data, customerId, adwordsId);
					}
				}
			};

			UtilityTimeHelper u = new UtilityTimeHelper();
			u.method(t);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void get_Daily_AdNetworkPerformance_Data_From_Adwords(final List<Map<String, Object>> urlList) {

		try {
			TimerTask t = new TimerTask() {

				@Override
				public void run() {

					String date = getDailyDateFromUtilityHelper();
					date = date.replaceAll("/", "-");

					int size = urlList.size();

					for (int i = 0; i < size; i++) {
						final String daily_account_ad_network_performance = "SELECT AccountPerformance.Date,AccountPerformance.Week,AccountPerformance.ExternalCustomerId,"
								+ "AccountPerformance.AdNetworkType1,AccountPerformance.AccountDescriptiveName,AccountPerformance.AccountCurrencyCode,AccountPerformance.AccountTimeZoneId,"
								+ "AccountPerformance.AveragePosition,AccountPerformance.Conversions,AccountPerformance.Impressions,AccountPerformance.Clicks,"
								+ "AccountPerformance.Ctr,AccountPerformance.Cost,AccountPerformance.CostPerConversion,AccountPerformance.AllConversionRate,"
								+ "AccountPerformance.VideoViews,AccountPerformance.VideoViewRate,AccountPerformance.AverageCpv,AccountPerformance.ViewThroughConversions,"
								+ "AccountPerformance.SearchBudgetLostImpressionShare,AccountPerformance.SearchExactMatchImpressionShare,AccountPerformance.SearchImpressionShare,"
								+ "AccountPerformance.SearchRankLostImpressionShare,AccountPerformance.InvalidClicks,AccountPerformance.InvalidClickRate,"
								+ "AccountPerformance.AllConversionValue FROM AccountPerformance WHERE StartDate='"
								+ date + "' AND EndDate='" + date + "'";

						source = getDataSource((String) urlList.get(i).get("finalUrl" + i));
						jdbc = new JdbcTemplate(source);

						List<Map<String, Object>> daily_account_ad_network_performance_data = jdbc
								.queryForList(daily_account_ad_network_performance);

						// System.out.println(daily_account_ad_network_performance_data);
						System.out.println("Ok");

						String customerId = (String) urlList.get(i).get("client_stamp" + i);

						String adwordsId = (String) urlList.get(i).get("client_customer_id" + i);

						cassandraDao.writeDataToAdNetworkAccountPerformanceCassandraDaily(
								daily_account_ad_network_performance_data, customerId, adwordsId);
					}
				}
			};

			UtilityTimeHelper u = new UtilityTimeHelper();
			u.method(t);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void get_Monthly_CampaignPerformance_Data_From_Adwords(final List<Map<String, Object>> urlList) {

		try {
			TimerTask t = new TimerTask() {

				@Override
				public void run() {

					String startDate1camp = getDateStartMonthly();
					startDate1camp = startDate1camp.replaceAll("/", "-");
					String endDate1camp = getDateEndMonthly();
					endDate1camp = endDate1camp.replaceAll("/", "-");

					int size = urlList.size();

					for (int i = 0; i < size; i++) {
						final String campaign_performance = "SELECT CampaignPerformance.Month,CampaignPerformance.AccountCurrencyCode,"
								+ "CampaignPerformance.AdvertisingChannelType,CampaignPerformance.AccountDescriptiveName,CampaignPerformance.AccountTimeZoneId,"
								+ "CampaignPerformance.CampaignId,CampaignPerformance.CampaignName,CampaignPerformance.CampaignStatus,CampaignPerformance.Cost,"
								+ "CampaignPerformance.Impressions,CampaignPerformance.Clicks,CampaignPerformance.Ctr,CampaignPerformance.Conversions,"
								+ "CampaignPerformance.ConversionRate,CampaignPerformance.CostPerConversion,CampaignPerformance.VideoViews,"
								+ "CampaignPerformance.VideoViewRate,CampaignPerformance.ViewThroughConversions,CampaignPerformance.InvalidClicks,"
								+ "CampaignPerformance.InvalidClickRate,CampaignPerformance.ImpressionReach,CampaignPerformance.SearchBudgetLostImpressionShare,"
								+ "CampaignPerformance.SearchExactMatchImpressionShare,CampaignPerformance.SearchImpressionShare,"
								+ "CampaignPerformance.SearchRankLostImpressionShare,CampaignPerformance.AveragePosition,CampaignPerformance.AverageTimeOnSite,"
								+ "CampaignPerformance.BounceRate,CampaignPerformance.ClickAssistedConversions,CampaignPerformance.AllConversionValue"
								+ " FROM CampaignPerformance WHERE " + "StartDate='" + startDate1camp
								+ "' and EndDate='" + endDate1camp + "'";

						source = getDataSource((String) urlList.get(i).get("finalUrl" + i));
						jdbc = new JdbcTemplate(source);

						List<Map<String, Object>> monthly_campaign_performance_data = jdbc
								.queryForList(campaign_performance);
						// System.out.println(monthly_campaign_performance_data);

						System.out.println("Ok");

						String customerId = (String) urlList.get(i).get("client_stamp" + i);

						String adwordsId = (String) urlList.get(i).get("client_customer_id" + i);

						cassandraDao.writeDataToCampaignAccountPerformanceCassandraMonthly(
								monthly_campaign_performance_data, customerId, adwordsId);
					}
				}
			};

			UtilityTimeHelper u = new UtilityTimeHelper();
			u.method(t);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public void get_Monthly_AccountPerformance_Data_From_Adwords(final List<Map<String, Object>> urlList) {

		try {
			TimerTask t = new TimerTask() {

				@Override
				public void run() {

					String startDate1acc = getDateStartMonthly();
					startDate1acc = startDate1acc.replaceAll("/", "-");
					String endDate1acc = getDateEndMonthly();
					endDate1acc = endDate1acc.replaceAll("/", "-");

					int size = urlList.size();

					for (int i = 0; i < size; i++) {
						final String monthly_account_performance_query = "SELECT AccountPerformance.Month,AccountPerformance.ExternalCustomerId,"
								+ "AccountPerformance.AccountDescriptiveName,AccountPerformance.AccountCurrencyCode,AccountPerformance.AccountTimeZoneId,"
								+ "AccountPerformance.AveragePosition,AccountPerformance.Conversions,AccountPerformance.Impressions,AccountPerformance.Clicks,"
								+ "AccountPerformance.Ctr,AccountPerformance.Cost,AccountPerformance.CostPerConversion,AccountPerformance.AllConversionRate,"
								+ "AccountPerformance.VideoViews,AccountPerformance.VideoViewRate,AccountPerformance.AverageCpv,AccountPerformance.ViewThroughConversions,"
								+ "AccountPerformance.SearchBudgetLostImpressionShare,AccountPerformance.SearchExactMatchImpressionShare,"
								+ "AccountPerformance.SearchImpressionShare,AccountPerformance.SearchRankLostImpressionShare,AccountPerformance.InvalidClicks,"
								+ "AccountPerformance.InvalidClickRate,AccountPerformance.AllConversionValue"
								+ " FROM AccountPerformance WHERE " + "StartDate='" + startDate1acc + "' and EndDate='"
								+ endDate1acc + "'";

						source = getDataSource((String) urlList.get(i).get("finalUrl" + i));
						jdbc = new JdbcTemplate(source);

						List<Map<String, Object>> account_performance_monthly_data = jdbc
								.queryForList(monthly_account_performance_query);
						// System.out.println(account_performance_monthly_data);
						System.out.println("Ok");

						String customerId = (String) urlList.get(i).get("client_stamp" + i);

						String adwordsId = (String) urlList.get(i).get("client_customer_id" + i);

						String clientMonthlyBudget = String.valueOf(urlList.get(i).get("clientMonthlyBudget" + i));

						String clientWeeklyBudget = String.valueOf(urlList.get(i).get("clientWeeklyBudget" + i));

//						Map<String, Object> monthlyData = account_performance_monthly_data.get(0);
//
//						String currentMonthDate = (String) monthlyData.get("Month");
						
						String dates = "";
						if (account_performance_monthly_data.size() > 0) {
							Map<String, Object> monthlyData = account_performance_monthly_data.get(0);
							dates = (String) monthlyData.get("Month");
						} else {
							DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
							Date date1 = new Date(System.currentTimeMillis());
							dates = dateFormat.format(date1).toString();
						}

						Long budget = service.getmonthlyClientBudget(clientMonthlyBudget, clientWeeklyBudget,
								dates);

						cassandraDao.writeDataToAccountPerformanceToCassandraMonthly(account_performance_monthly_data,
								customerId, adwordsId, budget);
					}
				}
			};

			UtilityTimeHelper u = new UtilityTimeHelper();
			u.method(t);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {

		}
	}

	public void get_Monthly_AdNetworkPerformance_Data_From_Adwords(final List<Map<String, Object>> urlList) {

		try {
			TimerTask t = new TimerTask() {

				@Override
				public void run() {

					String startDate1net = getDateStartMonthly();
					startDate1net = startDate1net.replaceAll("/", "-");
					String endDate1net = getDateEndMonthly();
					endDate1net = endDate1net.replaceAll("/", "-");

					int size = urlList.size();

					for (int i = 0; i < size; i++) {
						final String monthly_account_ad_network_performance = "SELECT AccountPerformance.Month,AccountPerformance.ExternalCustomerId,"
								+ "AccountPerformance.AdNetworkType1,AccountPerformance.AccountDescriptiveName,AccountPerformance.AccountCurrencyCode,"
								+ "AccountPerformance.AccountTimeZoneId,AccountPerformance.AveragePosition,AccountPerformance.Conversions,"
								+ "AccountPerformance.Impressions,AccountPerformance.Clicks,AccountPerformance.Ctr,AccountPerformance.Cost,"
								+ "AccountPerformance.CostPerConversion,AccountPerformance.AllConversionRate,AccountPerformance.VideoViews,"
								+ "AccountPerformance.VideoViewRate,AccountPerformance.AverageCpv,AccountPerformance.ViewThroughConversions,"
								+ "AccountPerformance.SearchBudgetLostImpressionShare,AccountPerformance.SearchExactMatchImpressionShare,"
								+ "AccountPerformance.SearchImpressionShare,AccountPerformance.SearchRankLostImpressionShare,AccountPerformance.InvalidClicks,"
								+ "AccountPerformance.InvalidClickRate,AccountPerformance.AllConversionValue"
								+ " FROM AccountPerformance WHERE " + "StartDate='" + startDate1net + "' and EndDate='"
								+ endDate1net + "'";

						source = getDataSource((String) urlList.get(i).get("finalUrl" + i));
						jdbc = new JdbcTemplate(source);

						List<Map<String, Object>> monthlyly_account_ad_network_performance_data = jdbc
								.queryForList(monthly_account_ad_network_performance);
						// System.out.println(monthlyly_account_ad_network_performance_data);

						System.out.println("Ok");

						String customerId = (String) urlList.get(i).get("client_stamp" + i);

						String adwordsId = (String) urlList.get(i).get("client_customer_id" + i);

						cassandraDao.writeDataToAdNetworkAccountPerformanceCassandraMonthly(
								monthlyly_account_ad_network_performance_data, customerId, adwordsId);
					}
				}
			};

			UtilityTimeHelper u = new UtilityTimeHelper();
			u.method(t);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
