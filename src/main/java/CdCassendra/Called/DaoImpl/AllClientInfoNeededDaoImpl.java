package CdCassendra.Called.DaoImpl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.stereotype.Component;

import CdCassendra.Called.Dao.AllClientInfoNeededDao;
import CdCassendra.Called.Utility.Constants;

@Component
public class AllClientInfoNeededDaoImpl implements AllClientInfoNeededDao {

	DataSource dataSource;
	JdbcTemplate jdbcTemplate;

	public List<Map<String, Object>> getAllClientInfo() {
		String sql = "SELECT " + Constants.internalClientId + ", " + Constants.OAuthClientId + ", "
				+ Constants.OAuthClientSecret + ", " + Constants.InitiateOAuth + ", " + Constants.OAuthSettingsLocation
				+ ", " + Constants.ClientCustomerId + ", " + Constants.Logfile + ", " + Constants.DeveloperToken 
				+ ", " + Constants.clientMonthlyBudget + ", " + Constants.clientWeeklyBudget + " FROM client_details";
		List<Map<String, Object>> results = jdbcTemplate.queryForList(sql);

		return results;
	}

	public AllClientInfoNeededDaoImpl(DataSource dataSource) {
		jdbcTemplate = new JdbcTemplate(dataSource);
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

	public List<Map<String, Object>> createURLFromAdwordsData(List<Map<String, Object>> listAdwordsId) {
		List<Map<String, Object>> mapList = new ArrayList<Map<String, Object>>();
		
		Map<String, Object> map = null;
		try {
			for (int i = 0; i < listAdwordsId.size(); i++) {
				Map<String, Object> mapAccess = new HashMap<String, Object>();
				map = listAdwordsId.get(i);
				String url = "jdbc:googleadwords:" + Constants.OAuthClientId + "=" + map.get(Constants.OAuthClientId)
						+ ";" + Constants.OAuthClientSecret + "=" + map.get(Constants.OAuthClientSecret) + ";"
						+ Constants.InitiateOAuth + "=" + map.get(Constants.InitiateOAuth) + ";"
						+ Constants.OAuthSettingsLocation + "=" + map.get(Constants.OAuthSettingsLocation) + ";"
						+ Constants.ClientCustomerId + "=" + map.get(Constants.ClientCustomerId) + ";"
						+ Constants.Logfile + "=" + map.get(Constants.Logfile) + ";" + Constants.DeveloperToken + "="
						+ map.get(Constants.DeveloperToken);
				mapAccess.put("finalUrl" + i, url);
				mapAccess.put("client_stamp" + i, map.get(Constants.internalClientId));
				mapAccess.put("client_customer_id" + i, map.get(Constants.ClientCustomerId));
				mapAccess.put("clientMonthlyBudget"  +i,map.get(Constants.clientMonthlyBudget)  == null ? 0 : map.get(Constants.clientMonthlyBudget));
				mapAccess.put("clientWeeklyBudget"  +i,map.get(Constants.clientWeeklyBudget) == null ? 0 : map.get(Constants.clientWeeklyBudget));
				mapList.add(mapAccess);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return mapList;
	}

}
