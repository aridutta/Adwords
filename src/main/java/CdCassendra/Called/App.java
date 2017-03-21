package CdCassendra.Called;

import java.util.List;
import java.util.Map;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import CdCassendra.Called.DaoImpl.AllClientInfoNeededDaoImpl;
import CdCassendra.Called.DaoImpl.FetchDataFromAdwordsDaoImpl;

/**
 * Hello world!
 *
 */
public class App {

	public static void main(String[] args) {

		ApplicationContext ctx = new ClassPathXmlApplicationContext("spring.xml");

		AllClientInfoNeededDaoImpl dao = ctx.getBean("dao", AllClientInfoNeededDaoImpl.class);

		FetchDataFromAdwordsDaoImpl daoFetch = ctx.getBean("fetchDao", FetchDataFromAdwordsDaoImpl.class);

		List<Map<String, Object>> list = dao.getAllClientInfo();

		List<Map<String, Object>> urlList = dao.createURLFromAdwordsData(list);

		daoFetch.get_Weekly_AccountPerformance_Data_From_Adwords(urlList);

		daoFetch.get_Weekly_AdNetworkPerformance_Data_From_Adwords(urlList);

		daoFetch.get_Daily_AccountPerformance_Data_From_Adwords(urlList);

		daoFetch.get_Daily_AdNetworkPerformance_Data_From_Adwords(urlList);

		daoFetch.get_Daily_CampaignPerformance_Data_From_Adwords(urlList);

		daoFetch.get_Weekly_CampaignPerformance_Data_From_Adwords(urlList);

		daoFetch.get_Monthly_AccountPerformance_Data_From_Adwords(urlList);

		daoFetch.get_Monthly_AdNetworkPerformance_Data_From_Adwords(urlList);

		daoFetch.get_Monthly_CampaignPerformance_Data_From_Adwords(urlList);

	}

}
