package CdCassandra.Called.sub;

public interface ServiceBudget {

	public long getmonthlyClientBudget(String clientMonthlyBudget, String clientWeeklyBudget, String date);

	public long getweeklyClientBudget(String clientMonthlyBudget, String clientWeeklyBudget, String date);

	public long getdailyClientBudget(String clientMonthlyBudget, String clientWeeklyBudget, String date);

}
