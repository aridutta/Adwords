package CdCassandra.Called.sub;

public class ServiceBudgetImpl implements ServiceBudget {

	private int dayInMonths(int month, int year) {

		int daysInMonth = 0;
		if (month == 4 || month == 6 || month == 9 || month == 11)

			daysInMonth = 30;
		else if (month == 2)

			daysInMonth = (year % 4 == 0 && ((year % 100 == 0 && year % 400 == 0))) ? 29 : 28;
		else {

			daysInMonth = 31;
		}
		return daysInMonth;
	}

	public long getmonthlyClientBudget(String clientMonthlyBudget, String clientWeeklyBudget, String currentMonthDate) {

		Long budgetInfo = null;
		int month = 0;
		int year = 0;
		Long budgetdetail = 0L;
		boolean isMonthlyBudget = false;
		month = Integer.parseInt(currentMonthDate.split("-")[1]);
		year = Integer.parseInt(currentMonthDate.split("-")[0]);
		if (!clientMonthlyBudget.equalsIgnoreCase("0")) {
			isMonthlyBudget = true;
			budgetdetail = Long.parseLong(clientMonthlyBudget);
		} else if (!clientWeeklyBudget.equalsIgnoreCase("0")) {
			isMonthlyBudget= false;
			budgetInfo = Long.parseLong(clientWeeklyBudget);
			budgetdetail = budgetInfo / 7; 
			budgetdetail = budgetdetail * dayInMonths(month, year);
		}
		return budgetdetail;
	}

	public long getweeklyClientBudget(String clientMonthlyBudget, String clientWeeklyBudget, String currentMonthDate) {
		Long budgetInfo = null;
		int month = 0;
		int year = 0;
		month = Integer.parseInt(currentMonthDate.split("-")[1]);
		year = Integer.parseInt(currentMonthDate.split("-")[0]);
		Long budgetdetail= 0L;
		boolean isMonthlyBudget = false;
		if (!clientMonthlyBudget.equalsIgnoreCase("0")) {
			budgetInfo = Long.parseLong(clientMonthlyBudget);
			isMonthlyBudget= true;
			budgetInfo = budgetInfo / dayInMonths(month, year);
			budgetdetail = budgetInfo * 7;
		} else if (!clientWeeklyBudget.equalsIgnoreCase("0")) {
			isMonthlyBudget= false;
			budgetdetail = Long.parseLong(clientWeeklyBudget);
		}
	return budgetdetail;
	}

	public long getdailyClientBudget(String clientMonthlyBudget, String clientWeeklyBudget, String currentMonthDate) {
		Long budgetInfo = null;
		Long budgetdetail= 0L;
		int month, year;
		month = Integer.parseInt(currentMonthDate.split("-")[1]);
		year = Integer.parseInt(currentMonthDate.split("-")[0]);
		boolean isMonthlyBudget = false;
		if (!clientMonthlyBudget.equalsIgnoreCase("0")) {
			budgetInfo = Long.parseLong(clientMonthlyBudget);
			 isMonthlyBudget = true;
			budgetdetail = budgetInfo / dayInMonths(month, year);
		} else if (!clientWeeklyBudget.equalsIgnoreCase("0")) {
			budgetInfo = Long.parseLong(clientWeeklyBudget);
			 isMonthlyBudget = false;
			budgetdetail = budgetInfo / 7;
		}
		return budgetdetail;
	}

}
