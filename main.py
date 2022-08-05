import pandas as pd
import logging

logging.basicConfig(filename="ineuron_task_30july_agent_real_time_data.log", format='%(asctime)s : %(message)s',
                    filemode="w", level=logging.DEBUG)
logger = logging.getLogger()


class AgentRealTimeData:

    def __init__(self):
        pass

    def agent_avg_weekly_rating(self, df_agent_performance):
        df_agent_performance = pd.read_csv(r'D:\Next Steps\iNeuron\Datasets\30_july\AgentPerformance.csv')
        try:
            df_agent_performance['Date'] = df_agent_performance['Date'].astype('datetime64[ns]')
            df_agent_performance['Date_week'] = df_agent_performance['Date'].dt.isocalendar().week
            df_agent_performance.drop(df_agent_performance[df_agent_performance['Total Chats'] == 0].index, inplace=True)
            df_agent_avg_rating_per_week = df_agent_performance.groupby(['Agent Name', 'Date_week'])['Average Rating'].mean()
            print(df_agent_avg_rating_per_week)
        except Exception as e:
            logging.error("Error occurred while working on dataframe: "+ str(e))

    def agent_total_working_day(self, df_agent_performance):
        df_agent_performance = pd.read_csv(r'D:\Next Steps\iNeuron\Datasets\30_july\AgentPerformance.csv')
        try:
            df_agent_performance['Date'] = df_agent_performance['Date'].astype('datetime64[ns]')
            df_agent_performance['Date_day'] = df_agent_performance['Date'].dt.isocalendar().day
            df_agent_performance.drop(df_agent_performance[df_agent_performance['Total Chats'] == 0].index, inplace=True)
            df_agent_total_working_day = df_agent_performance.groupby(['Agent Name'])['Date_day'].count()
            print(df_agent_total_working_day)
        except Exception as e:
            logging.error("Error occurred while working on dataframe: " + str(e))

    def total_query_taken(self, df_agent_performance):
        df_agent_performance = pd.read_csv(r'D:\Next Steps\iNeuron\Datasets\30_july\AgentPerformance.csv')
        try:
            df_total_query = df_agent_performance.groupby(['Agent Name'])['Total Chats'].count()
            print(df_total_query)
        except Exception as e:
            logging.error("Error occurred while working on dataframe: " + str(e))

    def total_feedback_received(self, df_agent_performance):
        df_agent_performance = pd.read_csv(r'D:\Next Steps\iNeuron\Datasets\30_july\AgentPerformance.csv')
        try:
            df_total_feedback = df_agent_performance.groupby(['Agent Name'])['Total Feedback'].count()
            print(df_total_feedback)
        except Exception as e:
            logging.error("Error occurred while working on dataframe: " + str(e))

    def avg_rating_three_and_half_to_four(self, df_agent_performance):
        df_agent_performance = pd.read_csv(r'D:\Next Steps\iNeuron\Datasets\30_july\AgentPerformance.csv')
        try:
            df_rating_three_four = df_agent_performance[df_agent_performance['Average Rating'] >= 3.5]
            df_rating_three_four = df_rating_three_four[df_rating_three_four['Average Rating'] <= 4]
            df_rating_three_four = df_rating_three_four['Agent Name'].unique()
            print("Name of agents having average rating between 3.5 and 4: ")
            print(df_rating_three_four)
        except Exception as e:
            logging.error("Error occurred while working on dataframe: " + str(e))

    def avg_rating_less_than_three_and_half(self, df_agent_performance):
        df_agent_performance = pd.read_csv(r'D:\Next Steps\iNeuron\Datasets\30_july\AgentPerformance.csv')
        try:
            df_rating_three = df_agent_performance[df_agent_performance['Average Rating'] < 3.5]
            df_rating_three = df_rating_three['Agent Name'].unique()
            print("Name of agents having average rating less than 3.5: ")
            print(df_rating_three)
        except Exception as e:
            logging.error("Error occurred while working on dataframe: " + str(e))

    def avg_rating_more_than_four_and_half(self, df_agent_performance):
        df_agent_performance = pd.read_csv(r'D:\Next Steps\iNeuron\Datasets\30_july\AgentPerformance.csv')
        try:
            df_rating_four = df_agent_performance[df_agent_performance['Average Rating'] > 4.5]
            df_rating_four = df_rating_four['Agent Name'].unique()
            print("Name of agents having average rating greater than 4.5: ")
            print(df_rating_four)
        except Exception as e:
            logging.error("Error occurred while working on dataframe: " + str(e))

    def feedback_agent_more_than_four_and_half_avg(self, df_agent_performance):
        df_agent_performance = pd.read_csv(r'D:\Next Steps\iNeuron\Datasets\30_july\AgentPerformance.csv')
        try:
            df_rating_four = df_agent_performance[df_agent_performance['Average Rating'] > 4.5]
            agent_count = len(df_rating_four['Agent Name'].unique())
            print("Count of agents having average rating greater than 4.5: ", agent_count)
        except Exception as e:
            logging.error("Error occurred while working on dataframe: " + str(e))

    def agent_avg_weekly_response_time(self, df_agent_performance):
        df_agent_performance = pd.read_csv(r'D:\Next Steps\iNeuron\Datasets\30_july\AgentPerformance.csv')
        try:
            df_agent_weekly_resp = df_agent_performance
            df_agent_weekly_resp.drop(df_agent_weekly_resp[df_agent_weekly_resp['Average Response Time'] == '00:00:00'].index, inplace=True)
            df_agent_weekly_resp['Date'] = df_agent_weekly_resp['Date'].astype('datetime64[ns]')
            df_agent_weekly_resp['Date_week'] = df_agent_weekly_resp['Date'].dt.isocalendar().week
            df_agent_weekly_resp['Average Response Time'] = df_agent_weekly_resp['Average Response Time'].astype('datetime64[ns]')
            print(df_agent_weekly_resp.groupby(['Agent Name', 'Date_week'])['Average Response Time'].mean())
        except Exception as e:
            logging.error("Error occurred while working on dataframe: " + str(e))

    def agent_avg_weekly_resolution_time(self, df_agent_performance):
        df_agent_performance = pd.read_csv(r'D:\Next Steps\iNeuron\Datasets\30_july\AgentPerformance.csv')
        try:
            df_agent_weekly_resol = df_agent_performance
            df_agent_weekly_resol.drop(df_agent_weekly_resol[df_agent_weekly_resol['Average Resolution Time'] == '00:00:00'].index, inplace=True)
            df_agent_weekly_resol['Date'] = df_agent_weekly_resol['Date'].astype('datetime64[ns]')
            df_agent_weekly_resol['Date_week'] = df_agent_weekly_resol['Date'].dt.isocalendar().week
            df_agent_weekly_resol['Average Resolution Time	'] = df_agent_weekly_resol['Average Resolution Time'].astype('datetime64[ns]')
            print(df_agent_weekly_resol.groupby(['Agent Name', 'Date_week'])['Average Resolution Time'].mean())
        except Exception as e:
            logging.error("Error occurred while working on dataframe: " + str(e))

    def list_of_all_agents(self, df_agent_performance, df_agent_login):
        try:
            df_agent_performance = pd.read_csv(r'D:\Next Steps\iNeuron\Datasets\30_july\AgentPerformance.csv')
            df_agent_login = pd.read_csv(r'D:\Next Steps\iNeuron\Datasets\30_july\AgentLogingReport.csv')
            print(df_agent_performance['Agent Name'].unique())
            print(df_agent_login['Agent'].unique())
        except Exception as e:
            logging.error("Error occurred while working on dataframe: " + str(e))

    def chat_percentage_feedback(self, df_agent_performance):
        try:
            df_agent_performance = pd.read_csv(r'D:\Next Steps\iNeuron\Datasets\30_july\AgentPerformance.csv')
            df_agent_performance["new"] = (df_agent_performance['Total Feedback'] / df_agent_performance['Total Chats']) * 100
            print(df_agent_performance)
        except Exception as e:
            logging.error("Error occurred while working on dataframe: " + str(e))

    def agent_hr_per_week(self, df_agent_login):
        try:
            df_agent_login = pd.read_csv(r'D:\Next Steps\iNeuron\Datasets\30_july\AgentLogingReport.csv')
            df_agent_login['Date'] = df_agent_login['Date'].astype('datetime64[ns]')
            df_agent_login['Duration'] = df_agent_login['Duration'].astype('datetime64')
            df_agent_login['Date_week'] = df_agent_login['Date'].dt.isocalendar().week
            df_agent_login['hr'] = df_agent_login['Duration'].dt.hour
            df_agent_login['min'] = df_agent_login['Duration'].dt.minute
            df_agent_login['sec'] = df_agent_login['Duration'].dt.second
            print(df_agent_login.groupby(['Agent', 'Date_week'])['hr', 'min', 'sec'].sum())
        except Exception as e:
            logging.error("Error occurred while working on dataframe: " + str(e))

    def agent_hr_per_month(self, df_agent_login):
        try:
            df_agent_login = pd.read_csv(r'D:\Next Steps\iNeuron\Datasets\30_july\AgentLogingReport.csv')
            df_agent_login['Date'] = df_agent_login['Date'].astype('datetime64[ns]')
            df_agent_login['Duration'] = df_agent_login['Duration'].astype('datetime64[ns]')

            df_agent_login['Date_month'] = df_agent_login['Date'].dt.month
            df_agent_login['hr'] = df_agent_login['Duration'].dt.hour
            df_agent_login['min'] = df_agent_login['Duration'].dt.minute
            df_agent_login['sec'] = df_agent_login['Duration'].dt.second
            print(df_agent_login.groupby(['Agent', 'Date_month'])['hr', 'min', 'sec'].sum())
        except Exception as e:
            logging.error("Error occurred while working on dataframe: " + str(e))

# Code execution starts from here
if __name__ == '__main__':
    class_object = AgentRealTimeData()
    df_agent_performance = pd.read_csv(r'D:\Next Steps\iNeuron\Datasets\30_july\AgentPerformance.csv')
    df_agent_login = pd.read_csv(r'D:\Next Steps\iNeuron\Datasets\30_july\AgentLogingReport.csv')

    class_object.agent_avg_weekly_rating(df_agent_performance)
    class_object.agent_total_working_day(df_agent_performance)
    class_object.total_query_taken(df_agent_performance)
    class_object.total_feedback_received(df_agent_performance)
    class_object.avg_rating_three_and_half_to_four(df_agent_performance)
    class_object.avg_rating_less_than_three_and_half(df_agent_performance)
    class_object.avg_rating_more_than_four_and_half(df_agent_performance)
    class_object.feedback_agent_more_than_four_and_half_avg(df_agent_performance)
    class_object.agent_avg_weekly_response_time(df_agent_performance)
    class_object.agent_avg_weekly_resolution_time(df_agent_performance)
    class_object.list_of_all_agents(df_agent_performance,df_agent_login)
    class_object.chat_percentage_feedback(df_agent_performance)
    class_object.agent_hr_per_week(df_agent_login)
    class_object.agent_hr_per_month(df_agent_login)