import music
from datetime import datetime, timedelta, date
import yaml_config.util as util_conf
from config import default_config
import seaborn as sns
from matplotlib import pyplot as plt


LATENCY_DAYS = 1

current_dt = (datetime.today() - timedelta(LATENCY_DAYS)).strftime("%Y-%m-%d")
load_exec = music.load_data.Load_Incremental()


def date_range():

    trans_tbl = ".".join(
        [music.conf_all["bq_project"], music.conf_all["dataset"], "final_trans"]
    )
    print(music.conf_all["SQL"]["RANGE_DATE"].format(trans_tbl))

    date_range = load_exec.validate_date(
        music.conf_all["SQL"]["RANGE_DATE"].format(trans_tbl), ["max_date", "min_date"]
    )
    delta = date_range["max_date"] - date_range["min_date"]

    return date_range["min_date"], delta.days


# full load of all days by calling daily incremental
def full_load():
    start_date, days = date_range()
    for i in range(days):
        start_str = datetime.strftime(start_date, "%Y-%m-%d")
        df = load_exec.loop_sql(start_str)

        start_date = start_date + timedelta(1)


def display_1(df, col_1, col_2):
    fig, ax = plt.subplots()
    max_y = df[col_1].max()
    g = sns.lineplot(x=df[col_2], y=df[col_1])
    fig.suptitle("recent 30 day")
    plt.ylim((0, max_y))
    plt.ylabel(col_1, fontsize=12)
    plt.xlabel(col_2, fontsize=12)
    plt.show()


# Answers
test_table = ".".join(
    [music.conf_all["bq_project"], music.conf_all["dataset"], "auto_tx3"]
)
profile_table = ".".join(
    [music.conf_all["bq_project"], music.conf_all["dataset"], "profile"]
)
last30day_sql = music.conf_all["SQL"]["QUERY1"].format(test_table, 29)
# allday_sql = music.conf_all["SQL"]["QUERY_MONTH"].format(test_table)
answer1_df = load_exec.load_data(last30day_sql)
print(answer1_df)
display_1(answer1_df, "active_users", "dt")


artist_sql = music.conf_all["SQL"]["QUERY_TOP_ARTIST"].format(test_table)
answer2_df = load_exec.load_data(artist_sql)
print(answer2_df)

artist_sql = music.conf_all["SQL"]["QUERY_MOST_COUNTRIES"].format(
    profile_table, test_table
)
answer3_df = load_exec.load_data(artist_sql)
print(answer3_df)
