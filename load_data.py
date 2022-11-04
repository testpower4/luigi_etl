from google.cloud import bigquery
from google.oauth2 import service_account
import os
from datetime import datetime, timedelta, date
import time
import yaml_config.util as util_conf
from google.cloud.bigquery import LoadJobConfig
from google.cloud.exceptions import NotFound
import uuid
import numpy as np


def tbl_exists(client, table_ref):

    try:
        client.get_table(table_ref)
        return True
    except NotFound:
        return False


class Load_Incremental:
    def __init__(self):

        sa = "c:\REPLACE_YOUR_FOLDER\cred\inter.json"  # os.path.join("config", "inter.json")
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = sa
        self.create_conn()
        self.conf = util_conf.load_config()
        self.trans_tbl = ".".join(
            [self.conf["bq_project"], self.conf["dataset"], self.conf["source_table"]]
        )
        self.final_tbl = ".".join(
            [self.conf["bq_project"], self.conf["dataset"], self.conf["final_table"]]
        )
        self.artist_tbl = ".".join(
            [self.conf["bq_project"], self.conf["dataset"], self.conf["artist_table"]]
        )
        self.track_tbl = ".".join(
            [self.conf["bq_project"], self.conf["dataset"], self.conf["track_table"]]
        )

    def create_conn(self):

        self.client = bigquery.Client()

    def create_conn_local(self, key_path):

        credentials = service_account.Credentials.from_service_account_file(
            key_path,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )

        self.client = bigquery.Client(
            credentials=credentials,
            project=credentials.project_id,
        )

    def load_data(self, query, dtypes=None):

        query_job = self.client.query(query)
        if dtypes is None:
            return query_job.result().to_dataframe()
        else:
            return query_job.result().to_dataframe(dtypes=dtypes)

    def validate_date(self, sql_str, col, rows=0):
        """
        params:
            col is the list of columns
        """
        df_last_dt = self.load_data(sql_str)
        if df_last_dt.shape[0]:
            return df_last_dt[col].iloc[rows]

        else:
            print("no dates data found")

    def append_bq(self, df, dest_table=None):
        # job_config = bigquery.LoadJobConfig()
        dest_table = self.final_tbl if dest_table is None else dest_table
        if df.shape[0]:
            print(df.shape[0], "records inserted")
            load_job = self.client.load_table_from_dataframe(df, dest_table)
            time.sleep(5)

    def loop_sql(self, date_str):

        # load trans
        if tbl_exists(self.client, self.final_tbl):
            del_sql = f"DELETE FROM {self.final_tbl}  WHERE  CAST(visit_datetime as DATE) = '{date_str}'"
            self.load_data(del_sql)
            time.sleep(5)

        uid = uuid.uuid4().hex
        daily_sql = self.conf["LOAD"]["DAILY_DEDUP"].format(
            uid, self.trans_tbl, date_str
        )
        df = self.load_data(daily_sql)
        self.append_bq(df, dest_table=self.final_tbl)

        # load artist
        artist_sql = self.conf["LOAD"]["ARTIST"].format(
            self.trans_tbl, date_str, self.artist_tbl
        )
        df = self.load_data(
            artist_sql,
            {"artist_id": np.dtype("object"), "artist_name": np.dtype("object")},
        )
        self.append_bq(df, dest_table=self.artist_tbl)

        # load track
        track_sql = self.conf["LOAD"]["TRACK"].format(
            self.trans_tbl, date_str, self.track_tbl
        )
        df = self.load_data(
            track_sql,
            {"track_id": np.dtype("object"), "track_name": np.dtype("object")},
        )
        self.append_bq(df, dest_table=self.track_tbl)

    def load_to_bucket(self):
        # TODO add upload function
        pass

    def auto_load(self, uri, tableRef, schema=None):

        jobConfig = LoadJobConfig()
        jobConfig.field_delimiter = "\t"
        jobConfig.source_format = "CSV"
        jobConfig.autodetect = True
        if schema:
            jobConfig.schema = schema
        else:
            pass
        bigqueryJob = self.client.load_table_from_uri(
            uri, tableRef, job_config=jobConfig
        )
        bigqueryJob.result()

    def test_load(self, uri, tableRef, schema=None):

        jobConfig = LoadJobConfig()
        jobConfig.field_delimiter = "\t"
        jobConfig.source_format = "CSV"
        jobConfig.autodetect = False
        jobConfig.quote_character = ""
        jobConfig.max_bad_records = 50
        if schema:
            jobConfig.schema = schema
        else:
            pass
        bigqueryJob = self.client.load_table_from_uri(
            uri, tableRef, job_config=jobConfig
        )
        r = bigqueryJob.result()
        print(r)
        print(r.error_result, r.errors, r.job_type)


# https://googleapis.dev/python/bigquery/latest/generated/google.cloud.bigquery.job.QueryJobConfig.html#google.cloud.bigquery.job.QueryJobConfig.allow_large_results
# https://developers.google.com/resources/api-libraries/documentation/bigquery/v2/python/latest/bigquery_v2.jobs.html
