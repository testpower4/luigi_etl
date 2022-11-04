import luigi
import os
import yaml_config.util as util_conf
import get_data
import load_data
import datetime as dt
from config import default_config

conf_all = util_conf.load_config()

dest_folder = os.path.join(os.path.dirname(os.path.abspath(__file__)), "temp")
down_file = os.path.join(
    dest_folder,
    "downloaded.tar.gz",
)


load_exec = load_data.Load_Incremental()


class Get_Data(luigi.Task):
    def output(self):
        return luigi.LocalTarget(down_file)

    def run(self):
        get_data.retrieve_source(conf_all["remote_data"], down_file)
        get_data.extract(down_file, dest_folder)


class Load_Data(luigi.Task):
    """
    loop specified files and use default schema and auto-detect
    """

    params = luigi.Parameter(default=dt.datetime.now().strftime("%Y-%m-%d"))

    def output(self):
        return luigi.LocalTarget(f"_TRANSFER_BUCKET_TO_DB_PROCESSED_{self.params}")

    def run(self):
        for new_tbl, uri in conf_all["uploaded_file"].items():
            table = ".".join([conf_all["bq_project"], conf_all["dataset"], new_tbl])
            load_exec = load_data.Load_Incremental()
            load_exec.auto_load(uri, table)
            print(f"Auto - Loading files to gcp for date {self.params}")


class Custom_Data(luigi.Task):
    """
    load transaction file with designated schema
    """

    params = luigi.Parameter(default=dt.datetime.now().strftime("%Y-%m-%d"))

    def output(self):
        return luigi.LocalTarget(
            f"_CUSTOMISED_TRANSFER_BUCKET_TO_DB_PROCESSED_{self.params}"
        )

    def run(self):
        _tbl = ".".join(
            [
                conf_all["bq_project"],
                conf_all["dataset"],
                conf_all["test_table"],
            ]
        )
        load_exec.test_load(
            conf_all["uploaded_file"]["auto_tx3"], _tbl, default_config.SCHEMA_TX
        )


class Load_SQL(luigi.Task):

    params = luigi.Parameter(default=dt.datetime.now().strftime("%Y-%m-%d"))

    def output(self):
        return luigi.LocalTarget(f"_LOAD_DAILY_TO_TABLE_PROCESSED_{self.params}")

    def run(self):
        df = load_exec.loop_sql(self.params)
