from setuptools import setup
import load_data


load_exec = load_data.Load_Incremental()

# create google tables
for k, v in load_exec.conf["CREATE"].items():
    load_exec.load_data(v)


with open("requirements.txt") as f:
    requirements = f.read().splitlines()

version = "0.1"

setup(name="ETL", version=version, install_requires=requirements)
