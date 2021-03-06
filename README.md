# Site Sonar MonAlisa Client

This repository contains the source code for a Site Sonar MonAlisa Client. This tool is used to retrieve the output of jobs submitted to the WLCG from Site Sonar Job Submission tool.

## Prerequisites:

Please make sure following prerequisites are met before starting the program

- Run `install.sh` if this is the first time you are running the client
- Run `bin/recompile.sh` to compile the programs
- Start a MySQL instance and source the `db.sql` SQL file available in [Site Sonar Job Submission Tool repository](https://gitlab.cern.ch/kwijethu/site-sonar) to create the database
- Update the database configurations in `conf/App.properties`
- Test the database connection by running `testDB.sh`

## Starting the client

- To start the client run `run.sh`
- A single run will last about 48 hours. Therefore it is advised to run the client as a background program using `nohup run.sh &`
  > Once the tool is started it will keep listening to data published to the topic `SiteSonar` in MonAlisa.
