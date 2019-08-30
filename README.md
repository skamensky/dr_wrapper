# demandtoolswrapper
A python wrapper around the Demand Tools command line interface.
[Demand Tools](https://www.validity.com/demandtools/) is a deduplication and ETL tool for Salesforce.

This package arose from the need to integrate the product deeper into my automation pipeline than the [JobBuilder](https://helpconsole.validity.com/DemandToolsJobBuilder/)
allowed.

As of now only scenario types that I've used are supported but pull requests are welcome if anyone would like to add more.

This implementation supports multiprocessing by default but since DemandTools doesn't return all errors to standard out, there may be some runs that appear to have succeeded but haven't actually.
Use the multiprocessing functionality at your own risk.

Thanks to [black](https://github.com/psf/black) for formatting my code for me :).
### Setup
1. Add the folder containing the 'demandtools.exe' file to the `PATH` environment variable
3. Create an environment variable named `DEMANDTOOLSLOGDIRECTORY` and set the value to the folder containing the DemandTools logs folder
2. Log into DemandTools via the GUI and ensure you check off the option to stay logged in

### Examples
#### Example 1
Chain `DemandToolsCommand`'s together, watch DemandTools' logs,and run functions post scenario completion
```import os
import dt_wrapper as dt 
from queue import Queue

LOG_QUEUE = Queue()


def main():
    """
    this function takes many different single dedupe scenarios, runs them on the lead table,
    exports the duplicates without merging, and populates a custom field on the lead called 'Duplicate_Of__c'
    so duplicates can be tracked in Salesforce
    """

    # Logatcher is a context manager that watches the DemandTools log
    # and prints the results stream as DemandTools writes to the file
    # the manager optionally takes a threading queue with which to populate the logs
    with dt.LogWatcher(log_queue=LOG_QUEUE):
        mark_dedupe_scenario_dir = "MassEffect Scenarios"
        mark_dedupe_scenario_path = "auto_mark_dupes.MExml"

        # the demandtoolswrapper keeps track of the extension of various scenario types
        dedupe_ext = dt.DemandToolsCommand._demand_tool_extension["dedupe"]

        lead_dedupe_dir = "dedupe_dir"
        scenario_dir = "confident_scenarios"
        # only get specific scenario types
        dedupe_scenarios = [
            os.path.join(scenario_dir, p)
            for p in os.listdir(scenario_dir)
            if p.endswith(dedupe_ext)
        ]

        output_dir = "output_dir"
        for scenario in dedupe_scenarios:
            output_file = os.path.join(
                output_dir, os.path.basename(scenario).replace(dedupe_ext, ".csv")
            )
            file_prep_output = os.path.join(
                output_dir,
                "{scenario_name}_master_nonmaster.csv".format(
                    scenario_name=os.path.basename(scenario).replace(dedupe_ext, "")
                ),
            )

            find_dupe_scenario = dt.DemandToolsCommand(
                scenario_path=scenario, output_file=output_file, log_queue=LOG_QUEUE
            )

            # you can pass a function that should run once the scenario is finished running. Can be chained with multiple scenario runs
            find_dupe_scenario.post_run_func = lambda: None
            # DemandToolsCommand can also be passed a queue which will be populated with all print statements it produces
            mark_as_dupe = dt.DemandToolsCommand(
                scenario_path=mark_dedupe_scenario_path,
                input_file=file_prep_output,
                log_queue=LOG_QUEUE,
            )
            find_dupe_scenario.run()
            try:
                mark_as_dupe.run()
            except dt.DemandToolsInputFileDoesNotExist as e:
                print(
                    '"{file}" not found. This usually happens when no dupes were found.'.format(
                        file=mark_as_dupe.input_file_nice_name
                    )
                )


if __name__ == "__main__":
    main()
```
#### Example 2
Use DemandTools as an ETL to export all tables and then upload the CSV's to a Postgres database.
```
import dt_wrapper as dt 


def sync_local_files_with_postgres(cursor, conn):
    """
    sync_local_files_with_postgres is out of the scope for this example, but it simply runs
    copies the output CSV's to a postgres DB
    """


def download_sf_data_as_csvs(cursor, conn):
    bulk_backup_scenario = dt.DemandToolsCommand(
        scenario_path="Scenarios\Misc\backupAllTables.BBxml",
        output_file="output_directory",
        # this is passed as an encoding argument to DemandTools
        extra_dt_args="utf8",
        post_run_func=sync_local_files_with_postgres,
        # you can feed args or kwargs to the post_run_func
        post_run_func_args=(cursor, conn),
    )

    bulk_backup_scenario.run()
```

#### Example 3 
Run 20+ DemandTools processes concurrently, expect Demand Tools exceptions, and retry a few times until success or permanent failure.
It turns out this is faster than running them sequentially and not running into exceptions.
It can use concurrency because it's not uploading anything back into Salesforce until all of the scenarios run to completion
(i.e. the output of one scenario has no impact on the input of the next scenario).

```
import os
from queue import Queue
from dt_wrapper import (
    DemandToolsCommand,
    DemandToolsMultiProcessDBWriteConflictException,
    DemandToolsObjectReferenceException,
    LogWatcher,
)

LOG_QUEUE = Queue()


def run_demand_tools_scenarios():
    # find all scenarios in a directory by examining the extensions
    scenarios_paths = DemandToolsCommand.get_scenarios_in_path(
        path="account_dedupe_scenarios_loose"
    )
    scenarios_paths.extend(
        DemandToolsCommand.get_scenarios_in_path(
            path="account_dedupe_scenarios_loose_confident"
        )
    )
    dt_processes = []
    for scenario_path in scenarios_paths:
        output_file_base_name = (
            os.path.splitext(os.path.basename(scenario_path))[0] + ".csv"
        )
        output_file = os.path.join("output_dir", output_file_base_name)
        command = DemandToolsCommand(
            scenario_path=scenario_path,
            output_file=output_file,
            # can retry on predefined exceptions, otherwise fail on any exception
            exceptions_to_retry_on=[
                DemandToolsMultiProcessDBWriteConflictException,
                DemandToolsObjectReferenceException,
            ],
            retry_count=3,
            log_queue=LOG_QUEUE,
        )
        dt_processes.append(command)
    # if you have multiple organizations on your machine, you can specify an organization_id for the relevant log file
    with LogWatcher(log_queue=LOG_QUEUE, organization_id="00Di0000000gwFuEAI"):
        # run with multiprocessing enabled
        [dt_command.start() for dt_command in dt_processes]
        [dt_command.join() for dt_command in dt_processes]
```

### Future plans

Restrict arguments to conform to the documentation which accounts for all possible valid scenarios.
Syntax could be taken from DemandTool's [Scenario Synax Documentation](https://helpconsole.validity.com/DemandToolsJobBuilder/#pageid=demandtools_job___scenario_syntax
) and DemandTool's [JobBuilder documentation](
https://helpconsole.validity.com/DemandTools/default.aspx#pageid=scheduled_processes)
