# Instructions

## Scenario

You are working for the government and they currently store all of their grant information given to various projects in the country in csv's.

They want to be able to migrate this csv data into a database and then build a crud application on top of this db to adopt a more optimal workflow for data entry.

For your first task you will need to take the provided csv called `input/neh-grants-2010-2019-csv-1.csv` and import it into a database, this can be done using  sqlite database for the sake of simplicity.

You can create a connection to a sqlite database using the code below:

`import sqlite3`
`conn = sqlite3.connect('example.db')`

Next you will need to create some reports of this data which are the following:

> Fetch a list of participants who are co project directors who worked on projects within a certain state, the state will be provided as a parameter input
> Aggregate of the total number of supplements given per year
> Count of each project per state with aggregated grants for each state

The report functions just need to return the final data as a dict or json as the intention would be to return these reports as part of an api called

You are able to write unit tests if you wish but its not the main part of the test so no need to worry about them. This test is to assess basic code structure and programming practices, the code should be clear and reusable as if it would be used in a production system.

## Execution

* Install dependencies, run `pip install -r requirements.txt`

* Run `main/main.py`

* Output:

> `output/Participants.json`
> `output/Supplements.json`
> `output/Projects.json`
