import pandas as pd
import tamr_toolbox as tbox


# Initialize Tamr match client -- note port must be 9170 for RealTime match service
match_client = tbox.utils.client.create(
    username="user", password="pw", host="localhost", port=9170
)

# Get client attached to default Tamr port to access projects, and get relevant mastering project
tamr_client = tbox.utils.client.create(username="user", password="pw", host="localhost", port=9100)
project = tamr_client.projects.by_name("my_mastering_proj")


# Get source data and rename columns to match project unified dataset as needed
filename = "/Data/source_data/incoming_employees.csv"
employee_data = pd.read_csv(filename)
employee_data.rename(columns={"last_name": "family_name"})

# Get RealTime match results and add them into the data
lookup_results = tbox.realtime.matching.match_query(
    match_client=match_client,
    project=project,
    records=employee_data.to_dict("records"),
    type="clusters",
    max_num_matches=1,
)
employee_data["tamr_id"] = employee_data.index.map(
    lambda k: lookup_results[k][0]["matchedRecordId"] if lookup_results[k] else None
)
