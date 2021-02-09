"""Snippet for exporting taxonomy of a Tamr Categorization project to a csv file"""
import csv
import tamr_toolbox as tbox

# Create the Tamr client
tamr = tbox.utils.client.create(username="user", password="pw", host="localhost")

# Get a Tamr categorization project by ID
my_project = tamr.projects.by_resource_id("2")

# Export the taxonomy to a csv file
export_file_path = "path_to_the_csv_file/file_name.csv"
records_written = tbox.data_io.csv.from_taxonomy(
    my_project,
    export_file_path,
    csv_delimiter=",",
    flatten_delimiter="|",
    quote_character='"',
    quoting=csv.QUOTE_MINIMAL,
    overwrite=False,
)
print(f"Wrote {records_written} categories to {export_file_path}")
