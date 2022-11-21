# Tamr Toolbox

The Tamr Toolbox is a Python library created to provide a simple interface for common interactions with Tamr and common data workflows that include Tamr. The more specialized [Tamr Python Client](https://github.com/Datatamer/tamr-client) library is used for direct interactions with Tamr in both the development of the Tamr Toolbox and in the recommended use of the Tamr Toolbox.  


## Basic Installation

`pip install 'tamr-toolbox[all]'`
 
 [See more installation options](installation.md)

## Basic Example
**project.config.yaml**  
```yaml
logging_dir: "/home/data/project/logs"

my_tamr_instance:
    host: "0.0.0.0"
    protocol: "http"
    port: "9100"
    username: "example-username"
    password: "example-password"

my_project_ids: ["2", "8", "9"]
```
**example-script.py**  
```python
from tamr_toolbox import utils
from tamr_toolbox import workflow


# Read in config from yaml
config = utils.config.from_yaml("/home/data/project/conf/project.config.yaml")

# Make a logger for your script
logger = utils.logger.create(__name__, log_directory=config["logging_dir"])

# Optionally, configure tamr_toolbox to write to the same log file
utils.logger.enable_toolbox_logging(log_directory=config["logging_dir"])

# Create a Tamr Client 
tamr_client = utils.client.create(**config["my_instance_name"])

# Use the Tamr Client to create a list of Projects from project ids
my_projects = [tamr_client.projects.by_resource_id(p_id) for p_id in config["my_project_ids"]]

# Write your own logging message
logger.info(f"Running projects loaded from config: {[p.name for p in my_projects]}")

# Use the tamr_toolbox.workflow module to run a list of projects 
workflow.jobs.run(my_projects)

```

## Reference
  * [Installation](installation.md)
  
  * [Examples](examples.md)

  * [Modules](modules.md)
