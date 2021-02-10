# Installation

**Pip Install**

`pip install tamr_toolbox[all]`
  

**Optional Features**

Some features of Tamr-Toolbox require additional dependencies. You can opt-in to these features during installation. By including `tamr_toolbox[all]` in the pip installation command, you will install the dependencies required for all optional features. To include dependencies required for one or more optional features use `tamr_toolbox[feature_1, feature_2]`. A minimal installation is achieved by omitting the `[]` entirely.

In some cases you may already have a version of the library installed and would prefer to use that instead. Or perhaps
you would like better control over what version you are installing. 
If doing so, please use at least minimum version of the library specified below. 
You will then want to install a version of tamr-toolbox
without that library included (such as the version with no optional features) so that it does not attempt to change 
your existing version.

***All optional features (suggested)***

Install instructions:
`pip install tamr_toolbox[all]`

***No optional features***

Install instructions:
`pip install tamr_toolbox`

***Optional Feature: Google Translate***

Install instructions:
`pip install tamr_toolbox[translation]`

Required for [Translation Enrichment](modules/enrichment/translation)

Library: [GoogleTranslate](https://github.com/googleapis/python-translate) (Tamr-toolbox uses version == 2.0.1)

Note: You will additionally need your own google API key in order to use translation capabilities.

***Optional Feature: Mock API Testing***

Install instructions:
`pip install tamr_toolbox[testing]`

Required for [Testing](modules/utils)

Library: [Responses](https://github.com/getsentry/responses) (Tamr-Toolbox uses version == 0.10.14)

***Optional Feature: Pandas dataframes***

Install instructions:
`pip install tamr_toolbox[pandas]`

Required for [DataFrame I/O](modules/data_io/dataframe)

Library: [Pandas](https://pandas.pydata.org/pandas-docs/stable/) (Tamr-Toolbox uses version >= 0.21.0)

***Optional Feature: Slack Notifications***

Install instructions:
`pip install tamr_toolbox[slack]`

Required for [Slack](modules/notifications/slack)

Library: [Slack Client](https://github.com/slackapi/python-slackclient) (Tamr-Toolbox uses version >= 2.7.2)



**Offline installation**

Download `tamr_toolbox` and its dependencies on a machine with the same operating system and python version as your target system, that has online access to PyPI:

```bash
pip download tamr_toolbox[all] -d tamr-toolbox-requirements
zip -r tamr-toolbox-requirements.zip tamr-toolbox-requirements
```

Deliver the `.zip` file to the target machine where you want `tamr-toolbox` installed. You can do this via email, cloud drives, `scp` or any other mechanism.

Finally, install `tamr-toolbox` from the saved dependencies:

```bash
unzip tamr-toolbox-requirements.zip
pip install --no-index --find-links=tamr-toolbox-requirements tamr_toolbox[all]
```

If you are not using a virtual environment, you may need to specify the `--user` flag if you get permissions errors:

```bash
pip install --user --no-index --find-links=tamr-toolbox-requirements tamr_toolbox[all]
```
