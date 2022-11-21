# Notifications

## Slack Notifications
To send Slack notifications, you'll need to have your own Slack app. If you don't have one,
you may follow steps 1 and 2 to create it. Otherwise, please proceed to Step 3.

Note: This module also requires an optional dependency. See [Installation](../../installation.md) for details.


```eval_rst
Step 1: Set up a Slack environment to receive messages
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
1. Create a `Slack app <https://api.slack.com/apps/new>`_
2. Specify the app Name, such as **my_slack_app**
3. Choose a **Development Slack Workspace** you want to receive messages upon
4. Click **Create App**
5. Go to **Building Apps for Slack** -> **Add features and functionality** -> **Permissions**
6. In **Scopes**, click **Add an OAuth Scope** and add **chat:write** to **Bot Token Scopes**
7. Click **Install App to Workspace**
8. Click **Allow**
9. Copy **Bot User OAuth Access Token** as your :code:`slack_api_token`


Step 2: Add the Slack app to a Slack channel
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Make sure you add the Slack app to a *public* Slack channel via **Details** -> **More** -> **Add apps** on the channel UI.
You may locate **Details** using a circled exclamation mark on its left side.


Step 3: Use Cases
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
* You can customize and send any Slack messages by calling :code:`notifications.slack.send_message()`.
* If you have already created a *slack client* (*NOT* your `Slack app <https://api.slack.com/apps/new>`_!), you can directly pass it via the argument **slack_client**, otherwise you may create one using :code:`WebClient(token=slack_api_token)`
* :code:`notifications.slack.monitor_job()` monitors a single job/operation kicked off by either the UI (using a job ID), or the `Tamr-Client <https://github.com/Datatamer/tamr-client>`_ (using a Tamr operation). Notifications will stop if 1) the job is resolved, or 2) the timeout is reached. To reduce redundancy, a Slack message will be generated *ONLY* when the job is established, or the job state gets updated. For example, when a job switches from *RUNNING* to *SUCCEEDED*, a slack message including [**host_ip**, **job_id**, **job_description**, **job state: SUCCEEDED**] will be posted to the public Slack channel which your Slack app joins at STEP 3.
* You may further customize/limit what job states (e.g., *SUCCEEDED*, *FAILED*, *CANCELLED*, *PENDING*, *RUNNING*) you would like to be notified using the argument **notify_states**.

.. literalinclude:: ../../examples/snippets/notifications/enable_slack_notifications.py
  :language: python

```


## Email Notifications

```eval_rst
.. literalinclude:: ../../examples/snippets/notifications/enable_email_notifications.py
  :language: python

```


