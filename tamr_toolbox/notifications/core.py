from typing import Union, List, Optional

from tamr_unify_client import Client
from tamr_unify_client.operation import Operation

from tamr_toolbox.models.operation_state import OperationState
from tamr_toolbox.utils.operation import from_resource_id, get_details
from tamr_toolbox.utils.operation import monitor


class _BaseNotifier(object):
    def __init__(self):
        self.sent_messages = []

    def send_message(self, message: str, title: str, *args, **kwargs) -> None:
        """
        Sends a notification

        Args:
            message: The message to send
            title: The title of the message to send

        Examples:
            .. literalinclude:: ../../examples/notifications/enable_email_notifications.py

        Raises:
            NotImplementedError: If called directly from the `_BaseNotifier' class,
                instead of a pre-defined notification type
        """
        raise NotImplementedError("Do not call `_BaseNotifier` directly. "
                                  "Use e.g. `EmailNotifier` instead.")

    def monitor_job(self,
                    tamr: Client,
                    operation: Union[int, str, Operation],
                    notify_states: Optional[List[OperationState]] = None,
                    poll_interval: Optional[float] = 1,
                    timeout: Optional[float] = None,
                    *args,
                    **kwargs) -> None:
        """Monitors a Tamr Operation and sends a message when the job status is updated

        Args:
            tamr: A Tamr client
            operation: A job ID or a Tamr operation
            notify_states : States for which notifications should be sent, use None for all states
            poll_interval: Time interval (in seconds) between subsequent polls
            timeout: Time (in seconds) to wait before timing out

        Returns:
            A list of messages with their response codes
        """
        if not notify_states:
            notify_states = [
                OperationState.SUCCEEDED,
                OperationState.FAILED,
                OperationState.CANCELED,
                OperationState.PENDING,
                OperationState.RUNNING]

        if isinstance(operation, Operation):
            op = operation
        else:
            op = from_resource_id(tamr=tamr, job_id=operation)

        # Send message for initial state
        status = OperationState[op.state]
        if status in notify_states:
            self.send_message(message=get_details(op), title=f"Job {operation.resource_id}: {status}")

        while status not in [
            OperationState.SUCCEEDED,
            OperationState.FAILED,
            OperationState.CANCELED]:

            # Send message on state change
            try:
                op = monitor(
                    operation=op,
                    poll_interval_seconds=poll_interval,
                    timeout_seconds=timeout)

                status = OperationState[op.state]
                self.send_message(message=get_details(op), title=f"Job {operation.resource_id}: {status}")
            except TimeoutError:
                timeout_message = (
                    f"The job {op.resource_id}: {op.description} took longer "
                    f"than {timeout} seconds to resolve."
                )
                self.send_message(message=timeout_message, title=f"Job {op.resource_id}: Timeout")
