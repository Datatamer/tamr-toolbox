from dataclasses import dataclass
from typing import Union, List, Optional

from tamr_unify_client import Client
from tamr_unify_client.operation import Operation

from tamr_toolbox.models.operation_state import OperationState
from tamr_toolbox.utils.operation import from_resource_id
from tamr_toolbox.utils.operation import monitor


@dataclass
class _BaseNotifier(object):
    def monitor_job(self,
                    tamr: Client,
                    operation: Union[int, str, Operation],
                    poll_interval_seconds: float = 1,
                    timeout_seconds: Optional[float] = None,
                    notify_states: Optional[List[OperationState]] = None,
                    *args,
                    **kwargs) -> None:
        """Monitors a Tamr Operation and sends a message when the job status is updated

        Args:
            tamr: A Tamr client
            operation: A job ID or a Tamr operation
            poll_interval_seconds: Time interval (in seconds) between subsequent polls
            timeout_seconds: Time (in seconds) to wait
            notify_states : States for which notifications should be sent, use None for all states

        Returns:
            A list of messages with their response codes
        """

        def get_op_details(operation: Operation) -> str:
            return (
                f"Host: {operation.client.host} \n Job: {operation.resource_id} \n "
                f"Description: {operation.description} \n Status: {operation.state} "
            )

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
            self.send_message(message=get_op_details(op), title=f"Job {operation.resource_id}: {status}")

        while status not in [
            OperationState.SUCCEEDED,
            OperationState.FAILED,
            OperationState.CANCELED]:

            # Send message on state change
            try:
                op = monitor(
                    operation=op,
                    poll_interval_seconds=poll_interval_seconds,
                    timeout_seconds=timeout_seconds)

                status = OperationState[op.state]
                self.send_message(message=get_op_details(op), title=f"Job {operation.resource_id}: {status}")
            except TimeoutError:
                timeout_message = (
                    f"The job {op.resource_id}: {op.description} took longer "
                    f"than {timeout_seconds} seconds to resolve."
                )
                self.send_message(message=timeout_message, subject_line=f"Job {op.resource_id}: Timeout", **kwargs)

    def send_message(self, message, title, *args, **kwargs):
        raise NotImplementedError("Do not call `_BaseNotifier` directly. "
                                  "Use e.g. `EmailNotifier` instead.")
