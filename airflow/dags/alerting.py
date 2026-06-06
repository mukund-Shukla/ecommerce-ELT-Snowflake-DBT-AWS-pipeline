import os
import json
import logging
import urllib.request
from datetime import datetime

logger = logging.getLogger(__name__)


def _send_slack(message: str):
    webhook_url = os.getenv('SLACK_WEBHOOK_URL')
    if not webhook_url:
        logger.warning("SLACK_WEBHOOK_URL not set — skipping alert")
        return

    payload = json.dumps({"text": message}).encode('utf-8')
    req = urllib.request.Request(
        webhook_url,
        data=payload,
        headers={'Content-Type': 'application/json'}
    )
    try:
        urllib.request.urlopen(req, timeout=10)
        logger.info("Slack alert sent")
    except Exception as e:
        logger.error(f"Slack alert failed: {e}")


def on_failure_callback(context):
    dag_id      = context['dag'].dag_id
    task_id     = context['task_instance'].task_id
    exec_date = context['logical_date']
    log_url     = context['task_instance'].log_url

    message = (
        f":red_circle: *Pipeline Failed*\n"
        f"*DAG:* {dag_id}\n"
        f"*Task:* {task_id}\n"
        f"*Logical Date:* {exec_date}\n"
        f"*Logs:* {log_url}"
    )
    _send_slack(message)


def on_success_callback(context):
    dag_id    = context['dag'].dag_id
    exec_date = context['logical_date']
    duration  = context['dag_run'].end_date - context['dag_run'].start_date \
                if context['dag_run'].end_date else 'N/A'

    message = (
        f":large_green_circle: *Pipeline Succeeded*\n"
        f"*DAG:* {dag_id}\n"
        f"*Execution Date:* {exec_date}\n"
        f"*Duration:* {duration}"
    )
    _send_slack(message)

def on_retry_callback(context):
    dag_id    = context['dag'].dag_id
    task_id   = context['task_instance'].task_id
    try_num   = context['task_instance'].try_number

    message = (
        f":yellow_circle: *Task Retrying*\n"
        f"*DAG:* {dag_id}\n"
        f"*Task:* {task_id}\n"
        f"*Attempt:* {try_num}"
    )
    _send_slack(message)

