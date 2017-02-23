# Copyright 2014 Scopely, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Module for setting up an sqs queue subscribed to
an sns topic polling for messages pertaining to our
impending doom.

"""
import json

import boto3

from shudder.config import CONFIG
import shudder.metadata as metadata


INSTANCE_ID = metadata.get_instance_id()
QUEUE_NAME = "{prefix}-{id}".format(prefix=CONFIG['sqs_prefix'],
                                    id=INSTANCE_ID)


def create_queue():
    """Creates the SQS queue and returns the queue url and metadata"""
    conn = boto3.client('sqs', region_name=CONFIG['region'])
    queue_metadata = conn.create_queue(QueueName=QUEUE_NAME, Attributes={'VisibilityTimeout':'3600'})
    """Get the SQS queue object from the queue URL"""
    sqs = boto3.resource('sqs', region_name=CONFIG['region'])
    queue = sqs.Queue(queue_metadata['QueueUrl'])
    return conn, queue


def subscribe_sns(queue):
    """Subscribes the SNS topic to the queue."""
    conn = boto3.client('sns', region_name=CONFIG['region'])
    sub = conn.subscribe(TopicArn=CONFIG['sns_topic'], Protocol='sqs', Endpoint=queue.attributes.get('QueueArn'))
    sns_arn = sub['SubscriptionArn']
    return conn, sns_arn


def should_terminate(msg):
    """Check if the termination message is about our instance"""
    first_box = json.loads(msg.body)
    body = json.loads(first_box['Message'])
    termination_msg = 'autoscaling:EC2_INSTANCE_TERMINATING'
    return body.get('LifecycleTransition') == termination_msg \
        and INSTANCE_ID == body['EC2InstanceId']


def clean_up_sns(sns_conn, sns_arn, queue):
    """Clean up SNS subscription and SQS queue"""
    queue.delete()
    sns_conn.unsubscribe(SubscriptionArn=sns_arn)


def poll_queue(conn, queue):
    """Poll SQS until we get a termination message."""
    messages = queue.receive_messages()
    for message in messages:
        message.delete()
        return should_terminate(message)
    return False
