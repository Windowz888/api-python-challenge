import os
import json
import boto3

SNS_TOPIC_ARN = os.environ["SNS_TOPIC_ARN"]
AWS_REGION    = os.environ.get("AWS_REGION", "us-east-1")
sns_client    = boto3.client("sns", region_name=AWS_REGION)

def handler(event, context):
    # 1) Parse JSON body
    try:
        body = json.loads(event.get("body") or "{}")
    except json.JSONDecodeError:
        return {
            "statusCode": 400,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"error": "Request body must be valid JSON."})
        }

    email = body.get("email")
    tags  = body.get("tags")

    # 2) Validate email + tags array
    if not email or not isinstance(tags, list) or not tags:
        return {
            "statusCode": 400,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({
                "error": "Body must include 'email' (string) and non‐empty array 'tags'."
            })
        }

    # 3) Create the email subscription
    try:
        resp = sns_client.subscribe(
            TopicArn              = SNS_TOPIC_ARN,
            Protocol              = "email",
            Endpoint              = email,
            ReturnSubscriptionArn = True
        )
        subscription_arn = resp.get("SubscriptionArn")
    except Exception as e:
        return {
            "statusCode": 500,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"error": f"Unable to subscribe: {str(e)}"})
        }

    # 4) Attach filter policy with the full tags list
    filter_policy = {"tags": tags}
    try:
        sns_client.set_subscription_attributes(
            SubscriptionArn = subscription_arn,
            AttributeName   = "FilterPolicy",
            AttributeValue  = json.dumps(filter_policy)
        )
    except Exception as e:
        return {
            "statusCode": 500,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({
                "message": "Subscription created; please confirm via email, but failed to set filter policy.",
                "subscriptionArn": subscription_arn,
                "filter_error": str(e)
            })
        }

    # 5) Success test
    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({
            "message": "Subscription created. Please check your inbox and confirm the subscription.",
            "subscriptionArn": subscription_arn
        })
    }

