import os
import json
import boto3

SNS_TOPIC_ARN = os.environ["SNS_TOPIC_ARN"]
AWS_REGION    = os.environ.get("AWS_REGION", "us-east-1")
THUMB_BUCKET  = os.environ["THUMB_BUCKET"]
RAW_BUCKET    = os.environ["RAW_BUCKET"]

sns_client = boto3.client("sns", region_name=AWS_REGION)

def handler(event, context):
    for record in event.get("Records", []):
        if record.get("eventName") not in ("INSERT", "MODIFY"):
            continue

        new_image = record.get("dynamodb", {}).get("NewImage")
        if not new_image:
            continue

        # Extract the thumbnail and full‐size keys
        thumb_key = new_image.get("thumbnailKey", {}).get("S")
        full_key  = new_image.get("fullKey", {}).get("S")
        tags_map  = new_image.get("tags", {}).get("M", {})

        if not thumb_key or not tags_map:
            continue

        # Build the public URLs
        thumb_url = f"https://{THUMB_BUCKET}.s3.{AWS_REGION}.amazonaws.com/{thumb_key}"
        full_url  = f"https://{RAW_BUCKET}.s3.{AWS_REGION}.amazonaws.com/{full_key}"

        
        tags_dict = {}
        for bird, num_attr in tags_map.items():
            try:
                tags_dict[bird] = int(num_attr.get("N", "0"))
            except:
                tags_dict[bird] = 0

        tag_names = list(tags_dict.keys())

        # Compose the SNS message
        message_body = {
            "thumbnailUrl": thumb_url,
            "fullUrl":      full_url,
            "tags":         tags_dict,
            "eventType":    record.get("eventName")
        }

        # Publish to SNS with the “tags” attribute as a JSON array
        try:
            sns_client.publish(
                TopicArn=SNS_TOPIC_ARN,
                Subject=f"New BirdMedia Item: {', '.join(tag_names)}",
                Message=json.dumps(message_body),
                MessageAttributes={
                    "tags": {
                        "DataType": "String.Array",
                        "StringValue": json.dumps(tag_names)
                    }
                }
            )
        except Exception as e:
            print(f"[NotifyLambda] Error publishing to SNS: {e}")

    return { "statusCode": 200, "body": json.dumps({"message":"Processed"}) }
