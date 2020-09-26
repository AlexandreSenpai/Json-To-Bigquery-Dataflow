# Json To Bigquery Dataflow Template
A simple apache beam template made using python to use on Google Cloud Platform Dataflow.

## How to use it?

Requirements
- Python 3.8
- GCP service account with dataflow permissions

> This is a very specific json converter template, so you just can run it using a specific json schema. I'm letting this json sample public, so you can use it to test the code.

> Remember: This is a `Dataflow Runner` watch for unexpected billings.

```py
    python3 .\main.py --input gs://animeon-cdn-bucket/sample.json --temp_location gs://temp/directory --table_spec project:dataset.table --project project_id
```

input:

<img src='https://cdn.discordapp.com/attachments/709471533533495337/759314490545799208/unknown.png'>

output: 

<img src='https://cdn.discordapp.com/attachments/709471533533495337/759314141986160640/unknown.png' />

Thanks for your visiting!