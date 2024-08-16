import requests
import json
# from datetime import datetime

example_df = {
  'index': 7528, 'unique_id': 'fbe53c66-3442-4773-b19e-d3ec6f54dddf',
  'title': 'No Title Data Available',
  'description': 'No description available Story format',
  'poster_name': 'User Info Error', 'follower_count': 'User Info Error',
  'tag_list': "N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e",
  'is_image_or_video': "multi-video(story page format)",
  'image_src': 'Image src error.', 'downloaded': 0,
  'save_location': 'Local save in /data/mens-fashion',
  'category': 'mens-fashion'
  }

invoke_url = 'https://foyd3wyk4c.execute-api.us-east-1.amazonaws.com/dev/streams/streaming-126ca3664fbb-pin/record'
 
payload = json.dumps({
    # "StreamName": "streaming-126ca3664fbb-pin",
    "Data": example_df,
    #{
    #         #Data should be send as pairs of column_name:value, with different columns separated by commas      
    #         "index": example_df["index"], "name": example_df["name"],
    #         "age": example_df["age"], "role": example_df["role"]
    #         },
    "PartitionKey": "partition-pin"
})

headers = {'Content-Type': 'application/json'} # NOT application/x-amz-json-1.1 ??

response = requests.request("PUT", invoke_url, headers=headers, data=payload)
print(response.status_code)



