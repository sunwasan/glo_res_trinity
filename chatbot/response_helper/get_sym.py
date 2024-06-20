import os
import sys

file_dir = os.path.dirname(__file__)
chatbot_dir = os.path.join(file_dir, '..')
bloomberg_dir = os.path.join(chatbot_dir, '..')
dirs = [chatbot_dir, bloomberg_dir, file_dir]

sys.path.extend(d for d in dirs if d not in sys.path)

import polars as pl 
import pandas as pd 
import json 

data_dir = os.path.join(bloomberg_dir,'data', 'bloomberg.csv')
def generate_row(val:list) -> dict :
    all_rows = []
    for v in val:
        broker = v[0]
        comment = v[1]
        target = v[2]
        row =  {
          "type": "box",
          "layout": "horizontal",
          "contents": [
              {
                  "type": "text",
                  "text": f"{broker}",
                  "size": "sm",
                  "color": "#555555",
                  "align": "start",
                  "flex": 3,
              },
              {
                  "type": "text",
                  "text": f"{comment}",
                  "size": "sm",
                  "color": "#111111",
                  "align": "end",
                  "offsetStart": "xxl",
                  
              },
              {
                  "type": "text",
                  "text": f"{target}",
                  "size": "sm",
                  "color": "#111111",
                  "weight": "regular",
                  "align": "end"
              }
          ]
      }
        all_rows.append(row)
    return all_rows

def get_sym(sym:str):
    df = pl.read_csv(data_dir).to_pandas()

    df = df[df['symbol'] == sym]
    val = df[['broker', 'comment', 'target']].values.tolist()
    all_rows = generate_row(val)

    data = {
        "type": "bubble",
        "size":"giga",
        "body": {
            "type": "box",
            "layout": "vertical",
            
            "contents": [
                {
                    "type": "text",
                    "text": "RESULT",
                    "weight": "bold",
                    "color": "#1DB446",
                    "size": "sm"
                },
                {
                    "type": "text",
                    "text": "Brown Store",
                    "weight": "bold",
                    "size": "xxl",
                    "margin": "md",
                    "contents": [
                        {
                            "type": "span",
                            "text": f"{sym}"
                        }
                    ]
                },
                {
                    "type": "separator",
                    "margin": "xxl"
                },
                {
                    "type": "box",
                    "layout": "vertical",
                    "margin": "xxl",
                    "spacing": "sm",
                    "contents": [
                        {
                            "type": "box",
                            "layout": "horizontal",
                            "contents": [
                                {
                                    "type": "text",
                                    "text": "BROKER",
                                    "size": "sm",
                                    "color": "#111111",
                                    "weight": "bold",
                                    "offsetStart": "xs",
                                    "adjustMode": "shrink-to-fit",
                                    "scaling": True,
                                    "wrap": True,
                                    "align": "start",
                                    "margin": "xs",
                                    "flex": 3
                                },
                                {
                                    "type": "text",
                                    "text": "COMMENT",
                                    "size": "sm",
                                    "color": "#111111",
                                    "align": "end",
                                    "weight": "bold",
                                    "position": "relative",
                                    "offsetStart": "xxl",
                                    
                                },
                                {
                                    "type": "text",
                                    "text": "TARGET",
                                    "size": "sm",
                                    "color": "#111111",
                                    "weight": "bold",
                                    "offsetBottom": "none",
                                    "offsetStart": "none",
                                    "align": "end"
                                }
                            ]
                        },
                        {
                            "type": "box",
                            "layout": "vertical",
                            "margin": "xxl",
                            "spacing": "sm",
                            "contents": all_rows  # Insert all_rows here
                        }
                    ]
                },
                {
                    "type": "separator",
                    "margin": "xxl"
                },
                {
                    "type": "box",
                    "layout": "horizontal",
                    "margin": "md",
                    "contents": [
                        {
                            "type": "text",
                            "text": pd.to_datetime('today').strftime('%Y-%m-%d'),
                            "color": "#aaaaaa",
                            "size": "xs",
                            "align": "end"
                        }
                    ]
                }
            ]
        },
        "styles": {
            "footer": {
                "separator": True
            }
        }
    }
    
    json_data = json.dumps(data, indent=4)
    return json_data