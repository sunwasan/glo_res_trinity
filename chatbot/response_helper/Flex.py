import pandas as pd



class Flex:
    def __init__ (self, sym):
        self.sym = sym 
        
    def header_component(self, text:str = "RESULT"):
        
        sym = self.sym
        res = [{
            "type": "text",
            "text": text,
            "weight": "bold",
            "color": "#1DB446",
            "size": "sm"
        }, {
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
        }]
        
        self.header = res
        return res

    def separator_component(self):
        res = {
            "type": "separator",
            "margin": "xxl"
        }
        self.separator = res
        return [res]

    def footer_component(self, text:str = pd.to_datetime('today').strftime('%Y-%m-%d') ):

        res =  [{
            "type": "box",
            "layout": "horizontal",
            "margin": "md",
            "contents": [
                {
                    "type": "text",
                    "text": text,
                    "color": "#aaaaaa",
                    "size": "xs",
                    "align": "end"
                }
            ]
        }]
        
        self.footer = res
        return res
        
        
        
    def generate_row_header(self, col_header:list):
        header = []
        
        order = ['start', 'center', 'end']
        
        
        for i, col in enumerate(col_header):
            col = col.upper()
            header.append({
                "type": "text",
                "text": col,
                "size": "sm",
                "color": "#111111",
                "align": order[i],
                "wrap": True,
                "flex": 3,
                "weight": "bold"
            })
        
        res =  {
            "type": "box",
            "layout": "horizontal",
            "contents": header
        }
        
        
        return res
    
    def generate_row(self, row:list):
        n_col = len(row)
        
        for i in range(n_col):
            row[i] = str(row[i])
            
        res = []
        
        align = ['start', 'center', 'end']
        
        # if only 2 col then start and end
        # if 3 col then start, center, end
        # if 1 col then start only
        
        
        for i in range(n_col):
            if i == 0:
                align = 'start'
            elif i == n_col - 1:
                align = 'end'
            else:
                align = 'center'
                
            res.append({
                "type": "text",
                "text": row[i],
                "size": "sm",
                "color": "#111111",
                "align": align,
                "wrap": True,
                "flex": 3
            })
        
        return {
            "type": "box",
            "layout": "horizontal",
            "contents": res
        }
        
        
    def generate_df(self, df, header:bool = False):
        all_rows = []
        if header:
            all_rows.append(self.generate_row_header(df.columns))
        
        for i in range(len(df)):
            all_rows.append(self.generate_row(df.iloc[i]))
        
        return all_rows        

        
    def table_3_1_1(self, df, include_header=False):
        def generate_3_1_1_header(row):
            
            first_col_setup = {
                "type":"text",
                "text":row[0],
                "size": "sm",
                "color": "#111111",
                "offsetStart": "xs",
                "adjustMode": "shrink-to-fit",
                "scaling": True,
                "wrap": True,
                "align": "start",
                "margin": "xs",
                "flex": 3,
                "weight": "bold"
            }
            second_col_setup = {
                "type":"text",
                "text":row[1],
                "size": "sm",
                "color": "#111111",
                "align": "end",
                "position": "relative",
                "offsetStart": "xxl",
                "weight": "bold"
            }
            
            third_col_setup = {
                "type":"text",
                "text":row[2],
                "size": "sm",
                "color": "#111111",
                "offsetBottom": "none",
                "offsetStart": "none",
                "align": "end",
                "weight": "bold"
            }

            headers =  [first_col_setup, second_col_setup, third_col_setup]
            
            res = {
                "type": "box",
                "layout": "horizontal",
                "contents": headers
            }
            
            return [res]
        
        def generate_3_1_1_row(row):
            
            broker = row[0]
            comment = row[1]
            target = row[2]
            
            res =  {
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
            
            
            return [res]
            
            
            
            
        all_rows = []
        if include_header:
            all_rows.extend(generate_3_1_1_header(df.columns))
        
        for i in range(len(df)):
            all_rows.extend(generate_3_1_1_row(df.iloc[i].values))
        
        
        res = {
            "type": "box",                                 
            "layout": "vertical",
            "margin": "xxl",
            "spacing": "sm",
            "contents": all_rows
        }
        
        
        return res
         
    def table_4_col(self, df, include_header=False):
        def generate_row(row, header=False):
            col1, col2, col3, col4 = row[0], row[1], row[2], row[3]
            
            all_col = [col1, col2, col3, col4]
            col_text = []
            col_color = []

            for col in all_col:
                if isinstance(col, dict):
                    color = col['color']
                    col_text.append(col['text'])
                else:
                    color = "#555555"
                    col_text.append(col)
                    
                col_color.append(color)
                    
            # col_text = [col1, col2, col3, col4]
            # col_color = ["#555555", "#111111", "#111111", "#111111"]

            if header:
                col_text = [col.upper() for col in col_text]
                weight = 'bold'
            else:
                weight = 'regular'
            
            col1_setup = {
                "type": "text",
                "text": col_text[0],
                "size": "sm",
                "color": '#111111',
                "align": "start",
                # "weight": 'regular',
                "weight": weight
            }
            col2_setup = {
                "type": "text",
                "text": col_text[1],
                "size": "sm",
                "color": '#111111',
                "align": "start",
                "offsetStart": "xxl",
                "weight": weight
            }
            col3_setup = {
                "type": "text",
                "text": col_text[2],
                "size": "sm",
                "color": '#111111',
                "align": "end",
                "offsetEnd": "xxl",
                "weight": weight
            }
            col4_setup = {
                "type": "text",
                "text": col_text[3],
                "size": "sm",
                "color": '#111111',
                "align": "end",
                "weight": weight
            }
            
            row_res = [col1_setup, col2_setup, col3_setup, col4_setup]
            
            row_res_block = {
                "type": "box",
                "layout": "horizontal",
                "contents": row_res
            }
            
            return [row_res_block]
     
        all_rows = []
        if include_header:
            all_rows.extend(generate_row(df.columns, True))
        
        for i in range(len(df)):
            all_rows.extend(generate_row(df.iloc[i].values))
            
        res = {
            "type": "box",
            "layout": "vertical",
            "spacing": "sm",
            "contents": all_rows
        }
        
        return res
   
   
    
    def green_text(self, text:str):
        return {
            "type": "text",
            "text": text,
            "weight": "bold",
            "color": "#1DB446",
            "size": "sm"
        }     
                
    
    def generate_flex(self, components:list):
        
        comps = []
        for comp in components:
            if comp:
                if type(comp) == list:
                    comps.extend(comp)
                elif type(comp) == dict:
                    comps.append(comp)
                else:
                    pass
        # for comp in components:
            
        #     comps.extend(comp)        
        return {
          "type": "bubble",
        "size":"giga",
        "body": {
            "type": "box",
            "layout": "vertical",
            
            "contents":comps
            }
        }
        
   
    
         
    
        
        
        
        
        
    