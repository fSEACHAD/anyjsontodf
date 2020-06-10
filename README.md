# anyjsontodf

Transforms a JSON file to a pandas dataframe (Python)

Usage example:

import anyjsontodf as jd

# import json library

import json

# load json file

filename = "myjsonfile.json"

JSON = None

try:

    with open(filename, "r") as json_file:
    
        res = json.load(json_file)
        
        JSON = res    
        
except Exception as e:

        print(f"The file {filename} cannot be read: {e}")   

df = jd.jsontodf(JSON, verbose = jd.VERBOSE_REDUCED) # VERBOSE_REDUCED shows you the progress in terms of regs added to the dataframe

print(f"The DF obtained has the shape {df.shape}") # shows the shape of the df obtained
        
excel_filename = "MyJson.xlsx"

tab = "data"

jd.saveToExcel(df, excel_filename, tab) # a helper function to save the df as EXCEL is included in the package

You can read an article in Medium about this software in: 

The package is provided as is. No free paid support provided. You can send any feedback (greatly appreciated) to fernando.garcia.varela@seachad.com



