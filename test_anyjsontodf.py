# -*- coding: utf-8 -*-
"""
Created on Mon Jun  8 10:28:22 2020

@author: Fernando
"""


# =============================================================================
# 1) import library
# =============================================================================
import anyjsontodf as jd

# =============================================================================
# 2) Load JSON
# =============================================================================
# import json library
import json

# load json file
filename = "D:/OneDrive - Seachad/03 - Clientes/SEIDOR/IPCOSELL/API_Calls_Microsoft_BORRAR/ChequearLGV/G_users.json"
JSON = None
try:
    with open(filename, "r") as json_file:
        res = json.load(json_file)
        JSON = res    
except Exception as e:
        print(f"The file {filename} cannot be read: {e}")            

# =============================================================================
# 3) Magic Happens!
# =============================================================================

df = jd.jsontodf(JSON, verbose = jd.VERBOSE_REDUCED) # VERBOSE_REDUCED shows you the progress in terms of regs added to the dataframe
print(f"The DF obtained has the shape {df.shape}") # shows the shape of the df obtained

# =============================================================================
# 4) Enjoy your df (in this case we will write as an EXCEL file)
# saveToExcel is a minifunction provided to save the data as an excel file "MyJson", the data will be inside a tab called "data"
# =============================================================================

excel_filename = "MyJson.xlsx"
tab = "data"

jd.saveToExcel(df, excel_filename, tab)
