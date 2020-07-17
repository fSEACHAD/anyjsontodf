# -*- coding: utf-8 -*-
"""
Created on Wed May  6 18:20:23 2020

@author: Fernando


schema JSON que soporta:
    
lista
    elementos
    diccionario
        elementos
        diccionario
        lista
            elementos
            diccionario
            lista
            lista vacía
    lista
        elementos
        diccionario
        lista
        lista vacía
    lista vacia


v1
    transforma JSON en formatos RAW pandas (se puede exportar a EXCEL, DDBB, ...)
    tratar valor null de un JSON
    setSubBlockToElements (está función es muy lenta, habría que hacerla más rápida) 
    pruebas:
        simple_dict OK
        ldict_1 - OK
        ldict_2 - OK
        filename_2 - las listas vacías no las está incluyendo y deberían ser elementos vacíos  (pendiente) - quizá deberá coger los df_columns y rellenar con vacíos en el caso de que sea un elemento LEAF
        filename_1 - a la hora de crear el dataframe estoy enviando más elementos que columnas... (OK)
    admite listas vacías
    admite listas que contienen, además de diccionarios, elementos 

v2
    checked with "configuration_machine", "G_SScores","G_users","M_RoleDefinitions_Aggregated","CSPFacturacion", "CSPProducts"        

"""

# para la codificación de JSON si se copia dentro del código
true = True
false = False
null = None

# =============================================================================
# --- Configuración general
# =============================================================================

# AUCHTUNG (siempre debería estar a FALSE)
generate_sensible_information = False # queremos generar la información sensible de configuración del programa? vuelca la info de ENVIRONMENT VARIABLES y la pone en Structs Folder
access = {}
# =============================================================================
# 1) Obtiene las Claves para el programa desde ENV Variables en el sistema
# =============================================================================
import os
import sys
import re
import copy # para hacer copias de diccionarios

# --- DICCIONARIO simple  ----------------------------------------------------------------------

access = {
        # "CM_work_folder" : "", # directorio de trabajo
        # "CM_reports_folder" : "", # directorio dónde se producen los reports (normalmente cuando traen información agregada)
        # "CM_structs_folder" : "", # directorio dónde se leen las estructuras de llamadas a las API
        # "CM_log_folder" : "", # directorio dónde se dejan los logs de los programas
        # "CM_Cloudmore_Base_Address" : "", # información para el API
        # "CM_Cloudmore_Server_Endpoint" : "", # información para el API
        # "CM_Cloudmore_API_Endpoint" : "", # información para el API
        # "CM_Reseller_Id" : "", # información para el API
        # "CM_Cloudmore_Secret" : "", # información para el API
        # "CM_username" : "", # información para el API
        # "CM_password" : "" # información para el API
        }


# =============================================================================
# Lee el fichero de configuración "miniconfig.json" para permitir la cohabitación de múltiples entornos de ejecución
# y de la carga de diferentes versiones de las librerías
# Permite tener varias versiones de librerías y de scripts cohabitando en varios espacios de computación, evitando PATHS
# El fichero miniconfig debe existir en el directorio de ejecución de la librería
# =============================================================================
# def cargaInicial():
    
#     from path import Path
#     global access
#     import json
#     import os
    
#     global access
    
#     # lectura y carga de miniconfig.json
#     miniconfig = {}
#     filename = "miniconfig.json"
    
#     try:
#         with open(filename) as json_file:
#             miniconfig = json.load(json_file)
#     except Exception as e:
#         print( f"Error de apertura del fichero de configuración {filename} - error {e}")    
#     else:
#         json_file.close()
 
#     # es el primer elemento del json (que al final es una lista de diccionarios)
#     miniconfig = miniconfig[0]
    
#     dirpath = os.getcwd()
#     access["CM_work_folder"] = dirpath 

#     # lectura y asignación de claves
#     try:    
#         access["CM_path_to_libraries"] = miniconfig["CM_path_to_libraries"]
#         access["CM_reports_folder"] = miniconfig["CM_reports_folder"]
#         access["CM_structs_folder"] = miniconfig["CM_structs_folder"]
#         access["CM_log_folder"] = miniconfig["CM_log_folder"]
#         access["CM_config_folder"] = miniconfig["CM_config_folder"]
#     except Exception as e:
#         print( f"Error de asignación de claves de miniconfig  - error {e}")    
    
#     import sys
#     # para buscar las librerías
#     sys.path.append(access["CM_path_to_libraries"])


# ---------------------------------------------
# CONFIGURACION INICIAL
# ---------------------------------------------
access = {
        #"CM_miniconfig_base" : "" # directorio de trabajo
        }

import IPL
access = IPL.getAccess()
# ---------------------------------------------
# ---------------------------------------------

    
# cargaInicial() 

# =============================================================================
# Set working directory
# =============================================================================
from datetime import datetime
import os
import sys

from pathlib import Path # esto debería servir para MACOS y para WINDOWS
# os.chdir(Path(access["CM_work_folder"]))
# =============================================================================
#  Configuración del programa
# =============================================================================
work_folder = ""
reports_folder = ""
structs_folder = ""
log_folder = ""


# =============================================================================
# Configuración del sistema de tracking and logging
# =============================================================================
# import LoggingAndTracking as lg # de momento tiene que estar en el directorio de trabajo (hay que ver como configurar para que lo coja de Github o de dónde corresponda)
# from inspect import getargvalues, stack, getframeinfo, currentframe
# import item as i # (guardado en d:\Anaconda3\lib)
# import common
# =============================================================================
# Fin de configuración del sistema de tracking and logging
# =============================================================================




from path import Path

# =============================================================================
# Función facilita que devuelve el tipo de un elemento
# =============================================================================
def tipo(i):
    if isinstance(i, list):
        return TIPO_LIST
    elif isinstance(i, dict):
        return TIPO_DICT
    else:
        return TIPO_ELEMENT


# =============================================================================
# limpiamos el primer punto y el último punto porque pueden haber quedado arrastrados en el proceso 
# =============================================================================
def quitar_punto(elemento):
    
    elemento = elemento.lstrip(".")
    elemento = elemento.rstrip(".")
        
    return elemento


# usado para solvertar el problema de la lista vacía.
# si llega una lista vacía, añado este elemento. Luego, tanto en la generación de columnas con en la generación del registro (en generateColumnList y en createDFFromLeats) elimino la información
EMPTY_KEY = "EMPTY"
EMPTY_VALUE = ""
EMPTY_DICT = {EMPTY_KEY:EMPTY_VALUE}

# =============================================================================
# Crea los elementos del JSON y los codifica manteniendo su jerarquía para poder viajar entre elementos hacia adelante, atrás y transversal
# =============================================================================

apano = False

conta_empty = -1
contador = -1
B_SB_prefix = ""
# level_B_SB_prefix = []
# pJSONtoDF()

def JSONelements_(
        clave = "", # clave del elemento (realmente es el nombre de la columna)
        item = None, # elemento que se recibe
        level = 0, # nivel de profundidad al que se encuentra el elemento
        columns_list = {}, # diccionario de columnas (luego saco los valores únicos para la lista de columnas final)
        prefix = "", # prefijo que voy arrastando para crear el nombre de la columna calificado (así permito nombres de columnas duplicados en diferentes profundidades)
        B_SB_prefix = "",
        list_count = 0, # contador de lista
        list_count_in_dictionary = 0, # identificador de lista dentro del diccionario (una lista sólo puede existir dentro de un diccionario, nunca dentro de otra lista)
        father_dict = 0, # nivel de diccionario anterior (para búsqueda horizontal) - esto elimina colisiones entre codificación de elementos que se encuentren a mismo nivel y diccionario, pero en diferente número
        dict_in_level = 0, # nivel de diccionario del elemento.
        dict_count = 0, # contador de diccionarios
        count_element = -1, # número del elemento dentro de su diccionario
        father_absolute_dictionary = 0, # absolute_dictionary_count que es el padre de este nuevo diccionario
        SB_T = -1, # asignactión de subbloque temporal -- muy importante, todos los elementos que pertenecen al mismo subbloque pertenecen al mismo registro (se utiliza para componer el registro completo, a base de elementos que pertenecen a los mismos subbloques)
        LCID = 0, # Lista Count In Dictionary (contador absoluto de lista dentro de un diccionario) - nos apunta si hay listas al mismo nivel en el diccionario
        prior_LCID = 0,
        initialize = False
        ):
    
    global apano 
    global contador
    # apano = False
    global conta_empty
    
    # vamos a conseguir una ristra de títulos
    global count_element_absolute
    global level_prefix, level_code, level_dict, b_sb_prefix, level_B_SB_prefix
    global clave_valor, element_list, count_element_absolute    
    global absolute_dict_count
    global gSB_T, max_gSB_T
    global gLCID, max_gLCID
    
    level_prefix[level] = prefix # voy guardando los prefijos de nombre de columna por nivel
    level_B_SB_prefix[level] = B_SB_prefix

    level_code[level] = count_element 
    
    if initialize == True: # es la primera ejecución, limpio el diccionario con el nombre de todas las columnas decoradas por nivel
        columns_list = {}
    
    current_sb = SB_T
    if isinstance(item, list):
        
        list_count += 1 # si elemento que me viene es una lista, aumento el contador de listas
        
        father_absolute_dictionary = absolute_dict_count     

        gSB_T += 1 # aumento el contador de subbloques
        max_gSB_T = gSB_T
        
        
        # gLCID += 1
        # LCID = gLCID
        # max_gLCID = gLCID
        # LCID = gLCID
        # prior_LCID = LCID
        # LCID += 1
        
        if len(item)==0: # lista vacia. Si viene una lista vacía, creo un elemento 
            myd = EMPTY_VALUE
            # conta_empty += 1
            # myd = {"EMPTY" : "EMPTY"}
            dict_count -= 1
            item.append(myd)

        
        for i in item: # rastreo los elementos contenidos en la lista (pueden ser diccionarios o elementos, no listas)

            if isinstance(i, dict) or tipo(i) == 3: # el siguiente elemento es un diccionario o un elemento?
                sumar = 1 # incremento el nivel de profundidad
                
                # LCID = prior_LCID

                #     LCID = prior_LCID
                if isinstance(i, dict):                
                    absolute_dict_count += 1  # incremento el contador absoluto de diccionarios
                    dict_in_level += 1 # incremento el contador de diccionarios dentro del nivel en que me encuentro (LCID)
                    # if item == "EMPTY_dict": # si es un diccionario FAKE no incremento el SB
                    #     gSB_T -= 1
                
                gSB_T += 1 # aumento el identificador de subbloque
                max_gSB_T = gSB_T
                
                # 20200614
                local_B_SB_prefix = level_B_SB_prefix[level]                
                B_SB_prefix = f"{local_B_SB_prefix}"                
                #å 20200614
                
                if tipo(i) == 3: # es un elemento
                    # gSB_T -= 1 # vuelvo a dejarlo como antes                    
                    sumar += 1 # me aseguro que los niveles van de 2 en 2 que es como lo entiende el programa
                    count_element += 1 # LE - contador de elementos en el nivel
                    count_element_absolute +=1 # E - número absoluto de elemento                   
                    
                # if isinstance(i, dict):
                #     dict_count += 1 # ---- v2  incremento el contador de diccionarios sólo si lo que viene es un diccionario!                  
                    
                # mando el elemento a la función para que se procese
                JSONelements_(item = i, 
                              level = level+sumar, 
                              columns_list = columns_list,
                              prefix = prefix, 
                              B_SB_prefix = B_SB_prefix,
                              list_count = list_count, 
                              # list_count_in_dictionary = list_count_in_dictionary, # identificador de lista dentro del diccionario (una lista sólo puede existir dentro de un diccionario, nunca dentro de otra lista)
                              
                              father_dict = father_dict, 
                              dict_in_level = dict_in_level, 
                              dict_count = dict_count+1, 
                              count_element = count_element, 
                              father_absolute_dictionary = father_absolute_dictionary,
                              SB_T = gSB_T,
                              LCID = LCID,
                              prior_LCID = prior_LCID)
             
    elif isinstance(item, dict):

        local_prefix = level_prefix[level]    
        local_B_SB_prefix = level_B_SB_prefix[level]
        
        # guardo esta información para que el elemento FAKE que creo tenga la misma información que el resto de elementos que dependen de este diccionario, sin que la lista que viene cambie la información
        mem_father_dict = father_dict
        prior_LCID = LCID
        mem_SB = SB_T
        mem_dict_ADC = absolute_dict_count
        
        for k,v in item.items():
            
            sumar = 1 # si un diccionario contiene otros diccionarios, no aumento el level de profundidad
            
# =============================================================================
# ELEMENT            
# =============================================================================
            if tipo(v) == 3: # es un elemento
                # gSB_T += 1                
                # father_dict = dict_in_level  
                count_element += 1 # contador de elementos en el nivel
                count_element_absolute +=1 # contador absoluto de elementos
                prefix = f"{local_prefix}"
                B_SB_prefix = f"{local_B_SB_prefix}"
                # LCID = prior_LCID         

# =============================================================================
# LIST
# =============================================================================
            code_SB_T = gSB_T    
            if tipo(v) == 1: # es un elemento list?
                
                #dict_count += 1
# APAÑO -------------------------------------------------------------------------------------------      
                if apano:               
                    prior_LCID = LCID      
                    
                    LCID = LCID + 1                   
                    
                    # XXX: 202006.001
                    gSB_T += 1
                    # 202006.001
                    
                    # antes de cualquier lista me ocupo de meterle un elemento, porque el marcador de bloques espera empezar con un nivel 2 *****
                    current_ADC = absolute_dict_count # antes de cambiar ningún valor, lo memorizo
                    absolute_dict_count = mem_dict_ADC
                    count_element += 1 # contador de elementos en el nivel
                    count_element_absolute +=1 # contador absoluto de elementos
                    prefix = f"{local_prefix}" 
                    # B_SB_prefix = B_SB_prefix
                    # le añado un element "EMPTY" para notificar que llega una lista, así siempre me aseguro que el primer elemento de un bloque NO es una lista y comienza con un L == 2
                    JSONelements_(clave = "EMPTY_list", 
                                  item = "", 
                                  level = level+sumar, 
                                  columns_list = columns_list, 
                                  prefix = prefix, 
                                  B_SB_prefix = B_SB_prefix,
                                  list_count = list_count, 
                                  # list_count_in_dictionary = list_count_in_dictionary + 1,
                                  
                                  father_dict = mem_father_dict, 
                                  dict_in_level = dict_in_level, 
                                  dict_count = dict_count, 
                                  count_element = count_element,
                                  father_absolute_dictionary = father_absolute_dictionary,
                                  # SB_T = mem_SB,
                                  SB_T = gSB_T,
                                  # LCID = mem_LCID,
                                  LCID = LCID,
                                  prior_LCID = prior_LCID)
    
                    absolute_dict_count = current_ADC # restauro valor original

                    # LCID = mem_LCID # restauro la codificación anterior
             
# FIN APAÑO ------------------------------------------------------------------------------------------- 
                
                SB_T = gSB_T
                father_dict = dict_in_level               
                prefix = f"{local_prefix}.{k}"
                B_SB_prefix = f"{local_B_SB_prefix}.{code_SB_T}"
                # list_count_in_dictionary +=1
                # msntengo el LCID, pero guardándome enviar, para este diccionario, siempre el mismo, no el absoluto

                # LCID = LCID + 1

# =============================================================================
# DICT
# =============================================================================
            if tipo(v) == 2: # es un elemento dictionary? (un diccionario puede venir en una lista o en un diccionario)

                # dict_in_level += 1                
                absolute_dict_count += 1
                father_dict = dict_in_level   
                B_SB_prefix = f"{local_B_SB_prefix}.{SB_T}"                
                # dict_count += 1 # incremento el contador absoluto de diccionarios   
                # incremento el contador de Subbloques, pero guardándome enviar, para este diccionario, siempre el mismo, no el absoluto
                gSB_T = max_gSB_T +1
                SB_T = gSB_T # OJO!!! añadido para que todos los subbloques sean diferentes

                prefix = f"{local_prefix}.{k}"

                sumar = 0 # un diccionario NO aumenta el nivel
                # LCID = prior_LCID

# APAÑO -------------------------------------------------------------------------------------------    
                # creo un elemento "FAKE"
            
                if apano:
                    count_element += 1 # contador de elementos en el nivel
                    count_element_absolute +=1 # contador absoluto de elementos
                    aditional = 0
                    if not level + sumar % 2 == 0:
                        aditional = 1 # tengo que garantizar que el niveñ con el que califico al elemento FAKE es par (si viene de lista y diccionario, sólo le habrá sumado uno al nivel y será impar)
                    # prefix = f"{local_prefix}"               
                    # le añado un element "EMPTY" para notificar que llega una lista, así siempre me aseguro que el primer elemento de un bloque NO es una lista y comienza con un L == 2
                    JSONelements_(clave = "EMPTY_dict", 
                                  item = "", 
                                  level = level+sumar+aditional, 
                                  columns_list = columns_list, 
                                  prefix = f"{local_prefix}", 
                                  B_SB_prefix = B_SB_prefix,
                                  list_count = list_count, 
                                  # list_count_in_dictionary = list_count_in_dictionary + 1,
                                  
                                  father_dict = father_dict, 
                                  dict_in_level = dict_in_level, 
                                  dict_count = dict_count, 
                                  count_element = count_element,
                                  father_absolute_dictionary = father_absolute_dictionary,
                                  SB_T = SB_T,
                                  LCID = LCID,
                                  prior_LCID = prior_LCID)
                    
                    # SB_T = code_SB_T # un diccionario FAKE no debería aumentar el número de SB
# APAÑO ------------------------------------------------------------------------------------------- 



                
            JSONelements_(clave = k, 
                          item = item[k], 
                          level = level+sumar, 
                          columns_list = columns_list, 
                          prefix = prefix, 
                          B_SB_prefix = B_SB_prefix,
                          list_count = list_count, 
                          # list_count_in_dictionary = list_count_in_dictionary + 1,
                          
                          father_dict = father_dict, 
                          dict_in_level = dict_in_level, 
                          dict_count = dict_count, 
                          count_element = count_element,
                          father_absolute_dictionary = father_absolute_dictionary,
                          SB_T = SB_T,
                          LCID = LCID,
                          prior_LCID = prior_LCID)

    else:
        clave_columna = {"L" : level,
                         # "LD" : dict_in_level,
                         "D" : dict_count,
                         "LE" : count_element } # un elemento puede saber a qué columna pertenece en base a su level y número de elemento
        
        if not prefix == "": 
            prefix = f"{prefix}.{clave}"
            B_SB_prefix = f"{B_SB_prefix}.{SB_T}"
            l_prefix = prefix
            l_prefix = quitar_punto(l_prefix)

        else:
            prefix = clave
            l_prefix = f"{clave}"
            l_prefix = quitar_punto(l_prefix)
         
        # father_dict = level_dict[level]
        own_dict = dict_in_level
        block = 0 # dejo preparada la info del bloque del elemento
        val = item
        if val == None:
            val = ""
            

        element_code = {
            "key" : l_prefix, # nombre de la columna
            "value" : val, # valor del item
            "PBSB" : get_B_SB(B_SB_prefix), # sigue la codificación B (block) SB (subblock arrastrado)
            "B" : block, # block (número de diccionario o elementos de primer nivel en cualquier elemento de la lista del JSON)
            "SB" : SB_T, # subblock - group of elements that forms a "slice" of a register   
            # "SB_T" : SB_T, # subbloque temporal
            "L" : level, # level - deep level
            "FD" : father_dict, # father dictionary in prior level
            "LD" : dict_in_level, # dictionary sequence inside this level
            # "RLCID" : list_count_in_dictionary, # identificador relativo de lista dentro de un diccionario (es como el nivel de profundidad DENTRO de un diccionario)
            "D" : dict_count, # absolute count of diccionaries in lwcwl
            "LE" : count_element, # element inside this level
            "E" : count_element_absolute, # contador absoluto de elementos
            "ADC" : absolute_dict_count, # contador absoluto de diccionarios
            "FADC" : father_absolute_dictionary, # absolute_dictionary_count que es el padre de este diccionario
            "LCID" : LCID, # contador de lista DENTRO de cada diccionario (nos apunta si tenemos listas anidadas), es un contador absoluto
            "LF" : "", # es elemento LEAF?
            "FLF" : "" # elemento final LEAF?
            } # absolute count of elements
        
        # codifico la columna
        concat_code = f"{level}.{father_dict}.{dict_in_level}.{dict_count}.{count_element}.{count_element_absolute}"
        columns_list[concat_code] = l_prefix
        
        
        element_list.append(element_code)
        # print(f"\t{clave} :\t {item} \t[{block}].{level}.{father_dict}.{dict_in_level}.{dict_count}.{count_element}.{count_element_absolute}")    
       
    return columns_list, element_list


empty_dict = {"KEY":"VALUE"}



# =============================================================================
# Devuelve el nombre de la columna de cualquier elemento
# =============================================================================
def nameColumnFromElement(i, lk):
    """
    Recibe un elemento del tipo "element_code" y devuelve a qué columna pertenece

    Parameters
    ----------
    i : TYPE
        DESCRIPTION.
    lk : TYPE
        DESCRIPTION.

    Returns
    -------
    None.

    """
    v = i
    # creo la clave concatenada del elemento
    level = v["L"]
    father_dict = v["FD"]
    dict_in_level = v["LD"]
    dict_count = v["D"]
    count_element = v["LE"]
    count_element_absolute = v["E"]
    concat_code = f"{level}.{father_dict}.{dict_in_level}.{dict_count}.{count_element}.{count_element_absolute}"
    # busco, en la lista de keys (columnas), a qué elemento se corresponde
    return lk[concat_code]

# --- SUPPORT FUNCTIONS ---------------------------------------------------------------------------------------------------------

# =============================================================================
# Muestro los elementos de forma fácil
# =============================================================================
def _printElementList(element_list, # lista de elementos del JSON
                      filename = None, # queremos sacarlo a fihcero?
                      infoleaf = False, # queremos identificador de LEAF?
                      list_of_begin_end_block_pointer = [], # si queremos indicador de LEAR hay que enviarlo
                      list_max_level_per_block = []): # si queremos indicador de LEAF hay que enviarlo
    for l in element_list:
       print(f" ELEMENT: {elementToString(l)}")  
       
    if filename != None:
        cadena = ""
        f=open(filename,"a")
        for l in element_list:
           
            cadena = cadena + f"\n ELEMENT: {elementToString(l)}"
            # if infoleaf:
            #    is_leaf = isleaf(element_list, l["B"], list_of_begin_end_block_pointer, list_max_level_per_block, l["L"], l["LD"], l["D"], l["E"])
               # if is_leaf:
               #     cadena = cadena + " -- (*)"
            
        f.write(cadena)
        f.close()

# =============================================================================
# Imprime los elementos separándolos por bloques
# =============================================================================
def _printElementListByBlock(element_list, # lista de elementos del JSON
                     filename = None, # queremos sacarlo a fihcero?
                     infoleaf = False, # queremos identificador de LEAF?
                     list_of_begin_end_block_pointer = [] # si queremos indicador de LEAR hay que enviarlo
                     ): 
    import codecs    
    for n in range(0, len(list_of_begin_end_block_pointer)): # rastreamos cada bloque
        begin = list_of_begin_end_block_pointer[n][0]
        end = list_of_begin_end_block_pointer[n][1]+1
        
        print(f" \n\n**** \t BLOCK: {n} *** \n\n")  
        
        for l in range(begin, end):    
            element = element_list[l]
            print(f" ELEMENT: {elementToString(element)}")  
       
    if filename != None:
        cadena = f"******************* {datetime.datetime.now()} *********************************************************"
        # f=open(filename,"w")
        
        with codecs.open(filename, "a", encoding="utf-8") as f:            
            for n in range(0, len(list_of_begin_end_block_pointer)): # rastreamos cada bloque
                begin = list_of_begin_end_block_pointer[n][0]
                end = list_of_begin_end_block_pointer[n][1]+1
    
                cadena = cadena + f" \n\n**** \t BLOCK: {n} *** \n\n"            
                for l in range(begin, end):    
                    element = element_list[l]
                    cadena = cadena + f"\n ELEMENT: {elementToString(element)}"
                
            f.write(cadena)
            f.close()




        
# =============================================================================
# Imprime los elementos, recibiendo una lista de indices, que transforma en una lista de elementos
# =============================================================================
def _printElementListByListOfIndex(element_list, element_list_index ):
    element_list_result = []
    for i in element_list_index:
        element = element_list[i]
        element_list_result.append(element)
    _printElementList(element_list_result)

# =============================================================================
# Convierte la cadena recibida de subbloques, en una lista de subbloques con índices enteros
# =============================================================================
def get_B_SB(res):
    s = res
    res = s.split(".")
    
    # si res tiene algún valor a '' lo quitamos
    if '' in res:
        vacio = res.index('')
        del res[vacio]
    
    results = [int(x) for x in res]
    
    return results
    
# =============================================================================
# Muestra toda la información de un element
# =============================================================================
def elementToString(leaf):
    # output = f"\t{leaf['key'][:65]:65} :\t {str(leaf['value'])[:20]:>20} \t[{leaf['B']:>3}].{leaf['SB']:>3}\t-->{leaf['L']:>3}.[{leaf['FD']:>3}].({leaf['LD']:>3}).{leaf['D']:>3}.{leaf['LE']:>3} *{leaf['LCID']:>3}*--- {leaf['ADC']:>3}.{leaf['FADC']:>3} -- {leaf['E']:>3} {leaf['LF']:>3} {leaf['FLF']:>3}"
    
    output = f"\t{leaf['key'][:65]:65} :\t {str(leaf['PBSB'])[:20]:>20} \t[{leaf['B']:>3}].{leaf['SB']:>3}\t-->{leaf['L']:>3}.[{leaf['FD']:>3}].({leaf['LD']:>3}).{leaf['D']:>3}.{leaf['LE']:>3} *{leaf['LCID']:>3}*--- {leaf['ADC']:>3}.{leaf['FADC']:>3} -- {leaf['E']:>3} {leaf['LF']:>3} {leaf['FLF']:>3}"
    return output

# =============================================================================
# # imprime en un fichero, o por pantalla si no se le dice nada
# =============================================================================
def print_f(cadena, filename = "", initializeFile = False):
    print(cadena)
    openmode  = "a"
    if initializeFile:
        openmode = "w"
    if not filename == "":
        f=open(filename,openmode)
        f.write(cadena+"\n")
        f.close()

# --- LEAF ----------------------------------------------------------------------

MARK_LEAF = "*"
MARK_TERMINAL_LEAF = "T"


# =============================================================================
# Devuelve una lista con los punteros a los ultimos niveles 4 de un bloque 
# =============================================================================
def getMaxLevelPerBlock(element_list, list_of_begin_end_block_pointer):
    # 1) consigo la lista de punteros a todos los niveles 4
    list_pointers_level4_per_B = []
    list_LCID_level4_per_B = []
    
    for n in range(0, len(list_of_begin_end_block_pointer)): # rastreamos cada bloque
        begin = list_of_begin_end_block_pointer[n][0]
        end = list_of_begin_end_block_pointer[n][1]+1
        list_pointers_level4 = []       
        
        # creo una lista con todos los niveles (es una lista temporal para poder operar con listas de python) - sólo necesito los niveles del bloque corriente
        levels_list = []
        last_level_4 = -1
        last_level_4_LCID = -1
        for l in range(begin, end):    
            element = element_list[l]    
            levels_list.append(element["L"])
            
        if len(levels_list)>0:
            # ahora consigo el puntero al último level_4 del bloque
            list_pointers_level4 = get_indexes_pervalue(levels_list, 4)
            last_level_4 = list_pointers_level4[len(list_pointers_level4)-1]
            last_level_4_LCID = element_list[last_level_4]["LCID"]
        if last_level_4 != -1:
            last_level_4 += begin # calculamos el índice correcto del elemento gracias a su desplazamiento con el comienzo del bloque
        
        list_pointers_level4_per_B.append(last_level_4)
        
        list_LCID_level4_per_B.append(last_level_4_LCID)
        
        return list_pointers_level4_per_B, list_LCID_level4_per_B


# =============================================================================
# Marca en el indicador FLF las final LEAVES de cada bloque, para construir el registro hacia atrás    
# =============================================================================
def markFinalLEAVES(element_list, list_of_begin_end_block_pointer):
    
    # ESTRATEGIA
    # FIXME: 001 Está marcando mal las FINAL LEAVES cuando hay varias listas al mismo nivel
    
    # consigo el mayor level 4 (es dónde residirán las FLF) de cada bloque
    # para ello rastreo todos los elementos y me quedo con el último que aparece
    # FIXME: Cambiar, dentro de la función max_level_per_block, que llame a getMaxLevelPerBlock que es más limpia
    list_leaf_level_per_block, list_last_LCID_per_block = max_level_per_block(element_list, list_of_begin_end_block_pointer)  
    # print(f" elemento con último level por bloque {list_leaf_level_per_block}  LCID {list_last_LCID_per_block}")
    
    # del elemento conseguido, busco todos los elementos que estén a su nivel (es decir, level 4 o level 2 si no hay level 4 y que tengan su mismo LCID)
    list_possible_elements = getElementsLevel4FinalLeaves(element_list, list_of_begin_end_block_pointer, list_leaf_level_per_block, list_last_LCID_per_block)  
    # print(f"todos los level posibles para FLF {list_possible_elements}")
    # list_possible_elements = list_leaf_level_per_block
    
    # ahora tengo todos los level_4 o level 2 dónde buscar su máxima profundidad, busco su máximo nivel de profundidad (realmente pueden ser diversos elementos), ese máximo nivel de profundidad me dará las FDF
    list_final_FLF, list_elements_max_deep, list_LCID_max_deep_per_block = getMaxDeepLevelOfLevel4(element_list, list_of_begin_end_block_pointer, list_possible_elements)
    # print(f"{list_final_FLF} {list_elements_max_deep} {list_LCID_max_deep_per_block}")  

    # finalmente marco todos los elementos FLF
    markFinalFLFs(element_list, list_of_begin_end_block_pointer, list_elements_max_deep )
    # _printElementListByBlock(element_list, f"{filename}.txt", infoleaf = True, list_of_begin_end_block_pointer = list_of_begin_end_block_pointer) # si queremos indicador de LEAR hay que enviarlo


# =============================================================================
# Me llega una lista con elementos FLF, tengo que localizar sus subbloques y marcar todos los elementos como FLF
# los elementos que me interesan han de tener el nivel máximo de profundidad, el LCID y conseguir sus subbloques    
# =============================================================================
def markFinalFLFs_NEW(element_list, list_of_begin_end_block_pointer, list_final_FLF, list_elements_max_deep, list_LCID_max_deep_per_block):
    for block in range(0,len(list_of_begin_end_block_pointer)):
        n = block
        begin = list_of_begin_end_block_pointer[n][0]
        end = list_of_begin_end_block_pointer[n][1]+1

        for i in list_elements_max_deep[block]:
            element = element_list[i]
            # element = element_list[list_elements_max_deep[block]]
            L = element["L"]
            SB = element["SB"]
            LCID = element["LCID"]
            # ahora tengo que buscar, dentro del bloque, todos los elementos que coincidan
            list_of_FLF_subblocks = getAllElementsBy_B_L_LCID(element_list, list_of_begin_end_block_pointer, block, L, LCID)
            for subblock in list_of_FLF_subblocks:
                # obtengo todos los elementos de cada subblock
                list_of_elements = getSubBlockElementsBySubblock(element_list, list_of_begin_end_block_pointer, block, subblock)
                # marco todos los elementos del subblock como FLF
                for e in list_of_elements:
                    element_list[e]["FLF"] = MARK_TERMINAL_LEAF

# def markFinalFLFs_OLD(element_list, list_of_begin_end_block_pointer, list_elements_max_deep ):
#     for block in range(0,len(list_of_begin_end_block_pointer)):
#         n = block
#         begin = list_of_begin_end_block_pointer[n][0]
#         end = list_of_begin_end_block_pointer[n][1]+1

#         for i in list_elements_max_deep[block]:
#             element = element_list[i]
#             L = element["L"]
#             SB = element["SB"]
#             LCID = element["LCID"]
#             # ahora tengo que buscar, dentro del bloque, todos los elementos que coincidan
#             list_of_FLF_subblocks = getAllElementsBy_B_L_LCID(element_list, list_of_begin_end_block_pointer, block, L, LCID)
#             for subblock in list_of_FLF_subblocks:
#                 # obtengo todos los elementos de cada subblock
#                 list_of_elements = getSubBlockElementsBySubblock(element_list, list_of_begin_end_block_pointer, block, subblock)
#                 # marco todos los elementos del subblock como FLF
#                 for e in list_of_elements:
#                     element_list[e]["FLF"] = MARK_TERMINAL_LEAF

# =============================================================================
# Asumimos que los elementos que tengan tantos B_SB como los de máximo nivel que nos llegan, serán las FLF
# =============================================================================
def markFinalFLFs(element_list, list_of_begin_end_block_pointer, list_elements_max_deep ):
    for block in range(0,len(list_of_begin_end_block_pointer)):
        n = block
        begin = list_of_begin_end_block_pointer[n][0]
        end = list_of_begin_end_block_pointer[n][1]+1

        for i in list_elements_max_deep[block]:
            element = element_list[i]
            deep = element["PBSB"]
            LCID = element["LCID"]
            L = element["L"]
            B = element["B"]
            # ahora tengo que buscar, dentro del bloque, todos los elementos que coincidan
            list_of_FLF_subblocks = getAllElementsByDepth(element_list, list_of_begin_end_block_pointer, block, deep, LCID, L)
            # print(f"markFinalFLFs list_of_SB {list_of_FLF_subblocks}")
            for subblock in list_of_FLF_subblocks:
# 20200615 - configuration_machine devuelve 36 registros en lugar de 18                
                for x in subblock: # puede devolver varios subbloques
                    # obtengo todos los elementos de cada subblock
                    list_of_elements = getSubBlockElementsBySubblock(element_list, list_of_begin_end_block_pointer, block, x)
                    # print(f"markFinalFLFs elements in SB {x} -> {list_of_elements}")
                    # marco todos los elementos del subblock como FLF
                    for e in list_of_elements:
                        # print(f"MarkFinalFLFs element {e}")
                        element_list[e]["FLF"] = MARK_TERMINAL_LEAF


                 
# =============================================================================
# Devuelve la lista de SB de elementos que tengan B_SB (nivel de profundidad) todos los elementos iguales hasta el último
# XXX: está comprobado para profundidades de dos niveles, no lo he comprobado para profundidades de más niveles, puede dar error 
# para más niveles probando que tienen que tener el mismo #deep y comenzar con el mismo SB                  
# =============================================================================
# def getAllElementsByDepth_OLD(element_list, list_of_begin_end_block_pointer, deep, LCID, L):
#     list_of_list_of_SB = []
#     list_SB = []
    
#     for block in range(0,len(list_of_begin_end_block_pointer)):
#         n = block
#         begin = list_of_begin_end_block_pointer[n][0]
#         end = list_of_begin_end_block_pointer[n][1]+1
        
#         list_SB.clear()

#         for i in range(begin, end):    
#             element = element_list[i]   
#             l_deep = element["PBSB"]
#             l_LCID = element["LCID"]
#             l_L = element["L"]
#             # print(f"{len(l_deep)} {len(deep)} {l_deep[0]} {deep[0]}")
            
#             if len(l_deep)!=0 and len(deep)!=0: # si no hay l_deep o deep para comparar nos vamos
#                 # le quitamos 2 para quedarnos con el identificador de profundidad de lista en el nivel que nos interesa (ahí es dónde estará cualquier FLF)
#                 if len(l_deep) == len(deep) and l_deep[0] == deep[0] and l_LCID == LCID and l_L == L: # esto hace que configuration_machine funcione correctamente (tiene varios FLF que pueden estar seguidos)
#                 # if len(l_deep) == len(deep) and l_deep[0:len(l_deep)-1] == deep[0:len(deep)-2] and l_CID == LCID: # esto hace que configuration_machine funcione correctamente (tiene varios FLF que pueden estar seguidos)
#                 # if len(l_deep) == len(deep) and l_deep[:-1] == deep[:-1]: # esto hace que G_SScores funcione correctamente (tiene varios FLF que pueden estar seguidos)
                    
#                     SB = element["SB"]
#                     if not SB in list_SB:
#                         list_SB.append(SB)
#         list_of_list_of_SB.append(list_SB.copy())
#     return list_of_list_of_SB
             
def getAllElementsByDepth(element_list, list_of_begin_end_block_pointer, block, deep, LCID, L):
    list_of_list_of_SB = []
    list_SB = []
    
    begin = list_of_begin_end_block_pointer[block][0]
    end = list_of_begin_end_block_pointer[block][1]+1
    
    list_SB.clear()


    # si no hay nivel superior a 2, todos los SB se cogen
    niveles_superiores = False
    for i in range(begin, end):
        element = element_list[i] 
        if element["L"] > 2:
            niveles_superiores = True
            break
    
    # si hay niveles superiores a 2...
    if niveles_superiores == True:
        for i in range(begin, end):    
            element = element_list[i]   
            l_deep = element["PBSB"]
            l_LCID = element["LCID"]
            l_L = element["L"]

            
            if len(l_deep)!=0 and len(deep)!=0: # si no hay l_deep o deep para comparar nos vamos
                # print(f"{len(l_deep)} {len(deep)} {l_deep[0]} {deep[0]}")                
                # le quitamos 2 para quedarnos con el identificador de profundidad de lista en el nivel que nos interesa (ahí es dónde estará cualquier FLF)
                if len(l_deep) == len(deep) and l_deep[0] == deep[0] and l_LCID == LCID and l_L == L: # esto hace que configuration_machine funcione correctamente (tiene varios FLF que pueden estar seguidos)
                # if len(l_deep) == len(deep) and l_deep[0:len(l_deep)-1] == deep[0:len(deep)-2] and l_CID == LCID: # esto hace que configuration_machine funcione correctamente (tiene varios FLF que pueden estar seguidos)
                # if len(l_deep) == len(deep) and l_deep[:-1] == deep[:-1]: # esto hace que G_SScores funcione correctamente (tiene varios FLF que pueden estar seguidos)
                    
                    SB = element["SB"]
                    if not SB in list_SB:
                        list_SB.append(SB)
                        
    # si sólo hay niveles 2, me quedo con todos los subbloques
    if niveles_superiores == False:                        
        for i in range(begin, end):    
            element = element_list[i] 
            SB = element["SB"]
            if not SB in list_SB:
                list_SB.append(SB)
                        
    list_of_list_of_SB.append(list_SB.copy())
    return list_of_list_of_SB

   
# =============================================================================
# DEEPEST
# buscamos los elementos deepest in the ocean!
# si el nivel enviado es None, entonces nos devuelve los elementos, SB y el nivel de mayor profundidad de ebtre los LEAF temporables del bloque (que no sean MARK_TERMINAL_LEAF)
# si el nivel enviado tiene un valor, nos devuelve el de nivel inferior al enviado (-2), hasta que se encuentre con un nivel 2    
# =============================================================================
def getElementsDeepest(element_list, list_of_begin_end_block_pointer, block, deep_level = None):

    # si el deep_level que buscamos es el de mayor nivel
    deep = -1

    list_elements_deepest_in_the_ocean = []
    list_SB_deepest_in_the_ocean = []

    n = block    
    begin = list_of_begin_end_block_pointer[n][0]
    end = list_of_begin_end_block_pointer[n][1]+1
    
    for l in range(begin, end):    
        
        element = element_list[l]    
        
        if element["FLF"] == MARK_TERMINAL_LEAF:
            continue
        
        # buscamos el mayor nivel posible
        if element["LF"] == MARK_LEAF and element["FLF"] != MARK_TERMINAL_LEAF:
            if deep_level == None:
                if element["L"] > deep:
                    deep = element["L"]
                    continue
                continue
            if element["L"] > deep and element["L"] == deep_level - 2:
                deep = element["L"]
                
    # una vez que tenemos el nivel máximo solicitado, conseguidmos todos los elementos que sean LF y que tengan ese nivel, los sumamos a una lista y devolvemos los elementos y sus SB
    for l in range(begin, end):    
        element = element_list[l]    
        if element["LF"] != MARK_LEAF:
            continue
        if element["FLF"] == MARK_TERMINAL_LEAF:
            continue
        if element["L"] == deep:
            if not element["E"] in list_elements_deepest_in_the_ocean:
                list_elements_deepest_in_the_ocean.append(element["E"])
            if not element["SB"] in  list_SB_deepest_in_the_ocean: 
                list_SB_deepest_in_the_ocean.append(element["SB"])
    
    return list_elements_deepest_in_the_ocean, list_SB_deepest_in_the_ocean, deep

### XXX: Tests de ligaduras para construir registros
def _tests():
    
    # LOCALIZACION ULTIMO LELVE 4
    list_leaf_level_per_block, list_last_LCID_per_block = max_level_per_block(element_list, list_of_begin_end_block_pointer)  
    print(f" elemento con último level_4 por bloque {list_leaf_level_per_block}  LCID {list_last_LCID_per_block}")
    
    # ELEMENTOS CON EL MISMO LCID
    # del elemento conseguido, busco todos los elementos que estén a su nivel (es decir, level 4 y que tengan su mismo LCID)
    list_possible_elements = getElementsLevel4FinalLeaves(element_list, list_of_begin_end_block_pointer, list_leaf_level_per_block, list_last_LCID_per_block)  
    print(f"todos los level_4 posibles para FLF {list_possible_elements}")
   
    # LOCALIZACION FLF
    # ahora tengo todos los level_4 dónde buscar su máxima profundidad, busco su máximo nivel de profundidad (realmente pueden ser diversos elementos), ese máximo nivel de profundidad me dará las FDF
    list_final_FLF, list_elements_max_deep, list_LCID_max_deep_per_block = getMaxDeepLevelOfLevel4(element_list, list_of_begin_end_block_pointer, list_possible_elements)
    print(f"LEVELS for FLF {list_final_FLF} ELEMENTS FOR FLF {list_elements_max_deep} LCID FOR START LOOKING {list_LCID_max_deep_per_block}")  

    # -------------------------------------------------------------------------  
    # -------------------------------------------------------------------------  
    # ESTRATEGIA para construcción de registro desde un elemento concreto
    # -------------------------------------------------------------------------  
    # -------------------------------------------------------------------------  
    index = 343
    E = index
    result = isLF(element_list, list_of_begin_end_block_pointer, list_max_level_per_block, E = index)
    print(f"ELEMENTS ES LF? {index} {result}")      
    
    # ELEMENTS SB
    # recogemos todos los elementos que pertenecen al subbloque del elemento que nos envían
    list_SB_FLF = getSubBlockElementsByIndex(element_list, list_of_begin_end_block_pointer, E = index)
    print(f"ELEMENTS OF SAME SB OF {index} ELEMENTS  {list_SB_FLF} ")  

    # LINK DIRECT
    # elementos a los que se ligaría este subbloque porque pertenecen al diccionario anterior
    list_elements_LinkDirect, list_subblocks_LinkDirect = getElementsEnlaceInmediato(element_list, list_of_begin_end_block_pointer, E = index)
    print(f"LINK DIRECT ELEMENTS  {list_elements_LinkDirect} SB  {list_subblocks_LinkDirect} ")  

    # DEEPEST
    # buscamos los elementos deepest in the ocean!
    # si el nivel enviado es None, entonces nos devuelve los elementos, SB y el nivel de mayor profundidad
    # si el nivel enviado tiene un valor, nos devuelve el de nivel inferior al enviado (-2), hasta que se encuentre con un nivel 2
    # deep_level = None
    # block = 0
    # list_elements_deepest_in_the_ocean, list_SB_deepest_in_the_ocean, deep = getElementsDeepest(element_list, list_of_begin_end_block_pointer, block, deep_level)
    # print(f"ELEMENTS OF BLOCK {block} DEEPEST IN THE OCEAN {list_elements_deepest_in_the_ocean} SB {list_SB_deepest_in_the_ocean} DEEP {deep}")   

    # LF
    # buscmmos todos los elementos LF por debajo del elemento enviado y con LCID inferior 
    l_list_elements_same, l_ilst_subblocks_same, resultado = getLeafsBelowIndexSharingLCID(element_list, list_of_begin_end_block_pointer, E = index)     
    print(f"ELEMENTS ENLACE LF: SAME LEVEL {l_list_elements_same} SB {l_ilst_subblocks_same}")  
    
    # l_list_elements_same, l_ilst_subblocks_same, resultado = getLFSameLevel(element_list, list_of_begin_end_block_pointer, E = index)     
    # print(f"ELEMENTS ENLACE LF: SAME LEVEL {l_list_elements_same} SB {l_ilst_subblocks_same}")  
    
    


# =============================================================================
# Marcamos todos los elementos de este subloque en este bloque, como LEAF
# =============================================================================
def markSubBlockAsLeaf(element_list, list_of_begin_end_block_pointer, E, isleaf = True):
    # obtenemos todos los elementos de este subbloque
    sub_block = getSubBlockElementsByIndex(element_list, list_of_begin_end_block_pointer, E)
    # rastreamos los elementos pertenecientes al subbloque y los ponemos a LEAF = *
    mark = ""
    if isleaf == True:
        mark = MARK_LEAF

    for i in sub_block:
        # print(f"{i} -> {mark}")
        element = element_list[i]
        element["LF"] = mark
    
    return sub_block
    


# =============================================================================
# Marcamos todas las LEAF de los element list
# =============================================================================
def markTemporalLeafs(element_list, list_of_begin_end_block_pointer, list_max_level_per_block):
    for element in element_list:
        E = element["E"]
        isLF(element_list, list_of_begin_end_block_pointer, list_max_level_per_block, E, mark_leafs = True)


# =============================================================================
# Los final LEAFS serán siempre
# si no hay nivel 4 encontrado, todos los niveles 2
# si encontramos un nivel 4, hay que poner su máximo nivel como FLF                    
# =============================================================================
def markFinalLeafs(element_list, list_of_begin_end_block_pointer, list_max_level_per_block, list_max_LCID_per_block):
    # print("HOLA ***************************************************************************")
    FLF_in_block = [] # apuntamos si un bloque tiene FLF
    for n in range(0, len(list_of_begin_end_block_pointer)): # rastreamos cada bloque
        begin = list_of_begin_end_block_pointer[n][0]
        end = list_of_begin_end_block_pointer[n][1]+1
        # fd_list_per_block = [] # marco todos los FD como INEXISTENTES, de momento 
        lists_at_same_level = False
        
        for l in range(begin, end):
            element = element_list[l]
            
            # cogemos los elementos de máxima profundidad, 
            
            # si el nivel es el máximo
            if element["L"] == list_max_level_per_block[n]:
                # hay otros elementos al mismo nivel pero con LCID menor? (este es el indicador de listas al mismo nivel)
                if element["LCID"] < list_max_LCID_per_block[n]: 
                    print(f"lists at same level")
                    lists_at_same_level = True

                    
        if lists_at_same_level:
            for l in range(begin, end):
                element = element_list[l]
                # si el nivel es el máximo
                if element["L"] == list_max_level_per_block[n]:
                    # hay otros elementos al mismo nivel pero con LCID menor? (este es el indicador de listas al mismo nivel)
                    # si el nivel de list count in dictionary es el máximo
                    if element["LCID"] == list_max_LCID_per_block[n]:
                        element["FLF"] = MARK_TERMINAL_LEAF
                        if not n in FLF_in_block: 
                            FLF_in_block.append(n) # marco que este bloque tiene FLFs

        # finalmente, si en el bloque no hay ninguna FLF, entonces todas lo son
        for n in range(0, len(list_of_begin_end_block_pointer)): # rastreamos cada bloque
            # voy a marcar todas como FLF si el bloque no ha tenido listas al mismo nivel (es decir, que tiene FLFs)    
            # consigo todas las LEAF de un bloque
            # y las marco como FLF                            
            
            if not n in FLF_in_block:
                B = n
                list_of_LF, list_of_SB = getListOfLeafPerBlock(element_list, list_of_begin_end_block_pointer, B)
                # marco todos esos elementos como MARK_TERMINAL_LEAF
                for i in list_of_LF:
                    element = element_list[i]
                    element["FLF"] = MARK_TERMINAL_LEAF


# E = 24
# isLF(element_list, list_of_begin_end_block_pointer, list_max_level_per_block, E)


# =============================================================================
# Wrapper de la funcion isleaf que sólo hay que enviarle el index del elemento    
# =============================================================================
# isLF(element_list, list_of_begin_end_block_pointer, list_max_level_per_block, E)                    
                    
def isLF(element_list, list_of_begin_end_block_pointer, list_max_level_per_block, E, mark_leafs = False):
     
    block = element_list[E]["B"]
    L = element_list[E]["L"]
    LD = element_list[E]["LD"]
    D = element_list[E]["D"]
    LCID = element_list[E]["LCID"]
    ADC = element_list[E]["ADC"]
    FADC = element_list[E]["FADC"]
    E = E

    retorno = True # por defecto está a True, si encuentro que está encadenado, entonces lo pongo a False (no LEAF)

    # cuál es el mayor level de este block? - realmente es cuál es el último índice de elemento con level_4
    max_level = element_list[list_max_level_per_block[block][0]]["L"]
    
    
    # un elemento de L 2 puede ser LEAF si en el bloque no hay ningún nivel superior
    if max_level != 2:
        if L == 2: # ojo, si no hay niveles superiores, entonces sí pueden ser todos leafs
            retorno = False # por defecto entiendo que un nivel 2 no puede ser LEAF

# XXX: añadido para CSPFacturacion    
    # hay que buscar desde el primer elemento del subbloque del elemento que nos piden (puede que haya algún elemento desperdigado por ahí, porque los JSON son muy permisivos)
    list_elements_SB = getSubBlockElementsByIndex(element_list, list_of_begin_end_block_pointer, E)
    # cojo el de menor índice para operar
    E = min(list_elements_SB)
    FD = element_list[E]["FD"]
# fin añadido   
    deep = element_list[E]["PBSB"]
    
    # rastreamos todos los elementos del bloque
    n = block
    begin = list_of_begin_end_block_pointer[n][0]
    end = list_of_begin_end_block_pointer[n][1]+1
    nivel_superior = False # significa que aún no hemos visto elementos de nivel superior
   
    # print(f"Buscamos chequear si LF para el elemento E {E} --- si cumple L {L+2} FD [{LD}] D {D+1} - FADC {ADC}")
    for l in range(E, end): # buscamos desde el elemento que nos dicen en adelante
        element = element_list[l]
        l_L = element["L"]
        # if element["L"] == L+2: # sólo elementos de nivel superior (L==L+2), pero si estamos con cualquier de nivel 2, entonces no puede ser LEAF!
        #     nivel_superior = True
            # print(f"Mirando elemento {l}  L {element['L']} [{element['FD']}] {element['D']}")
            # no es LEAF si un elemento de nivel superior está apuntando a este elemento (su FD = a nuestro LD) y los D (contadores de diccionario) son iguales
            # if element["FD"] == LD and element["D"] == D and element["LCID"] == LCID: # si el FD del nivel superior es igual al LD de este elemento y los D son iguales y los LCID son iguales
            # FIXME: 001 Diccionarios contíguos?
            
# XXX: 2020.06.05 - quitado            
        # mirar el L---
        # print(f"{element['FD']} - {LD} - {element['LCID']} {LCID} - {l_L} {L} ")
        
        if element["FD"] == LD and element["LCID"] == LCID and l_L > L: # si el FD del nivel superior es igual al LD de este elemento y los LCID son iguales
            # print(f"ELEMENT NO LF same LCID {l} - L {element['L']} [{element['FD']}] {element['D']}")
            retorno = False
            break
# fin quitado
                
# XXX: añadido 
            # if element["FD"] == LD and element["D"] == D+1 and element["LCID"] != LCID+1 and element["FADC"] == ADC: # si el dict_count es contíguo, significa que no puede ser LF
            #     print(f"ELEMENT NO LF contiguous D {l} - L {element['L']} [{element['FD']}] {element['D']}  {element['FADC']}  {element['ADC']}")
            #     retorno = False
            #     break
            # if element["FD"] == LD and element["LCID"] == LCID and element["D"] == D+1 and element["FADC"] == ADC: # si el dict_count es contíguo, significa que no puede ser LF
            # if element["FD"] == LD and element["FADC"] == ADC: # si el dict_count es contíguo, significa que no puede ser LF
            #     # print(f"ELEMENT NO LF contiguous D {l} - L {element['L']} [{element['FD']}] {element['D']}  {element['FADC']}  {element['ADC']}")
            #     retorno = False
            #     break            
            
            # 20200616 chequeamos si es LF o no en base a B_SB

        l_deep = element["PBSB"]
        # print(f"{l_deep} {deep}")
        
        # PBSB 
        # L menor
        # mirar LCID
        
        if compare_pre_lists(deep, l_deep):
            retorno = False
            break                   
            
            # 20200616
            
            # if element["FD"] == LD and element["LCID"] != LCID+1 and (element["FADC"] == ADC+1 or element["FADC"] == ADC): # si los LCID no son contiguos, no puede ser LEAF
            #     print(f"ELEMENT NO LCID {l} - L {element['L']} [{element['FD']}] {element['LCID']}  {element['FADC']}  {element['ADC']}")
            #     retorno = False
            #     break           
       
            
# fin añadido    
           
# XXX: añadido para CSPFacturacion
        # tenemos que buscar en todos los elementos porque no vale con irme a un nivel superior, puede haber elementos en el mismo nivel porque sean listas al mismo nivel    
        # if nivel_superior == True and element["L"] <= L: # si hemos visto un nivel superior y el nivel que viene es un nivel menor dejamos de buscar
        #     # XXX: añadido para CSPFacturacion
        #     # retorno = True
        #     break
# fin añadido
            
    if mark_leafs == True:    
        
        if retorno == True:
            sb = markSubBlockAsLeaf(element_list, list_of_begin_end_block_pointer, E, True)    
        else:
            sb = markSubBlockAsLeaf(element_list, list_of_begin_end_block_pointer, E, False)                 
        return retorno, sb
       
    return retorno

# =============================================================================
# Devuelve una lista con los elementos leaf finales (todos los que sean FLF)
# =============================================================================
def getFinalLeafList(element_list, list_of_begin_end_block_pointer, list_max_level_per_block, E):
    # rastreo todos los elementos leaf por subbloque/bloque
    # me quedo con el que tenga mayor profundidad 
    # miro si hay LCID diferentes (eso signficará que hay listas al mismo nivel y, por tanto, la de mayor LCID debería ser el LEAF)
    # consigo todos sus subbloques
    # y empiezo a generar registros
    return

# =============================================================================
# Devuelve todas las LF de un bloque
# =============================================================================
def getListOfLeafPerBlock(element_list, list_of_begin_end_block_pointer, B):
    list_of_LF = [] # lista de elementos LF
    list_of_SB = [] # lista de SB de esos elementos
    n = B
    begin = list_of_begin_end_block_pointer[n][0]
    end = list_of_begin_end_block_pointer[n][1]+1
    
    for l in range(begin, end):
        element = element_list[l]
        if element["LF"] == MARK_LEAF:
            list_of_LF.append(element["E"])
            SB = element["SB"]
            if not SB in list_of_SB:
                list_of_SB.append(SB)

                
    return list_of_LF, list_of_SB

# =============================================================================
# Recupero la lista de LF que tienen el mismo LD entre ellas pero con un LD menor que el elemento que envío, 
# por debajo del índice de elemento que me envían o -1 si no encuentro ninguna
# =============================================================================
def getLeafsBelowIndexSharingLCID(element_list, list_of_begin_end_block_pointer, E):
    list_of_LF = [] # lista de elementos LF
    list_of_SB = []
    
    resultado = False # resultado erróneo
    
    n = element_list[E]["B"]
    begin = list_of_begin_end_block_pointer[n][0]
    end = list_of_begin_end_block_pointer[n][1]+1
    
    
    # find first LF con LCID lower
    # por como se estructura un JSON siempre debemos buscar hacia el comienzo del bloque
    LF_to_list = -1
    for l in range(E-1, begin-1, -1):
        element = element_list[l]
        if element["LF"] == MARK_LEAF:
            if element["LCID"] < element_list[E]["LCID"]:
                LF_to_list = element["LCID"]
                break
            
    # # if nothing found            
    # if LF_to_list == -1:         
    # for l in range(E-1, begin-1, -1):
    #     element = element_list[l]
    #     if element["LF"] == MARK_LEAF:
    #         if element["LCID"] < element_list[E]["LCID"]:
    #             LF_to_list = element["LCID"]
    #             break

            
    # hemos encontrado algo?
    if LF_to_list != -1:
        
        isFLF = False
        if element_list[E]["FLF"] == MARK_TERMINAL_LEAF: # si es un elemento FLF, esperamos a estar fuera de la zona de FLFs (puede que haya varios FLFs juntos)
            isFLF = True

        newE = E    
        if isFLF == True:
            newE  = -1
            # buscamos el último elemento FLF antes de pasar a elementos no FLF (en G_SScores suele ocurrir que hay muchas FLF juntas)        
            for l in range(E, begin-1, -1):
                element = element_list[l]
                if element["FLF"] == MARK_TERMINAL_LEAF:
                    newE = l
                else:
                    break
        
        resultado = True
        # cargo todos los LF que tienen ese LCID
        for l in range(newE-1, begin-1, -1):
            element = element_list[l]
            # OJO! si en el transcurso de la búsqueda me encuentro con otro FLF, paro porque me estoy metiendo en otro registro diferente (salvo que los SB sean contíguos)
            # FIXME: Cuidado, esto lo para cuando hay 2 FLF juntas!!!!!
            
            # 20200717 - si el FLF que me encuentro está al mismo PBSB no paro, porque el elemento que tengo que encontrar está después de esa zona de nuevos FLF
            # if element["FLF"] == MARK_TERMINAL_LEAF:
            #     break   
            # ----> cambiado a: 
            numero_elementos = int(((element["L"]-2)/2)-1)
            if numero_elementos < 0:
                numero_elementos = 0
            elements_from_PBSB = element["PBSB"][:numero_elementos]
            current_elements_from_PSBS = element_list[E]["PBSB"][:numero_elementos]
            if element["FLF"] == MARK_TERMINAL_LEAF and elements_from_PBSB != current_elements_from_PSBS:
                break            
            # 20200717

            if element["LF"] == MARK_LEAF:
                if element["LCID"] == LF_to_list:
                    list_of_LF.append(element["E"])
                    # cargamos la lista de subbloques
                    if not element["SB"] in list_of_SB:
                        list_of_SB.append(element["SB"])
           
    return list_of_LF, list_of_SB, resultado


# lista_2 = [1,20,23,32,21]
# lista_1 = [1,20,23,26,29]
# lista_3 = [1,20,24,26,29]
# element["PBSB"]
# numero_elementos = int(((element["L"]-2)/2)-1)
# element["PBSB"][:numero_elementos]


# =============================================================================
# Devuelve la lista de E y SB que tienen FLF marcado a MARK_TERMINAL_LEAF por bloque
# =============================================================================
def getFLFByBlock(element_list, list_of_begin_end_block_pointer):

    list_of_list_elements = []
    list_of_list_blocks = []
    
    # rastreo cada bloque
    for n in range(0, len(list_of_begin_end_block_pointer)): # rastreamos cada bloque
        begin = list_of_begin_end_block_pointer[n][0]
        end = list_of_begin_end_block_pointer[n][1]+1
        
        # una lista de elementos y de bloques por cada bloque
        list_elements = []
        list_blocks = []        
        
        for l in range(begin, end):
            element = element_list[l]
            # busca en L+1 (si existe) en los subbloques superiores al del elemento si hay algún elemento cuyo FD coincida con este LD y si no coincide es que este código es un LEAF, si no hay L+1 es seguro ya que es un LEAF
            FLF = element["FLF"]
            E = element["E"]
            SB = element["SB"]
            # es un elemento terminal?
            if FLF == MARK_TERMINAL_LEAF:
                if not E in list_elements:
                    list_elements.append(E)
                if not SB in list_blocks:
                    list_blocks.append(SB)
            
        list_of_list_elements.append(list_elements.copy()) 
        list_of_list_blocks.append(list_blocks.copy())
    # ahora encontramos los subblocks
        
    # retornamos lista de elementos LEAF y subblocks LEAF
    return list_of_list_elements, list_of_list_blocks

     
    
    
    


# --- LEVEL ----------------------------------------------------------------------

# =============================================================================
# Devuelve los indices de todos los elementos de un bloque que estén en un nivel concreto
# =============================================================================
def getAllElementsAtLevel(element_list, list_of_begin_end_block_pointer, block, level):
    list_all_elements_at_level = []
     # creamos una lista de FD por bloque, así es muy sencillo saber si un LD está presente o no y entonces sabes si es un LEAF
    n = block
    begin = list_of_begin_end_block_pointer[n][0]
    end = list_of_begin_end_block_pointer[n][1]+1
    fd_list_per_block = [] # marco todos los FD como INEXISTENTES, de momento 
    # fd_list_per_block = [] # marco todos los FD como INEXISTENTES, de momento 
    for l in range(begin, end):
        element = element_list[l]
        # busca en L+1 (si existe) en los subbloques superiores al del elemento si hay algún elemento cuyo FD coincida con este LD y si no coincide es que este código es un LEAF, si no hay L+1 es seguro ya que es un LEAF
        
        L = element["L"]
        if level == L:
            # construimos el código para que se sepa qué tipo de elemento es un LEAF (pendiente)
            list_all_elements_at_level.append( l ) # marcamos en el índice de lista de bloques, sus FDs
   
    return list_all_elements_at_level


# a = list(range(10,-1,-1))

# =============================================================================
# Devuelve los indices de todos los elementos de un bloque que estén en un nivel concreto y con el mismo LD
# =============================================================================
def getAllElementsAtLevelAndLevelDictionary(element_list, list_of_begin_end_block_pointer, block, level, level_dictionary, level_element_to_look_for, subblock, current_E):
    list_all_elements_at_level = []
     # creamos una lista de FD por bloque, así es muy sencillo saber si un LD está presente o no y entonces sabes si es un LEAF
    n = block
    begin = list_of_begin_end_block_pointer[n][0]
    end = list_of_begin_end_block_pointer[n][1]+1
    fd_list_per_block = [] # marco todos los FD como INEXISTENTES, de momento 
    father_absolute_dictionary = element_list[current_E]["FADC"]
    father_dictionary = element_list[current_E]["FD"]
    # fd_list_per_block = [] # marco todos los FD como INEXISTENTES, de momento 
    for l in range(begin, end):
        coger = False
        
        element = element_list[l]
        # busca en L+1 (si existe) en los subbloques superiores al del elemento si hay algún elemento cuyo FD coincida con este LD y si no coincide es que este código es un LEAF, si no hay L+1 es seguro ya que es un LEAF
        
        L = element["L"]
        LD = element["LD"]
        FD = element["FD"]
        LE = element["LE"]
        SB = element["SB"]
        D = element["D"]
        E = element["E"]
        ADC = element["ADC"]
        FADC  = element["FADC"]
        
        # si buscamos level 2 entonces cogemos todos los elementos
        if level == L and level == 2:
            coger = True

        # if level == L and father_dictionary == LD: # si el elemento está en el diccionario padre, lo cogemos
        #     coger = True

        # XXX: 2020.06.04        
        if level == L and father_dictionary == LD and father_absolute_dictionary == ADC: # si el elemento está en el diccionario padre, lo cogemos
            coger = True
        
        if coger == True:
            list_all_elements_at_level.append( l ) # marcamos en el índice de lista de bloques, sus FDs
        
    
    return list_all_elements_at_level


# =============================================================================
# Obtiene todos los elementos hacia atrás en su primer nivel de enlace directo
# =============================================================================
def getDirectLink(element_list, list_of_begin_end_block_pointer, current_E):
    
    element = element_list[current_E]
    
    block = element["B"]
    subblock = element["SB"]
    level = element["L"] - 2
    LE = element["LE"]
    level_dictionary = element["FD"]
    level_element_to_look_for = LE - 1    
    subblock = element["SB"]
    
    return getAllElementsAtLevelAndLevelDictionary(element_list, list_of_begin_end_block_pointer, block, level, level_dictionary, level_element_to_look_for, subblock, current_E)

# --- SUBBLOCK ----------------------------------------------------------------------

# =============================================================================
# Comparar dos listas quitando el último elemento de la segunda lista (para la comparación de SB)
# =============================================================================
def compare_pre_lists(lista1, pre_lista2):

    # # según el nivel decido qué parte del PBSB cojo    
    # elements = [0,0,0,0,1,0,2,0,3,0,4,0,5,0,6,0,7,0,8,0,9,0,10]
    
    a = lista1
    b = pre_lista2[:-1]
    if a == b:
        return True
    else:
        return False

# =============================================================================
# Lista con los elementos de mayor profundidad de cada nivel, devuelve lista de elementos por bloque y lista de subbloques por bloque
# estos elementos son los verdaderos LEAF
# =============================================================================
def getListElementsMaxLevelPerBlock(element_list, list_of_begin_end_block_pointer, list_max_level_per_block):
    # rastreo todos los bloques
    list_of_lists_of_deeper_elements = []
    list_of_lists_of_deeper_subblocks = []
    for block in range(0,len(list_of_begin_end_block_pointer)):
        n = block
        begin = list_of_begin_end_block_pointer[n][0]
        end = list_of_begin_end_block_pointer[n][1]+1
        list_of_deeper_elements = []
        list_of_deeper_subblocks = []        
        # rastreo los elementos de cada bloque
        for i in range(begin, end):

            # miro el elemento
            element = element_list[i]
            # si el elemento es uno de los que tiene la mayor profundidad
            if element["L"] == list_max_level_per_block[n]:
                # añado el índice del elemento
                list_of_deeper_elements.append(i)
                # SB = element["SB"]   
                # print(f"AÑADO {i} con subbloque {SB}")
                
                # # añado su subbloque
                # if not SB in list_of_deeper_subblocks:
                #     print(f"AÑADO subbloque {SB}")
                #     list_of_deeper_subblocks.append(SB)
                
        list_of_lists_of_deeper_elements.append(list_of_deeper_elements.copy())    
        # list_of_lists_of_deeper_subblocks.append(list_of_deeper_subblocks.copy())
         
        # ahora tengo que mirar, dentro del bloque, si tiene el mayor LIDC y me quedo con esos
        
    return list_of_lists_of_deeper_elements


# =============================================================================
# Mira si en este bloque hay un nivel por encima del primer nivel (2), en ese caso todos los elementos de nivel 2 no pueden ser LEAF
# está buscando el level 4 que tenga mayor profundidad    
# devuelvo una lista adicional con los máximos LCID (list count in dictionary) para luego saber si son o no LEAFs finales (recordemos que estamos tratando con posibilidad de listas al mismo nivel)    
# =============================================================================
def max_level_per_block(element_list, list_of_begin_end_block_pointer):
    list_leaf_level_per_block = []
    list_last_LCID_per_block = []
    for block in range(0,len(list_of_begin_end_block_pointer)):
        n = block
        begin = list_of_begin_end_block_pointer[n][0]
        end = list_of_begin_end_block_pointer[n][1]+1
        max_level = -1
        max_LCID = -1
        
        # for i in range(begin, end):
        #     element = element_list[i]
            # if element["L"] > max_level:
            #     max_level = element["L"]
                
        # ahora rastreo todos los elementos y me quedo con todos los de mayor profundidad en el último level 4    
        last_level_4 = -1
        for i in range(begin, end):
            element = element_list[i]
            if element["L"] == 4:
                last_level_4 = element["E"]
   
        # si no ha habido ningún level 4 estamos en un JSON sólo con diccionarios, con lo que me quedo con el primer elemento, que será level 2
        if last_level_4 == -1:
            last_level_4 = begin
    
        # me quedo con su LCID
        LCID = element_list[last_level_4]["LCID"]       
    
        # # ahora tenemos que buscar, dentro de el rango de elementos, el de mayor profundidad
        # for i in range(begin, end):        
        #     if element_list[i]["L"] == 4 and element_list[i]["LCID"] == LCID:
        #         if element["L"] > max_level:
        #             max_level = element["L"]


        # mando listas porque originalmente eran listas de elementos
        list_leaf_level_per_block.append([last_level_4])
        list_last_LCID_per_block.append([LCID])
        
    return list_leaf_level_per_block, list_last_LCID_per_block

# =============================================================================
# Devuelve los índices de los elementos con valor máximo de la lista que recibe como parámetro
# =============================================================================
def get_indexes_maxvalue(l):
    return [i for i, x in enumerate(l) if x == max(l)]

# =============================================================================
# Devuelvo un puntero a todos los valores de una lista con un valor predeterminado
# =============================================================================
def get_indexes_pervalue(l, value):
    return [i for i, x in enumerate(l) if x == value]
    
# =============================================================================
# Dado una lista de level_4 por bloque, busco los elementos con su máximo nivel de profundidad y los devuelvo
# =============================================================================
def getMaxDeepLevelOfLevel4(element_list, list_of_begin_end_block_pointer, list_possible_elements):
    
    # FIXME: 002 Creo que no está sabiendo coger el máximo nivel de profundidad--- coge el elemento 23 cuando debería ser el elemento 53. creo que para la búsqueda antes de tiempo (sólo cuando vuelva a encontrarse con un nivel 4 o menor debería parar, y quedarse con el ultimo elemento encontrado)
    
    list_leaf_level_per_block = []
    list_last_LCID_per_block = []
    
    max_deep_per_block = []
    list_elements_max_deep = []    
    list_LCID_max_deep_per_block = []
    
    # rastreo los elementos
    for block in range(0,len(list_of_begin_end_block_pointer)):
        n = block
        begin = list_of_begin_end_block_pointer[n][0]
        end = list_of_begin_end_block_pointer[n][1]+1
        
        max_level = -1
        last_level_seen = -1
        last_LCID_seen = -1
        
        end_busqueda = -1
        
        last_level_seen = -1      
        last_level_seen_element = -1
        # PASO 1: busco desde los los elementos que me han llegado (son punteros a niveles 4)
        for x in list_possible_elements[block]:
            last_level_seen = -1
            # print(f"---- ANALIZANDO ELEMENTO {x} ----")            
            # examinamos desde el elemento al que estamos apuntando hasta el final del bloque
            for i in range(x, end):

                element = element_list[i]
                # print(f"ANALIZANDO ELEMENTO {i} - L {element['L']} last_level_seen {last_level_seen}")
                if element["L"] < last_level_seen: # he bajado a un nivel inferior?

                    if element["L"] == 4 or element["L"] == 2: # hemos bajado a otro nivel 4 que no es el nivel final?
                        # print(f"HEMOS PASADO A UN NIVEL INFERIOR: element {i} current level {element['L']} last_level_seen {last_level_seen}")
                        end_busqueda = i-1 # me quedo con la referencia al elemento anterior
                          
                        break
                else:
                    last_level_seen = element["L"]  
                    last_level_seen_element = i
                    last_LCID_seen = element["LCID"]                  

                    # if last_LCID_seen != element["LCID"]: # (ojo, si tienen el mismo LCID todavía no me quedo con él) - esto no lo entiendo
                    #     continue
                    
                    # print(f"last_level_seen {last_level_seen} del elemento {last_level_seen_element}")
                
                    # PASO 2: busco desde los los elementos que me han llegado
                    # cojo todos loe elementos del subloque perteneciente al elemento last_level_seen_element y esos son los que pertenecen al máximo nivel 
        lista_de_elementos = getSubBlockElementsByIndex(element_list, list_of_begin_end_block_pointer, last_level_seen_element)
            
    
        max_deep_per_block.append(last_level_seen) # añado el mayor nivel por bloque
        list_leaf_level_per_block.append(lista_de_elementos)
        list_LCID_max_deep_per_block.append(last_LCID_seen)
        
    return max_deep_per_block, list_leaf_level_per_block, list_LCID_max_deep_per_block     
        
               
            
# =============================================================================
# Devolvemos todos los elementos que conciden con B, L y LCID            
# =============================================================================
def getAllElementsBy_B_L_LCID(element_list, list_of_begin_end_block_pointer, B, L, LCID):            
    begin = list_of_begin_end_block_pointer[B][0]
    end = list_of_begin_end_block_pointer[B][1]+1
    list_of_FLF_subblocks = []
    for i in range(begin, end):
        element = element_list[i]    
        if element["L"] == L and element["LCID"] == LCID:
            # cojo su subblock
            SB = element["SB"]
            if SB not in list_of_FLF_subblocks:
                list_of_FLF_subblocks.append(SB)
    return list_of_FLF_subblocks
# list_of_FLF_subblocks = getAllElementsBy_B_L_LCID(element_list, list_of_begin_end_block_pointer, 0, 6, 3)
    

# =============================================================================
# Obtenemos los elementos que estando a nivel 4 deben contener las FLF
# =============================================================================
def getElementsLevel4FinalLeaves(element_list, list_of_begin_end_block_pointer, list_leaf_level_per_block, list_last_LCID_per_block):
    list_possible_elements = []
    for block in range(0,len(list_of_begin_end_block_pointer)):
        list_elements = []
        n = block
        begin = list_of_begin_end_block_pointer[n][0]
        end = list_of_begin_end_block_pointer[n][1]+1

# 20200625 - si no hay niveles superiores a 2, cojo todos los elementos        
        buscar_niveles_4 = False
        for i in range(begin, end):
            element = element_list[i]
            L = element["L"]
            if L > 2:
                buscar_niveles_4 = True
                break

# 20200625

        if buscar_niveles_4 == True:
            # consigo una lista con los elementos que tienen posibilidad de tener FLF
            for i in range(begin, end):
                element = element_list[i]
                coger = True
                if element["L"] == 4 and element["LCID"] == list_last_LCID_per_block[n][0]:
# 20200615 - configuration_machine no detecta correctamente las LF y cree que todo son FLF                
                    # antes de añadirla tengo que averiguar si realmente puede ser una FLF (es decir, que no es un LINK DIRECT de algún elemento)
                    l_B_SB = element["PBSB"]
                    # print(f"posible element {i} l_B_SB {l_B_SB}")
                    # si encuentro que esta lista está contenida en otra, es que es LINK DIRECT, con lo que no podré añadirlo
                    for x in range(begin, end):
                        o_element = element_list[x]
                        o_l_B_SB = o_element["PBSB"]
                        # print(f"\tcomparo con {i} o_l_B_SB {o_l_B_SB} resultado {compare_pre_lists(l_B_SB, o_l_B_SB)}")
    
                        if compare_pre_lists(l_B_SB, o_l_B_SB):
                            # print(f"\t\t coger=False")
                            coger = False
                            break
                    if coger == True:
                        # print(f"\t\t incluimos element {i}")
# 20200615 - fin                    
                        list_elements.append(i)

        # en este bloque sólo tenemos niveles 2
        if buscar_niveles_4 == False:
            for i in range(begin, end):            
                list_elements.append(i)

        list_possible_elements.append(list_elements)
    return list_possible_elements
   
# =============================================================================
# Marco los bloques de registros dentro del JSON
# Como un JSON es totalmente sequencial, detecto un nuevo bloque cuando el nivel cambia nuevamente a 2 (es el nivel más bajo)
# devuelve puntero al inicio y final de cada bloque        
# =============================================================================
def markBlocks(element_list):
    list_of_begin_block_pointer = []
    list_of_begin_end_block_pointer = [] # construimos esta lista con la información de la anterior
    pair = 2*[0]
    block = -1
    prior_element = 0
    prior_element_fd = 0
    
    begin__block_pointer = -1
    # prior_block = -1
   
    for n in range(0, len(element_list)):
        element = element_list[n]
        
        # if element["L"] == 2 and element["FD"] == 0:
        #     if element["L"] != prior_element:
        #         # prior_block = block                  
        #         block += 1
        #         begin__block_pointer = n
        #         list_of_begin_block_pointer.append(n)
        #     if element["L"] == 2 and prior_element_fd == 1: # si nos encontramos, dentro del mismo bloque, diccionarios después de que haya habido listas, el FD estará a 1 y habrá que cambiar de bloque
        #         block += 1
        #         # if block != prior_block - 1: # estamos en el caso de que sólo había diccionario y ahora elementos en una lista, con lo que no volvemos a incrementar el block, los bloques tienen que ser correlativos
        #         #     block -= 1
        #         being__block_pointer = n 
        #         list_of_begin_block_pointer.append(n)

        if element["L"] == 2 and element["LE"] == 0: # nuevo block
                block += 1
                begin__block_pointer = n
                list_of_begin_block_pointer.append(n)
                
        element["B"] = block
        prior_element = element["L"]
        prior_element_fd = element["FD"] # guardo el elemento FD, si aparecen más diccionarios tras las listas, el FD estará puesto a 1
 
    for n in range(0, len(list_of_begin_block_pointer)):
        pair[0] = list_of_begin_block_pointer[n]
        try:
            pair[1] = list_of_begin_block_pointer[n+1]-1 # si existe el elemento n+1 me quedo con el anterior
        except:
            pair[1] = len(element_list)-1 # si no hay elemento siguiente, me quedo con el último elemento de la lista
        list_of_begin_end_block_pointer.append(pair.copy())

    return list_of_begin_end_block_pointer

# =============================================================================
# Le pone el subbloque correspondiente al elemento que se le envía
# =============================================================================
def setSubBlock(element, SB): # (pendiente)
    element["SB"] = SB

# =============================================================================
# Devuelve todos los indices de elemento que tienen el mismo subloque asignado en el bloque al que pertenece el elemento
# añado que si los DC no son iguales, no pertenecen al mismo subbloque, ya que pertenecen a listas en el mismo nivel    
# =============================================================================
def getSubBlockElementsByElement(element_list, list_of_begin_end_block_pointer, block, SB, L, FD, LD, D, E, act = True):
    
    global list_block_subblocks_elements
    # key = f"{block}.{SB}"
    # # recupero todos los elementos que tengan esta entrada, si existen
    # if key in list_block_subblocks_elements:
    #     print("ENCONTRADO!!!!")
    #     return list_block_subblocks_elements[key]
    
    temp_sub_block = []
    element = element_list[E]
    # si hemos asignado el subbloque antes, devolvemos el subbloque y seguimos
    if element["SB"] != "": 
        subblock = element["SB"]
        subblock = getSubBlockElementsBySubblock(element_list, list_of_begin_end_block_pointer, block, subblock)        
        return subblock

    begin = list_of_begin_end_block_pointer[block][0]
    end = list_of_begin_end_block_pointer[block][1]+1
    for l in range(begin, end): # rastreamos todos los elementos del bloque
        element = element_list[l]
        if element["SB"] != "": # si el elemento tiene asignado subbloque, deberíamos seguir
            continue
        # if L == element["L"] and LD == element["LD"] and FD == element["FD"] and D ==  element["D"]: 
        # si llega nivel 2 entonces tomamos todos los elementos
        if L == 2 and element["L"]==2:
            temp_sub_block.append(l)
        else:    
            if L == element["L"] and LD == element["LD"] and FD == element["FD"]: 
                temp_sub_block.append(l)
            
    # lista para permitir la búsqueda correcta de elementos y enlazarlos cuando son LEAF       
    # find_sub_block = []        
    # element = element_list[E]
    # current_L = element["L"]
    # for l in range(E, end): # rastreamos todos los elementos del bloque, desde el elemento que me envían (no miro hacia atrás)
    #     element = element_list[l] # como hace una búsqueda secuencial, me quedo con el nivel que tengo y sólo busco elementos que tengan un nivel superior, en el momento en que me llegue un nivel inferior, paro
    #     if L == element["L"] and LD == element["LD"] and FD == element["FD"] and D ==  element["D"]: 
    #         temp_sub_block.append(l)            
           
            
            
    sub_block = temp_sub_block    
    # en el nivel 2 cojo todos los elementos sin distinción
    # return sub_block            

    print(f"ELEMENTS IN SUBBLOCK {sub_block}")


    if L != 2:        
        if act == True: # esta variable sólo la uso para comparar el resultado del nuevo método de asignación de subbloques que tiene en cuenta que los elementos han de ser correlativos           
            # los elementos de un subbloque han de ser correlativos, desde el elemento E en adelante, todos los que no lo sean saldrán del subbloque
            # para ello cojo, desde el elemento E en adelante
            
            # debo ir desde E hacia atrás, hasta que dejen de ser correlativos y quedarme con ese primer elemento, que será E
            a = sub_block
            E = _firstElementInSubBlock(a, E)            
            
            coger = False
            a = []
            for i in temp_sub_block:
                if i == E: # he detectado E, con lo que empiezo a coger a partir de ahí
                    coger = True
                if coger:    
                    a.append(i)    
            # a = temp_sub_block
            # genero una lista correlativa desde el primer elemento de la lista anterior        
            b = list(range(a[0], a[0]+len(a))) 
            result = []
            # voy agregando mientras los elementos sean correlativos
            for n in range(0,len(a)):
                if a[n] == b [n]:
                    result.append(a[n])
            # copio la lista para devolverla    
            sub_block = result
            # devuelvo dos listas, una para poder devolver todos los elementos cuando se tenga que construir un registro
            # la otra para permitir que la búsqueda hacia atrás de LEAFS sea la correcta y no haga búsqueda transversal de elementos
            # return sub_block, find_sub_block
        
    return sub_block
           


# =============================================================================
# Calcula el primer elemento del subbloque, calculado desde cualquier index contenido en el subbloque
# =============================================================================
def _firstElementInSubBlock(a, E):

    p_E = a.index(E)

    minimum = E
    if p_E != 0: # estamos en el primer elemento
        # vamos hacia atrás correlativamente y nos quedamos con el primer elemento
        b = list(range(a[p_E], -1, -1)) # genero una lista decreciente para poder comparar
        count = 0
        for n in range(p_E, -1, -1):

            if a[n] != b[count]:
               break 
            minimum = a[n]           
            count += 1    
    return minimum    
# coger desde un elemento en adelante
    
# a = [18,19,22,23]
# b = list(range(a[1], a[1]+len(a)))
# result = []
# for n in range(0,len(a)):
#     if a[n] == b [n]:
#         result.append(a[n])
# print(result)


# =============================================================================
# Devuelve todos los elementos de un subblock, enviándole un subblock y block
# añado que si los DC no son iguales, no pertenecen al mismo subbloque, ya que pertenecen a listas en el mismo nivel       
# =============================================================================
def getSubBlockElementsBySubblock(element_list, list_of_begin_end_block_pointer, block, subblock):
    sub_block = []
    begin = list_of_begin_end_block_pointer[block][0]
    end = list_of_begin_end_block_pointer[block][1]+1
    for l in range(begin, end): # rastreamos todos los elementos del bloque
        element = element_list[l]
        if element["SB"] == subblock:
            # L = element["L"]
            # FD = element["FD"]
            # LD = element["LD"]
            # D = element["D"]
            # E = element["E"]
            
            sub_block.append(l)
            
            # sub_block = getSubBlockElementsByElement(element_list, list_of_begin_end_block_pointer, block, L, FD, LD, D, E)
            # break
    return sub_block


# =============================================================================
# Devuelve todos los elementos de un subblock, enviándole un index
# =============================================================================
def getSubBlockElementsByIndex(element_list, list_of_begin_end_block_pointer, E):
    sub_block = []
    element = element_list[E]
    # cogemos el subblock del indice que nos indican
    B = element["B"]
    SB = element["SB"]
    sub_block = getSubBlockElementsBySubblock(element_list, list_of_begin_end_block_pointer, block = B, subblock = SB) 
    return sub_block

 
  
# =============================================================================
# Compara los elementos de dos listas y devuelve si contienen los mismos elementos, aunque estén desordenados
# =============================================================================
def _unorderedListsAreEqual(a,b):
    result = True
    # primero compruebo que tienen el mismo número de elementos
    if len(a) == len(b):
        # compruebo los elementos repetidos
        for i in a:
            if not a.count(i) == b.count(i):
                result = False
                break
        # compraro los elementos no repetidos
        for i in a:
            if i in b:
                continue
            else:
                result = False
                break
    else:
        result = False
    return result   

        
def testUnorderedlists():
    # los dos OK
    a = [22,23,24,25]    
    b = [22,23,24,25]    
   
    print(a == b)
    print(_unorderedListsAreEqual(a,b))    
    
    # función OK
    a = [22,23,24,25]    
    b = [22,23,25,24]    
   
    print(a == b)
    print(_unorderedListsAreEqual(a,b))

    # ambos False
    a = [22,22,24,25]    
    b = [22,23,25,24]    
   
    print(a == b)
    print(_unorderedListsAreEqual(a,b))

    # ambos False
    a = [22,22,23,24,25]    
    b = [22,23,25,24]    
   
    print(a == b)
    print(_unorderedListsAreEqual(a,b))

    # función True
    a = [21,22,22,23,24,25]    
    b = [22,23,25,24,22,21]    
   
    print(a == b)
    print(_unorderedListsAreEqual(a,b))

    # ambos False
    a = [21,22,22,23,24]    
    b = [22,23,24,22,21,22]    
   
    print(a == b)
    print(_unorderedListsAreEqual(a,b))

    # ambos False
    a = [20]    
    b = [21]    
   
    print(a == b)
    print(_unorderedListsAreEqual(a,b))

# =============================================================================
# Devuelve todos los elementos de un bloque con un nivel concreto (el del elemento que llega)
# =============================================================================
def getElementsInBlockByLevel(element_list, list_of_begin_end_block_pointer, E):
    
    list_of_elements = []
    
    block = element_list[E]["B"]
    level = element_list[E]["L"]
    
    begin = list_of_begin_end_block_pointer[block][0]
    end = list_of_begin_end_block_pointer[block][1]+1
    for l in range(begin, end): # rastreamos todos los elementos del bloque
        element = element_list[l]
        if element["L"] == level:
            list_of_elements.append(l)
    return list_of_elements
        


# --- REG ----------------------------------------------------------------------

# =============================================================================
# Creo un registro desde el elemento LEAF hacia atrás. Devuelvo una list de índices
# un registro sólo contiene los indices de los elementos que lo componen
# esta es la función recursiva de creación de registros    
# =============================================================================
registro = []



STRATEGY_OLD = 1
STRATEGY_NEW = 2

strategy = STRATEGY_OLD

# =============================================================================
# Devuelve los elementos de enlace inmediato tradicional (FD = LD) - añado ADC y FADC para que no coja elementos erróneos de diccionarios adyacentes
# =============================================================================
def getElementsEnlaceInmediato(element_list, list_of_begin_end_block_pointer, E):
    global strategy
    if strategy == STRATEGY_NEW:
        return getElementsLINKDIRECT(element_list, list_of_begin_end_block_pointer, E)
    
    # lo que devuelve la función
    l_registro = []
    subblocks_list = []
    
    # obtengo información relevante del elemento
    element = element_list[E]
    block = element["B"]
    level = element["L"]
    level_to_look_for = level - 2
    subblock = element["SB"]
    level_element_to_look_for = element["LE"] # *
    # ADC = element["ADC"]
    # FADC = element["FADC"]
    
    if level_to_look_for != 0:
        father_dictionary = element["FD"]
        level_dictionary_to_look_for = father_dictionary
        subblock = element["SB"]
        current_E = element["E"]
        level_element_to_look_for = level_element_to_look_for - 1

        # miro hacia atrás y consigo el primer element que debe estar enlazado (eso me vale porque luego opero con todos los elementos de su subbloque)
        # si el level == 2 cojo todos los elementos del nivel 2, pero si no, sólo los que estén por debajo del subbloque actual (así no hago una búsqueda transversal y me traigo más elementos de otros subbloques superiores)
        list_elements_prior_level = getDirectLink(element_list, list_of_begin_end_block_pointer, E)
        
        for i in list_elements_prior_level:
            if not i in l_registro:
                l_registro.append(i) 
                
        # consigo la lista de subbloques de los elementos devueltos 
        
        for i in list_elements_prior_level:
            SB = element_list[i]["SB"]
            if SB in subblocks_list:
                continue
            else:
                subblocks_list.append(SB)
  
    
    return l_registro, subblocks_list

# =============================================================================
# Nueva aproximación: usa B_SB_prefix
# aquí nunca enlazará con elementos de nivel 2 por cómo se construye el B_SB así que se enlazan directamente en createReg antes de llamar a esta función    
# =============================================================================
def getElementsLINKDIRECT(element_list, list_of_begin_end_block_pointer, E):
    element = element_list[E]
    B = element["B"]
    SB = element["SB"]
    B_SB = element["PBSB"]
    
    # consigo los subbloques que tengan la misma sublista en este subbloque (todos los elementos menos el último) porque estarán enlazados
    # rastreo dentro del bloque
    l_registro = []
    l_list_subblocks = []
    
    begin = list_of_begin_end_block_pointer[B][0]
    end = list_of_begin_end_block_pointer[B][1]+1
    for l in range(begin, end): # rastreamos todos los elementos del bloque
        l_element = element_list[l]    
        l_B_SB = l_element["PBSB"]
        l_SB = l_element["SB"]
        # no debo comparar el SB en el que me encuentro        
        if l_SB == SB:
            # print(f"no se compara {l}")
            continue
        # print(f"comparamos {l} con l_B_SB {l_B_SB} con {E} con B_SB {B_SB}")
        if compare_pre_lists(l_B_SB, B_SB):
            # print(f"son iguales {l} y {E}")
            # añado el elemento
            if not l in l_registro:
                l_registro.append(l)
            # añado el SB    
            SB = l_element["SB"]
            if not SB in l_list_subblocks:
                l_list_subblocks.append(SB)
                
    return l_registro, l_list_subblocks


def _test_getListElementsDirectLink():
    getElementsEnlaceInmediato(element_list, list_of_begin_end_block_pointer, 24)

# =============================================================================
# Obtiene los elementos que podrían venir de una lista al mismo nivel
# =============================================================================
def getElementsEnlaceLCID(element_list, list_of_begin_end_block_pointer, E):
    # consigo información relevante del elemento
    o_element = element_list[E]
    block = o_element["B"]
    
    # acumuladores temporales
    l_list_elements_same = []
    l_ilst_subblocks_same = []
    
    l_list_elements_level_1 = []
    l_ilst_subblocks_level_1 = []    
    
    # rastreo dentro del bloque
    begin = list_of_begin_end_block_pointer[block][0]
    end = list_of_begin_end_block_pointer[block][1]+1
    for l in range(begin, end): # rastreamos todos los elementos del bloque
        element = element_list[l]
        
        # LOOK FOR SAME LEVEL 
        if element["LCID"] == o_element["LCID"]-1 and element["L"] ==  o_element["L"]:
            # lo añado a la lista evitando duplicados
            e = element["E"]
            if not e in l_list_elements_same:
                l_list_elements_same.append(e)
            sb = element["SB"]
            if not sb in l_ilst_subblocks_same:
                l_ilst_subblocks_same.append(sb)
                
        # LOOK FOR LEVEL - 1
        # es un elemento que me interesa?
        if o_element["L"]-2 != 2:      
            if element["LCID"] == o_element["LCID"]-1 and element["L"] ==  o_element["L"]-2:
                # lo añado a la lista evitando duplicados
                e = element["E"]
                if not e in l_list_elements_level_1:
                    l_list_elements_level_1.append(e)
                sb = element["SB"]
                if not sb in l_ilst_subblocks_level_1:
                    l_ilst_subblocks_level_1.append(sb)
                
    return l_list_elements_same, l_ilst_subblocks_same, l_list_elements_level_1, l_ilst_subblocks_level_1

def _test_getListElementsPerLCID():
    getElementsEnlaceLCID(element_list, list_of_begin_end_block_pointer, 21)

# =============================================================================
# Crea un registro tirando desde un nodo terminal (FLF) y replicando todo lo que encuentra hacia atrás
#
# 1) FLF enlaza con su primer SB de enlace, si lo hay 
# 2) consigue la lista de subbloques que están a su mismo L peron con LCIF -1 y los envía a la función recursiva para que hagan lo mismo
# 3) si llega un momento en que no hay más como en 1), entonces enlaza hacia detrás como siempre    
# =============================================================================
LIST_REGISTROS = []    

# XXX: 202006.001 - obligo a que la función de createreg sólo haga las busquedas correspondientes a LINKDIRECT o a LF MISMO NIVEL
SEARCH_METHOD_TOTAL = 1
SEARCH_METHOD_LINKDIRECT = 2
SEARCH_METHOD_LF = 3

gContadorTraza = -1

VERBOSE_TOTAL = 1
VERBOSE_REDUCED = 2

g_verbose = -1

advance_counter = -1

def createReg(element_list, 
              list_of_begin_end_block_pointer, 
              index, 
              deep_level = None, 
              l_registro = [], # contiene los punteros a los elementos
# XXX: 202006.001 - incluyo una referencia a los registros que han sido procesados             
              l_processed_SB = [], # SB que han sido procesados, si no está aquí, hay que procesarlo
# 202006.001              
              formateo = "", 
              traza = False, 
              calledAsRecursive = False, # entramos en la función en modo recursivo o no (impacta en el campo deep_level)
              filename_traza = "kk.txt",
              search_method = None,
              contador = -1
              ):
    
    global advance_counter
    global LIST_REGISTROS # esta lista tiene que vaciarse ANTES de llamar a createReg

    global gContadorTraza
    global g_verbose
    # print_f(f"\n**********", filename_traza)            
    # print_f(f"CREATEREG para index {index}", filename_traza)

    advance_counter += 1
    if g_verbose == VERBOSE_TOTAL or g_verbose == VERBOSE_REDUCED:
        if not advance_counter % 100:
            print(f"createReg {advance_counter}")        



    # obtengo información relevante del elemento
    # global registro
    element = index
    element = element_list[element]
    B = element["B"]
    SB = element["SB"]
    # SB_T = element["SB_T"]
    # FD = element["FD"]
    L = element["L"]
    # LD = element["LD"]
    # D = element["D"]
    # LE = element["D"]
    E = element["E"]
    value = element["value"]
 
    
    processed_SB = l_processed_SB.copy()
    gContadorTraza += 1
    contador += 1
    
    FORMATEO = formateo + f"{contador}.\t"     
    if traza:
        print_f(f"\n{FORMATEO}**********", filename_traza)            
        print_f(f"{FORMATEO}CREATEREG para index {index} SB [{SB}] processed_SB {processed_SB}", filename_traza)    
    
   
    registro = l_registro.copy()
    
    # # evitamos procesar elementos que ya han sido procesados
    # if E in registro:
    #     if traza:
    #         print_f(f"{FORMATEO}El INDICE {E} YA HA SIDO PROCESADO", filename_traza)
    #     return LIST_REGISTROS.copy()
    
    # 1) FLF enlaza con su primer SB de enlace, si lo hay 
    # 2) consigue la lista de subbloques que están a su mismo L peron con LCIF -1 y los envía a la función recursiva para que hagan lo mismo
    # 3) si llega un momento en que no hay más como en 1), entonces enlaza hacia detrás como siempre

# XXX: 202006.001 CSP Facturación devuelve columnas vacías

    l_subblock = []
    for l in registro:
        l_SB = element_list[l]["SB"]
        if not l_SB in l_subblock:
            l_subblock.append(l_SB)  
            
# 202006.001 CSP Facturación devuelve columnas vacías

    if traza:
        if g_verbose == VERBOSE_TOTAL:
            print_f(f"{FORMATEO}REGISTRO RECIBIDO PARA PROCESAR {registro} SB [{l_subblock}]", filename_traza)
        else:
            print_f(f"{FORMATEO}REGISTRO RECIBIDO PARA PROCESAR SB [{l_subblock}]", filename_traza)
            


# XXX: 202006.001 CSP Facturación devuelve columnas vacías
    
    # if SB in l_subblock:
    #     return registro        

    if SB in processed_SB:
        return registro        
    else:
        processed_SB.append(SB) # notifico que este SB lo procesamos
 
  
    
# 202006.001 CSP Facturación devuelve columnas vacías


# ***********************************************************
# SB: 
# añado los elementos pertenecientes al subbloque a la lista para componer el registro
# ***********************************************************
    list_SB_FLF = getSubBlockElementsByIndex(element_list, list_of_begin_end_block_pointer, E = E)
    # print(f"ELEMENTS OF SAME SB OF {index} ELEMENTS  {list_SB_FLF} ")  
    if traza:
        print_f(f"{FORMATEO}ELEMENTS: IN SUBBLOCK {list_SB_FLF}", filename_traza)
    for i in list_SB_FLF:
        if not i in registro:
            registro.append(i)    

    if L == 2:
        LIST_REGISTROS.append(registro.copy())
        return LIST_REGISTROS.copy()        

# ***********************************************************
# LINK DIRECT: 
# consigo los elementos de enlace inmediato de este elemento
# seguimos teniendo un registro único, y vamos incrementando la info de nuestro registro que nos llega            
# *********************************************************** 
            
    if search_method == SEARCH_METHOD_TOTAL or search_method == SEARCH_METHOD_LINKDIRECT:   

        seguir_buscando = True
        
        if element_list[E] == 4:
            # añado todos los elementos del nivel 2 y sigo
            list_level_2 = (element_list, list_of_begin_end_block_pointer, element_list[E]["B"], 2)
            # los añado al registro a devolver
            for i in list_level_2:
               if not i in registro:
                   registro.append(i)           
            seguir_buscando = False # que no siga buscando
            
        if seguir_buscando == True: 
            list_elements_LinkDirect, list_subblocks_LinkDirect = getElementsEnlaceInmediato(element_list, list_of_begin_end_block_pointer, E = E)
            # print(f"LINK DIRECT ELEMENTS  {list_elements_LinkDirect} SB  {list_subblocks_LinkDirect} ")  
            if traza:
                if g_verbose == VERBOSE_TOTAL:
                    print_f(f"{FORMATEO}ELEMENTS: LINK DIRECT {list_elements_LinkDirect} SB {list_subblocks_LinkDirect}", filename_traza)
                else:
                    print_f(f"{FORMATEO}ELEMENTS: LINK DIRECT SB {list_subblocks_LinkDirect}", filename_traza)
                    
            # los añado al registro, evitando duplicados
            if len(list_elements_LinkDirect)>0:
                index = min(list_elements_LinkDirect) # al poder estar desordenadas, cojo el elemento con menor valor (el primero!)
                
                for i in list_elements_LinkDirect:
                    if not i in registro:
                        registro.append(i)
        
        # XXX: 202006.001 añadido para arreglar columnas vacías en CSPFacturacion    
            copia_local_registro = registro.copy()
            if len(list_subblocks_LinkDirect)>0:
         
                for i in list_subblocks_LinkDirect:
        # XXX: 202006.001 si el elemento que voy a enviar tiene el mismo subbloque al que estoy estudiando, no lo envío, porque genera un bucle infinito            
                    if not i == SB:            
        # 202006.001 si el elemento que voy a enviar tiene el mismo subbloque al que estoy estudiando, no lo envío, porque genera un bucle infinito            
                        # consigo sus elementos y opero sólo con el primero (el resto comparten todos la misma información)
                        l1_list_elements = getSubBlockElementsBySubblock(element_list, list_of_begin_end_block_pointer, block = B, subblock = i)
                        index = min(l1_list_elements) # al poder estar desordenadas, cojo el elemento con menor valor (el primero!)
                        if traza:
                            l_subblock = []
                            for l in registro:
                                l_SB = element_list[l]["SB"]
                                if not l_SB in l_subblock:
                                    l_subblock.append(l_SB)                    
                            if g_verbose == VERBOSE_TOTAL:                                
                                print_f(f"{FORMATEO}ELEMENTS: LINKDIRECT REGISTRO TIENE AHORA {registro} SB [{l_subblock}]", filename_traza)
                            else:
                                print_f(f"{FORMATEO}ELEMENTS: LINKDIRECT REGISTRO TIENE AHORA SB [{l_subblock}]", filename_traza)
                        
                        if not element_list[index]["L"] == 2: # si es un L == 2 no seguimos procesando
                            if traza:
                                if g_verbose == VERBOSE_TOTAL:                                
                                    print_f(f"{FORMATEO}ELEMENTS: LINKDIRECT (Before) MANDAMOS EL INDEX {index}", filename_traza)                   
                                else:
                                    print_f(f"{FORMATEO}ELEMENTS: LINKDIRECT (Before) MANDAMOS EL INDEX {index}", filename_traza)                   
                                
                            l_registro = registro.copy()                  
                            # index = l_list_elements[0]
                            registro = createReg(element_list, list_of_begin_end_block_pointer, index, l_registro = registro, l_processed_SB = processed_SB, formateo = FORMATEO, traza = traza, calledAsRecursive = True, filename_traza = filename_traza, search_method = SEARCH_METHOD_LINKDIRECT, contador = contador) 
                            
                            # incrementamos lo que nos llega a nuestro registro original
                            for indice in registro:
                                if not indice in l_registro:
                                    l_registro.append(indice)
                            registro = l_registro.copy()
                            
                            # registro = copia_local_registro.copy()           
        
                            if traza:
                                l_subblock = []
                                for l in registro:
                                    l_SB = element_list[l]["SB"]
                                    if not l_SB in l_subblock:
                                        l_subblock.append(l_SB)                    
                                if g_verbose == VERBOSE_TOTAL:                                       
                                    print_f(f"{FORMATEO}ELEMENTS: LINKDIRECT (After) REGISTRO TIENE AHORA {registro} SB [{l_subblock}]", filename_traza)
                                else:
                                    print_f(f"{FORMATEO}ELEMENTS: LINKDIRECT (After) REGISTRO TIENE AHORA SB [{l_subblock}]", filename_traza)
                                
                            
            if search_method == SEARCH_METHOD_LINKDIRECT: # si sólo queremos aumentar el registro, devolvemos inmediatamente la información
                return registro
                    
            if search_method == SEARCH_METHOD_TOTAL: # si estoy procesando el registro original (no he entrado buscando más LINKDIRECT) - dejo que ahora se procesen las hojas de LF
                search_method = SEARCH_METHOD_LF
    # 202006.001 fin añadido para arreglar columnas vacías en CSPFacturacion    
         

# =============================================================================
# LF 
# Leaf que comparten un LCID por debajo del elemento que enviamos             
# tenemos que crear un registro nuevo por cada valor que nos llega                
# =============================================================================
    listas_same_level = False
    if search_method == SEARCH_METHOD_LF:                    

        copia_local_registro = registro.copy()
        
        l_list_elements_same, l_list_subblocks_same, resultado = getLeafsBelowIndexSharingLCID(element_list, list_of_begin_end_block_pointer, E = E)
        
        # hemos encontrado algo?
        if resultado == True:
            # l_list_elements_same, l_ilst_subblocks_same, l_list_elements_level_1, l_ilst_subblocks_level_1 = getElementsEnlaceLCID(element_list, list_of_begin_end_block_pointer, E = E)     
            if traza:
                if g_verbose == VERBOSE_TOTAL:                  
                    print_f(f"{FORMATEO}ELEMENTS: LF SAME LEVEL {l_list_elements_same} SB {l_list_subblocks_same}", filename_traza)   
                else:
                    print_f(f"{FORMATEO}ELEMENTS: LF SAME LEVEL SB {l_list_subblocks_same}", filename_traza)   
                        
            
            if len(l_list_subblocks_same)>0:
                listas_same_level = True            
                for i in l_list_subblocks_same:
                    # consigo sus elementos y opero sólo con el primero (el resto comparten todos la misma información)
                    l1_list_elements = getSubBlockElementsBySubblock(element_list, list_of_begin_end_block_pointer, block = B, subblock = i)
                    index = min(l1_list_elements) # al poder estar desordenadas, cojo el elemento con menor valor (el primero!)
                    if traza:
                        l_subblock = []
                        for l in registro:
                            l_SB = element_list[l]["SB"]
                            if not l_SB in l_subblock:
                                l_subblock.append(l_SB)                     
                        
                        if g_verbose == VERBOSE_TOTAL:                  
                            print_f(f"{FORMATEO}ELEMENTS: LF SAME LEVEL REGISTRO TIENE AHORA {registro} SB[{l_subblock}]", filename_traza)
                        else:
                            print_f(f"{FORMATEO}ELEMENTS: LF SAME LEVEL REGISTRO TIENE AHORA SB[{l_subblock}]", filename_traza)
                            
                        print_f(f"{FORMATEO}ELEMENTS: LF SAME LEVEL (Before) MANDAMOS EL INDEX {index}", filename_traza)
        
                    # index = l_list_elements[0]
                    registro = createReg(element_list, list_of_begin_end_block_pointer, index, l_registro = registro, l_processed_SB = processed_SB, formateo = FORMATEO, traza = traza, calledAsRecursive = True, filename_traza = filename_traza, search_method = SEARCH_METHOD_TOTAL, contador = contador) 
                    registro = copia_local_registro.copy()           

                    if traza:
                        l_subblock = []
                        for l in registro:
                            l_SB = element_list[l]["SB"]
                            if not l_SB in l_subblock:
                                l_subblock.append(l_SB)       
                        if g_verbose == VERBOSE_TOTAL:                                   
                            print_f(f"{FORMATEO}ELEMENTS: LF SAME LEVEL (After) REGISTRO TIENE AHORA {registro} SB [{l_subblock}]", filename_traza)
                        else:
                            print_f(f"{FORMATEO}ELEMENTS: LF SAME LEVEL (After) REGISTRO TIENE AHORA SB [{l_subblock}]", filename_traza)
                            

             
                
    
        if listas_same_level == False:            
            # ***********************************************************
            # añado todos los elementos de nivel 2 de este bloque al registro
            # ***********************************************************
            list_elements_level_2 = getAllElementsAtLevel(element_list, list_of_begin_end_block_pointer, B, 2)
            for i in list_elements_level_2:
                if not i in registro:
                    registro.append(i)
            # en este momento tenemos un registro completo, lo añadimos al conjunto de registros        
            if traza:
                if g_verbose == VERBOSE_TOTAL:        
                    l_subblock = []
                    for l in registro:
                        l_SB = element_list[l]["SB"]
                        if not l_SB in l_subblock:
                            l_subblock.append(l_SB)                     
                    print_f(f"{FORMATEO}----> DEVOLVEMOS EL REGISTRO {registro} SB {l_subblock} <-----------------------------------------------------------------", filename_traza)
                else:
                    print_f(f"{FORMATEO}----> DEVOLVEMOS EL REGISTRO SB {l_subblock} <-----------------------------------------------------------------", filename_traza)
                    
            LIST_REGISTROS.append(registro.copy())

    
    return LIST_REGISTROS.copy()


# =============================================================================
# index = 36
# 
# id_list_outside = id(LIST_REGISTROS)
# 
# LIST_REGISTROS[:] = []
# list_regs = createReg(element_list, list_of_begin_end_block_pointer, index)
# print_f(f"\nPara index {index} ********************", "kk_createReg.txt", True)
# for e in list_regs:
#     print_f(f"\nPara index {index} len {len(e)} REGS {e}", "kk_createReg.txt")
# =============================================================================

# --- COLUMNS ----------------------------------------------------------------------

# =============================================================================
# Devolvemos dos listas
# una con las columnas y otra con los valores de una lista de elementos    
# =============================================================================
def getColumnsFromListOfElements(element_list, registro):
    columns_list = []
    values_list = []
    for i in registro:
        element = element_list[i]
        column = element["key"]
        value = element["value"]
        columns_list.append(column)
        values_list.append(value)
    return columns_list, values_list        

# =============================================================================
# Devuelve True si la columna está contenida en la sublista de elementos enviada    
# =============================================================================
def isColumnContainedInSublistOfElements(elements_list, column, sublist_of_elements):
    for index in sublist_of_elements:
        element = element_list[index]
        if column == element["key"]:
            return True, index
    return False, -1        

# =============================================================================
# Devuelve una lista conteniendo, en el orden de las columnas, los valores de registro, y el resto ""
# =============================================================================
def getRegistroMappedToColumns(elements_list, df_columns, sublist_of_elements):
    output_registro = []
    default_value = ""
    for column in df_columns:
        # vemos si la columna está contenida en sublist_of_elements
        isContained, index = isColumnContainedInSublistOfElements(elements_list, column, sublist_of_elements)
        if isContained:
            element = element_list[index]
            value = element["value"]
        else:
            value = default_value
        output_registro.append(value)
    return output_registro

# =============================================================================
# Obtiene la lista de columnas UNICAS para el dataframe a crear
# =============================================================================
def getColumnsList(columns_list):
    list_of_columns = {}
    column_to_ignore = list(EMPTY_DICT.keys())[0]
    for k,v in columns_list.items():
        # elimino toda referencia a diccionarios vacíos
        if column_to_ignore in v:
            continue
        list_of_columns[v] = "OK"
    # devolvemos una lista con las claves encontradas
    return list(list_of_columns.keys())

# --- DF ----------------------------------------------------------------------
name_file = ""
# =============================================================================
# Creamos un DF con la información enviada (JSONFlatten avanzado...)
# =============================================================================
def createDFFromLeafs(element_list, df_columns, list_of_begin_end_block_pointer, traza = False, filename_traza = "kk.txt"):
    global LIST_REGISTROS
    global g_verbose
    global name_file
    if traza:
        print_f(f"\n\n\n******************* {datetime.datetime.now()} *********************************************************\n\n", filename_traza, initializeFile=True)            
    column_to_ignore = list(EMPTY_DICT.keys())[0]
    # global registro
    registro = []
    # rastreamos, uno a uno, los subblocks que son LEAF y enviamos a componer el registro con su primer elemento (todos comparten la misma información)
    # añadimos el registro a la lista de registros
    # convertir la lista en un DF de pandas
    import pandas as pd    
    # introduzco la información como una fila adicional en el df
    df = pd.DataFrame(columns=df_columns)
    # # número de columnas
    # if traza:
    #     print_f(f"COLUMNAS: {len(df_columns)}", filename_traza)
    #     for l in df_columns:
    #         print_f(f"COLUMNA: {l}", filename_traza)
    #     print_f(f"\n\n", filename_traza)
       
        
    # obtenemos la lista de FLF y sus SB
    leaf_elements_in_block, subblock_leaf_list = getFLFByBlock(element_list, list_of_begin_end_block_pointer)

    # rastreo los subloques de los elementos leaf (realmente un leaf es un subbloque entero y todos sus elementos)
    df_list = []
    list_processed_subblocks = {}
    for block in range(0, len(subblock_leaf_list)):
        for subblock in subblock_leaf_list[block]:
            # if not subblock in list_processed_subblocks: #» si no hemos procesado el subblock
            l_list_elements = getSubBlockElementsBySubblock(element_list, list_of_begin_end_block_pointer, block, subblock)
            # cojo el primer elemento de todos los que comparten el subblock
            index = l_list_elements[0]
            if traza:
                print_f(f"*******************************************************************************************", filename_traza)            
                print_f(f"createDFFromLeafs {name_file} TRABAJAMOS EL ELEMENTO: {index}", filename_traza)
                print_f(f"*******************************************************************************************", filename_traza)            
            
           
            # vaciamos la lista acumulada de registros
            LIST_REGISTROS.clear()
            # LIST_REGISTROS, list_processed_subblocks = createReg(element_list, list_of_begin_end_block_pointer, index, l_processed_SB = [], traza = traza, calledAsRecursive = False, filename_traza = filename_traza, search_method = SEARCH_METHOD_TOTAL)
            LIST_REGISTROS = createReg(element_list, list_of_begin_end_block_pointer, index, l_processed_SB = [], traza = traza, calledAsRecursive = False, filename_traza = filename_traza, search_method = SEARCH_METHOD_TOTAL)
            
            for registro in LIST_REGISTROS:
            
                if traza:
                    print_f(f"*******************************************************************************************", filename_traza)            
                    print_f(f"createDFFromLeafs {name_file} TOTAL ELEMENTS-REGISTRO: {index} #{len(registro)} {registro}", filename_traza)
                    t_SB = []
                    for e in registro:
                        if not element_list[e]["SB"] in t_SB:
                            t_SB.append(element_list[e]["SB"])
                    print_f(f"createDFFromLeafs {name_file} TOTAL ELEMENTS-REGISTRO (SB): {index} #{len(t_SB)} {t_SB}", filename_traza)
                    print_f(f"*******************************************************************************************", filename_traza)            
                        
                
                
                
                
                list_of_elements = []
                for n in registro:
                    e = element_list[n]
                    list_of_elements.append(e)
                
# rastreamos el registro quitando el que tenga como columna una que esté como columna vacía
                list_of_elements = [] # contiene la lista de elementos limpia (al principio hacía un pop en registro, pero mofifica la lista a medida que la rastreamos y produce errores)
                for e in registro:
                    if column_to_ignore in element_list[e]["key"]:
                        # print(f"ELIMINADO")
                        # borramos el elemento
                        continue
                    else:
                        list_of_elements.append(e)
                            
                df_list.append(list_of_elements)
            
            # # número de registros (filas)
            # len(df_list)
            # # número de columnas
        if traza:
            print(f"createDFFromLeafs {name_file} TOTAL FILAS: {len(df_list)}")
    contador = 0
    for registro in df_list:
            # creamos un df ordenado, con valores y columnas      
            columns_from_registro, values_from_registro = getColumnsFromListOfElements(element_list, registro)
            # if traza:
            #     print_f(f"TOTAL COLUMNS-REGISTRO: {len(columns_from_registro)} TOTAL VALUES-REGISTRO: {len(values_from_registro)}", filename_traza)          
            #     print_f(f"COLUMNS-REGISTRO: {columns_from_registro} VALUES-REGISTRO: {values_from_registro}", filename_traza)          
            #     for n in range(0, len(columns_from_registro)):
            #         print_f(f"{columns_from_registro[n][:65]:65} = {values_from_registro[n][:65]:65}",filename_traza)
         
           
            # creamos un pandas.DataFrame con columnas y valores
            df_to_append = pd.DataFrame([values_from_registro], columns=columns_from_registro)
            
            # print(registro)
            # print(columns_from_registro)  
            # print(values_from_registro)
            # print(df_to_append)
            
            # if traza:
            #     # print(f"DF_TO_APPEND: {df_to_append}")          
            #     print_f(f"DF_TO_APPEND: {df_to_append}", filename_traza)
            
            # lo añadimos al df original
            contador += 1
            if g_verbose == VERBOSE_TOTAL or g_verbose == VERBOSE_REDUCED:
                if not contador % 100:
                    print(f"createDFFromLeafs {name_file} REGS ADDED TO DATAFRAME {contador}")
                
            if traza:
                print_f(f"*******************************************************************************************", filename_traza)            
                print_f(f"createDFFromLeafs {name_file} df_to_append: {df_to_append}", filename_traza)
            
            df = df.append(df_to_append)

            # if traza:
            #     # print(f"DF_TO_APPEND: {df_to_append}")          
            #     print_f(f"DF ADD REG ****** ", filename_traza)


    if g_verbose == VERBOSE_TOTAL or g_verbose == VERBOSE_REDUCED:
        print(f"createDFFromLeafs {name_file} REGS ADDED TO DATAFRAME {contador}")    
    return df



# =============================================================================
# Grabamos el fichero en excel
# =============================================================================
def saveToExcel(df, filename, tab):
    
    # para no perder los datos anteriores, si los hubiere, intento abrir el fichero que me dicen, si no existe, lo creo
    from pathlib import Path # esto debería servir para MACOS y para WINDOWS
    import pandas as pd
    from openpyxl import load_workbook
    from datetime import datetime

    writer = pd.ExcelWriter(str(Path(filename)))
    
    tab = tab[:30]    
    df["execution_date"] = datetime.now().strftime("%Y-%m-%d")             
    df.to_excel(writer, tab, index=False)
    # escribimos el EXCEL
    writer.save()
    writer.close() # flush

# =============================================================================
# Convertimos una lista de índices en una lista de elementos
# =============================================================================
def _convertIndexListInElementsList(registro, element_list):
    list_registro = []
    for i in registro:
        element = element_list[i]
        list_registro.append(element)
    return list_registro

# *****************************************************************

# --- PROCESO ----------------------------------------------------------------------

def getConfiguracionProceso(sys_argv):
    # lg.loggingConfiguration(sys_argv[0], True, log_folder = Path(access["CM_log_folder"]), god_mode = GOD_MODE, level_tracking = trackingLevel)
    return

# --- FUNCION PRINCIPAL ----------------------------------------------------------------------

TIPO_LIST = 1
TIPO_DICT = 2
TIPO_ELEMENT = 3

# tamaños máximos admitidos en el algoritmo (se pueden cambiar - cuidado con la memoria que piden)
SIZE = 10000 # niveles de profundidad
SIZE_FDs_IN_BLOCK = 10000 # cuántos FDs admito por bloque?
SIZE_BLOCKS = 10000 # cuántos bloques (registros de level == 2) admitimos?

level_prefix = SIZE*[None] # guarda los prefijos por niveles de profundidad
level_B_SB_prefix = SIZE*[None] # guarda los prefijos por niveles de profundidad
level_code = SIZE*[None] # guarda los prefijos por niveles de profundidad
level_dict = SIZE*[None] # ultimo diccionario creado en un nivel (en una lista)

clave_valor = { "key" : "", "value" : ""} # guarda pares clave, valor
element_list = [] # guarda secuencias de registros
count_element_absolute = -1 # identificador único de elemento

absolute_dict_count = -1    
gSB_T = -1
max_gSB_T = -1
gLCID = 0
max_gLCID = -1

list_block_subblocks_elements = {}
list_max_level_per_block = []

# =============================================================================
# Esta función encapsula todo el trabajo interno desde coger el JSON hasta devolver un DF (dataframe de pandas)
# =============================================================================
def jsontodf(JSON, # JSON to flatten
             extended_info = False, # si queremos grabar en un fichero TXT toda la info de elementos
             filename = "result.txt", # en ese caso, nombre del fichero dónde grabar la info
             verbose = None
             ):
# FIXME: hay que meter un BOOLEAN de inicializacion para que columns_list se inicialice en la función JSONtoDF
    
# sólo para depurar, quitar después     *************************************************************************************************************************************
    # import jsons as js
    # JSON = loadDepurar(depurar(), index = 11)
    # filename = "simple_dict"
    # extended_info = True 
# fin - sólo para depurar, quitar después    ********************************************************************************************************************************
    global strategy
    strategy = STRATEGY_NEW # estrategia a usar para los LINK_DIRECT    
    global advance_counter
    advance_counter = -1 # para mostrar avance si verbose está, al menos, a VERBOSE_REDUCED en createReg
    global g_verbose
    g_verbose = verbose
    
    global apano
    apano = True
    
# 1) INICIALIZAMOS VALORES
    
    # hay que inicializar los contadores de SB
    global gSB_T, max_gSB_T, absolute_dict_count, gLCID, max_LCID

    absolute_dict_count = -1    
    gSB_T = -1
    max_gSB_T = -1
    gLCID = 0
    max_LCID = -1

    
    # logger = lg.arguments(currentframe())
    # leemos el JSON    
    global max_level # marcamos si hay algún nivel superior a 2, en este caso todos los elementos a nivel 2 no pueden ser LEAF
    list_max_level_per_block = []
    
    global element_list, count_element_absolute
    element_list.clear()

    global list_block_subblocks_elements
    list_block_subblocks_elements.clear()
    
    count_element_absolute = -1 # identificador único de elemento    

    global level_prefix, level_code, level_dict, level_B_SB_prefix
    
    level_prefix.clear()
    level_B_SB_prefix.clear()
    level_code.clear()
    level_dict.clear()
    
    level_prefix = SIZE*[None] # guarda los prefijos por niveles de profundidad
    level_B_SB_prefix = SIZE*[None] # guarda los prefijos por niveles de profundidad    
    level_code = SIZE*[None] # guarda los prefijos por niveles de profundidad
    level_dict = SIZE*[None] # ultimo diccionario creado en un nivel (en una lista)    
    
    item = JSON    

# 2) CODIFICAMOS ELEMENTOS
    columns_list, element_list = JSONelements_(item = item, initialize = True)

# 3) OBTENEMOS LA LISTA DE COLUMNAS    
    # obtenemos la lista de columnas del dataframe a crear
    df_columns = getColumnsList(columns_list)

# 4) MARCAMOS LOS BLOQUES DE REGISTRO    
    # marcamos los bloques de la lista de registros para poder identificar el leaf dentro de cada bloque
    list_of_begin_end_block_pointer = markBlocks(element_list)

# 5) MARCAMOS LA LISTA DE FLF
    # marcamos las FLF que serán los registros a componer por createReg
    markFinalLEAVES(element_list, list_of_begin_end_block_pointer)
   
    # # detectamos los niveles máximos en cada bloque, para determinar si los niveles 2 son o no LEAF
    # # el max LCID nos dice el máximo a qué elemento pertenece, siempre ha de ser a los últimos elementos de nivel 4
    list_max_level_per_block, list_max_LCID_per_block = max_level_per_block(element_list, list_of_begin_end_block_pointer)
    
    # # marcamos los LEAF temporales (serán los finales si no hay listas al mismo nivel en un diccicionario)
    markTemporalLeafs(element_list, list_of_begin_end_block_pointer, list_max_level_per_block)
    
    # # hacamos una segunda pasada para ver si hay listas al mismo nivel en un diccionario, en ese caso marcamos los LEAF finales
    # markFinalLeafs(element_list, list_of_begin_end_block_pointer, list_max_level_per_block, list_max_LCID_per_block)


    # imprimo los elementos para chequear que lo ha hecho bien
    filename_traza = f"{filename}_TRAZA.txt"
    if extended_info:
        output = "\n\n{filename} ----------- key:\tvalue\tB:SB:\t-->L FD LD D LE *LCID* --- ADC FADC -- E LF FLF\n\n"
        print_f(output,  f"{filename}.txt", initializeFile = True)
        _printElementListByBlock(element_list, f"{filename}.txt", infoleaf = True, list_of_begin_end_block_pointer = list_of_begin_end_block_pointer) # si queremos indicador de LEAR hay que enviarlo
        # _printElementList(element_list, f"{filename}.txt", infoleaf = True, list_of_begin_end_block_pointer = list_of_begin_end_block_pointer) # si queremos indicador de LEAR hay que enviarlo
        # detectamos los leaf de cada nivel
        # leaf_elements_in_block, subblock_leaf_list = detectLeafByLevel(element_list, list_of_begin_end_block_pointer, list_max_level_per_block)
        # filename_traza = f"{filename}_TRAZA.txt"
       
    
    df = createDFFromLeafs(element_list, df_columns, list_of_begin_end_block_pointer, traza = extended_info, filename_traza = filename_traza)
    
    return df

import datetime


FILE = 1
LISTA = 0
# =============================================================================
# --- You can try the library with these files and then call MonthyPyton function
# =============================================================================
def depurar():
    import jsons as js
  
    d = {
        "CSPFacturacion" : 
                {
                 "path" : "O365_CSPBillingReports_MonthlyBillingPerOrganization_CSPFacturacion.json",
                 "type" : FILE            
                },
        "CSPFacturacion_MOD" : 
                {
                 "path" : "D:/OneDrive - Seachad/SEIDOR/Temporal Reports Prueba FGV/O365_BillingReports_MonthlyBillingPerOrganization_ASYNC_CSPFacturacion_MOD.json",
                 "type" : FILE            
                },                
        "CSPUsers" : 
                {
                 "path" : "D:/OneDrive - Seachad/SEIDOR/Temporal Reports Prueba FGV/9bf4bc20-1a79-46b2-aa65-2e7c9096e2de_Csp_USERS.json",
                 "type" : FILE            
                },
        "CSPProducts" : 
                {
                 "path" : "D:/OneDrive - Seachad/03 - Clientes/SEIDOR/IPCOSELL/API_Calls_Microsoft_BORRAR/ChequearLGV/000e11e4-ff70-47a0-a056-69ba38089e30_Csp_Products.json",
                 "type" : FILE            
                },    
        "ForceManager_Filtered" : 
                {
                 "path" : "D:/OneDrive - Seachad/03 - Clientes/SEIDOR/OPERACIONES CLOUD/ForceManager/Reports/RESULT_Filtered_opportunities__salesForecastDate__GT_2019-01-01_AND__LT_2019-05-31_.json",
                 "type" : FILE            
                },     
        "lista_con_listas_mismo_nivel_1B" : 
                {
                 "path" : js.lista_con_listas_mismo_nivel_1B,
                 "type" : LISTA            
                },   
   
        "ldict_2" : 
                {
                 "path" : js.ldict_2,
                 "type" : LISTA            
                },  
        "simple_dict" : 
                {
                 "path" : js.simple_dict,
                 "type" : LISTA            
                },              
         "G_SScores" : 
                {
                 "path" : "D:/OneDrive - Seachad/03 - Clientes/SEIDOR/IPCOSELL/API_Calls_Microsoft_BORRAR/ChequearLGV/G_SScores.json",
                 "type" : FILE            
                },   
         "G_SScores_MOD_NS" : 
                {
                 "path" : "D:/OneDrive - Seachad/03 - Clientes/SEIDOR/IPCOSELL/API_Calls_Microsoft_BORRAR/ChequearLGV/G_SScores_MOD.json",
                 "type" : FILE            
                },                  
         "G_users" : 
                {
                 "path" : "D:/OneDrive - Seachad/03 - Clientes/SEIDOR/IPCOSELL/API_Calls_Microsoft_BORRAR/ChequearLGV/G_users.json",
                 "type" : FILE            
                },                
         "M_RoleDefinitions_Aggregated" : 
                {
                 "path" : "D:/OneDrive - Seachad/03 - Clientes/SEIDOR/IPCOSELL/API_Calls_Microsoft_BORRAR/ChequearLGV/M_RoleDefinitions_Aggregated.json",
                 "type" : FILE            
                },
         "M_RoleDefinitions_Aggregated_MOD_NS" : 
                {
                 "path" : "D:/OneDrive - Seachad/03 - Clientes/SEIDOR/IPCOSELL/API_Calls_Microsoft_BORRAR/ChequearLGV/M_RoleDefinitions_Aggregated_MOD.json",
                 "type" : FILE            
                },
        "configuration_machine" : 
                {
                 "path" : "configuration_machine.json",
                 "type" : FILE            
                },
        "Reseller_Subscriptions" : 
                {
                 "path" : "O365_Reseller_CspSubscriptions_ResellerSubs.json",
                 "type" : FILE            
                }                 ,
        "Reseller_Subscriptions_MOD" : 
                {
                 "path" : "O365_Reseller_CspSubscriptions_ResellerSubs_MOD.json",
                 "type" : FILE            
                }                 
                                
        }

    
    return d

# =============================================================================
# Carga el JSON que se le pida, de la lista de JSONs de prueba que hay en la función DEPURAR
# =============================================================================
def _testJSON(key):
    
    # key = "simple_dict"
    d = depurar()
    
    fromfile = None
    retorno = None
    
    if key in d:
        path = d[key]["path"]
        tipo = d[key]["type"]

        if tipo == FILE:
            filename = path
            try:
                import json
                with open(filename, "r") as json_file:
                    res = json.load(json_file)
                    fromfile = res    
            except Exception as e:
                    print(f"El fichero {filename} no se puede leer: {e}")            
        else:
            fromfile = path     
        retorno = fromfile
        

    return retorno

# =============================================================================
# Comienzo del programa para pruebas
# =============================================================================
execution_report_json_filename = "EXECUTION_REPORT_{date}"
this_script = "JSONFlattener v1.py"        



def _MontyPython():
# XXX: MontyPython    
    
    
    # cargaInicial()
    
    global strategy
    strategy = STRATEGY_NEW # estrategia a usar para los LINK_DIRECT
    import jsons as js
    global apano
    global name_file
    
    apano = True # usa la antigua forma de codificar elementos y no añade elementos EMPTY
    reg_count = []

    # a testear
    # lista_ejecucion = ["configuration_machine", "G_SScores","G_users","M_RoleDefinitions_Aggregated","CSPFacturacion", "CSPProducts"]
    # lista_ejecucion = ["CSPFacturacion_MOD"]
    # pruebas unitarias
    # lista_ejecucion = ["configuration_machine"]
    # lista_ejecucion = ["G_SScores"]
    # lista_ejecucion = ["G_SScores_MOD_NS"]
    lista_ejecucion = ["G_users"]    
    # lista_ejecucion = ["M_RoleDefinitions_Aggregated_MOD_NS"]    
    # lista_ejecucion = ["CSPFacturacion"]    
    # lista_ejecucion = ["CSPProducts"]   
    # lista_ejecucion = ["Reseller_Subscriptions_MOD"]  
# G_SScores    
# G_users
# M_RoleDefinitions_Aggregated
# CSPFacturacion
# CSPProducts
    
    # nivel de TRAZA (muestra registros o sólo subbloques)
    global g_verbose
    g_verbose = VERBOSE_REDUCED
    
    extended_info = False
    
    mensaje = f"ejecutado con extended_info {extended_info}\n"
    reg_count.append(mensaje)    
    from datetime import datetime
    mensaje = f"ejecutado el {datetime.now()}\n"
    reg_count.append(mensaje)    
    
    for i in lista_ejecucion:

        key = i
        fromfile = _testJSON(key)
        filename = key
    
        if fromfile != None:
        
            # ejecución            
            # lo envío a la función para hacerle el flatten
            import datetime
            _start = datetime.datetime.now()
            
            JSON = fromfile
            
            # import json
            # JSON = json.dumps(slack)
            
            print(len(JSON))
            

            
            name_file = filename
            df = jsontodf(JSON, 
                          extended_info = extended_info, # escribe en disco un ficherito con la estructura de los elementos que ha encontrado
                          filename = filename,
                          verbose = VERBOSE_REDUCED
                          )

            _end = datetime.datetime.now()    
            print(f"TIME INVESTED {_end-_start}")
            
            mensaje = f"filename {filename} df.shape {df.shape} time spent {_end-_start}\n"
            reg_count.append(mensaje)
            
            # generate EXCEL file
            filename = f"{filename}_GENERATED_NEW.xlsx" # (pendiente) podría venir el fichero en el que agregar en el fichero de proceso
            tab = "Data"
            saveToExcel(df, filename, tab)

    for message in reg_count:           
        print(message)



# =============================================================================
#  FIN - DEBUG - quitar    
# =============================================================================
_MontyPython()




