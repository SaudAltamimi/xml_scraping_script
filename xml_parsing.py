import pandas as pd
import argparse
from bs4 import BeautifulSoup
import os
import datetime


def read_xml_file(file_path,encoding='utf-8'):
    '''
    A function to open the the twb file
    INPUT:
        file_path
    OUTPUT:
        xml_doc : xml object
    '''
    xml_doc = open(file_path,
                   encoding=encoding)

    return xml_doc


def get_formula(column):
    '''
    This function was created to extract the formula linked with column's calculations,
    and to avoid errors if a certain column does not has a calculation formula
    '''
    # check if it has a calculation or not
    if column.calculation == None:
        return None

    else:
        formula = column.calculation.attrs.get("formula")
        formula = formula.replace("\r\n", " ")
        return formula

def parse_required_metadata(xml_doc,file_name):
    '''
    INPUT:
        xml_doc : Xml object
        file_name : the name of the target file
    OUTOUT:
        df_schema1 : The first required data stored in A pandas DataFrame
        df_schema2 : The second required data stored in A pandas DataFrame
    '''
    # create an empty dataframe with schemas to store the data
    df_schema1 = pd.DataFrame(columns=['wb_name',
                                       'ws_name',
                                       'ds_id',
                                       'col_caption',
                                       'col_name',
                                       'col_formula'])

    df_schema2 = pd.DataFrame(columns=['wb_name',
                                       'dash_name',
                                       'ws_name'])

    # Create BeautifulSoup object
    soup = BeautifulSoup(xml_doc, 'lxml')

    # outfile1 schema:
    # loop through every worksheet inside the twb
    for worksheet in soup.find('worksheets').find_all('worksheet'):
        # extract all the datasource dependencies inside each worksheet
        datasource_dependencies= worksheet.table.view.find_all('datasource-dependencies')
        for datasource in datasource_dependencies:
            if datasource['datasource']!='Parameters':
                # loop throught every column inside a datascource
                for column in (datasource.find_all('column')):
                    # store the required data
                    df_schema1 = df_schema1.append({'wb_name': file_name,
                                                    'ws_name' : worksheet.attrs.get('name'),  #worksheets.worksheet.name
                                                    'ds_id': datasource['datasource'], #worksheets.worksheet.table.view.datasource-dependencies.datasource
                                                    'col_caption':column.attrs.get('caption'),#worksheets.worksheet.table.view.datasource-dependencies.column.caption
                                                    'col_name':column.attrs.get('name').strip('[]'),
                                                    'col_formula':get_formula(column) # a function to extract the formula linked to each column
                                   },
                                   ignore_index=True)

    # outfile2 schema:
    # loop through every dashboard inside the twb file
    for dashboard in soup.find_all('window', {'class': 'dashboard'}):
        #print(dashboard['name'])
        for viewpoint in dashboard.find_all('viewpoints'):
            for worksheet in viewpoint.find_all('viewpoint'):
                # store the required data
                df_schema2 = df_schema2.append({'wb_name': file_name, #twb file name
                                                'dash_name':dashboard['name'],#(window class='dashboard').name
                                                'ws_name': worksheet['name']#(window class='dashboard').viewpoints.viewpoint.name
                               },
                               ignore_index=True)

    return df_schema1,df_schema2

if __name__=='__main__':

    parser = argparse.ArgumentParser(description='Parsing an xml file to get some valuable metadata, Example of how to run ($python xml_parsing.py twb_samples output_files)')

    parser.add_argument("input_path",
                        type=str,
                        help="Path to the folder which contains twb (tableau xml) files",
                        default='twb_files')

    parser.add_argument("output_path",
                        type=str,
                        help="Path to keep the outputs",
                        default='output_files')

    args = parser.parse_args()
    # create an empty dataframe with schemas to store all the data
    df_all_schema1 = pd.DataFrame(columns=['wb_name',
                                           'ws_name',
                                           'ds_id',
                                           'col_caption',
                                           'col_name',
                                           'col_formula',
                                           'folder_path'])

    df_all_schema2 = pd.DataFrame(columns=['wb_name',
                                          'dash_name',
                                          'ws_name',
                                          'folder_path'])
    # loop through every twb file inside the input folder
    for subdir, dirs, files in os.walk(args.input_path):
        for file in files:
            #print os.path.join(subdir, file)
            filefolder = subdir + os.sep
            filepath = filefolder + file

            # make sure that we are only reading twb files
            if filepath.endswith(".twb"):
                # read the file
                xml_doc = read_xml_file(filepath)

                #extract the file name
                file_name = file
                # parse the required meta-data
                df_schema1,df_schema2 = parse_required_metadata(xml_doc,file_name=file_name)

                # adding the path information to both schemas
                df_schema1['folder_path'] = filefolder
                df_schema2['folder_path'] = filefolder
                # combinig all the files data in one dataframe
                df_all_schema1 = df_all_schema1.append(df_schema1,
                                                       ignore_index=True)
                df_all_schema2 = df_all_schema2.append(df_schema2,
                                                        ignore_index=True)



    # getting the datetime for now
    dt = datetime.datetime.now()
    dt_str = dt.strftime("%Y%m%d_%H%M%S")

    # create an output dir if not exists
    if not os.path.exists(args.output_path+os.sep):
        os.makedirs(args.output_path+os.sep)

    # saving the results
    df_all_schema1.to_csv(args.output_path+os.sep+f'datasource_dependencies_{dt_str}.csv',index=False)
    df_all_schema2.to_csv(args.output_path+os.sep+f'dashboards_{dt_str}.csv',index=False)

    
