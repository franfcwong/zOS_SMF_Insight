#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import json
from dash import Dash, html, dcc, dash_table
#import plotly.express as px
from dash.dependencies import Input, Output
from functools import reduce
from datetime import timedelta

def sum_up_time(timeList):
    totalMicroSecs = 0
    for tm in timeList:
        MicroParts = [s for s in tm.split('.')]
        timeParts = [int(s) for s in MicroParts[0].split(':')]
        totalMicroSecs += ((timeParts[0] * 60 + timeParts[1]) * 60 + 
                           timeParts[2]) * 1000000 + int(MicroParts[1])
    return timedelta(microseconds=totalMicroSecs)

def format_timedelta(td):
    hours, remainder = divmod(td.total_seconds(), 3600)
    minutes, seconds = divmod(remainder, 60)
    hours, minutes, seconds = int(hours), int(minutes), int(seconds)
    if hours <10:
        hours = '0%s' % int(hours)
    if minutes < 10:
        minutes = '0%s' % minutes
    if seconds < 10:
        seconds = '0%s' % seconds
    return '%s:%s:%s' % (hours, minutes, seconds)

app = Dash(__name__)
with open('/home/wong/Documents/jsonlist.json') as f:
    data = json.load(f)
    #data = pd.read_json(f)

columns_needed = ['header',
                  'dfha06ds', 
                  'dfhdsgds', 'dfhxmgds', 'dfhsmsds',
                  'dfha14ds', 'dfha20ds', 'dfha06ds', 'dfhnqgds', 'dfha17ds',
                  'dfha08ds', 'dfhd2gds', 'dfhd2rds', 'dfhmqgds']

new_list =[]
dfs_list = []
dictfilt = lambda x, y: dict([ (i,x[i]) for i in x if i in set(y)])

for item in data:
    newItem = dictfilt(item, columns_needed)
    new_list.append(newItem)

#Flatten data
df_overall = pd.json_normalize(data)

# process dfha06ds
df_a06ds = None
if 'dfha06ds' in df_overall.keys():
    df_a06ds = pd.json_normalize([elem for elem in new_list if ("dfha06ds" in elem.keys() and
                                                                isinstance(elem['dfha06ds'], list))], 
                             'dfha06ds',
                             meta=[['header', 'smfstprn'],
                                   ['header', 'dateTime']],
                             errors='ignore',
                             record_prefix='dfha06ds.')

    df_a06ds = df_a06ds.groupby(['header.smfstprn'])['dfha06ds.a06teot','dfha06ds.a06csvc',
                    'dfha06ds.a06tete','dfha06ds.a06teoe'].sum() #.reset_index()
    df_a06ds['a06csvc'] = df_a06ds['dfha06ds.a06csvc'].apply(lambda x:'🔴' if x > 0 else '🟢')
    df_a06ds['a06tete'] = df_a06ds['dfha06ds.a06tete'].apply(lambda x:'🔴' if x > 0 else '🟢')
    df_a06ds['a06teoe'] = df_a06ds['dfha06ds.a06teoe'].apply(lambda x:'🔴' if x > 0 else '🟢')
    dfs_list.append(df_a06ds)
# process dfhdsgds.dsgtcbm
df_dsgds = None
if 'dfhdsgds.dsgtcbm' in df_overall.keys():
    df_dsgds = pd.json_normalize([elem for elem in new_list if "dfhdsgds" in elem.keys()], 
                             ['dfhdsgds','dsgtcbm'],
                             meta=[['header']],
                             errors='ignore')
    df_dsgds['header.smfstprn'] = df_dsgds.apply(lambda x: x['header']['smfstprn'], axis=1)
    df_dsgds.drop(columns='header', inplace=True)

    df_dsgds = df_dsgds.loc[df_dsgds['dsgtcbnm'] == 'QR'].groupby(['header.smfstprn'])['dsgact'].max() #.reset_index()
    dfs_list.append(df_dsgds)
# process dfhxmgds
df_xmgds = None
df_xmgds_1 = None
if 'dfhxmgds' in df_overall.keys():
    df_xmgds_1 = pd.json_normalize([elem for elem in new_list if ("dfhxmgds" in elem.keys() and
                                                                  isinstance(elem['dfhxmgds'], list))],
                             'dfhxmgds',
                             meta=[['header','smfstprn']],
                             errors='ignore',
                             record_prefix='dfhxmgds.')
if 'dfhxmgds.xmgpat' in df_overall.keys():
    if df_xmgds_1 is None:
        df_xmgds = df_overall[['header.smfstprn','dfhxmgds.xmgpat','dfhxmgds.xmgtamxt']].dropna()
    else:
        df_xmgds = pd.concat([df_xmgds_1[['header.smfstprn','dfhxmgds.xmgpat','dfhxmgds.xmgtamxt']], 
                              df_overall[['header.smfstprn','dfhxmgds.xmgpat','dfhxmgds.xmgtamxt']].dropna()], axis=0)
else:
    df_xmgds = df_xmgds_1

if df_xmgds is not None:        
    df_xmgds = df_xmgds.groupby(['header.smfstprn'])['dfhxmgds.xmgpat','dfhxmgds.xmgtamxt'].max() #.reset_index()
    df_xmgds['xmgtamxt'] = df_xmgds['dfhxmgds.xmgtamxt'].apply(lambda x:'🔴' if x > 0 else '🟢')
    dfs_list.append(df_xmgds)
# process dfhsmsds.smsbody
df_smsds = None
if 'dfhsmsds.smsbody' in df_overall.keys():
    df_smsds = pd.json_normalize([elem for elem in new_list if "dfhsmsds" in elem.keys()],
                             ['dfhsmsds', 'smsbody'],
                             meta=[['header']],
                             errors='ignore',
                             record_prefix='dfhsmsds.')
    df_smsds['header.smfstprn'] = df_smsds.apply(lambda x: x['header']['smfstprn'], axis=1)
    df_smsds.drop(columns='header', inplace=True)

    condition1 = (df_smsds['dfhsmsds.smsdsaname'].str.startswith('E'))
    condition2 = (df_smsds['dfhsmsds.smsdsaname'].str.startswith('E','G'))
    df_smsds1 = df_smsds[condition1].groupby(['header.smfstprn'])['dfhsmsds.smssos'].sum().reset_index()
    df_smsds2 = df_smsds[~condition2].groupby(['header.smfstprn'])['dfhsmsds.smssos'].sum().reset_index()
    df_smsds1['smssos1'] = df_smsds1['dfhsmsds.smssos'].apply(lambda x:'🔴' if x > 0 else '🟢')
    df_smsds2['smssos2'] = df_smsds2['dfhsmsds.smssos'].apply(lambda x:'🔴' if x > 0 else '🟢')
    dfs_list.append(df_smsds1)
    dfs_list.append(df_smsds2)
# process dfha17ds
df_a17ds_1 = None
df_a17ds = None
if 'dfha17ds' in df_overall.keys():
    df_a17ds_1 = pd.json_normalize([elem for elem in new_list if ( "dfha17ds" in elem.keys() and 
                                    isinstance(elem['dfha17ds'], list))],
                             record_path=['dfha17ds'],
                             meta=[['header','smfstprn']],
                             errors='ignore',
                             record_prefix='dfha17ds.')
if 'dfha17ds.a17dshsw' in df_overall.keys():
    if df_a17ds_1 is None:
        df_a17ds = df_overall[['header.smfstprn','dfha17ds.a17dshsw']].dropna()
    else:
        df_a17ds = pd.concat([df_a17ds_1[['header.smfstprn','dfha17ds.a17dshsw']],
                              df_overall[['header.smfstprn','dfha17ds.a17dshsw']].dropna()], axis=0)
elif df_a17ds_1 is not None:
    df_a17ds = df_a17ds_1

if df_a17ds is not None:        
    df_a17ds = df_a17ds.groupby(['header.smfstprn'])['dfha17ds.a17dshsw'].max().reset_index()
    df_a17ds['a17dshsw'] = df_a17ds['dfha17ds.a17dshsw'].apply(lambda x:'🔴' if x > 0 else '🟢')
    dfs_list.append(df_a17ds)
# process dfha08ds
df_a08ds_1 = None
if 'dfha08ds' in df_overall.keys():
    df_a08ds_1 = pd.json_normalize([elem for elem in new_list if ( "dfha08ds" in elem.keys() and 
                                    isinstance(elem['dfha08ds'], list))],
                             record_path=['dfha08ds'],
                             meta=[['header','smfstprn']],
                             errors='ignore',
                             record_prefix='dfha08ds.')
if 'dfha08ds.a08bkhsw' in df_overall.keys():
    if df_a08ds_1 is None:
        df_a08ds = df_overall[['header.smfstprn','dfha08ds.a08bkhsw']].dropna()
    else:
        df_a08ds = pd.concat([df_a08ds_1[['header.smfstprn','dfha08ds.a08bkhsw']],
                              df_overall[['header.smfstprn','dfha08ds.a08bkhsw']].dropna()], axis=0)
else:
    df_a08ds = df_a08ds_1

if df_a08ds is not None:        
    df_a08ds = df_a08ds.groupby(['header.smfstprn'])['dfha08ds.a08bkhsw'].max() #.reset_index()
    df_a08ds['a08bkhsw'] = df_a08ds['dfha08ds.a08bkhsw'].apply(lambda x:'🔴' if x > 0 else '🟢')
    dfs_list.append(df_a08ds)

# process dfha14ds
df_a14ds = None
df_a14ds_1 = None
if 'dfha14ds' in df_overall.keys():
    df_a14ds_1 = pd.json_normalize([elem for elem in new_list if ( "dfha14ds" in elem.keys() and 
                                    isinstance(elem['dfha14ds'], list))],
                             record_path=['dfha14ds'],
                             meta=[['header','smfstprn']],
                             errors='ignore',
                             record_prefix='dfha14ds.')
if 'dfha14ds.a14estas' in df_overall.keys():
    if df_a14ds_1 is None:
        df_a14ds = df_overall[['header.smfstprn','dfha14ds.a14estas',
                               'dfha14ds.a14estaq','dfha14ds.a14estaf',
                               'dfha14ds.a14estao','dfha14ds.a14estam']].dropna()
    else:
        df_a08ds = pd.concat([df_a08ds_1[['header.smfstprn','dfha14ds.a14estas',
                                          'dfha14ds.a14estaq','dfha14ds.a14estaf',
                                          'dfha14ds.a14estao','dfha14ds.a14estam']],
                              df_overall[['header.smfstprn','dfha14ds.a14estas',
                                          'dfha14ds.a14estaq','dfha14ds.a14estaf',
                                          'dfha14ds.a14estao','dfha14ds.a14estam']].dropna()], axis=0)
else:
    df_a14ds = df_a14ds_1

if df_a14ds is not None:        
    df_a14ds = df_a14ds.groupby(['header.smfstprn'])['dfha14ds.a14estas',
                                                     'dfha14ds.a14estaq',
                                                     'dfha14ds.a14estaf',
                                                     'dfha14ds.a14estao',
                                                     'dfha14ds.a14estam'].max() #.reset_index()
    df_a14ds['a14estaf'] = df_a14ds['dfha14ds.a14estaf'].apply(lambda x:'🔴' if x > 0 else '🟢')
    df_a14ds['a14estao'] = df_a14ds['dfha14ds.a14estao'].apply(lambda x:'🔴' if x > 0 else '🟢')
    df_a14ds['a14estam'] = df_a14ds['dfha14ds.a14estam'].apply(lambda x:'🔴' if x > 0 else '🟢')
    dfs_list.append(df_a14ds)    

# process dfhnqgds.nqgbody
df_nqgds = None
if 'dfhnqgds.nqgbody' in df_overall.keys():
    df_nqgds = pd.json_normalize([elem for elem in new_list if "dfhnqgds" in elem.keys()],
                             ['dfhnqgds', 'nqgbody'],
                             meta=[['header']],
                             errors='ignore',
                             record_prefix='dfhnqgds.')
    df_nqgds['header.smfstprn'] = df_nqgds.apply(lambda x: x['header']['smfstprn'], axis=1)
    df_nqgds.drop(columns='header', inplace=True)

    df_nqgds = df_nqgds.groupby(['header.smfstprn']).agg({'dfhnqgds.nqgtnqsi' :'sum',
                                                          'dfhnqgds.nqgtnqwt' : sum_up_time,
                                                          'dfhnqgds.nqggnqwt' : sum_up_time})
    #df_nqgds_2 = df_nqgds.groupby(['header.smfstprn'])['dfhnqgds.nqgtnqwt'].apply(sum_up_time).reset_index()

    df_nqgds['nqgtnq_gnqwt'] = df_nqgds['dfhnqgds.nqgtnqwt'] + df_nqgds['dfhnqgds.nqggnqwt']
    df_nqgds['nqg_avgwt'] = (df_nqgds['dfhnqgds.nqgtnqwt'] + 
                             df_nqgds['dfhnqgds.nqggnqwt']) / df_nqgds['dfhnqgds.nqgtnqsi']
    df_nqgds['total_qwt'] = df_nqgds['nqgtnq_gnqwt'].apply(lambda x: format_timedelta(x))
    df_nqgds['avg_qwt'] = df_nqgds['nqg_avgwt'].apply(lambda x: format_timedelta(x))
    dfs_list.append(df_nqgds)


df_merged = reduce(lambda left, right: pd.merge(left,right, on=['header.smfstprn'],
                                               how='outer'), dfs_list)
 
#app.layout = html.Div(children=[
    #html.H1(children='Hello Dash'),
    
    #html.Div(children='''
    #         Dash: A web application framework for your data.
    #         '''),
             
             #dcc.Graph(
             #    id='example-graph',
             #    figure=fig), 

app.layout = dash_table.DataTable(
                id='datatable-interactivity',
                data=df_merged.to_dict('records'),
                #columns=[{"name": i, "id": i} for i in df.columns],
                filter_action="native",
                sort_action="native",
                page_action="native",
                css=[{
                    'selector': 'table',
                    'rule': 'table-layout: fixed'
                    }],
                columns=[
                    {'name': 'CICS Region', 'id': 'header.smfstprn', 'type': 'text'},
                    {'name': 'Highest QR TCB', 'id': 'dsgact', 'type': 'numeric'},
                    {'name': 'Peak active user Xacts', 'id': 'dfhxmgds.xmgpat', 'type': 'numeric'},
                    {'name': 'Times at MAXTASK', 'id': 'xmgtamxt', 'type': 'numeric'},
                    {'name': 'SOS on any of DSAs', 'id': 'smssos1'},
                    {'name': 'SOS on any of EDSAs', 'id': 'smssos2'},
                    {'name': 'Waits for VSAM File String', 'id': 'a17dshsw'},
                    {'name': 'Waits for LSR Pool String', 'id': 'a08bkhsw'},
                    {'name': 'Total allocates', 'id': 'dfha14ds.a14estas','type':'numeric'},
                    {'name': 'Queue allocates', 'id': 'dfha14ds.a14estaq', 'type':'numeric'},
                    {'name': 'Failed link allocates', 'id': 'a14estaf'},
                    {'name': 'Failed other reasons', 'id': 'a14estao'},
                    {'name': 'Max o/s allocates', 'id': 'a14estam'},
                    {'name': 'Terminal Xacts', 'id': 'a06teot'}, 
                    {'name': 'Terminal Storage Violation', 'id': 'a06csvc'}, 
                    {'name': 'Terminal Xmit Error', 'id': 'a06tete'},
                    {'name': 'Terminal Xact Error', 'id':'a06teoe'},
                    {'name': 'Total enqueue issued', 'id': 'dfhnqgds.nqgtnqsi', 'type':'numeric'},
                    {'name': 'Total enqueue Wait Time', 'id': 'total_qwt'},
                    {'name': 'Average enqueue Wait Time', 'id': 'avg_qwt'},
                    ], 
                style_table={'border': '1px solid red',
                             'height': 400, 'overflowY': 'auto'},
                style_data={
                    'width': '{}%'.format(100. / 20),
                    'textOverflow': 'hidden'
                    },
                page_current=0,
                page_size=20,
                style_cell={
                    'height': 'auto',
                    'minWidth': '30px','width': '30px', 'maxWidth': '50px',
                    'whiteSpace': 'normal'
                    },
              
                style_data_conditional=[
                    {
                        'if': {
                            'column_id': ['a08bkhsw', 'header.smfstprn',
                                          'a14estam', 'a06teoe']
                        },
                        'borderRight': '1px solid red',
                    }
                ],
                style_header_conditional=[
                    {
                        'if': {
                            'column_id': ['a08bkhsw', 'header.smfstprn',
                                          'a14estam', 'a06teoe']
                        },
                        'borderRight': '1px solid red',
                    }
                ]
            )#,
     #       html.Div(id="out")
#])


if __name__ == '__main__':
    app.run_server(debug=True)