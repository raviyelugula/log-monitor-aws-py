import io
import os
import streamlit as st
import pandas as pd
import boto3


def read_prefix_to_df():
    s3 = boto3.resource('s3')
    bucket = s3.Bucket('aws-ec2-logs-307592787224')
    prefix_objs = bucket.objects.filter(Prefix='ec2-search-logs')
    prefix_df = []
    for obj in prefix_objs:
        if '.csv' in obj.key:
            print(f' &&&&&&&&&&{obj.key} &&&&&&&&&&& ')
            key = obj.key
            body = obj.get()['Body'].read()
            temp = pd.read_csv(io.BytesIO(body), encoding='utf8', index_col = False)
            prefix_df.append(temp)
    return pd.concat(prefix_df)


def make_clickable(df):
    url = df['url']
    title = df['page_title']
    return f'<a target="_blank" href="{url}">{title}</a>'


st.title("Productivity Monitor")
st.markdown("This dashbaord will help to monitor the usage of chrome browser")
st.sidebar.title("Filters")

data = read_prefix_to_df()
machine = pd.Series('all').append(data['machine_name'].drop_duplicates())
machine_choice = st.sidebar.selectbox('Machine', machine, index = 0)

if machine_choice != 'all':
    domain = pd.Series('all').append(data.loc[(data['machine_name']==machine_choice)]['domain'].drop_duplicates())
else:
    domain = pd.Series('all').append(data['domain'].drop_duplicates())
domain_choice = st.sidebar.selectbox('Domain', domain, index = 0)


show_data = data
if domain_choice != 'all':
    show_data = show_data.loc[(show_data['domain']==domain_choice)]

if machine_choice != 'all':
    show_data = show_data.loc[(show_data['machine_name']==machine_choice)]

show_data['url'] = show_data.apply(lambda row: f'<a target="_blank" href="{row.url}">{row.page_title}</a>', axis=1)

show_data.drop('page_title', axis=1, inplace=True)


# CSS to inject contained in a string
hide_table_row_index = """
            <style>
            thead tr th:first-child {display:none}
            tbody th {display:none}
            </style>
            """

# Inject CSS with Markdown
st.markdown(hide_table_row_index, unsafe_allow_html=True)


show_data = show_data.to_html(escape=False)
st.write(show_data, unsafe_allow_html=True)
