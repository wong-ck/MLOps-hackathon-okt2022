#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Feb 27 16:33:16 2021

@author: praneeth
"""

import streamlit as st
import pandas as pd

import plotly.express as px


NAME_DIC = {
    'MF':"Mette Frederiksen",
    'JEJ':"Jakob Ellemann-Jensen",
    'MM':"Morten Messerschmidt",
}


st.title("Danish Election Trending")
st.markdown("""
    General elections are scheduled to be held in Denmark on 1 November 2022
    after the incumbent Prime Minister Mette Frederiksen announced this on 5 
    October, after an ultimatum by the Social Liberals (B).All 179 members 
    of the Folketing will be elected; 175 members in Denmark proper, two in 
    the Faroe Islands and two in Greenland [(Wikipedia)](https://en.wikipedia.org/wiki/2022_Danish_general_election).

    We are tracking the number and positivity of the post from various 
    social media, and we hope this will provide some insight on how the 
    people view about the leaders and parties in Denmark. 
""")

@st.cache(persist=True)
def load_data():
    df = pd.read_csv("tweets_dev.csv")
    return(df)



def run():
    #st.subheader("How do people view about Danish Politics?")
    
    df = load_data()
    
    
    
    disp_head = st.sidebar.radio('Select Line Graph Display Option:',('Counts', 'Positivity'),index=0)
   
    
   
    #Multi-Select
    #sel_plot_cols = st.sidebar.multiselect("Select Columns For Scatter Plot",df.columns.to_list()[0:4],df.columns.to_list()[0:2])
    
    #Select Box
    #x_plot = st.sidebar.selectbox("Select X-axis Column For Scatter Plot",df.columns.to_list()[0:4],index=0)
    #y_plot = st.sidebar.selectbox("Select Y-axis Column For Scatter Plot",df.columns.to_list()[0:4],index=1)
    
    
    #if disp_head=="Counts":
    #    st.dataframe(df.head())
    #else:
    #   st.dataframe(df)
    #st.table(df)
    #st.write(df)
    
    
    #Scatter Plot
    
    if disp_head=="Counts":
        fig = px.line(df, x='datetime', y='half_hour_count' ,color='candidate', log_y=True, markers=True)
    else:
        fig = px.line(data_frame=df,x='datetime',y='positivity',color='candidate', markers=True)
        
    fig.for_each_trace(lambda t: t.update(name = NAME_DIC[t.name]))

    fig.update_layout({
        'plot_bgcolor': 'rgba(0, 0, 0, 0)',
        'legend_title':'Candidate',
        'xaxis_title':'time',
        'yaxis_title': disp_head.lower(),
        #'xaxis_tickformat':"%d %B ",
    })

    fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor='lightgray',tickformat="%I:%M %p")
    fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor='lightgray')
   
    st.write("\n")
    st.subheader(f"{disp_head} of party leaders over time")
    st.plotly_chart(fig, use_container_width=True)
    
    
    #Add images
    #images = ["<image_url>"]
    #st.image(images, width=600,use_container_width=True, caption=["Iris Flower"])
   
    
   
    
   
if __name__ == '__main__':
    run()    
