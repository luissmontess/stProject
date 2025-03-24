import streamlit as st
import requests
import pandas as pd
import json

def post_spark_job(user, repo, job, token):
    url = f'https://api.github.com/repos/{user}/{repo}/dispatches'
    payload = { "event_type": job }

    headers = {
        'Authorization': f'Bearer {token}',
        'Accept': 'application/vnd.github.v3+json',
        'Content-type': 'application/json'
    }

    st.subheader("Dispatch Request Sent:")
    st.json(payload)
    response = requests.post(url, json=payload, headers=headers)
    
    st.subheader("GitHub Response:")
    st.write(response.status_code)
    st.json(response.json() if response.content else {"message": "No content"})

def get_spark_results(url_results):
    response = requests.get(url_results)
    st.subheader("GET Request Response:")
    st.write(response.status_code)

    if response.status_code == 200:
        try:
            data = response.json()
            df = pd.json_normalize(data)
            st.dataframe(df)
        except Exception as e:
            st.error(f"Failed to parse JSON: {e}")
            st.text(response.text)
    else:
        st.error("Failed to fetch data")

# UI

st.title("Walmart Top 10 Spark Job")
st.header("Trigger GitHub Spark Workflow")

github_user  = st.text_input('GitHub user', value='luissmontess')
github_repo  = st.text_input('GitHub repo', value='stProject')
spark_job    = st.text_input('Spark job event name', value='spark')
github_token = st.text_input('GitHub token', type='password')

if st.button("POST spark-submit"):
    post_spark_job(github_user, github_repo, spark_job, github_token)

st.header("View Spark Results (from GitHub)")

default_result_url = "https://raw.githubusercontent.com/luissmontess/stProject/main/results/top_expensive_purchases.json"
result_url = st.text_input("Result URL (JSON file)", value=default_result_url)

if st.button("GET spark results"):
    get_spark_results(result_url)
