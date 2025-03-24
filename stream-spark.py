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
            st.subheader("Raw JSON Data:")
            st.write(data)

            if isinstance(data, list):
                df = pd.DataFrame(data)
                st.subheader("Tabular View:")
                st.dataframe(df)
            else:
                st.info("The result is not a list of records.")
        except Exception as e:
            st.error(f"Failed to parse JSON: {e}")
            st.text("Raw response:")
            st.text(response.text)
    else:
        st.error("Failed to fetch data")

# === UI === #

st.title("🛍️ Walmart Spark Analysis")
st.write("Trigger your GitHub Spark workflow and explore the top 10 Walmart purchase insights.")

# --- Trigger Spark Job Section ---
st.header("🚀 Trigger Spark Workflow")

github_user  = st.text_input('GitHub user', value='luissmontess')
github_repo  = st.text_input('GitHub repo', value='stProject')
spark_job    = st.text_input('Spark job event name', value='spark')
github_token = st.text_input('GitHub token', type='password')

if st.button("POST spark-submit"):
    post_spark_job(github_user, github_repo, spark_job, github_token)

# --- View Spark Results Section ---
st.header("📊 View Spark Results")

base_url = f"https://raw.githubusercontent.com/{github_user}/{github_repo}/main/results/"

result_files = {
    "Top 10 Expensive Purchases": "top_expensive_purchases.json",
    "Top 10 Spending Cities": "top_spending_cities.json",
    "Top 10 Rated Products": "top_rated_products.json"
}

selected_result = st.selectbox("Choose a result to view:", list(result_files.keys()))
result_url = base_url + result_files[selected_result]

st.write(f"Fetching result from:\n`{result_url}`")

if st.button("GET spark results"):
    get_spark_results(result_url)
