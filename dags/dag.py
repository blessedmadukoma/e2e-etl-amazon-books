from airflow import DAG
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
import pandas as pd
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 7, 6),
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36",
    "Referer": "https://www.amazon.com/"
}

# dag - directed acyclic graph
dag = DAG(
    "fetch_and_store_amazon_books",
    default_args=default_args,
    description="A DAG to fetch and store Amazon books data from Amazon to PostgreSQL",
    # schedule_interval="0 0 * * *",  # every day at midnight
    schedule_interval=timedelta(days=1),
)

# tasks or functions: 
# Task 1: fetch amazon data (extract), Task 2:  2. clean data (transform)
def get_amazon_data_books(num_books, **kwargs):
    base_url = f"https://www.amazon.com/s?k=data+engineering+books"

    books = []

    seen_titles = set()

    page = 1

    while len(books) < num_books:
        url = f"{base_url}&page={page}"

        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            soup = BeautifulSoup(response.content, "html.parser")

            # book_containers = soup.find_all("div", class_="s-result-item")
            book_containers = soup.find_all("div", {"class": "s-result-item"})

            for book in book_containers:
                title = book.find("span", {"class": "a-text-normal"})
                author = book.find("a", {"class": "a-size-base"})
                # author = book.find("span", class_="a-size-medium a-color-base a-text-ellipsis")
                price = book.find("span", {"class": "a-price-whole"})
                rating = book.find("span", {"class": "a-icon-alt"})
                # description = book.find("span", {"class": "a-size-base-plus"})
                
                # if title and author and price and rating and description:
                if title and author and price and rating:
                    book_title = title.text.strip()

                    # check if title has not been seen before
                    if book_title not in seen_titles:
                        seen_titles.add(title)

                        book_info = {
                            "Title": title,
                            "Author": author.text.strip(),
                            "Price": price.text.strip(),
                            "Rating": rating.text.strip(),
                            # "date_updated": t1,
                        }

                        books.append(book_info)
            
            # increment the page number for the next iteration
            page += 1
        else:
            print("failed to retreive the page")
            break

    # limit to the requested number of books
    books = books[:num_books]

    # convert the list of dictionaries into a DataFrame
    df = pd.DataFrame(books)

    # remove duplicates based on 'Title' column
    df.drop_duplicates(subset="Title", inplace=True)

    # push the DataFrame to XCom
    kwargs['ti'].xcom_push(key='book_data', value=df.to_dict('records'))

    # return df

# Task 3: create and store data in postgres database (load)
def insert_book_data_to_postgres(**kwargs):
    book_data = kwargs['ti'].xcom_pull(key='book_data', task_ids='fetch_book_data')

    if not book_data:
        raise ValueError('Book data not found')
    
    postgres_hook = PostgresHook(postgres_conn_id='amazon_books_connection')

    insert_query = """
        INSERT INTO books(title, authors, price, rating)
        VALUES (%s, %s, %s, %s)
    """

    for book in book_data:
        postgres_hook.run(insert_query, parameters=(book['Title'], book['Author'], book['Price'], book['Rating']))

# operators: PythonOperator and PostgresOperator
fetch_books_data_task = PythonOperator(
    task_id="fetch_book_data",
    python_callable=get_amazon_data_books,
    op_args=[50],  # Number of books to fetch: 50 books
    # op_kwargs={"num_books": 50},
    dag=dag,
)

create_table_task = PostgresOperator(
    task_id="create_books_table",
    postgres_conn_id="amazon_books_connection",
    sql="""
        CREATE TABLE IF NOT EXISTS books (
            id SERIAL PRIMARY KEY,
            title TEXT NOT NULL,
            authors TEXT,
            price TEXT,
            rating TEXT
        );
    """,
    dag=dag,
)

insert_book_data_task = PythonOperator(
    task_id="insert_book_data",
    python_callable=insert_book_data_to_postgres,
    dag=dag,
)

# hooks - allow airflow connections to external dbs e.g. postgres

# dependencies: hierarchy in which tasks or functions are performed
fetch_books_data_task >> create_table_task >> insert_book_data_task