import sys
import time
from datetime import date

import pandas as pd
import requests
from dateutil.relativedelta import relativedelta
from faker import Faker
from sqlalchemy import create_engine
from sqlalchemy.dialects.mysql import insert


class GenerateTransaction:
    def __init__(self, host, user, password, port, db):
        self.host = host
        self.user = user
        self.password = password
        self.port = port
        self.db = db

        # Create connection to database
        self.engine_uri = (
            f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{self.port}/{self.db}"
        )
        self.conn = create_engine(self.engine_uri, echo=False)

    def insert_on_duplicate(self, table, conn, keys, data_iter):
        """
        Insert rows into a table in chunks on duplicate key update.
        This is used when importing large amounts of data that may already exist in the target
        Insert data into a table using INSERT ... ON DUPLICATE KEY UPDATE statement.
        This has been tested for MySQL specific syntax and may not work on other databases.

        :param table: Table name where the data should be inserted.
            :type table: str
        :param conn: Database connection object.
            :type conn: Connection Object
        :param keys: A list of columns that are used for checking duplicates.
        :param data_iter: An iterator yielding tuples of data to insert.
                          Each tuple must contain all columns in `keys`.
        -------
        Returns
        -------
        None
            The function does not return anything directly but commits the changes to the database.
        """
        insert_stmt = insert(table.table).values(list(data_iter))
        on_duplicate_key_stmt = insert_stmt.on_duplicate_key_update(insert_stmt.inserted)
        conn.execute(on_duplicate_key_stmt)

    def get_books(self):
        """
        Load books from API into 'Books' table.
        """

        print("Initiating book loading...")
        # Retrieving book data from OpenLibrary API
        url = "https://openlibrary.org/search.json?q=language:por&sort=random.hourly&limit=1000"
        headers = {"Accept-Encoding": "gzip"}

        print("Loading data from API")
        response = requests.get(url, headers=headers)
        data = response.json()
        docs = data["docs"]

        df = pd.DataFrame(docs)
        print("Data loaded into DataFrame successfully!")

        selected_df = df[
            [
                "key",
                "title",
                "edition_count",
                "edition_key",
                "first_publish_year",
                "number_of_pages_median",
                "author_key",
                "isbn",
                "subject_key",
            ]
        ]

        renamed_df = selected_df.rename(
            columns={
                "key": "work_key",
                "isbn": "isbn_id",
                "author_key": "author_keys",
                "subject_key": "genre",
                "edition_key": "edition_keys",
            }
        )

        reordered_df = renamed_df[
            [
                "work_key",
                "isbn_id",
                "title",
                "author_keys",
                "genre",
                "first_publish_year",
                "number_of_pages_median",
                "edition_count",
                "edition_keys",
            ]
        ]

        casted_df = (
            reordered_df.map(lambda x: ", ".join(x) if isinstance(x, list) else x)
            .fillna(0)
            .astype({"first_publish_year": "int64", "number_of_pages_median": "int64"})
        )

        # Truncate list columns by char size
        casted_df[["isbn_id", "author_keys", "genre", "edition_keys"]] = casted_df[
            ["isbn_id", "author_keys", "genre", "edition_keys"]
        ].map(lambda x: x[:1000] if isinstance(x, str) else x)

        print(f"Shape df: {casted_df.shape}")
        print(casted_df.dtypes)
        print(casted_df.head())

        # Extract data from MySQL database to delete existing books before insert new data
        db_df = pd.read_sql_query("SELECT work_key FROM book", self.conn)

        merged_df = casted_df.merge(db_df, how="outer", on="work_key", indicator=True)

        new_books_df = merged_df[merged_df["_merge"] == "left_only"].drop(columns=["_merge"])

        print(f"Final df shape: {new_books_df.shape}\n")

        try:
            print("\nStarting writing new data into MySQL table.")
            new_books_df.to_sql(
                "book",
                con=self.conn,
                if_exists="append",
                index=False,
                method=self.insert_on_duplicate,
            )
            print("Writing done successfully.")
        except Exception as e:
            RuntimeError(f"\nError while writing data into MySQL table: \n{e}")

    def get_authors(self):
        """
        Load books from API into 'Books' table.
        """

        print("Initiating looking for new books without authors in DB...\n")
        # Extract data from MySQL database to get authors keys
        book_df = pd.read_sql_query("SELECT author_keys FROM book", self.conn)

        # Split author keys by comma, to be exploded in different rows then
        book_df["author_keys"] = book_df["author_keys"].str.split(", ")

        exploded_df = book_df.explode(["author_keys"])

        # Remove duplicated keys and keep only the first occurrence
        deduplicated_df = exploded_df.drop_duplicates(subset=["author_keys"], keep="first")

        # Get existing ids from MySQL
        author_df = pd.read_sql_query("SELECT author_key AS author_keys FROM author", self.conn)

        # Drop ids already on author table (they will not be added again)
        merged_df = deduplicated_df.merge(author_df, "left", "author_keys", indicator=True)
        missing_authors = merged_df[merged_df["_merge"] == "left_only"].drop(columns=["_merge"])

        # Reset index to iterate from 0 to n
        missing_authors.reset_index(drop=True, inplace=True)

        print(f"Number of rows to search for data: {missing_authors.shape[0]}")

        batch = 100
        n_rows = missing_authors.shape[0]
        num_iterations = (n_rows // batch) + 1 if n_rows % batch != 0 else n_rows // batch

        print(f"""
            Total number of rows: {n_rows}
            Rows per iteration:   {batch}
            Number of iterations: {num_iterations}
        """)

        # Set uri to get author's information from Open Library API
        uri = "https://openlibrary.org/authors"

        # Time control
        tt_begin = time.time()

        if num_iterations > 0:
            for i in range(num_iterations):
                # Creating empty auxiliary df to be populated
                aux_df = pd.DataFrame(columns=["author_keys", "response_api"])

                # Define start and end index for this specific iteration
                i_start = i * 100
                i_end = i * 100 + 99 if i < num_iterations - 1 else n_rows
                print(
                    f"Processing iteration {i}. \nRange of this iteration: {i_start} - {i_end}.\n\n"
                )

                try:
                    # Time control per iteration
                    t_begin = time.time()

                    # Fetch authors' info using requests library
                    aux_df = pd.concat(
                        [
                            aux_df,
                            missing_authors.loc[i_start:i_end].assign(
                                response_api=missing_authors["author_keys"]
                                .loc[i_start:i_end]
                                .apply(lambda x: requests.request("get", f"{uri}/{x}.json").json())
                            ),
                        ]
                    )

                    t_end = time.time()
                    print(
                        f"This batch spent: {round((t_end - t_begin).__trunc__()/60, 2)} minutes running.\n\n"
                    )
                except Exception as e:
                    print(f"An error occurred during the request: {e}")
                    continue
                else:
                    # Split dict into new columns on the final DataFrame
                    splitted_df = (
                        aux_df.assign(
                            name=aux_df["response_api"].apply(lambda x: x.get("name")),
                            full_name=aux_df["response_api"].apply(lambda x: x.get("fuller_name")),
                            alternate_names=aux_df["response_api"].apply(
                                lambda x: x.get("alternate_names")
                            ),
                            birth_date=aux_df["response_api"].apply(lambda x: x.get("birth_date")),
                        )
                        .drop(columns=["response_api"])
                        .rename(columns={"author_keys": "author_key"})
                    )

                    new_authors_df = (
                        splitted_df.map(
                            lambda x: ", ".join(x) if isinstance(x, list) else x
                        ).infer_objects(copy=False)  # .fillna(0)
                        # .astype({"first_publish_year": "int64", "number_of_pages_median": "int64"})
                    )

                    # Truncate list columns by char size
                    new_authors_df[["alternate_names"]] = new_authors_df[["alternate_names"]].map(
                        lambda x: x[:255] if isinstance(x, str) else x
                    )

                    print(f"Final df shape: {new_authors_df.shape}")

                    # Write new data into MySQL table
                    try:
                        print("\nStarting writing new data into MySQL table.")
                        new_authors_df.to_sql(
                            "author",
                            con=self.conn,
                            if_exists="append",
                            index=False,
                            method=self.insert_on_duplicate,
                        )
                        print("Writing done successfully.")
                    except Exception as e:
                        RuntimeError(f"\nError while writing data into MySQL table: {e}\n\n")
                finally:
                    print(f"------------   Iteration {i} finished   ------------\n\n")

        else:
            print("No needs to fetch data from API.")
            sys.exit()

        tt_end = time.time()
        print(
            f"Total iteration spent: {round((tt_end - tt_begin).__trunc__() / 60, 2)} minutes running.\n\n"
        )

    def get_customers(self):
        """
        Generate customer data from Faker library and save it to a MySQL table.
        """

        print("Initiating generating customers' data...")
        # Iniate fake data generator using location
        fk = Faker(locale="pt_BR")

        # Define how many new customers to add
        n_cust = fk.random.choice(range(1, 1000))
        print(f"There are {n_cust} new customers.")

        # Create a list of dictionaries with customer information
        customer_list = []
        for _ in range(n_cust):
            male = 1 if fk.random.choice(range(1, 3)) == 1 else 0
            birth_dt = fk.date_of_birth()
            customer_list.append(
                {
                    "full_name": fk.name_male() if male == 1 else fk.name_female(),
                    "birth_date": birth_dt,
                    "genre": "male" if male == 1 else "female",
                    "document": fk.unique.cpf(),
                    "cellphone_number": fk.unique.cellphone_number(),
                    "birth_state": fk.estado_nome(),
                    "birth_city": fk.city(),
                    "current_postal_code": fk.postcode(),
                    "registration_date": fk.date_between_dates(
                        date_start=birth_dt, date_end=date.today()
                    ),
                }
            )

        # Convert the list of dicts into a DataFrame
        df = pd.DataFrame(customer_list)

        # Adjust some columns data types
        new_customers_df = df.map(lambda x: ", ".join(x) if isinstance(x, list) else x).fillna(0)

        # Write final df to the MySQL table
        try:
            print("\nStarting writing new data into MySQL table.")
            new_customers_df.to_sql("customer", self.conn, if_exists="append", index=False)
            print("Writing done successfully.")
        except Exception as e:
            RuntimeError(f"\nError while writing data into MySQL table: \n{e}")

    def get_rents(self):
        """
        Generate rental records based on existing customers and books.
        """
        # Iniate fake data generator using location
        fk = Faker(locale="pt_BR")

        # Retrieve existing books, customers and rent data
        books_df = pd.read_sql_query("SELECT id_book FROM book", self.conn)
        customers_df = pd.read_sql_query(
            "SELECT id_customer, registration_date FROM customer", self.conn
        )
        rent_df = pd.read_sql_query("SELECT * FROM rent", self.conn)

        print(f"Shape books_df: {books_df.shape}")

        # Get how many customers there are in MySQL
        n_rows_db = customers_df.shape[0]

        # Define how many new rentings to add
        random_number = fk.random.choice(range(100, 2000))
        n_rent = min(random_number, n_rows_db)
        print(f"There are {n_rent} new rentings.")

        # Filter random customers from 1 to n_rent or n_rows_db, depending  on which is smaller,
        # so we don't have more customers than available in MySQL table #min(n_rent, n_rows_db))
        selected_cust_df = customers_df.sample(n=fk.random.randint(1, n_rent)).reset_index(
            drop=True
        )

        # Group all books already rented by id_book and event_type, bringing last data of each group
        rent_grouped = (
            rent_df[["id_book", "event_type", "event_date"]]
            .groupby(["id_book", "event_type"])
            .last()
        )

        # Bring most  recent information about each book (Rent or Return, and date)
        recent_data = (
            rent_grouped.reset_index()
            .sort_values(by=["id_book", "event_date"], ascending=False, na_position="last")
            .drop_duplicates(subset=["id_book"], keep="first")
        )

        # Keep only the books that were returned
        returned_books = recent_data[recent_data["event_type"] == "Return"]

        # Keep only the books that are still rented
        unavailable_books = recent_data[recent_data["event_type"] == "Rent"]

        # Unify returned books with entire collection of books, excluding books that are currently rented
        available_books = pd.concat(
            [returned_books, books_df[~books_df["id_book"].isin(unavailable_books["id_book"])]]
        ).drop(columns=["event_type", "event_date"])

        # Choose randomly if any book will be returned or not, and, if yes, how many
        return_happens = fk.random.choice([True, False])
        n_unavailable = unavailable_books.shape[0]
        n_returns = fk.random.randrange(1, n_unavailable if n_unavailable > 0 else None, 1)

        # Create a list of dictionaries with return information
        return_list = []
        if return_happens:
            # Bring all data about renting to be returned
            returning_df = unavailable_books.merge(
                rent_df[rent_df["event_type"] == "Rent"], on=["id_book", "event_date", "event_type"]
            )

            for r in returning_df.sample(n=n_returns).itertuples():
                # Return date will be in a range between rent day and 100 days after
                return_date = r.event_date + relativedelta(days=100)
                # Penalty value will be 2% of rental value * days late
                days_to_return = relativedelta(r.expected_return_date, r.event_date)
                return_row = {
                    "id_book": r.id_book,
                    "id_customer": r.id_customer,
                    "event_date": fk.date_between_dates(r.event_date, return_date),
                    "expected_return_date": r.expected_return_date,
                    "event_type": "Return",
                    "rental_value": r.rental_value,
                    "penalty_value": 0
                    if days_to_return.days < 1
                    else days_to_return.days * r.rental_value * 0.02,
                }
                return_list.append(return_row)

        # Create a list of dictionaries with rent information
        rent_list = []
        for row in selected_cust_df.itertuples():
            # Define how many books will be rented by this customer
            # Limit's set on 5 because it's unlikely someone would rent more than 5 books at once
            selected_books_df = available_books.sample(n=fk.random.randint(1, 5)).reset_index(
                drop=True
            )

            event_date = fk.date_between_dates(row.registration_date)
            expected_return = fk.date_between_dates(event_date, event_date + relativedelta(days=35))

            for r in selected_books_df.itertuples():
                rent_row = {
                    "id_book": r.id_book,
                    "id_customer": row.id_customer,
                    "event_date": event_date,
                    "expected_return_date": expected_return,
                    "event_type": "Rent",
                    "rental_value": fk.pyfloat(
                        left_digits=2, right_digits=2, positive=True, min_value=6
                    ),
                    "penalty_value": 0.0,
                }
                rent_list.append(rent_row)

                available_books = available_books.loc[
                    ~available_books["id_book"].isin(selected_books_df["id_book"])
                ]

        print(f"Shape available_books após exclusão locados: {available_books.shape}")

        rented_df = pd.DataFrame(rent_list)
        returns_df = pd.DataFrame(return_list)

        print(f"Shape dos livros que serão locados: {rented_df.shape}")
        print(f"Shape dos livros que serão devolvidos: {returns_df.shape}")

        rented_df.to_sql(
            "rent", self.conn, if_exists="append", index=False, method=self.insert_on_duplicate
        )
        returns_df.to_sql(
            "rent", self.conn, if_exists="append", index=False, method=self.insert_on_duplicate
        )


def main():
    print("Executing script from main")


if __name__ == "__main__":
    main()
