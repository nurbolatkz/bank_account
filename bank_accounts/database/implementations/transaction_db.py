from typing import List, Optional
from uuid import UUID, uuid4
import psycopg2
import pandas as pd
from pandas import DataFrame, Series
from account.account import Account
from database.database import AccountDatabase
from database.database import ObjectNotFound
from transaction.transaction import Transaction, NotFoundTransactions


class TransactionDatabasePostgres(AccountDatabase):
    def __init__(self, connection: str,  *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.conn = psycopg2.connect(connection)
        cur = self.conn.cursor()
        cur.execute("""
        CREATE TABLE IF NOT EXISTS transactions (
            id varchar primary key ,
            sender varchar REFERENCES accounts (id) ,
            receiver varchar,
            balance_brutto decimal,
            balance_netto decimal,
            currency varchar,
            status varchar,
            date timestamp without time zone,
            amount decimal,
            commission decimal
        );
        """)
        self.conn.commit()

    def close_connection(self):
        self.conn.close()

    def _save(self, transaction: Transaction) -> None:
        cur = self.conn.cursor()
        cur.execute("""
                    INSERT INTO transactions ( id, sender, receiver,
                                               balance_brutto, balance_netto, currency,
                                               status, date, amount, commission) 
                    VALUES (%s, %s, %s,
                           %s, %s, %s,
                           %s, to_timestamp(%s, 'YYYY-MM-DD hh24:mi:ss:us'), %s, %s);
                    """, (str(transaction.id_), str(transaction.sender), str(transaction.receiver),
                          float(transaction.balance_brutto), float(transaction.balance_netto), transaction.currency,
                          transaction.status, transaction.date, transaction.amount, transaction.commission))
        self.conn.commit()

    def clear_all(self) -> None:
        cur = self.conn.cursor()
        cur.execute("DELETE FROM transactions;")
        self.conn.commit()

    def get_objects(self) -> List[Transaction]:
        cur = self.conn.cursor()
        cur.execute("SELECT * FROM accounts;")
        data = cur.fetchall()
        cols = [x[0] for x in cur.description]
        df = pd.DataFrame(data, columns=cols)
        return [self.pandas_row_to_account(row) for index, row in df.iterrows()]

    def pandas_row_to_account(self, row: Series) -> Transaction:
        return Transaction(
            id_=UUID(row["id"]),
            sender=UUID(row["sender"]),
            receiver=UUID(row["receiver"]),
            balance_brutto=row["balance_brutto"],
            balance_netto=row["balance_netto"],
            currency=row["currency"],
            status=row["status"],
            date=str(row["date"]),
            amount=row["amount"],
            commission=row["commission"]

        )

    def get_object(self, id_: UUID) -> Optional[Transaction]:
        cur = self.conn.cursor()
        cur.execute("SELECT * FROM transactions WHERE id = %s;", (str(id_),))
        print("Trying to find", str(id_))
        data = cur.fetchall()
        if len(data) == 0:
            raise ObjectNotFound("Postgres: Object not found")
        cols = [x[0] for x in cur.description]
        df = pd.DataFrame(data, columns=cols)
        return self.pandas_row_to_account(row=df.iloc[0])

    def _delete(self, id_: UUID) -> None:
        cur = self.conn.cursor()
        cur.execute("SELECT * FROM transactions WHERE id = %s;", (str(id_),))
        print("Trying to find", str(id_))
        data = cur.fetchall()
        if len(data) == 0:
            raise ObjectNotFound

        cur.execute("DELETE  FROM transactions WHERE id = %s;", (str(id_),))
        self.conn.commit()

    def get_transactions(self, sender: UUID) -> List[Transaction]:
        cur = self.conn.cursor()
        cur.execute("SELECT * FROM transactions WHERE sender = %s;", (str(sender),))
        data = cur.fetchall()
        if len(data) == 0:
            raise NotFoundTransactions("Not found transactions by this sender :D")
        cols = [x[0] for x in cur.description]
        df = pd.DataFrame(data, columns=cols)
        return [self.pandas_row_to_account(row) for index, row in df.iterrows()]
