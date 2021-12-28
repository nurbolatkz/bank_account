from typing import Type
from uuid import uuid4, UUID

import pytest

from account.account import Account
from database.implementations.pandas_db import AccountDatabasePandas
from database.implementations.postgres_db import AccountDatabasePostgres
from database.implementations.ram import AccountDatabaseRAM
from database.database import ObjectNotFound, AccountDatabase
from database.implementations.transaction_db import TransactionDatabasePostgres
from transaction.transaction import Transaction, NotEnoughAmount


class TestAllDatabases:
    def test_all_dbs(self, database_connected: AccountDatabase) -> None:
        # database_connected.clear_all()
        account = Account.random()
        account2 = Account.random()
        database_connected.save(account)
        database_connected.save(account2)
        got_account = database_connected.get_object(account.id_)
        assert account == got_account

        account_id = account.id_
        database_connected.delete(account)
        with pytest.raises(ObjectNotFound):
            database_connected.get_object(account_id)

        database_connected.save(account)
        all_objects = database_connected.get_objects()
        # assert len(all_objects) == 2
        for acc in all_objects:
            assert isinstance(acc, Account)

        account.currency = "USD"
        database_connected.save(account)
        got_account = database_connected.get_object(account.id_)
        assert account == got_account

    def test_connection(self, connection_string: str) -> None:
        database = AccountDatabasePostgres(connection=connection_string)
        database.save(Account.random())
        all_accounts = database.get_objects()
        print(all_accounts)
        database.close_connection()

    def test_transaction(self, connection_string: str) -> None:
        database = AccountDatabasePostgres(connection=connection_string)
        sender = Account.random()
        sender.balance = 500
        receiver = Account.random()
        receiver.balance = 300

        database.save(sender)
        database.save(receiver)

        transaction_id = uuid4()
        transaction = Transaction(transaction_id)

        assert isinstance(transaction, Transaction)
        transaction.transfer(sender.id_, receiver.id_, 200)

        sender = database.get_object(sender.id_)
        receiver = database.get_object(receiver.id_)

        assert sender.balance == 300
        assert receiver.balance == 500

        sender.currency = "USD"
        transaction.transfer(sender.id_, receiver.id_, 200)

        sender = database.get_object(sender.id_)
        receiver = database.get_object(receiver.id_)

        assert sender.balance == 100
        assert receiver.balance == 700
        assert transaction.status == 'SUCCESS'

        with pytest.raises(NotEnoughAmount):
            transaction.transfer(sender.id_, receiver.id_, 1000)
        all_accounts = database.get_objects()
        print(len(all_accounts))
        database.close_connection()

    def test_transaction_database(self, connection_string: str) -> None:
        database_con = TransactionDatabasePostgres(connection=connection_string)
        sender = Account(
            id_=UUID("efb345d0-ea1f-4d02-a0b8-34540bb0c5ae"),
            balance=300,
            currency='KZT'
        )

        receiver = Account(
            id_=UUID('d43d3f0e-1ea2-46a4-b0e1-c40b18b47018'),
            balance=500,
            currency='KZT'
        )

        transaction_id = uuid4()
        transaction = Transaction(transaction_id)

        transaction.transfer(sender.id_, receiver.id_, 200)
        database_con.save(transaction)

        got_transaction = database_con.get_object(transaction_id)
        assert transaction == got_transaction

