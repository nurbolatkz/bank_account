import datetime
import json
from dataclasses import dataclass
from decimal import Decimal
from uuid import UUID
from dateutil.parser import parse

from account.account import Account
from database.implementations.postgres_db import AccountDatabasePostgres


class NotEnoughAmount(ValueError):
    ...


class NotFoundTransactions(ValueError):
    ...


convert_commission = {
    "KZT-USD": 200,
    "USD-KZT": 212,
    "EUR-KZT": 215,
    "KZT-EUR": 217,
    "RUB-KZT": 158,
    "KZT-RUB": 162,
    "EUR-RUB": 234,
    "RUB-EUR": 241

}

conn_str = """ 
            dbname=account_con 
            port=5432 
            user=postgres 
            password=nureke
            host=localhost  """


@dataclass
class Transaction:
    id_: UUID
    sender: UUID = None
    receiver: UUID = None
    balance_brutto: Decimal = None
    balance_netto: Decimal = None
    currency: str = None
    status: str = None
    date: str = str(datetime.datetime.now())
    amount: Decimal = 0.0
    commission: Decimal = 0.0

    @classmethod
    def database_connect(cls):
        try:
            database_con = AccountDatabasePostgres(conn_str)
            return database_con
        except Exception as e:
            print(e)

    def withdraw(self, sender: Account, amount) -> None:
        if amount > sender.balance:
            raise NotEnoughAmount("Not enough money in your balance")
        sender.balance = sender.balance - (Decimal(amount) - Decimal(self.commission))

    def deposit(self, receiver: Account, amount, type_transaction: 0) -> None:

        if type_transaction == 0:
            self.sender = receiver
            self.receiver = receiver
            self.balance_brutto = receiver.balance

            receiver.balance = receiver.balance + Decimal(amount)
            self.save_params(sender=self.receiver, receive=self.receiver, amount=Decimal(amount))

        receiver.balance = receiver.balance + Decimal(amount)

    def transfer(self, sender_id: UUID, receiver_id:  UUID, amount) -> None:
        database = self.database_connect()
        sender = database.get_object(sender_id)
        receiver = database.get_object(receiver_id)

        self.sender = sender.id_
        self.receiver = receiver.id_

        convert_str = f"{sender.currency}-{receiver.currency}"
        self.commission = 0 if sender.currency == receiver.currency else convert_commission[convert_str]
        self.balance_brutto = sender.balance

        self.withdraw(sender, amount)
        self.deposit(receiver, amount, 1)

        database.close_connection()

        self.save_params(sender=sender, receiver=receiver, amount=Decimal(amount))

    def to_json(self) -> dict:
        dt = parse(self.date)
        date_str = f"{dt.day}.{dt.month}.{dt.year} {dt.hour}:{dt.minute}:{dt.second}"
        return {
            "id": str(self.id_),
            "sender": str(self.sender),
            "receiver": str(self.receiver),
            "balance_brutto": float(self.balance_brutto),
            "balance_netto": float(self.balance_netto),
            "currency": self.currency,
            "status": self.status,
            "date": date_str,
            "amount": float(self.amount),
            "commission": float(self.commission),
        }

    def to_json_str(self) -> str:
        return json.dumps(self.to_json())

    def save_params(self, sender: Account, receiver: Account, amount: Decimal) -> None:
        database = self.database_connect()

        self.currency = sender.currency
        self.balance_netto = sender.balance
        self.amount = amount

        self.status = "SUCCESS" if (self.balance_brutto - amount == self.balance_netto) else "FAILED"
        self.date = str(datetime.datetime.now())

        database.save(sender)
        database.save(receiver)

        database.close_connection()




