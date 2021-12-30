import json
from decimal import Decimal
from uuid import uuid4, UUID


from django.urls import path

from django.http import HttpResponse, HttpRequest
from django.shortcuts import render, redirect

from account.account import Account
from database.database import ObjectNotFound
from database.implementations.postgres_db import AccountDatabasePostgres
from database.implementations.ram import AccountDatabaseRAM
from database.implementations.transaction_db import TransactionDatabasePostgres
from transaction.transaction import Transaction

port: int = 5432
user: str = "postgres"
password: str = "nureke"
host: str = "localhost"
dbname = "account_con"
connection_str = f"dbname={dbname} port={port} user={user} password={password} host={host}"
try:
    database = AccountDatabasePostgres(connection=connection_str)
except Exception as e:
    print(e)

currency_ls = ['KZT', 'USD', 'RUB', 'EUR', 'GBP']


def accounts_list(request: HttpRequest) -> HttpResponse:
    accounts_lst = database.get_objects()
    return render(request, "index.html", context={"accounts": accounts_lst})


def index(request: HttpRequest) -> HttpResponse:
    return HttpResponse(content="""
    <html>
        <body>
           <h1>Hello, World!</h1> 
           <h3>Try to access <a href="/api/accounts/">/api/accounts/</a></h3>
        </body>
    </html>
    """)


def accounts(request: HttpRequest) -> HttpResponse:
    accounts_lst = database.get_objects()

    if request.method == "GET":
        json_obj = [account.to_json() for account in accounts_lst]
        return HttpResponse(content=json.dumps(json_obj))

    if request.method == "POST":
        try:
            account = Account.from_json_str(request.body.decode("utf8"))
            account.id_ = uuid4()
            try:
                database.get_object(account.id_)
                return HttpResponse(content=f"Error: object already exists, use PUT to update", status=400)
            except ObjectNotFound:
                database.save(account)
                return HttpResponse(content=account.to_json_str(), status=201)
        except Exception as e:
            return HttpResponse(content=f"Error: {e}", status=400)

    if request.method == "PUT":
        try:
            account = Account.from_json_str(request.body.decode("utf8"))
            database.get_object(account.id_)
            database.save(account)
            return HttpResponse(content="OK", status=200)
        except Exception as e:
            return HttpResponse(content=f"Error: {e}", status=400)

# returns json object of transactions


def transactions_lst(request: HttpRequest, sender_id: UUID) -> HttpResponse:
    database_con = TransactionDatabasePostgres(connection_str)
    try:
        account_transactions = database_con.get_transactions(sender_id)
    except Exception as e:
        return HttpResponse(content=f"Error: {e}", status=400)
    json_obj = [transaction.to_json() for transaction in account_transactions]
    database_con.close_connection()
    return HttpResponse(content=json.dumps(json_obj))


def add_account(request: HttpRequest) -> HttpResponse:
    if request.method == 'POST':
        account_id = uuid4()
        currency = request.POST.getlist('currency')[0]
        balance = 0
        database_con = AccountDatabasePostgres(connection_str)

        account = Account(
            id_=account_id,
            currency=currency,
            balance=Decimal(balance),
        )

        database_con.save(account)
        if database_con.get_object(account_id) == account:
            return redirect(index)
        else:
            return HttpResponse(content="Failed to save", status=400)

    return render(request, "add_account.html", context={"currency": currency_ls})


def transactions(request: HttpRequest, sender_id: UUID) -> HttpResponse:

    if request.method == 'post':
        id_ = uuid4()
        transaction = Transaction(id_, sender=sender_id)
        amount = request.POST.get('amount')
        transaction.deposit(sender_id, amount)
        return HttpResponse(content="Successfully added", status=200)
    database_con = TransactionDatabasePostgres(connection_str)
    try:
        account_transactions = database_con.get_transactions(sender_id)
    except Exception as e:
        return HttpResponse(content=f"Error: {e}", status=400)

    return render(request, "transaction_page.html", context={"transactions": account_transactions})







