from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List
from uuid import UUID

from account.account import Account


class ObjectNotFound(ValueError):
    ...


@dataclass
class AccountDatabase(ABC):  # <---- INTERFACE
    def save(self, data) -> None:
        print("I am going to save this:", data)
        return self._save(data)

    @abstractmethod
    def _save(self, data) -> None:
        ...

    @abstractmethod
    def clear_all(self) -> None:
        ...

    @abstractmethod
    def get_objects(self) -> List[Account]:
        ...

    @abstractmethod
    def get_object(self, id_: UUID) -> Account:
        ...

    def delete(self, data) -> None:
        print("I am going to delete this:", data)
        return self._delete(data.id_)

    @abstractmethod
    def _delete(self, id_: UUID) -> None:
        ...