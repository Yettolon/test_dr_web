from abc import ABC, abstractmethod
from collections import defaultdict
from copy import deepcopy
from typing import Dict, List, Tuple, Optional, Callable


class InMemoryDB:
    """Реализация базы данных в памяти с транзакциями"""

    def __init__(self) -> None:
        self._db: Dict[str, str] = {}
        self._counts: Dict[str, int] = defaultdict(int)
        self._history: List[Tuple[Dict[str, str], Dict[str, int]]] = []

    def _save_state(self) -> None:
        self._history.append((deepcopy(self._db), deepcopy(self._counts)))

    def set(self, key: str, value: str) -> None:
        self._save_state()
        old_value = self._db.get(key)
        if old_value:
            self._counts[old_value] -= 1
        self._db[key] = value
        self._counts[value] += 1

    def get(self, key: str) -> Optional[str]:
        return self._db.get(key)

    def unset(self, key: str) -> None:
        if key in self._db:
            self._save_state()
            old_value = self._db.pop(key)
            self._counts[old_value] -= 1

    def counts(self, value: str) -> None:
        print(self._counts.get(value, 0))

    def find(self, value: str) -> None:
        keys = [k for k, v in self._db.items() if v == value]
        print(" ".join(sorted(keys)) if keys else "NULL")

    def begin(self) -> None:
        self._history.append((deepcopy(self._db), deepcopy(self._counts)))

    def rollback(self) -> None:
        if not self._history:
            print("NO TRANSACTION")
        else:
            self._db, self._counts = self._history.pop()

    def commit(self) -> None:
        if not self._history:
            print("NO TRANSACTION")
        else:
            self._history.clear()


class Command(ABC):
    """Абстрактный базовый класс для команд"""

    @abstractmethod
    def execute(self) -> None:
        pass


class SetCommand(Command):
    """Команда для установки значения в базе данных"""

    def __init__(self, db: InMemoryDB, key: str, value: str) -> None:
        self.db = db
        self.key = key
        self.value = value

    def execute(self) -> None:
        self.db.set(self.key, self.value)


class GetCommand(Command):
    """Команда для получения значения из базы данных"""

    def __init__(self, db: InMemoryDB, key: str) -> None:
        self.db = db
        self.key = key

    def execute(self) -> None:
        value = self.db.get(self.key)
        print(value if value is not None else "NULL")


class UnsetCommand(Command):
    """Команда для удаления значения из базы данных"""

    def __init__(self, db: InMemoryDB, key: str) -> None:
        self.db = db
        self.key = key

    def execute(self) -> None:
        self.db.unset(self.key)


class CountsCommand(Command):
    """Команда для получения количества значения в базе данных"""

    def __init__(self, db: InMemoryDB, value: str) -> None:
        self.db = db
        self.value = value

    def execute(self) -> None:
        self.db.counts(self.value)


class FindCommand(Command):
    """Команда для вывода найденных установленных переменных для данного значения"""

    def __init__(self, db: InMemoryDB, value: str) -> None:
        self.db = db
        self.value = value

    def execute(self) -> None:
        self.db.find(self.value)


class BeginCommand(Command):
    """Команда для начала транзакции"""

    def __init__(self, db: InMemoryDB) -> None:
        self.db = db

    def execute(self) -> None:
        self.db.begin()


class RollbackCommand(Command):
    """Команда для отката последней транзакции"""

    def __init__(self, db: InMemoryDB) -> None:
        self.db = db

    def execute(self) -> None:
        self.db.rollback()


class CommitCommand(Command):
    """Команда для фиксации всех изменений в текущей транзакции"""

    def __init__(self, db: InMemoryDB) -> None:
        self.db = db

    def execute(self) -> None:
        self.db.commit()


class EndCommand(Command):
    """Команда для завершения работы программы"""

    def execute(self) -> None:
        raise SystemExit()


class UnknownCommand(Command):
    """Команда для обработки неизвестных команд"""

    def execute(self) -> None:
        print("UNKNOWN COMMAND")


class CommandParser:
    """Парсер команд"""

    def __init__(self, db: InMemoryDB) -> None:
        self.db = db
        self.command_map: Dict[str, Callable[[List[str]], Command]] = {
            "SET": lambda parts: (
                SetCommand(self.db, parts[1], parts[2])
                if len(parts) == 3
                else UnknownCommand()
            ),
            "GET": lambda parts: (
                GetCommand(self.db, parts[1]) if len(parts) == 2 else UnknownCommand()
            ),
            "UNSET": lambda parts: (
                UnsetCommand(self.db, parts[1]) if len(parts) == 2 else UnknownCommand()
            ),
            "COUNTS": lambda parts: (
                CountsCommand(self.db, parts[1])
                if len(parts) == 2
                else UnknownCommand()
            ),
            "FIND": lambda parts: (
                FindCommand(self.db, parts[1]) if len(parts) == 2 else UnknownCommand()
            ),
            "BEGIN": lambda parts: BeginCommand(self.db),
            "ROLLBACK": lambda parts: RollbackCommand(self.db),
            "COMMIT": lambda parts: CommitCommand(self.db),
            "END": lambda parts: EndCommand(),
        }

    def parse(self, line: str) -> Optional[Command]:
        parts = line.strip().split()
        if not parts:
            return None
        cmd = parts[0].upper()
        return self.command_map.get(cmd, lambda parts: UnknownCommand())(parts)


def main() -> None:
    db = InMemoryDB()
    parser = CommandParser(db)

    try:
        while True:
            line = input("> ")
            command = parser.parse(line)
            if command:
                try:
                    command.execute()
                except SystemExit:
                    break
    except EOFError:
        print("\nЗавершено по EOF.")
    except KeyboardInterrupt:
        print("\nВыход с помощью клавиатуры.")


if __name__ == "__main__":
    main()
