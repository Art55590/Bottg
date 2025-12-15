import sqlite3
from datetime import datetime, timedelta

DB_NAME = "bot.db"


def _get_conn():
    return sqlite3.connect(DB_NAME)


def _ensure_column(cur, table: str, column_def: str):
    """
    Простейший helper: пытаемся добавить колонку, если уже есть — просто игнорим ошибку.
    column_def: строка вида 'phone TEXT' или 'banned INTEGER DEFAULT 0'
    """
    try:
        cur.execute(f"ALTER TABLE {table} ADD COLUMN {column_def}")
    except Exception:
        # колонка уже есть или другая ошибка — молча пропускаем
        pass


def init_db():
    """Создание базы и таблиц + добавление недостающих колонок."""
    conn = _get_conn()
    cur = conn.cursor()

    # Таблица пользователей
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tg_id INTEGER UNIQUE,
            balance REAL DEFAULT 0,
            referrer_id INTEGER,
            activated INTEGER DEFAULT 0,
            phone TEXT,
            created_at TEXT,
            last_bonus_at TEXT,
            banned INTEGER DEFAULT 0
        )
        """
    )

    # На всякий случай добавляем недостающие колонки (если база старая)
    _ensure_column(cur, "users", "referrer_id INTEGER")
    _ensure_column(cur, "users", "activated INTEGER DEFAULT 0")
    _ensure_column(cur, "users", "phone TEXT")
    _ensure_column(cur, "users", "created_at TEXT")
    _ensure_column(cur, "users", "last_bonus_at TEXT")
    _ensure_column(cur, "users", "banned INTEGER DEFAULT 0")
    _ensure_column(cur, "users", "balance REAL DEFAULT 0")

    # Таблица выводов
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS withdrawals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tg_id INTEGER,
            method TEXT,
            details TEXT,
            amount REAL,
            status TEXT,
            created_at TEXT
        )
        """
    )

    # Таблица заявок по заданиям
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS task_submissions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tg_id INTEGER,
            task_id TEXT,
            status TEXT,
            proof_file_id TEXT,
            proof_caption TEXT,
            created_at TEXT
        )
        """
    )

    conn.commit()
    conn.close()


# ---------- USERS ----------

def create_user(tg_id, referrer_id=None):
    """
    Создаёт пользователя, если его ещё нет.
    Возвращает created_at при создании, иначе None.
    """
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute("SELECT id FROM users WHERE tg_id=?", (tg_id,))
    row = cur.fetchone()
    if row:
        conn.close()
        return None

    created_at = datetime.utcnow().isoformat()
    cur.execute(
        """
        INSERT INTO users (tg_id, balance, referrer_id,
                           activated, phone, created_at, last_bonus_at, banned)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (tg_id, 0.0, referrer_id, 0, None, created_at, None, 0),
    )
    conn.commit()
    conn.close()
    return created_at


def get_user(tg_id):
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT tg_id, balance, referrer_id, activated, phone, created_at, last_bonus_at, banned
        FROM users WHERE tg_id=?
        """,
        (tg_id,),
    )
    row = cur.fetchone()
    conn.close()
    return row


def activate_user(tg_id):
    """
    Активировать пользователя (подписка + телефон)
    Возвращает referrer_id, если нужно начислить бонус. Иначе None.
    """
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute("SELECT activated, referrer_id FROM users WHERE tg_id=?", (tg_id,))
    row = cur.fetchone()
    if not row:
        conn.close()
        return None

    activated, referrer_id = row
    if activated:
        conn.close()
        return None

    cur.execute("UPDATE users SET activated=1 WHERE tg_id=?", (tg_id,))
    conn.commit()
    conn.close()
    return referrer_id


def add_balance(tg_id, amount):
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute("UPDATE users SET balance = balance + ? WHERE tg_id=?", (amount, tg_id))
    conn.commit()
    conn.close()


def get_balance(tg_id):
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute("SELECT balance FROM users WHERE tg_id=?", (tg_id,))
    row = cur.fetchone()
    conn.close()
    return row[0] if row else 0.0


# ---------- PHONE & BONUS ----------

def set_phone(tg_id, phone):
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute("UPDATE users SET phone=? WHERE tg_id=?", (phone, tg_id))
    conn.commit()
    conn.close()


def get_phone(tg_id):
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute("SELECT phone FROM users WHERE tg_id=?", (tg_id,))
    row = cur.fetchone()
    conn.close()
    return row[0] if row else None


def is_phone_used(phone: str, except_id: int | None = None) -> bool:
    """
    Проверяем, используется ли номер телефона другим пользователем.
    except_id — tg_id пользователя, которого не учитываем (чтоб не ругаться на свой же номер).
    """
    conn = _get_conn()
    cur = conn.cursor()
    if except_id is None:
        cur.execute("SELECT id FROM users WHERE phone=?", (phone,))
    else:
        cur.execute(
            "SELECT id FROM users WHERE phone=? AND tg_id!=?",
            (phone, except_id),
        )
    row = cur.fetchone()
    conn.close()
    return row is not None


def get_last_bonus_at(tg_id):
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute("SELECT last_bonus_at FROM users WHERE tg_id=?", (tg_id,))
    row = cur.fetchone()
    conn.close()
    return row[0] if row else None


def set_last_bonus_at(tg_id, value: str):
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute("UPDATE users SET last_bonus_at=? WHERE tg_id=?", (value, tg_id))
    conn.commit()
    conn.close()


# ---------- BAN ----------

def is_banned(tg_id):
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute("SELECT banned FROM users WHERE tg_id=?", (tg_id,))
    row = cur.fetchone()
    conn.close()
    return bool(row[0]) if row else False


def ban_user(tg_id):
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute("UPDATE users SET banned=1 WHERE tg_id=?", (tg_id,))
    conn.commit()
    conn.close()


def unban_user(tg_id):
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute("UPDATE users SET banned=0 WHERE tg_id=?", (tg_id,))
    conn.commit()
    conn.close()


# ---------- WITHDRAWALS ----------

def create_withdrawal(tg_id, method, details, amount):
    conn = _get_conn()
    cur = conn.cursor()
    created_at = datetime.utcnow().isoformat()
    cur.execute(
        """
        INSERT INTO withdrawals (tg_id, method, details, amount, status, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (tg_id, method, details, amount, "new", created_at),
    )
    conn.commit()
    wid = cur.lastrowid
    conn.close()
    return wid


def get_withdraw(wd_id):
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, tg_id, method, details, amount, status, created_at
        FROM withdrawals
        WHERE id=?
        """,
        (wd_id,),
    )
    row = cur.fetchone()
    conn.close()
    return row


def set_withdraw_status(wd_id, status):
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute("UPDATE withdrawals SET status=? WHERE id=?", (status, wd_id))
    conn.commit()
    conn.close()


def list_new_withdrawals(limit: int = 30):
    """
    Список новых (status='new') заявок на вывод для /pending.
    """
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, tg_id, method, details, amount, status, created_at
        FROM withdrawals
        WHERE status='new'
        ORDER BY id ASC
        LIMIT ?
        """,
        (limit,),
    )
    rows = cur.fetchall()
    conn.close()
    return rows


# ---------- TASK SUBMISSIONS ----------

def create_task_submission(tg_id, task_id, proof_file_id, proof_caption):
    conn = _get_conn()
    cur = conn.cursor()
    created_at = datetime.utcnow().isoformat()
    cur.execute(
        """
        INSERT INTO task_submissions (tg_id, task_id, status, proof_file_id, proof_caption, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (tg_id, task_id, "pending", proof_file_id, proof_caption, created_at),
    )
    conn.commit()
    sid = cur.lastrowid
    conn.close()
    return sid


def get_task_submission(sub_id):
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, tg_id, task_id, status, proof_file_id, proof_caption, created_at
        FROM task_submissions
        WHERE id=?
        """,
        (sub_id,),
    )
    row = cur.fetchone()
    conn.close()
    return row


def set_task_status(sub_id, status):
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute("UPDATE task_submissions SET status=? WHERE id=?", (status, sub_id))
    conn.commit()
    conn.close()


def get_last_task_submission(tg_id, task_id):
    """
    Возвращает (id, status) последней заявки по заданию у юзера, либо None.
    Используется в main.py: last[1] — статус ('pending' / 'approved' / 'rejected').
    """
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, status
        FROM task_submissions
        WHERE tg_id=? AND task_id=?
        ORDER BY id DESC
        LIMIT 1
        """,
        (tg_id, task_id),
    )
    row = cur.fetchone()
    conn.close()
    return row


# ---------- STATS / TOP / USERS ----------

def get_stats():
    """
    Возвращает словарь:
    {
        "total_users": int,
        "activated_users": int,
        "with_phone": int,
        "banned_users": int,
        "new_24h": int,
    }
    """
    conn = _get_conn()
    cur = conn.cursor()

    # всего пользователей
    cur.execute("SELECT COUNT(*) FROM users")
    total_users = cur.fetchone()[0]

    # активированные
    cur.execute("SELECT COUNT(*) FROM users WHERE activated=1")
    activated_users = cur.fetchone()[0]

    # с телефоном
    cur.execute(
        "SELECT COUNT(*) FROM users WHERE phone IS NOT NULL AND phone != ''"
    )
    with_phone = cur.fetchone()[0]

    # забаненные
    cur.execute("SELECT COUNT(*) FROM users WHERE banned=1")
    banned_users = cur.fetchone()[0]

    # новые за 24 часа
    point = (datetime.utcnow() - timedelta(hours=24)).isoformat()
    cur.execute("SELECT COUNT(*) FROM users WHERE created_at > ?", (point,))
    new_24h = cur.fetchone()[0]

    conn.close()
    return {
        "total_users": total_users,
        "activated_users": activated_users,
        "with_phone": with_phone,
        "banned_users": banned_users,
        "new_24h": new_24h,
    }


def get_top_referrers(limit: int = 10):
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT referrer_id, COUNT(*) as cnt
        FROM users
        WHERE activated=1 AND referrer_id IS NOT NULL
        GROUP BY referrer_id
        ORDER BY cnt DESC
        LIMIT ?
        """,
        (limit,),
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def list_users(limit: int = 200):
    """
    Для /users и рассылки /all:
    возвращает строки вида:
    (tg_id, balance, referrer_id, activated, phone, created_at, last_bonus_at, banned)
    """
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT tg_id, balance, referrer_id, activated, phone, created_at, last_bonus_at, banned
        FROM users
        ORDER BY created_at ASC
        LIMIT ?
        """,
        (limit,),
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def list_all_users(limit: int = 200):
    """
    Запасная функция: (tg_id, balance, phone, activated, created_at, banned)
    """
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT tg_id, balance, phone, activated, created_at, banned
        FROM users
        ORDER BY id ASC
        LIMIT ?
        """,
        (limit,),
    )
    rows = cur.fetchall()
    conn.close()
    return rows
